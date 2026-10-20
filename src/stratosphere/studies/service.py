"""One reviewed finite experiment, retaining all candidates and preventing holdout optimization."""

import asyncio
import json
from datetime import UTC, datetime

from stratosphere.phase0.contracts import StrategySpec, canonical_bytes, fingerprint
from stratosphere.registry.workspace import WorkspaceDatabase, now
from stratosphere.research.data import require_online
from stratosphere.research.metrics import summarize
from stratosphere.research.service import run_worker
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.studies.contracts import (
    Candidate,
    RegistryStudyAnchor,
    RegistryStudyProtocol,
    StrategyAnchor,
    StudyProtocol,
    study_implementation_digest,
    study_sources,
)
from stratosphere.studies.data import fetch_month, normalize_month, pin_month
from stratosphere.studies.reporting import build_study_report, study_markdown
from stratosphere.studies.store import StudyStore
from stratosphere.studies.worker import CellRequest


class StudyService:
    def __init__(
        self,
        store: StudyStore,
        database: WorkspaceDatabase,
        *,
        pinner=pin_month,
        fetcher=fetch_month,
        runner=run_worker,
        online_check=require_online,
    ):
        self.store, self.database = store, database
        self.pinner, self.fetcher, self.runner, self.online_check = (
            pinner,
            fetcher,
            runner,
            online_check,
        )
        self.active_job = None
        self.preparing = False
        self.stop_state = "canceled"

    def recover(self):
        for job_id in self.store.jobs():
            if self.store.state(job_id) in {"running", "preparing"}:
                try:
                    with self.store.lock(job_id):
                        approved = self.store.exists(job_id, "approval.json")
                        self.store.event(
                            job_id, "paused" if approved else "failed", "Client interrupted"
                        )
                except ResearchError:
                    continue

    def registry_anchor(self, strategy_id, config=None):
        try:
            return RegistryStudyAnchor.from_record(self.database.strategy(strategy_id), config)
        except ValueError:
            raise ResearchError(
                "This registered definition differs from the supported BTCUSDT hourly SMA "
                "implementation; no study was prepared."
            ) from None

    async def prepare(self, parent_id, parent_plan, hypothesis, candidates, month, max_simulations):
        return await self._prepare(
            parent_id,
            StrategyAnchor.from_plan(parent_plan),
            hypothesis,
            candidates,
            month,
            max_simulations,
        )

    async def prepare_registered(
        self, parent_id, anchor, hypothesis, candidates, month, max_simulations
    ):
        anchor = RegistryStudyAnchor.model_validate(
            anchor.model_dump(mode="json") if isinstance(anchor, RegistryStudyAnchor) else anchor
        )
        await self.require_online()
        return await self._prepare(
            parent_id, anchor, hypothesis, candidates, month, max_simulations
        )

    async def _prepare(self, parent_id, anchor, hypothesis, candidates, month, max_simulations):
        if self.active_job or self.preparing:
            raise ResearchError("A study operation is already active")
        registered = isinstance(anchor, RegistryStudyAnchor)
        proposal = {
            "parent_strategy_id": parent_id,
            "hypothesis": hypothesis,
            "candidates": candidates,
            "development_month": month,
            "max_simulations": max_simulations,
        }
        if registered:
            proposal["registry_anchor"] = anchor.model_dump(mode="json")
        job_id = self.store.create(json.dumps(proposal, ensure_ascii=False))
        self.preparing = True
        try:
            with self.store.lock(job_id):
                self._verify_anchor(parent_id, anchor)
                development = await self.pinner(month)
                next_month = datetime.fromtimestamp(
                    (development.first_ns + development.count * 3600 * 10**9) // 10**9, UTC
                ).strftime("%Y-%m")
                holdout = await self.pinner(next_month)
                self._verify_anchor(parent_id, anchor)
                fields = {
                    "parent_strategy_id": parent_id,
                    "hypothesis": hypothesis,
                    "candidates": tuple(Candidate.model_validate(c) for c in candidates),
                    "development": development,
                    "holdout": holdout,
                    "max_simulations": max_simulations,
                    "implementation_sha256": study_implementation_digest(),
                }
                protocol = (
                    RegistryStudyProtocol(
                        schema_version="bounded-study/2", registry_anchor=anchor, **fields
                    )
                    if registered
                    else StudyProtocol(parent_plan=anchor, **fields)
                )
                self.store.write(job_id, "protocol.json", protocol.model_dump(mode="json"))
                self.store.write(
                    job_id,
                    "implementation.json",
                    {"sha256": protocol.implementation_sha256, "modules": study_sources()},
                )
                self.store.event(job_id, "awaiting_approval")
                self.database.attach_job(parent_id, job_id, protocol.digest, "awaiting_approval")
                return job_id, protocol
        except asyncio.CancelledError:
            self.store.event(job_id, "canceled", "Study proposal canceled")
            raise
        except Exception:
            self.store.event(job_id, "failed", "Study proposal failed; input retained")
            raise
        finally:
            self.preparing = False

    def _verify_anchor(self, parent_id, anchor):
        record = self.database.strategy(parent_id)
        actual = StrategySpec.model_validate(record["specification"])
        if record["spec_sha256"] != fingerprint(anchor.specification) or canonical_bytes(
            actual
        ) != canonical_bytes(anchor.specification):
            raise ResearchError("The study anchor belongs to another or changed strategy")

    def approve(self, job_id, digest):
        with self.store.lock(job_id):
            protocol = self.store.protocol(job_id)
            if self.store.state(job_id) != "awaiting_approval" or digest != protocol.digest:
                raise ResearchError("Approve exactly the pending reviewed study protocol")
            self._verify_anchor(protocol.parent_strategy_id, protocol)
            if protocol.implementation_sha256 != study_implementation_digest():
                raise ResearchError("Implementation changed; review a new study")
            self.store.write(job_id, "approval.json", {"protocol_sha256": digest})
            self.store.event(job_id, "paused", "Approved; ready to execute")

    async def require_online(self):
        try:
            await self.online_check()
        except ValueError:
            raise ResearchError(
                "Study operations require online access; retry when connected."
            ) from None

    async def open_report(self, job_id):
        await self.require_online()
        return self.report(job_id)

    def report(self, job_id):
        if self.store.state(job_id) != "completed":
            raise ResearchError("This study has no completed report")
        self.store.verify_seal(job_id)
        return self.store.read(job_id, "report.json")

    async def _snapshot(self, job_id, pin, role):
        self.database.claim_dataset(pin.url, pin.sha256, job_id, role)
        if self.store.exists(job_id, role + ".zip"):
            # Resume remains online; verify the remote pin without fetching holdout prices again.
            if await self.pinner(pin.month) != pin:
                raise ResearchError(
                    "Provider snapshot changed; the retained checkpoint is preserved"
                )
            snapshot = normalize_month(
                (self.store.directory(job_id) / (role + ".zip")).read_bytes(), pin
            )
        else:
            snapshot = await self.fetcher(pin)
            verified = normalize_month(snapshot.archive, pin)
            if (
                snapshot.bars != verified.bars
                or snapshot.normalized_sha256 != verified.normalized_sha256
            ):
                raise ResearchError("Monthly normalized data integrity mismatch")
            self.store.write(job_id, role + ".zip", snapshot.archive)
        metadata = {
            "pin": pin.model_dump(mode="json"),
            "normalized_sha256": snapshot.normalized_sha256,
        }
        if not self.store.exists(job_id, role + ".json"):
            self.store.write(job_id, role + ".json", metadata)
        elif self.store.read(job_id, role + ".json") != metadata:
            raise ResearchError("Monthly checkpoint integrity mismatch")
        return snapshot

    async def _cell(self, job_id, protocol, index, window, multiplier, strategy_id, selected=None):
        key = f"cell_{index}_{window}_{multiplier}"
        if self.store.exists(job_id, key + ".json"):
            outcome = self.store.read(job_id, key + ".json")
            started = self.store.read(job_id, key + "_started.json")
            status = outcome["status"] if outcome["status"] != "interrupted" else "failed"
            self.database.finish_operation(started["operation_id"], status, allow_recovery=True)
            return outcome
        if self.store.exists(job_id, key + "_started.json"):
            # A process died before writing its outcome. Never rerun the consumed slot.
            outcome = {"status": "interrupted", "detail": "Prior process stopped; slot not rerun"}
            self.store.write(job_id, key + ".json", outcome)
            retained = self.store.read(job_id, key + "_started.json")
            self.database.finish_operation(
                retained["operation_id"], "failed", "Process interrupted", allow_recovery=True
            )
            return outcome
        started = [p for p in self.store.directory(job_id).glob("cell_*_started.json")]
        if len(started) >= protocol.max_simulations:
            raise ResearchError("The approved simulation budget is exhausted")
        request = CellRequest(
            protocol=protocol, candidate_index=index, window=window, selected_index=selected
        )
        cells = ResearchStore(self.store.directory(job_id) / "runs")
        cell_id = cells.create("Trusted bounded study cell")
        cells.write(cell_id, "request.json", request.model_dump(mode="json"))
        for role in ("development", "holdout") if window == "holdout" else ("development",):
            cells.write(
                cell_id,
                role + ".zip",
                (self.store.directory(job_id) / (role + ".zip")).read_bytes(),
            )
        operation = self.database.operation(strategy_id, "study_cell", job_id=job_id)
        self.store.write(
            job_id,
            key + "_started.json",
            {
                "request": request.model_dump(mode="json"),
                "cost_multiplier": multiplier,
                "at": now(),
                "worker_attempt": cell_id,
                "strategy_id": strategy_id,
                "operation_id": operation,
            },
        )
        try:
            response = await self.runner(cells, cell_id, multiplier)
            for field, expected in (
                ("protocol_sha256", protocol.digest),
                ("candidate_index", index),
                ("window", window),
                ("cost_multiplier", multiplier),
            ):
                if response.get(field) != expected:
                    raise ResearchError("Native study result does not match its approved cell")
            result = response["result"]
            if (
                result.get("cost_multiplier") != multiplier
                or result.get("engine_version") != protocol.engine_version
            ):
                raise ResearchError("Native study engine/cost identity mismatch")
            metrics = summarize(result, protocol.config.initial_balance)
            curve = result["equity_curve"]
            metrics["segments"] = [
                {
                    "name": window,
                    "first_ns": curve[0]["timestamp_ns"],
                    "last_ns": curve[-1]["timestamp_ns"],
                    "observations": len(curve),
                    "net_equity_change": metrics["net_pnl"],
                    "convention": "flat cash reset; prior bars warm indicators only",
                }
            ]
            outcome = {"status": "completed", "metrics": metrics, "response": response}
        except asyncio.CancelledError:
            self.store.write(
                job_id, key + ".json", {"status": "canceled", "detail": "Consumed slot; not rerun"}
            )
            self.database.finish_operation(operation, "canceled")
            raise
        except Exception:
            outcome = {"status": "failed", "detail": "Native cell failed; attempt retained"}
        self.store.write(job_id, key + ".json", outcome)
        self.database.finish_operation(operation, outcome["status"])
        return outcome

    async def run(self, job_id, *, progress=None):
        if self.active_job or self.preparing:
            raise ResearchError("A study operation is already active")
        with self.store.lock(job_id):
            protocol = self.store.protocol(job_id)
            if self.store.state(job_id) != "paused":
                raise ResearchError("Only an approved paused study can run")
            if self.store.read(job_id, "approval.json").get("protocol_sha256") != protocol.digest:
                raise ResearchError("Study approval integrity mismatch")
            self._verify_anchor(protocol.parent_strategy_id, protocol)
            if protocol.implementation_sha256 != study_implementation_digest():
                raise ResearchError("Implementation changed; review a new study")
            self.active_job, self.stop_state = job_id, "canceled"
            self.store.event(job_id, "running")

            async def update(detail):
                if progress:
                    await progress(job_id, detail)

            try:
                await update("Validating development archive; final holdout remains unopened")
                development = await self._snapshot(job_id, protocol.development, "development")
                attempts = []
                for index, candidate in enumerate(protocol.candidates):
                    name = f"candidate_{index}.json"
                    if self.store.exists(job_id, name):
                        attempts.append(self.store.read(job_id, name))
                        continue
                    entry = {"index": index, "parameters": candidate.model_dump(), "cells": {}}
                    try:
                        spec = protocol.variant(candidate)
                        original = fingerprint(spec) == fingerprint(protocol.specification)
                        record = self.database.register(
                            spec,
                            f"SMA {candidate.fast}/{candidate.slow}",
                            {
                                "kind": "reviewed_study_variant",
                                "protocol_sha256": protocol.digest,
                                "candidate_index": index,
                            },
                            parent=None if original else protocol.parent_strategy_id,
                            comparison=not original,
                        )
                        entry["strategy_id"] = record["public_id"]
                        entry["strategy_sha256"] = record["spec_sha256"]
                    except ValueError:
                        entry.update(
                            status="invalid",
                            detail="Parameters do not fit the supported implementation/window",
                        )
                        self.store.write(job_id, name, entry)
                        attempts.append(entry)
                        continue
                    for window in ("development", "validation"):
                        for multiplier in (1, 2):
                            await update(
                                f"Candidate {index + 1}/{len(protocol.candidates)} · "
                                f"{window} · costs ×{multiplier}"
                            )
                            entry["cells"][f"{window}_{multiplier}"] = await self._cell(
                                job_id, protocol, index, window, multiplier, record["public_id"]
                            )
                    entry["status"] = (
                        "eligible"
                        if all(c["status"] == "completed" for c in entry["cells"].values())
                        else "ineligible"
                    )
                    self.store.write(job_id, name, entry)
                    attempts.append(entry)
                selection = select_candidate(protocol, attempts)
                if self.store.exists(job_id, "selection.json"):
                    retained = self.store.read(job_id, "selection.json")
                    if {k: v for k, v in retained.items() if k != "selected_at"} != selection:
                        raise ResearchError(
                            "Frozen selection differs from retained validation evidence"
                        )
                    selection = retained
                else:
                    selection["selected_at"] = now()
                    self.store.write(job_id, "selection.json", selection)
                holdout = {}
                selected = selection["selected_index"]
                holdout_snapshot = None
                if selected is not None:
                    await update("Selection frozen; acquiring the separate final holdout")
                    holdout_snapshot = await self._snapshot(job_id, protocol.holdout, "holdout")
                    for multiplier in (1, 2):
                        holdout[str(multiplier)] = await self._cell(
                            job_id,
                            protocol,
                            selected,
                            "holdout",
                            multiplier,
                            attempts[selected]["strategy_id"],
                            selected=selected,
                        )
                report = build_study_report(
                    job_id, protocol, attempts, selection, holdout, development, holdout_snapshot
                )
                for name, value in (("report.json", report), ("report.md", study_markdown(report))):
                    if not self.store.exists(job_id, name):
                        self.store.write(job_id, name, value)
                    elif name.endswith("json") and self.store.read(job_id, name) != value:
                        raise ResearchError("Retained study report integrity mismatch")
                if self.store.exists(job_id, "artifact_manifest.json"):
                    self.store.verify_seal(job_id)
                else:
                    self.store.seal(job_id)
                self.store.event(job_id, "completed")
                self.database.record_report(job_id, report, kind="study")
                return report
            except asyncio.CancelledError:
                self.store.event(
                    job_id,
                    self.stop_state,
                    "Completed attempts retained; consumed slots are not retried",
                )
                raise
            except Exception:
                self.store.event(job_id, "failed", "Study stopped; all completed evidence retained")
                raise
            finally:
                self.database.attach_job(
                    protocol.parent_strategy_id, job_id, protocol.digest, self.store.state(job_id)
                )
                self.active_job = None


def select_candidate(protocol, attempts):
    from decimal import Decimal

    eligible = [entry for entry in attempts if entry["status"] == "eligible"]
    eligible.sort(
        key=lambda e: (
            -Decimal(e["cells"]["validation_2"]["metrics"]["net_pnl"]),
            Decimal(e["cells"]["validation_2"]["metrics"]["max_drawdown_pct"]),
            e["index"],
        )
    )
    return {
        "protocol_sha256": protocol.digest,
        "criterion": protocol.selection,
        "attempted_candidates": len(attempts),
        "eligible_candidates": len(eligible),
        "selected_index": eligible[0]["index"] if eligible else None,
        "evidence": "development/validation only; final holdout unopened",
    }
