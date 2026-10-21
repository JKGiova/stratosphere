"""Read-only, paged inspection of sealed simulation evidence; no model or native starts."""

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Literal

from pydantic import Field

from stratosphere.phase0.contracts import Contract, fingerprint
from stratosphere.research.contracts import ResearchPlan
from stratosphere.research.store import ResearchError
from stratosphere.studies.contracts import load_study_protocol

PAGE_SIZE = 20
SECTIONS = "overview|attempts|metrics|trades|fills|equity|signals|baselines"
USAGE = "/report-data <job-id> [section] [cell|-] [page]"


class ReportQuery(Contract):
    job_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    section: Literal[
        "overview", "attempts", "metrics", "trades", "fills", "equity", "signals", "baselines"
    ] = "overview"
    cell: str | None = Field(
        default=None, pattern=r"^(base|stress|c[0-9]+:(development|validation):[12]|holdout:[12])$"
    )
    page: int = Field(default=1, ge=1, strict=True)

    @classmethod
    def arguments(cls, args):
        if not 1 <= len(args) <= 4:
            raise ResearchError("Usage: " + USAGE)
        job, section, cell, page = (*args, *[None] * (4 - len(args)))
        if page is not None and not re.fullmatch(r"[1-9][0-9]*", page):
            raise ResearchError("Use a positive whole page number. Usage: " + USAGE)
        return cls(
            job_id=job,
            section=section or "overview",
            cell=None if cell in {None, "-"} else cell,
            page=int(page) if page else 1,
        )

    def command(self, page=None):
        return f"/report-data {self.job_id} {self.section} {self.cell or '-'} {page or self.page}"


def natural_report_query(text):
    match = re.fullmatch(
        r"\s*(?:please\s+)?inspect\s+report\s+([a-f0-9]{32})"
        r"(?:\s+(" + SECTIONS + r"))?(?:\s+(\S+))?(?:\s+([1-9][0-9]*))?\s*",
        text,
        re.IGNORECASE,
    )
    if match:
        return ReportQuery.arguments(tuple(v for v in match.groups() if v is not None))
    return None


def evidence_hint(job_id):
    return f"\n\nInspect retained cells and detailed evidence: `/report-data {job_id}`."


@dataclass(frozen=True)
class EvidencePage:
    query: ReportQuery
    owner_strategy_id: str
    report_sha256: str
    plan_sha256: str
    context: dict
    rows: tuple
    total: int
    pages: int

    def markdown(self):
        q = self.query
        start = (q.page - 1) * PAGE_SIZE
        lines = [
            f"# Retained {q.section}",
            "",
            f"Cell `{q.cell or 'none'}` · page {q.page}/{self.pages} · "
            f"rows {start + 1 if self.total else 0}–{start + len(self.rows)} of {self.total}.",
            "",
            f"State `{self.context.get('status', 'report')}` · result strategy "
            f"`{self.context.get('strategy_id', 'choose a cell')}`",
        ]
        if not self.rows:
            lines += ["", "No retained rows in this section; this does not establish success."]
        for index, row in enumerate(self.rows, start):
            lines += [
                "",
                f"## Row {index}",
                "",
                "```json",
                json.dumps(row, ensure_ascii=False, indent=2),
                "```",
            ]
        lines += [
            "",
            "## References and navigation",
            "",
            f"Job `{q.job_id}` · owner `{self.owner_strategy_id}`",
            f"Plan: `{self.plan_sha256}`",
            f"Report bytes: `{self.report_sha256}`",
            "",
            f"Overview: `/report-data {q.job_id}`",
            f"Reviewed plan: `/show {q.job_id}`",
            "Sections: " + ", ".join(SECTIONS.split("|")),
        ]
        if q.page > 1:
            lines += [f"Previous: `{q.command(q.page - 1)}`"]
        if q.page < self.pages:
            lines += [f"Next: `{q.command(q.page + 1)}`"]
        lines += [
            "",
            "## Cell and report context",
            "",
            "Historical simulation evidence; provider entitlement and execution remain unverified. "
            "A selected variant's result does not belong to its parent or siblings. "
            "Amounts and timestamps preserve retained values and row order. "
            "Timestamps ending in _ns are Unix nanoseconds; prices/cash/P&L are USDT, "
            "base/quantity are BTC, and *_pct values are percentages in this supported scope.",
            "",
            "```json\n" + json.dumps(self.context, ensure_ascii=False, indent=2) + "\n```",
        ]
        return "\n".join(lines)


class ReportInspector:
    def __init__(self, research, studies, database):
        self.research, self.studies, self.database = research, studies, database

    async def open(self, query: ReportQuery, *, strategy_id):
        try:
            return await self._open(query, strategy_id)
        except ResearchError:
            raise
        except ValueError, KeyError, TypeError, IndexError, OSError:
            raise ResearchError(
                "Report evidence is inconsistent or unavailable; no test was run."
            ) from None

    async def _verified(self, job_id, strategy_id):
        owner = self.database.strategy_for_job(job_id)
        if owner["public_id"] != strategy_id:
            raise ResearchError("This report belongs to a different strategy agent.")
        study_dir = self.studies.store.directory(job_id)
        research_dir = self.research.store.directory(job_id)
        if study_dir.exists() and research_dir.exists():
            raise ResearchError("Ambiguous research/study job identity.")
        study = study_dir.exists()
        service = self.studies if study else self.research
        store, directory = service.store, service.store.directory(job_id)
        if (
            store.root.is_symlink()
            or directory.is_symlink()
            or any(p.is_symlink() for p in directory.iterdir())
        ):
            raise ResearchError("Report evidence paths cannot be symbolic links.")
        # The existing operation checks online access, completion and the full artifact seal.
        report = await service.open_report(job_id)
        if (directory / "artifact_manifest.json").is_symlink():
            raise ResearchError("Report evidence paths cannot be symbolic links.")
        manifest = store.read(job_id, "artifact_manifest.json")

        def read(name):
            path = directory / name
            if path.is_symlink() or name not in manifest:
                raise ResearchError("Report evidence is not covered by the retained seal.")
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != manifest[name]:
                raise ResearchError("Report evidence failed its integrity check.")
            return json.loads(data)

        if read("report.json") != report:
            raise ResearchError("Report evidence changed during inspection.")
        plan = (
            load_study_protocol(read("protocol.json"))
            if study
            else ResearchPlan.model_validate(read("plan.json"))
        )
        if (
            report["plan_sha256"] != plan.digest
            or report["strategy_sha256"] != fingerprint(plan.specification)
            or report["strategy_sha256"] != owner["spec_sha256"]
            or report["implementation_sha256"] != plan.implementation_sha256
        ):
            raise ResearchError("Report references do not match the exact strategy and plan.")
        job = next(j for j in self.database.jobs(strategy_id) if j["job_id"] == job_id)
        if job["plan_sha256"] != plan.digest:
            raise ResearchError("Report plan differs from its registered job.")
        for retained in self.database.reports(strategy_id):
            if retained["job_id"] == job_id and retained["report"] != report:
                raise ResearchError("Report differs from its immutable registry projection.")
        context = {
            "kind": "study" if study else "research",
            "implementation_sha256": report["implementation_sha256"],
            "engine_version": plan.engine_version,
            "config": plan.config.model_dump(mode="json"),
            "assumptions": report["assumptions"],
            "limitations": report["limitations"],
        }
        return report, plan, read, context, manifest, study

    async def snapshot(self, job_id, *, strategy_id):
        """Validate every retained cell once and capture the complete report for app-owned facts."""
        try:
            report, plan, read, context, manifest, study = await self._verified(job_id, strategy_id)
            query = ReportQuery(job_id=job_id)
            if study:
                self._study(query, report, plan, read, dict(context))
                cells = [
                    f"c{attempt['index']}:{key.replace('_', ':')}"
                    for attempt in report["attempts"]
                    for key in attempt["cells"]
                ] + [f"holdout:{key}" for key in report["holdout"]]
            else:
                cells = ["base", "stress"]
            for cell in cells:
                query = ReportQuery(job_id=job_id, section="metrics", cell=cell)
                reader = self._study if study else self._research
                reader(query, report, plan, read, dict(context))
            return {
                "report": report,
                "report_sha256": manifest["report.json"],
                "plan_sha256": plan.digest,
                "context": context,
            }
        except ResearchError:
            raise
        except ValueError, KeyError, TypeError, IndexError, OSError:
            raise ResearchError("Report evidence is inconsistent or unavailable.") from None

    async def _open(self, q, strategy_id):
        report, plan, read, context, manifest, study = await self._verified(q.job_id, strategy_id)
        if study:
            rows = self._study(q, report, plan, read, context)
        else:
            if q.cell is None and q.section not in {"overview", "baselines", "attempts"}:
                q = q.model_copy(update={"cell": "base"})
            rows = self._research(q, report, plan, read, context)
        if "cell_artifact" in context:
            context["cell_sha256"] = manifest[context["cell_artifact"]]
        # A view never recalculates or rewrites the historical result.
        if not isinstance(rows, list):
            raise ResearchError("Retained report rows have an unsupported shape.")
        pages = max(1, (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE)
        if q.page > pages:
            raise ResearchError(f"Page is outside this section; available pages: 1–{pages}.")
        offset = (q.page - 1) * PAGE_SIZE
        return EvidencePage(
            q,
            strategy_id,
            manifest["report.json"],
            plan.digest,
            context,
            tuple(rows[offset : offset + PAGE_SIZE]),
            len(rows),
            pages,
        )

    def _research(self, q, report, plan, read, context):
        if report["schema_version"] != "research-report/1":
            raise ResearchError("Unsupported research report format.")
        for key in ("source_sha256", "archive_sha256"):
            if report[key] != getattr(plan, key):
                raise ResearchError("Research dataset/source reference mismatch.")
        context["dataset"] = {
            k: report[k]
            for k in (
                "archive_sha256",
                "normalized_sha256",
                "source_url",
                "first_ns",
                "last_ns",
                "observations",
            )
        }
        context["config"] = plan.config.model_dump(mode="json")
        if q.section in {"overview", "baselines"}:
            if q.cell:
                raise ResearchError("Overview and baselines require cell '-'.")
            if q.section == "baselines":
                return [
                    {
                        "baseline": report["baseline"],
                        "unavailable": report["baseline_unavailable"],
                        "scope": "base costs; same quantity, not stress",
                    }
                ]
            return [
                {
                    "cells": ["base", "stress"],
                    "sections": SECTIONS.split("|"),
                    "usage": USAGE,
                    "example": f"/report-data {q.job_id} equity base 1",
                }
            ]
        if q.section == "attempts":
            raise ResearchError("Candidate attempts apply only to study reports.")
        cell = q.cell or "base"
        if cell not in {"base", "stress"}:
            raise ResearchError("Research cells are base or stress.")
        response = read("base.json" if cell == "base" else "stress.json")
        result = response["result"]
        multiplier = 1 if cell == "base" else 2
        if (
            response["plan_sha256"] != plan.digest
            or response["normalized_sha256"] != report["normalized_sha256"]
            or result["cost_multiplier"] != multiplier
            or result["engine_version"] != plan.engine_version
        ):
            raise ResearchError("Research response does not match its sealed cell.")
        context.update(
            strategy_id=self.database.strategy_for_job(q.job_id)["public_id"],
            cost_multiplier=multiplier,
            status="completed",
            engine_version=result["engine_version"],
            cell_artifact="base.json" if cell == "base" else "stress.json",
        )
        metrics = report["metrics"] if cell == "base" else report["doubled_cost_stress"]
        return self._section(q.section, result, metrics, context)

    def _study(self, q, report, plan, read, context):
        if (
            report["schema_version"] not in {"study-report/1", "study-report/2"}
            or report["protocol"] != plan.model_dump(mode="json")
            or report["job_id"] != q.job_id
            or report["parent_strategy_id"] != plan.parent_strategy_id
        ):
            raise ResearchError("Study report/protocol identity mismatch.")
        selection = read("selection.json")
        if report["selection"] != selection or len(report["attempts"]) != len(plan.candidates):
            raise ResearchError("Study selection or candidate denominator mismatch.")
        attempts = report["attempts"]
        for index, attempt in enumerate(attempts):
            checkpoint = {k: v for k, v in attempt.items() if k != "selection_status"}
            if (
                checkpoint != read(f"candidate_{index}.json")
                or attempt["index"] != index
                or attempt["parameters"] != plan.candidates[index].model_dump(mode="json")
            ):
                raise ResearchError("Study candidate checkpoint mismatch.")
            if attempt["status"] != "invalid":
                identity = self.database.strategy(attempt["strategy_id"])
                if (
                    attempt["strategy_sha256"] != fingerprint(plan.variant(plan.candidates[index]))
                    or identity["spec_sha256"] != attempt["strategy_sha256"]
                ):
                    raise ResearchError("Study candidate strategy identity mismatch.")
        context.update(
            hypothesis=report["hypothesis"],
            dataset_pins={
                "development": plan.development.model_dump(mode="json"),
                "holdout": plan.holdout.model_dump(mode="json"),
            },
            selection=selection,
            attempted_candidates=report["attempted_candidates"],
            invalid_candidates=report["invalid_candidates"],
            ineligible_candidates=report["ineligible_candidates"],
            native_attempts=report["native_attempts"],
            holdout_status=report["holdout_status"],
            metrics_strategy_id=report["metrics_strategy_id"],
            metrics_role=report["metrics_role"],
            development=report["development_normalized_sha256"],
            holdout=report["holdout_normalized_sha256"],
        )
        if q.section in {"overview", "attempts", "baselines"}:
            if q.cell:
                raise ResearchError("Overview, attempts and baselines require cell '-'.")
            if q.section == "baselines":
                return [
                    {"window": window, "benchmarks": value}
                    for window, value in report["baselines"].items()
                ]
            return [
                {
                    "candidate_index": a["index"],
                    "strategy_id": a.get("strategy_id"),
                    "parameters": a["parameters"],
                    "status": a["status"],
                    "detail": a.get("detail"),
                    "selection_status": a["selection_status"],
                    "cells": {key: value["status"] for key, value in a["cells"].items()},
                    "inspect": [
                        f"/report-data {q.job_id} metrics c{a['index']}:{window}:{m} 1"
                        for window in ("development", "validation")
                        for m in (1, 2)
                    ],
                    "selected_holdout": [
                        f"/report-data {q.job_id} metrics holdout:{m} 1" for m in (1, 2)
                    ]
                    if a["index"] == selection["selected_index"]
                    else [],
                }
                for a in attempts
            ]
        if not q.cell or q.cell in {"base", "stress"}:
            raise ResearchError("Choose an exact study cell: c0:validation:1 or holdout:1.")
        if q.cell.startswith("holdout:"):
            window, raw = q.cell.split(":")
            multiplier, index = int(raw), selection["selected_index"]
            if index is None:
                raise ResearchError("No selected candidate; no final holdout evidence.")
            outcome = report["holdout"].get(raw)
        else:
            candidate, window, raw = q.cell.split(":")
            index, multiplier = int(candidate[1:]), int(raw)
            if index >= len(attempts):
                raise ResearchError("Unknown study candidate index.")
            outcome = attempts[index]["cells"].get(f"{window}_{multiplier}")
        if outcome is None:
            raise ResearchError("This study cell was not executed; inspect attempts for its state.")
        key = f"cell_{index}_{window}_{multiplier}"
        if read(key + ".json") != outcome:
            raise ResearchError("Study cell checkpoint differs from its report.")
        context.update(
            candidate_index=index,
            strategy_id=attempts[index].get("strategy_id"),
            window=window,
            cost_multiplier=multiplier,
            status=outcome["status"],
            cell_artifact=key + ".json",
            config=plan.config.model_dump(mode="json"),
        )
        if outcome["status"] != "completed":
            return [outcome]
        response, metrics = outcome["response"], outcome["metrics"]
        result = response["result"]
        if (
            response["protocol_sha256"] != plan.digest
            or response["candidate_index"] != index
            or response["window"] != window
            or response["cost_multiplier"] != multiplier
            or result["cost_multiplier"] != multiplier
            or result["engine_version"] != plan.engine_version
        ):
            raise ResearchError("Study response does not match its sealed cell.")
        return self._section(q.section, result, metrics, context)

    @staticmethod
    def _section(section, result, metrics, context):
        context.update(
            pending_signal=result["pending_signal"],
            open_positions=result["open_positions"],
            balances=result["balances"],
            runtime_versions=result.get("runtime_versions", {}),
        )
        if section == "metrics":
            return [
                {"metric": key, "value": value} for key, value in metrics.items() if key != "trades"
            ]
        if section == "trades":
            return metrics["trades"]
        return result[{"fills": "fills", "equity": "equity_curve", "signals": "signals"}[section]]
