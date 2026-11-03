"""Model-free, single-program research jobs with immutable historical campaign readers."""

import asyncio
import hashlib
import json
import os
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from stratosphere.development.compiler import compile_strategy
from stratosphere.development.contracts import (
    CellPlan,
    Definition,
    PriceSnapshot,
    candidate_spec,
    implementation_digest,
)
from stratosphere.development.transport import worker
from stratosphere.phase0.contracts import fingerprint
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.workflow.discovery import digest
from stratosphere.yahoo.data import normalize
from stratosphere.yahoo.metrics import summarize_yahoo

LIMITS = {
    "max_programs": 3,
    "max_markets": 4,
    "max_candidates": 12,
    "max_native_starts": 24,
    "max_model_calls": 3,
    "deadline_seconds": 1200,
}
ENGINE_LIMITS = {
    "max_programs": 1,
    "max_markets": 4,
    "max_candidates": 4,
    "max_native_starts": 8,
    "max_model_calls": 0,
    "deadline_seconds": 1200,
}


def limits_for(plan):
    version = plan.get("schema_version")
    if version == "strategy-development-plan/1":
        return LIMITS
    if version == "strategy-development-plan/3":
        return ENGINE_LIMITS
    raise ResearchError("Unsupported development plan schema")


SELECTION = {
    "ranking": "stress_net_return_pct_minus_stress_max_drawdown_pct",
    "minimum_completed_trades": 5,
    "maximum_stress_drawdown_pct": "20",
    "require_positive_base_and_stress": True,
    "holdout": "none",
}


def write_code(directory, content):
    path = directory / "strategy.py"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def breakdown(result, snapshot):
    """Descriptive year and prior-20-bar direction buckets, never an independent test."""
    years, regimes = {}, {}
    previous = Decimal(snapshot.request.config.initial_balance)
    prices = []
    for row, bar in zip(result["equity_curve"], snapshot.bars, strict=True):
        year = str(datetime.fromtimestamp(bar.open_ns / 1e9, ZoneInfo(snapshot.timezone)).year)
        regime = "insufficient_history"
        if len(prices) >= 20:
            delta = prices[-1] - prices[-20]
            regime = "up" if delta > 0 else "down" if delta < 0 else "flat"
        change = Decimal(row["equity"]) - previous
        for mapping, key in ((years, year), (regimes, regime)):
            item = mapping.setdefault(key, {"observations": 0, "net_equity_change": "0"})
            item["observations"] += 1
            item["net_equity_change"] = str(Decimal(item["net_equity_change"]) + change)
        previous = Decimal(row["equity"])
        prices.append(Decimal(bar.close))
    return {
        "years": years,
        "prior_20_bar_direction": regimes,
        "meaning": "Same-sample equity attribution; no predictive regime or holdout claim",
    }


class StrategyDevelopment:
    def __init__(self, root, discovery, database, *, runner=worker, online_check=None):
        self.store = ResearchStore(root)
        self.cells = ResearchStore(self.store.root / "cells")
        self.datasets = ResearchStore(self.store.root / "datasets")
        self.discovery, self.database, self.runner = discovery, database, runner
        self.online_check = online_check or discovery.sources.require_online

    def clean(self, value):
        self.discovery.clean(value)

    def plan(self, identity):
        if self.is_continuous(identity):
            return self.history.plan(identity)
        path = self.store.directory(identity)
        if path.is_symlink() or any(p.is_symlink() for p in path.iterdir()):
            raise ResearchError("Development evidence cannot contain redirected paths")
        plan = self.store.read(identity, "plan.json")
        if digest(plan) != self.store.read(identity, "plan_seal.json")["sha256"]:
            raise ResearchError("Development plan changed")
        if plan["limits"] != limits_for(plan) or plan["selection"] != SELECTION:
            raise ResearchError("Development budget/selection policy changed")
        intake = self.discovery.open(plan["intake_id"])
        if digest(intake) != plan["intake_sha256"]:
            raise ResearchError("Development source brief changed")
        definition = Definition.model_validate(plan["definition"])
        if sorted(pin["index"] for pin in plan["datasets"]) != list(range(len(definition.markets))):
            raise ResearchError("Development plan lost attempted market coverage")
        if self.database.strategy(plan["owner"])["spec_sha256"] != fingerprint(
            definition.specification
        ):
            raise ResearchError("Development strategy owner changed")
        for data in plan["datasets"]:
            if data["state"] == "ready":
                if self.snapshot(data).request != definition.markets[data["index"]]:
                    raise ResearchError("Development dataset market ownership changed")
        return plan

    def snapshot(self, pin):
        folder = self.datasets.directory(pin["id"])
        if folder.is_symlink() or any(p.is_symlink() for p in folder.iterdir()):
            raise ResearchError("Development dataset paths changed")
        value = PriceSnapshot.model_validate(self.datasets.read(pin["id"], "snapshot.json"))
        raw = self.datasets.read(pin["id"], "returned_frame.json")
        if (
            value.digest != pin["sha256"]
            or normalize(raw, value.request, snapshot_contract=PriceSnapshot) != value
        ):
            raise ResearchError("Development dataset changed")
        return value

    @property
    def history(self):
        from stratosphere.development.history import LoopHistory

        return LoopHistory(self)

    def is_continuous(self, identity):
        path = self.store.directory(identity) / "plan.json"
        return (
            path.is_file()
            and self.store.read(identity, "plan.json").get("schema_version")
            == "strategy-development-plan/2"
        )

    async def prepare(self, intake_id, definition: Definition, *, focus="", parent=None):
        """Freeze one externally supplied typed program; never invoke or configure a model."""
        definition = Definition.model_validate(definition)
        definition = Definition.model_validate(definition.model_dump(mode="json"))
        if definition.status != "ready":
            raise ResearchError("Resolve the externally supplied definition before research")
        intake = self.discovery.open(intake_id)
        if intake["questions"] or intake["status"] != "research_brief":
            raise ResearchError("Resolve the intake's open questions before strategy development")
        if len(focus) > 4000:
            raise ResearchError("Development focus is limited to 4000 characters")
        self.clean({"focus": focus, "definition": definition.model_dump(mode="json")})
        if parent is not None:
            self.database.strategy(parent)
        await self.online_check()
        implementation = implementation_digest()
        identity = digest(
            {
                "intake": digest(intake),
                "focus": focus,
                "parent": parent,
                "definition": definition.model_dump(mode="json"),
                "protocol": "external-program/3",
                "implementation": implementation,
                "limits": ENGINE_LIMITS,
            }
        )[:32]
        self.store.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            self.store.directory(identity).mkdir(mode=0o700)
        except FileExistsError:
            # Re-delivery never refills a consumed planning/research budget.
            state = self.store.state(identity)
            if state not in {"policy_ready", "completed"}:
                raise ResearchError(
                    f"Development {identity} is {state}; inspect retained evidence; no replay"
                ) from None
            self.plan(identity)
            return identity
        self.store.write(
            identity,
            "request.json",
            {
                "intake_id": intake_id,
                "intake_sha256": digest(intake),
                "focus": focus,
                "definition_sha256": fingerprint(definition),
                "parent_strategy": parent,
            },
        )
        self.store.event(identity, "planning")
        try:
            with self.store.lock(identity):
                self.clean(definition.model_dump(mode="json"))
                self.store.write(identity, "definition.json", definition.model_dump(mode="json"))
                for category, index in definition.rule_indices.items():
                    if (
                        type(index) is not int
                        or not 0 <= index < len(intake["rules"])
                        or intake["rules"][index]["category"] != category
                    ):
                        raise ResearchError("Definition references an invalid intake rule")
                owner = self.database.register(
                    definition.specification,
                    definition.name,
                    {
                        "kind": "strategy-development/3",
                        "intake_id": intake_id,
                        "intake_sha256": digest(intake),
                        "conversion": "not_verified",
                    },
                    parent=parent,
                    comparison=parent is not None,
                )
                self.store.event(identity, "preparing")
                pins = []
                for index, market in enumerate(definition.markets):
                    data_id = hashlib.sha256((identity + str(index)).encode()).hexdigest()[:32]
                    self.datasets.root.mkdir(mode=0o700, parents=True, exist_ok=True)
                    self.datasets.directory(data_id).mkdir(mode=0o700)
                    self.datasets.write(data_id, "request.json", market.model_dump(mode="json"))
                    self.datasets.event(data_id, "fetching")
                    try:
                        raw = await self.runner(
                            self.datasets.directory(data_id), "fetch", "request.json"
                        )
                        self.clean(raw)
                        self.datasets.write(data_id, "returned_frame.json", raw)
                        snapshot = normalize(raw, market, snapshot_contract=PriceSnapshot)
                        self.datasets.write(
                            data_id, "snapshot.json", snapshot.model_dump(mode="json")
                        )
                        self.datasets.event(data_id, "ready")
                        pins.append(
                            {
                                "id": data_id,
                                "index": index,
                                "state": "ready",
                                "sha256": snapshot.digest,
                            }
                        )
                    except (ResearchError, ValueError, KeyError, ArithmeticError) as exc:
                        self.datasets.event(data_id, "blocked", "No automatic acquisition retry")
                        pins.append(
                            {"id": data_id, "index": index, "state": "blocked", "error": str(exc)}
                        )
                if implementation_digest() != implementation:
                    raise ResearchError("Implementation changed during development preparation")
                plan = {
                    "schema_version": "strategy-development-plan/3",
                    "campaign_id": identity,
                    "owner": owner["public_id"],
                    "intake_id": intake_id,
                    "intake_sha256": digest(intake),
                    "definition": definition.model_dump(mode="json"),
                    "focus": focus,
                    "definition_sha256": fingerprint(definition),
                    "implementation_sha256": implementation,
                    "datasets": pins,
                    "limits": ENGINE_LIMITS,
                    "selection": SELECTION,
                }
                self.store.write(identity, "plan.json", plan)
                self.store.write(identity, "plan_seal.json", {"sha256": digest(plan)})
                self.store.event(identity, "policy_ready")
                return identity
        except asyncio.CancelledError:
            self.store.event(identity, "canceled", "No replay")
            raise
        except Exception:
            if self.store.state(identity) != "needs_clarification":
                self.store.event(identity, "failed", "No replay")
            raise

    async def run(self, identity, *, owner):
        with self.store.lock(identity):
            plan = self.plan(identity)
            if plan["schema_version"] != "strategy-development-plan/3":
                raise ResearchError("Historical agent campaigns are read-only; no resume or replay")
            if owner != plan["owner"]:
                raise ResearchError("Research owner differs from the exact frozen strategy")
            if (
                self.store.state(identity) != "policy_ready"
                or plan["implementation_sha256"] != implementation_digest()
            ):
                raise ResearchError(
                    "Development is consumed or its implementation changed; no replay"
                )
            await self.online_check()
            self.store.write(
                identity,
                "authorization.json",
                {
                    "plan_sha256": digest(plan),
                    "authority": "finite-application-research-policy",
                    "limits": ENGINE_LIMITS,
                    "human_approval": False,
                    "order_authority": False,
                },
            )
            self.store.event(identity, "running")
            definition = Definition.model_validate(plan["definition"])
            results, programs, starts = [], [], 0
            spec = definition.specification
            parent = owner
            active_cell = None
            try:
                async with asyncio.timeout(ENGINE_LIMITS["deadline_seconds"]):
                    round_index = 0
                    program_record = self.database.register(
                        spec,
                        definition.name,
                        {
                            "kind": "development-program/1",
                            "campaign": identity,
                            "round": round_index,
                            "source_conformance": "not_verified",
                        },
                        parent=parent if round_index else None,
                        comparison=True,
                    )
                    parent = program_record["public_id"]
                    programs.append(
                        {
                            "round": round_index,
                            "strategy_id": parent,
                            "specification_sha256": fingerprint(spec),
                        }
                    )
                    for pin in plan["datasets"]:
                        if pin["state"] != "ready":
                            continue
                        market = definition.markets[pin["index"]]
                        concrete = candidate_spec(spec, market)
                        record = self.database.register(
                            concrete,
                            definition.name,
                            {
                                "kind": "development-market/1",
                                "campaign": identity,
                                "round": round_index,
                                "market": market.model_dump(mode="json"),
                            },
                            parent=parent,
                            comparison=True,
                        )
                        cell_id = hashlib.sha256(
                            (identity + f":{round_index}:{pin['index']}").encode()
                        ).hexdigest()[:32]
                        self.cells.root.mkdir(mode=0o700, parents=True, exist_ok=True)
                        self.cells.directory(cell_id).mkdir(mode=0o700)
                        code = compile_strategy(concrete)
                        write_code(self.cells.directory(cell_id), code)
                        self.cells.write(
                            cell_id, "specification.json", concrete.model_dump(mode="json")
                        )
                        snapshot = self.snapshot(pin)
                        cell_plan = CellPlan(
                            campaign_id=identity,
                            campaign_sha256=digest(plan),
                            strategy_id=record["public_id"],
                            specification=concrete,
                            request=market,
                            snapshot_sha256=snapshot.digest,
                            code_sha256=hashlib.sha256(code).hexdigest(),
                            implementation_sha256=plan["implementation_sha256"],
                        )
                        self.cells.write(
                            cell_id,
                            "input.json",
                            {
                                "plan": cell_plan.model_dump(mode="json"),
                                "snapshot": snapshot.model_dump(mode="json"),
                            },
                        )
                        self.cells.event(cell_id, "running")
                        item = {
                            "cell_id": cell_id,
                            "round": round_index,
                            "strategy_id": record["public_id"],
                            "market": market.model_dump(mode="json"),
                            "plan_sha256": cell_plan.digest,
                            "code_sha256": cell_plan.code_sha256,
                            "code_path": str(
                                (self.cells.directory(cell_id) / "strategy.py").resolve()
                            ),
                            "definition_path": str(
                                (self.cells.directory(cell_id) / "specification.json").resolve()
                            ),
                        }
                        self.store.write(
                            identity, f"candidate_{'abcdefghijkl'[len(results)]}.json", item
                        )
                        active_cell = cell_id
                        try:
                            metrics = {}
                            for multiplier, label in ((1, "base"), (2, "stress")):
                                if starts >= ENGINE_LIMITS["max_native_starts"]:
                                    raise ResearchError("Native development budget exhausted")
                                self.cells.write(
                                    cell_id,
                                    f"start_{label}.json",
                                    {
                                        "consumed": True,
                                        "ordinal": starts + 1,
                                        "plan_sha256": cell_plan.digest,
                                    },
                                )
                                starts += 1
                                result = await self.runner(
                                    self.cells.directory(cell_id),
                                    "simulate",
                                    "input.json",
                                    multiplier=multiplier,
                                )
                                self.clean(result)
                                if (
                                    result["plan_sha256"] != cell_plan.digest
                                    or result["snapshot_sha256"] != snapshot.digest
                                ):
                                    raise ResearchError("Native result identity changed")
                                self.cells.write(cell_id, f"result_{label}.json", result)
                                metrics[label] = summarize_yahoo(
                                    result, market.config.initial_balance
                                )
                                if label == "base":
                                    item["conditions"] = breakdown(result, snapshot)
                            item.update(state="completed", metrics=metrics)
                            self.cells.event(cell_id, "completed")
                        except (ResearchError, ValueError, KeyError, ArithmeticError) as exc:
                            item.update(state="failed", error=str(exc))
                            self.cells.event(cell_id, "failed", "Consumed starts cannot replay")
                        self.clean(item)
                        self.cells.write(cell_id, "report.json", item)
                        self.cells.write(cell_id, "seal.json", {"sha256": digest(item)})
                        results.append(item)
                        active_cell = None
                        self.store.write(
                            identity, f"partial_{'abcdefghijkl'[len(results) - 1]}.json", item
                        )
                    report = self.build_report(identity, plan, programs, results, starts)
                    self.store.write(identity, "report.json", report)
                    self.store.write(identity, "report.md", report_markdown(report))
                    self.store.write(identity, "report_seal.json", {"sha256": digest(report)})
                    self.store.event(identity, "completed")
                    return self.report(identity)
            except asyncio.CancelledError:
                if active_cell is not None:
                    self.cells.event(active_cell, "canceled", "Consumed native slots retained")
                self.store.event(identity, "canceled", "All consumed native slots remain consumed")
                raise
            except Exception:
                if active_cell is not None:
                    self.cells.event(active_cell, "failed", "Consumed native slots retained")
                self.store.event(identity, "failed", "Partial trials retained; no replay")
                raise

    def build_report(self, identity, plan, programs, results, starts):
        ranked = []
        for row in results:
            if row["state"] != "completed":
                continue
            base, stress = row["metrics"]["base"], row["metrics"]["stress"]
            qualifies = (
                Decimal(base["net_return_pct"]) > 0
                and Decimal(stress["net_return_pct"]) > 0
                and stress["completed_trades"] >= SELECTION["minimum_completed_trades"]
                and Decimal(stress["max_drawdown_pct"])
                <= Decimal(SELECTION["maximum_stress_drawdown_pct"])
            )
            score = Decimal(stress["net_return_pct"]) - Decimal(stress["max_drawdown_pct"])
            ranked.append(
                {
                    "cell_id": row["cell_id"],
                    "strategy_id": row["strategy_id"],
                    "score": str(score),
                    "passes_research_filters": qualifies,
                }
            )
        ranked.sort(key=lambda r: (-Decimal(r["score"]), r["cell_id"]))
        selected = next((r["cell_id"] for r in ranked if r["passes_research_filters"]), None)
        by_cell = {row["cell_id"]: row for row in results}
        market_summary = []
        seen_markets = set()
        # Prefer an eligible case for each symbol; a high-score one-trade case cannot hide it.
        for rank in sorted(ranked, key=lambda row: not row["passes_research_filters"]):
            row = by_cell[rank["cell_id"]]
            key = row["market"]["asset_class"], row["market"]["symbol"]
            if key in seen_markets:
                continue
            seen_markets.add(key)
            market_summary.append(
                {
                    "asset_class": key[0],
                    "symbol": key[1],
                    "best_sampled_cell_id": row["cell_id"],
                    "interval": row["market"]["interval"],
                    "score": rank["score"],
                    "passes_research_filters": rank["passes_research_filters"],
                }
            )
        return {
            "schema_version": (
                "strategy-development-report/3"
                if plan["schema_version"] == "strategy-development-plan/3"
                else "strategy-development-report/1"
            ),
            "campaign_id": identity,
            "owner": plan["owner"],
            "plan_sha256": digest(plan),
            "programs": programs,
            "results": results,
            "datasets": [
                pin | {"market": plan["definition"]["markets"][pin["index"]]}
                for pin in plan["datasets"]
            ],
            "ranking": ranked,
            "market_summary": market_summary,
            "selected_cell_id": selected,
            "native_starts": starts,
            "status": "candidate_for_review" if selected else "no_candidate_passed_filters",
            "execution": "blocked",
            "source_conformance": "not_verified",
            "holdout": "none",
            "limitations": [
                "Ranking covers only the frozen sampled instruments, intervals and windows.",
                "Variants used these same results: no independent holdout or global-best claim.",
                "Synthetic fixed-unit fills; no corporate actions, margin, funding or rollover.",
                "Final exposure stays open and is marked at the final close.",
                "EMA is SMA-seeded; Wilder RSI is change-seeded with flat-market value 50.",
                "Point-in-time stock membership, delisting and survivorship bias are unverified.",
                "Year/regime buckets describe this sample; they do not validate regimes.",
                "Compiled code/citations do not establish faithful conversion or MT5 suitability.",
                "No final approval, portfolio assignment or broker order was activated.",
            ],
        }

    def report(self, identity):
        if self.is_continuous(identity):
            return self.history.report(identity)
        plan = self.plan(identity)
        report = self.store.read(identity, "report.json")
        if (
            self.store.state(identity) != "completed"
            or report["plan_sha256"] != digest(plan)
            or digest(report) != self.store.read(identity, "report_seal.json")["sha256"]
        ):
            raise ResearchError("Development report changed or is incomplete")
        expected = {
            hashlib.sha256((identity + f":{program['round']}:{pin['index']}").encode()).hexdigest()[
                :32
            ]
            for program in report["programs"]
            for pin in plan["datasets"]
            if pin["state"] == "ready"
        }
        if (
            {row["cell_id"] for row in report["results"]} != expected
            or len(report["results"]) != len(expected)
            or len(report["programs"]) > limits_for(plan)["max_programs"]
        ):
            raise ResearchError("Development report lost or duplicated attempted candidates")
        ordinals = []
        for item in report["results"]:
            cell = self.cells.directory(item["cell_id"])
            if cell.is_symlink() or any(p.is_symlink() for p in cell.iterdir()):
                raise ResearchError("Development cell paths changed")
            if (
                item != self.cells.read(item["cell_id"], "report.json")
                or digest(item) != self.cells.read(item["cell_id"], "seal.json")["sha256"]
            ):
                raise ResearchError("Development cell report changed")
            payload = self.cells.read(item["cell_id"], "input.json")
            cell_plan = CellPlan.model_validate(payload["plan"])
            for label in ("base", "stress"):
                marker = cell / f"start_{label}.json"
                if marker.exists():
                    start = self.cells.read(item["cell_id"], marker.name)
                    if start["plan_sha256"] != cell_plan.digest or start["consumed"] is not True:
                        raise ResearchError("Development native reservation changed")
                    ordinals.append(start["ordinal"])
            snapshot = PriceSnapshot.model_validate(payload["snapshot"])
            if (
                cell_plan.campaign_id != identity
                or cell_plan.campaign_sha256 != digest(plan)
                or snapshot.digest != cell_plan.snapshot_sha256
                or snapshot.request != cell_plan.request
                or cell_plan.strategy_id != item["strategy_id"]
                or self.database.strategy(item["strategy_id"])["spec_sha256"]
                != fingerprint(cell_plan.specification)
            ):
                raise ResearchError("Development report/input owner changed")
            if cell_plan.digest != item["plan_sha256"] or (
                cell / "strategy.py"
            ).read_bytes() != compile_strategy(cell_plan.specification):
                raise ResearchError("Development code/input changed")
            for label in ("base", "stress") if item["state"] == "completed" else ():
                raw = self.cells.read(item["cell_id"], f"result_{label}.json")
                if (
                    raw["plan_sha256"] != cell_plan.digest
                    or raw["snapshot_sha256"] != snapshot.digest
                ):
                    raise ResearchError("Development native result identity changed")
                if (
                    summarize_yahoo(raw, cell_plan.request.config.initial_balance)
                    != item["metrics"][label]
                ):
                    raise ResearchError("Development metrics changed")
        if (
            sorted(ordinals) != list(range(1, report["native_starts"] + 1))
            or report["native_starts"] > limits_for(plan)["max_native_starts"]
        ):
            raise ResearchError("Development native budget ledger changed")
        if (
            self.build_report(
                identity, plan, report["programs"], report["results"], report["native_starts"]
            )
            != report
        ):
            raise ResearchError("Development selection changed")
        return report

    def evidence_for(self, owner):
        evidence = []
        for identity in self.store.jobs():
            if self.store.state(identity) != "completed":
                continue
            plan = self.plan(identity)
            report = self.report(identity)
            rounds = {p["round"] for p in report["programs"] if p["strategy_id"] == owner}
            rows = [
                row
                for row in report["results"]
                if row["strategy_id"] == owner or plan["owner"] == owner or row["round"] in rounds
            ]
            if not rows:
                continue
            evidence.append(
                {
                    "campaign_id": identity,
                    "owner": plan["owner"],
                    "intake_id": plan["intake_id"],
                    "source_conformance": "not_verified",
                    "holdout": "none",
                    "selected_cell_id": report["selected_cell_id"],
                    "results": [
                        {
                            "strategy_id": row["strategy_id"],
                            "market": row["market"],
                            "state": row["state"],
                            "conditions": row.get("conditions"),
                            "metrics": {
                                label: {
                                    key: values[key]
                                    for key in (
                                        "net_return_pct",
                                        "max_drawdown_pct",
                                        "completed_trades",
                                    )
                                }
                                for label, values in row.get("metrics", {}).items()
                            },
                        }
                        for row in rows
                    ],
                }
            )
        return evidence[-5:]

    def show(self, identity):
        if self.is_continuous(identity) and self.store.state(identity) != "completed":
            state = self.history.state(identity)
            return (
                f"# Persistent strategy loop `{identity}`\n\n"
                f"State: {self.store.state(identity)}. "
                f"Evaluated programs: {len(state['experiments'])}. "
                f"Reserved native slots: {state['native_reserved']}. "
                "Read-only checkpoint; the internal agent runner is removed. "
                "No resume or trading authority."
            )
        if self.store.state(identity) == "completed":
            return report_markdown(self.report(identity))
        directory = self.store.directory(identity)
        if directory.is_symlink() or any(p.is_symlink() for p in directory.iterdir()):
            raise ResearchError("Development evidence paths changed")
        lines = [
            "# Strategy development",
            f"Campaign `{identity}` · {self.store.state(identity)}.",
            "Incomplete retained evidence; no approval, replay or execution authority.",
        ]
        for path in sorted(directory.glob("candidate_*.json")):
            value = json.loads(path.read_text())
            self.clean(value)
            state = self.cells.state(value["cell_id"])
            lines.append(
                f"- Candidate `{value['cell_id']}` · {value['market']['symbol']} · {state}"
            )
        for path in sorted(directory.glob("partial_*.json")):
            value = json.loads(path.read_text())
            self.clean(value)
            lines.append(f"- `{value['cell_id']}` · {value['market']['symbol']} · {value['state']}")
        if (directory / "definition.json").is_file():
            definition = Definition.model_validate(self.store.read(identity, "definition.json"))
            lines.extend(definition.questions)
        return "\n\n".join(lines)


def report_markdown(report):
    default_limits = ENGINE_LIMITS if report["schema_version"].endswith("/3") else LIMITS
    limits = report.get("limits", default_limits)
    lines = [
        "# Strategy development",
        "",
        f"Campaign `{report['campaign_id']}` · agent owner `{report['owner']}`",
        f"Native starts: {report['native_starts']} / "
        f"{limits['max_native_starts']}. Status: {report['status']}.",
        "",
        "| Candidate | Round | Symbol | Interval | Base return | Stress return | State |",
        "| --- | ---: | --- | --- | ---: | ---: | --- |",
    ]
    if "stop_reason" in report:
        lines.extend(
            [
                "",
                f"Loop stopped: {report['stop_reason']}. Model slots: {report['model_calls']}.",
                f"Reserved native slots: {report['native_reserved']}. "
                f"Best experiment: {report['best_experiment_id'] or 'none'}.",
            ]
        )
    for item in report["results"]:
        metrics = item.get("metrics", {})
        lines.append(
            f"| `{item['cell_id']}` | {item['round']} | "
            f"{item['market']['symbol']} | {item['market']['interval']} | "
            f"{metrics.get('base', {}).get('net_return_pct', '—')} | "
            f"{metrics.get('stress', {}).get('net_return_pct', '—')} | {item['state']} |"
        )
        if item.get("error"):
            lines.append("\nFailed attempt: " + item["error"])
    for data in report["datasets"]:
        if data["state"] == "blocked":
            lines.append(
                f"\nBlocked dataset {data['market']['symbol']} "
                f"{data['market']['interval']}: " + data["error"]
            )
    lines += [
        "",
        "Selected candidate: " + (report["selected_cell_id"] or "none"),
        "Historical compiled code only; no MT5 execution or final approval.",
        "",
        *report["limitations"],
    ]
    lines += ["", "## Ranking within the declared scope", ""]
    for market in report["market_summary"]:
        lines.append(
            f"- {market['symbol']} {market['interval']}: best sampled candidate "
            f"`{market['best_sampled_cell_id']}`; "
            f"passes research filters: {market['passes_research_filters']}"
        )
    for row in report["ranking"]:
        lines.append(
            f"- `{row['cell_id']}`: stress return minus stress drawdown = {row['score']}; "
            f"passes research filters: {row['passes_research_filters']}"
        )
    for row in report["results"]:
        lines += [
            "",
            f"## Candidate {row['cell_id']}",
            f"Window: {row['market']['start']}–{row['market']['end']} exclusive; "
            f"fixed units: {row['market']['quantity']}.",
            f"Market hypothesis: {row['market']['hypothesis']}",
            f"Compiled code: {row['code_path']}",
            f"Exact definition: {row['definition_path']}",
            f"Code SHA-256: `{row['code_sha256']}`",
        ]
        if row["state"] == "completed":
            lines += [
                "Same-sample yearly equity changes: " + json.dumps(row["conditions"]["years"]),
                "Prior-20-bar direction buckets: "
                + json.dumps(row["conditions"]["prior_20_bar_direction"]),
            ]
    return "\n".join(lines)
