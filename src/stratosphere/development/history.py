"""Read-only v2 campaign/checkpoint validation. No inference, execution or continuation."""

from stratosphere.development.contracts import Definition
from stratosphere.development.history_contracts import Action
from stratosphere.development.progress import POLICY, evaluate
from stratosphere.development.research_tools import tune_indicator
from stratosphere.phase0.contracts import StrategySpec, fingerprint
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.workflow.discovery import digest

LIMITS = {
    "max_model_calls": 32,
    "max_native_starts": 128,
    "max_markets": 4,
    "max_searches": 4,
    "max_tool_errors": 3,
    "active_seconds": 3600,
}


def frozen_revision(baseline, proposed):
    old, new = baseline.model_dump(mode="json"), proposed.model_dump(mode="json")
    for field in set(old) - {
        "indicators",
        "entry_rules",
        "exit_rules",
        "market_conditions",
        "data_requirements",
    }:
        if old[field] != new[field]:
            raise ResearchError("Proposal changed frozen non-signal behavior")
    if any(
        old["data_requirements"][k] != new["data_requirements"][k]
        for k in ("input", "availability")
    ):
        raise ResearchError("Proposal changed frozen data requirements")


class ExperimentHistory:
    def __init__(self, base):
        self.base = base

    @property
    def experiments(self):
        service = type(self.base)(
            self.base.store.root / "loop-experiments",
            self.base.discovery,
            self.base.database,
            runner=self.base.runner,
            online_check=self.base.online_check,
        )
        service.datasets = self.base.datasets
        return service

    def experiment_id(self, identity, spec):
        return digest({"loop": identity, "program": fingerprint(spec)})[:32]

    def read_experiment(self, identity):
        return self.experiments.report(identity)


class LoopHistory:
    def __init__(self, base):
        self.base, self.store = base, base.store
        self.turns = ResearchStore(base.store.root / "loop-turns")
        self.tools = ExperimentHistory(base)

    def plan(self, identity):
        directory = self.store.directory(identity)
        if directory.is_symlink() or any(p.is_symlink() for p in directory.iterdir()):
            raise ResearchError("Loop evidence cannot contain redirected paths")
        plan = self.store.read(identity, "plan.json")
        seed = self.base.plan(plan["seed_campaign_id"])
        expected = seed | {
            "schema_version": "strategy-development-plan/2",
            "campaign_id": identity,
            "seed_campaign_id": plan["seed_campaign_id"],
            "limits": LIMITS,
            "progress_policy": POLICY,
        }
        if (
            plan != expected
            or digest(plan) != self.store.read(identity, "plan_seal.json")["sha256"]
        ):
            raise ResearchError("Frozen loop plan, data, owner or policy changed")
        return plan

    def turn_id(self, identity, index):
        return digest({"loop": identity, "turn": index})[:32]

    def history(self, identity):
        """Only complete hash-chained observations are folded; pending actions never replay."""
        plan = self.plan(identity)
        previous, history = digest(plan), []
        for index in range(LIMITS["max_model_calls"] - 1):
            turn = self.turn_id(identity, index)
            directory = self.turns.directory(turn)
            if not directory.exists():
                if any(
                    self.turns.directory(self.turn_id(identity, later)).exists()
                    for later in range(index + 1, LIMITS["max_model_calls"] - 1)
                ):
                    raise ResearchError("Loop consumed-turn ledger has a gap")
                break
            if directory.is_symlink() or any(p.is_symlink() for p in directory.iterdir()):
                raise ResearchError("Loop turn paths changed")
            request = self.turns.read(turn, "request.json")
            if request != {
                "loop": identity,
                "index": index,
                "previous_sha256": previous,
                "plan_sha256": digest(plan),
                "model_slot": index + 2,
            }:
                raise ResearchError("Loop model-slot chain changed")
            if not (directory / "outcome.json").exists():
                reservation = directory / "native_reservation.json"
                slots = (
                    self.turns.read(turn, reservation.name)["slots"] if reservation.exists() else 0
                )
                expected = 2 * sum(p["state"] == "ready" for p in plan["datasets"])
                if type(slots) is not int or slots not in {0, expected}:
                    raise ResearchError("Pending native reservation changed")
                history.append({"turn": turn, "pending": True, "native_reserved": slots})
                break
            outcome = self.turns.read(turn, "outcome.json")
            if digest(outcome) != self.turns.read(turn, "seal.json")["sha256"]:
                raise ResearchError("Loop observation changed")
            decision = (
                self.turns.read(turn, "action.json")
                if (directory / "action.json").exists()
                else None
            )
            if outcome["action_sha256"] != digest(decision):
                raise ResearchError("Loop action changed")
            if decision is not None:
                Action.model_validate(decision)
            reservation = directory / "native_reservation.json"
            slots = self.turns.read(turn, reservation.name)["slots"] if reservation.exists() else 0
            expected = 2 * sum(p["state"] == "ready" for p in plan["datasets"])
            if outcome["native_reserved"] != slots or (
                slots and (not decision or decision["tool"] != "backtest" or slots != expected)
            ):
                raise ResearchError("Loop native reservation changed")
            previous = digest({"request": request, "action": decision, "outcome": outcome})
            history.append(
                {"turn": turn, "action": decision, "outcome": outcome, "chain_sha256": previous}
            )
        return history

    def state(self, identity):
        plan = self.plan(identity)
        definition = Definition.model_validate(plan["definition"])
        state = {
            "current_specification": definition.specification.model_dump(mode="json"),
            "tested": False,
            "evaluated": False,
            "repair_allowed": False,
            "experiments": [],
            "hits": [],
            "observations": [],
            "best_experiment": None,
            "best_specification": None,
            "stop_reason": None,
            "errors": 0,
            "searches": 0,
            "native_reserved": 0,
            "active_seconds": 0.0,
        }
        seen = set()
        for item in self.history(identity):
            if item.get("pending"):
                state["pending"] = item["turn"]
                state["native_reserved"] += item["native_reserved"]
                break
            action, outcome = item["action"], item["outcome"]
            state["active_seconds"] += outcome["elapsed_seconds"]
            state["observations"].append({"action": action, "result": outcome["result"]})
            if action and action["tool"] == "search_sources":
                state["searches"] += 1
            if action and action["tool"] == "backtest" and outcome.get("native_reserved"):
                state["native_reserved"] += outcome["native_reserved"]
            if outcome["result"].get("stop_reason"):
                state["stop_reason"] = outcome["result"]["stop_reason"]
            if outcome["result"].get("error"):
                state["errors"] += 1
                if action and action["tool"] in {"test_program", "backtest"}:
                    state["repair_allowed"] = True
                continue
            if outcome["result"].get("stop_reason"):
                continue
            if not action:
                continue
            result, tool = outcome["result"], action["tool"]
            if tool in {"propose_program", "tune_indicator"}:
                if tool == "tune_indicator":
                    expected = tune_indicator(
                        StrategySpec.model_validate(state["current_specification"]),
                        action["indicator_id"],
                        action["period"],
                    ).model_dump(mode="json")
                    if result["specification"] != expected:
                        raise ResearchError("Retained indicator edit changed")
                proposed = (
                    action["specification"]
                    if tool == "propose_program"
                    else result["specification"]
                )
                spec = StrategySpec.model_validate(proposed)
                frozen_revision(definition.specification, spec)
                if not (state["evaluated"] or state["repair_allowed"]) or fingerprint(spec) in seen:
                    raise ResearchError("Retained proposal bypassed evaluation or duplicate gate")
                state.update(
                    current_specification=proposed,
                    tested=False,
                    evaluated=False,
                    repair_allowed=False,
                )
            elif tool == "test_program":
                state["tested"] = result["passed"]
            elif tool == "search_sources":
                state["hits"] = result["hits"]
            elif tool == "backtest":
                child = self.tools.read_experiment(result["experiment_id"])
                spec = StrategySpec.model_validate(state["current_specification"])
                if not state["tested"] or fingerprint(spec) in seen:
                    raise ResearchError(
                        "Retained native experiment bypassed test or duplicate gate"
                    )
                child_plan = self.tools.experiments.plan(result["experiment_id"])
                if (
                    child_plan["definition"]["specification"] != state["current_specification"]
                    or child_plan["datasets"] != plan["datasets"]
                    or result["experiment_id"] != self.tools.experiment_id(identity, spec)
                ):
                    raise ResearchError(
                        "Loop experiment belongs to another program or frozen scope"
                    )
                best = (
                    self.tools.read_experiment(state["best_experiment"])
                    if state["best_experiment"]
                    else None
                )
                progress = evaluate(best["results"] if best else None, child["results"])
                if result["progress"] != progress:
                    raise ResearchError("Loop continuation decision changed")
                seen.add(fingerprint(spec))
                state["experiments"].append(result["experiment_id"])
                state["evaluated"] = True
                if progress["reason"] == "technical_failure":
                    state["errors"] += 1
                    state["repair_allowed"] = True
                elif progress["continue"]:
                    state["best_experiment"] = result["experiment_id"]
                    state["best_specification"] = state["current_specification"]
                else:
                    state["stop_reason"] = "no_significant_progress"
            elif tool == "finish":
                state["stop_reason"] = "agent_finished"
            if result.get("stop_reason"):
                state["stop_reason"] = result["stop_reason"]
        if state["errors"] >= LIMITS["max_tool_errors"]:
            state["stop_reason"] = "technical_error_budget"
        return state

    def build_report(self, identity, state):
        plan = self.plan(identity)
        programs, rows, starts = [], [], 0
        for index, child_id in enumerate(state["experiments"]):
            child = self.tools.read_experiment(child_id)
            programs.extend(
                p | {"round": index, "experiment_id": child_id} for p in child["programs"]
            )
            rows.extend(r | {"round": index, "experiment_id": child_id} for r in child["results"])
            starts += child["native_starts"]
        report = self.base.build_report(identity, plan, programs, rows, starts)
        # Final choice belongs only to the retained best program, not an unsuccessful challenger.
        best = (
            self.tools.read_experiment(state["best_experiment"])
            if state["best_experiment"]
            else None
        )
        report.update(
            schema_version="strategy-development-report/2",
            stop_reason=state["stop_reason"],
            best_experiment_id=state["best_experiment"],
            selected_cell_id=best["selected_cell_id"] if best else None,
            status="candidate_for_review"
            if best and best["selected_cell_id"]
            else "no_candidate_passed_filters",
            native_reserved=state["native_reserved"],
            model_calls=1 + len(self.history(identity)),
            limits=LIMITS,
            progress_policy=POLICY,
        )
        report["limitations"].append(
            "Persistent tool loop; same-sample progress is not independent validation."
        )
        return report

    def report(self, identity):
        state = self.state(identity)
        history = self.history(identity)
        if not state["stop_reason"]:
            if len(history) >= LIMITS["max_model_calls"] - 1:
                state["stop_reason"] = "model_budget"
            elif state["active_seconds"] >= LIMITS["active_seconds"]:
                state["stop_reason"] = "time_budget"
        report = self.store.read(identity, "report.json")
        if (
            self.store.state(identity) != "completed"
            or report != self.build_report(identity, state)
            or digest(report) != self.store.read(identity, "report_seal.json")["sha256"]
        ):
            raise ResearchError("Persistent loop report or its history changed")
        return report
