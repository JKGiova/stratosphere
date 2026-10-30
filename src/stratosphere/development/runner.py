"""Persistent tool loop with immutable turn receipts and application-owned continuation."""

import asyncio
import time

from stratosphere.development.agent import Action, decide
from stratosphere.development.contracts import Definition, implementation_digest
from stratosphere.development.progress import POLICY, evaluate
from stratosphere.development.service import measured_context, report_markdown
from stratosphere.development.tools import Tools, frozen_revision, test_program
from stratosphere.models.streaming import ModelError
from stratosphere.phase0.contracts import StrategySpec, fingerprint
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.workflow.discovery import digest

# Resource ceilings, not a fixed number of refinement iterations. V1 remains immutable.
LIMITS = {
    "max_model_calls": 32,
    "max_native_starts": 128,
    "max_markets": 4,
    "max_searches": 4,
    "max_tool_errors": 3,
    "active_seconds": 3600,
}


class ContinuousRunner:
    def __init__(self, base):
        self.base = base
        self.store = base.store
        self.turns = ResearchStore(base.store.root / "loop-turns")
        self.tools = Tools(base)

    async def prepare(self, intake, provider, model_ref, *, focus="", parent=None):
        seed = await self.base.prepare(intake, provider, model_ref, focus=focus, parent=parent)
        original = self.base.plan(seed)
        identity = digest(
            {
                "seed": seed,
                "protocol": "persistent-strategy-loop/2",
                "limits": LIMITS,
                "progress": POLICY,
            }
        )[:32]
        if self.store.directory(identity).exists():
            self.plan(identity)
            return identity
        plan = original | {
            "schema_version": "strategy-development-plan/2",
            "campaign_id": identity,
            "seed_campaign_id": seed,
            "limits": LIMITS,
            "progress_policy": POLICY,
        }
        self.store.directory(identity).mkdir(mode=0o700)
        self.store.write(identity, "plan.json", plan)
        self.store.write(identity, "plan_seal.json", {"sha256": digest(plan)})
        self.store.write(identity, "loop.json", {"schema_version": "persistent-strategy-loop/2"})
        self.store.event(identity, "policy_ready")
        return identity

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
                history.append({"turn": turn, "pending": True})
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
            if tool == "propose_program":
                spec = StrategySpec.model_validate(action["specification"])
                frozen_revision(definition.specification, spec)
                if not (state["evaluated"] or state["repair_allowed"]) or fingerprint(spec) in seen:
                    raise ResearchError("Retained proposal bypassed evaluation or duplicate gate")
                state.update(
                    current_specification=action["specification"],
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

    def available_tools(self, state):
        allowed = ["inspect_brief", "compare_results"]
        if state["searches"] < LIMITS["max_searches"]:
            allowed.append("search_sources")
        if state["hits"]:
            allowed.append("read_source")
        if not state["evaluated"]:
            allowed.append("backtest" if state["tested"] else "test_program")
        if state["evaluated"] or state["repair_allowed"]:
            allowed.extend(("propose_program", "finish"))
        return allowed

    def context(self, identity, state):
        plan = self.plan(identity)
        return {
            "strategy_id": plan["owner"],
            "available_tools": self.available_tools(state),
            "focus": plan["focus"],
            "definition": plan["definition"],
            "progress_policy": POLICY,
            "current_specification": state["current_specification"],
            "tested": state["tested"],
            "evaluated": state["evaluated"],
            "repair_allowed": state["repair_allowed"],
            "best_specification": state["best_specification"],
            "observations": state["observations"][-8:],
            "remaining_model_calls": LIMITS["max_model_calls"] - 1 - len(self.history(identity)),
            "remaining_native_starts": LIMITS["max_native_starts"] - state["native_reserved"],
        }

    async def execute(self, identity, action, state):
        plan = self.plan(identity)
        spec = StrategySpec.model_validate(state["current_specification"])
        definition = Definition.model_validate(plan["definition"])
        tool = action.tool
        if tool not in self.available_tools(state):
            raise ResearchError("Tool is unavailable in the current application state")
        if tool == "inspect_brief":
            return {"brief": self.base.discovery.open(plan["intake_id"])}
        if tool == "search_sources":
            if state["searches"] >= LIMITS["max_searches"]:
                raise ResearchError("Public-source search budget exhausted")
            hits, attempts = await self.base.discovery.web.search(action.query)
            return {"hits": hits, "attempts": attempts}
        if tool == "read_source":
            return await self.tools.source(action.source_index, state["hits"])
        if tool == "propose_program":
            if not (state["evaluated"] or state["repair_allowed"]):
                raise ResearchError("Evaluate the current program before proposing another")
            frozen_revision(definition.specification, action.specification)
            attempted = [
                self.tools.experiments.plan(e)["definition"]["specification"]
                for e in state["experiments"]
            ]
            if action.specification.model_dump(mode="json") in attempted:
                return {
                    "stop_reason": "duplicate_program",
                    "meaning": "No native experiment repeated",
                }
            return {"accepted": True, "hypothesis": action.reason}
        if tool == "test_program":
            if state["evaluated"]:
                raise ResearchError("Propose a new program before testing again")
            return test_program(spec, definition.markets)
        if tool == "backtest":
            if not state["tested"] or state["evaluated"]:
                raise ResearchError("A new tested program is required; no native replay")
            slots = 2 * sum(pin["state"] == "ready" for pin in plan["datasets"])
            if state["native_reserved"] + slots > LIMITS["max_native_starts"]:
                return {"stop_reason": "native_budget"}
            # Persist the full possible reservation before any native start.
            turn = state["active_turn"]
            self.turns.write(turn, "native_reservation.json", {"slots": slots})
            child = await self.tools.backtest(identity, plan, spec)
            best = (
                self.tools.read_experiment(state["best_experiment"])
                if state["best_experiment"]
                else None
            )
            return {
                "experiment_id": child["campaign_id"],
                "progress": evaluate(best["results"] if best else None, child["results"]),
                "measured_results": measured_context(child["results"]),
            }
        if tool == "compare_results":
            return {
                "experiments": [
                    {"id": e, "results": measured_context(self.tools.read_experiment(e)["results"])}
                    for e in state["experiments"]
                ]
            }
        return {"stop_reason": "agent_finished"}

    def commit_turn(self, turn, result, elapsed):
        directory = self.turns.directory(turn)
        decision = (
            self.turns.read(turn, "action.json") if (directory / "action.json").exists() else None
        )
        reservation = (
            self.turns.read(turn, "native_reservation.json")["slots"]
            if (directory / "native_reservation.json").exists()
            else 0
        )
        outcome = {
            "action_sha256": digest(decision),
            "result": result,
            "elapsed_seconds": elapsed,
            "native_reserved": reservation,
        }
        self.base.clean(outcome)
        self.turns.write(turn, "outcome.json", outcome)
        self.turns.write(turn, "seal.json", {"sha256": digest(outcome)})

    async def reconcile(self, identity, state):
        turn = state["pending"]
        folder = self.turns.directory(turn)
        action = self.turns.read(turn, "action.json") if (folder / "action.json").exists() else None
        if action and action["tool"] == "backtest":
            spec = StrategySpec.model_validate(state["current_specification"])
            child = self.tools.experiment_id(identity, spec)
            if (
                self.tools.experiments.store.directory(child).exists()
                and self.tools.experiments.store.state(child) == "completed"
            ):
                report = self.tools.read_experiment(child)
                best = (
                    self.tools.read_experiment(state["best_experiment"])
                    if state["best_experiment"]
                    else None
                )
                result = {
                    "experiment_id": child,
                    "progress": evaluate(best["results"] if best else None, report["results"]),
                    "measured_results": measured_context(report["results"]),
                }
            else:
                result = {
                    "error": "Interrupted native attempt cannot replay",
                    "stop_reason": "unresolved_native_attempt",
                }
        else:
            result = {"error": "Interrupted turn consumed; no automatic tool replay"}
        self.commit_turn(turn, result, 180.0)

    async def run(self, identity, provider, *, owner):
        with self.store.lock(identity):
            plan = self.plan(identity)
            if owner != plan["owner"] or plan["implementation_sha256"] != implementation_digest():
                raise ResearchError("Exact owner and unchanged runtime required to resume the loop")
            if self.store.state(identity) == "completed":
                return self.report(identity)
            await self.base.online_check()
            self.store.event(identity, "running")
            try:
                state = self.state(identity)
                if state.get("pending"):
                    await self.reconcile(identity, state)
                while True:
                    state = self.state(identity)
                    history = self.history(identity)
                    if state["stop_reason"]:
                        break
                    if len(history) >= LIMITS["max_model_calls"] - 1:
                        state["stop_reason"] = "model_budget"
                        break
                    remaining = LIMITS["active_seconds"] - state["active_seconds"]
                    if remaining <= 0:
                        state["stop_reason"] = "time_budget"
                        break
                    index = len(history)
                    turn = self.turn_id(identity, index)
                    self.turns.root.mkdir(mode=0o700, parents=True, exist_ok=True)
                    self.turns.directory(turn).mkdir(mode=0o700)
                    self.turns.write(
                        turn,
                        "request.json",
                        {
                            "loop": identity,
                            "index": index,
                            "previous_sha256": history[-1]["chain_sha256"]
                            if history
                            else digest(plan),
                            "plan_sha256": digest(plan),
                            "model_slot": index + 2,
                        },
                    )
                    started = time.monotonic()
                    try:
                        async with asyncio.timeout(min(remaining, 180)):
                            action = await decide(provider, self.context(identity, state))
                            self.base.clean(action.model_dump(mode="json"))
                            self.turns.write(turn, "action.json", action.model_dump(mode="json"))
                            state["active_turn"] = turn
                            result = await self.execute(identity, action, state)
                    except ResearchError, ModelError, ValueError, ArithmeticError, TimeoutError:
                        result = {
                            "error": (
                                "Typed tool/model operation failed; "
                                "inspect retained action; no replay"
                            )
                        }
                    self.commit_turn(turn, result, time.monotonic() - started)
                report = self.build_report(identity, state)
                self.store.write(identity, "report.json", report)
                self.store.write(identity, "report.md", report_markdown(report))
                self.store.write(identity, "report_seal.json", {"sha256": digest(report)})
                self.store.event(identity, "completed")
                return self.report(identity)
            except asyncio.CancelledError:
                self.store.event(
                    identity, "paused", "Checkpoint retained; reconcile before continuation"
                )
                raise
            except Exception:
                self.store.event(
                    identity, "paused", "Inspect retained checkpoint; no native replay"
                )
                raise

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
