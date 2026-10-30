"""Allowlisted specialist tools, backed by existing sealed native experiments."""

import hashlib
import json
from dataclasses import dataclass

from stratosphere.development.compiler import compile_strategy
from stratosphere.development.contracts import Definition, candidate_spec
from stratosphere.development.service import LIMITS, SELECTION, StrategyDevelopment
from stratosphere.development.signals import SignalProgram
from stratosphere.phase0.contracts import fingerprint
from stratosphere.research.store import ResearchError
from stratosphere.sources.review import evidence_passages
from stratosphere.workflow.discovery import digest


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


def test_program(spec, markets):
    """Exercise compiler and signal runtime on reproducible synthetic closed OHLCV bars."""
    codes = []
    for market in markets:
        concrete = candidate_spec(spec, market)
        code = compile_strategy(concrete)
        compile(code, "application-generated-strategy.py", "exec")
        signals = SignalProgram(concrete.model_dump(mode="json"))
        held = False
        for index in range(max(80, concrete.data_requirements.warmup_bars + 20)):
            value = str(100 + (index % 31))
            signal = signals.push(
                dict.fromkeys(("open", "high", "low", "close"), value) | {"volume": "1000"}, held
            )
            if signal is not None:
                held = signal == "BUY"
        codes.append(hashlib.sha256(code).hexdigest())
    return {
        "passed": True,
        "native_backtest_completed": False,
        "next_required_tool": "backtest",
        "code_sha256": codes,
        "meaning": "Structural compilation and synthetic runtime checks; not source conformance",
    }


class ApplicationStop:
    """No inference: force the sealed execution adapter to perform exactly one program."""

    async def stream(self, messages):
        yield json.dumps(
            {
                "action": "stop",
                "reason": "Single-program application tool complete",
                "specification": None,
            }
        )


@dataclass
class Tools:
    base: object

    @property
    def experiments(self):
        service = StrategyDevelopment(
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

    async def backtest(self, identity, plan, spec):
        """Reuse frozen data. Child plans cannot acquire data, request revisions or trade."""
        executor = self.experiments
        child = self.experiment_id(identity, spec)
        if executor.store.directory(child).exists():
            # A completed child may have committed before its parent's tool receipt.
            if executor.store.state(child) == "completed":
                return executor.report(child)
            raise ResearchError("Unresolved native experiment; consumed slots cannot replay")
        baseline = Definition.model_validate(plan["definition"])
        definition = baseline.model_copy(update={"specification": spec})
        record = self.base.database.register(
            spec,
            definition.name,
            {"kind": "persistent-loop-program/2", "campaign": identity},
            parent=plan["owner"],
            comparison=True,
        )
        child_plan = {
            "schema_version": "strategy-development-plan/1",
            "campaign_id": child,
            "owner": record["public_id"],
            "intake_id": plan["intake_id"],
            "intake_sha256": plan["intake_sha256"],
            "definition": definition.model_dump(mode="json"),
            "focus": plan["focus"],
            "model_ref": plan["model_ref"],
            "implementation_sha256": plan["implementation_sha256"],
            "datasets": plan["datasets"],
            "limits": LIMITS,
            "selection": SELECTION,
        }
        executor.store.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        executor.store.directory(child).mkdir(mode=0o700)
        executor.store.write(child, "plan.json", child_plan)
        executor.store.write(child, "plan_seal.json", {"sha256": digest(child_plan)})
        executor.store.event(child, "policy_ready")
        return await executor.run(child, ApplicationStop(), owner=record["public_id"])

    async def source(self, index, hits):
        if index >= len(hits):
            raise ResearchError("Source index is outside the retained search results")
        snapshot = await self.base.discovery.acquire_url(hits[index]["url"])
        return {"source_id": snapshot.id, "passages": evidence_passages(snapshot)[:8]}

    def read_experiment(self, identity):
        return self.experiments.report(identity)
