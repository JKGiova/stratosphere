"""Persistent agent choices, frozen progress gates, reservations and restart reconciliation."""

import asyncio
import json

import pytest
from test_strategy_development import database as database
from test_strategy_development import (
    definition,
    runner_factory,
    specification,
)
from test_strategy_development import intake as intake
from test_strategy_intake import Replies

from stratosphere.development.agent import Action
from stratosphere.development.progress import evaluate
from stratosphere.development.service import StrategyDevelopment
from stratosphere.research.store import ResearchError


def action(tool, **kwargs):
    return Action(tool=tool, reason="Authored tool decision", **kwargs).model_dump(mode="json")


def rows(score, trades=8, state="completed"):
    return [
        {
            "state": state,
            "metrics": {
                "stress": {
                    "net_return_pct": str(score + 2),
                    "max_drawdown_pct": "2",
                    "completed_trades": trades,
                }
            },
        }
    ]


def test_progress_is_application_owned_and_requires_material_robust_gain():
    assert evaluate(None, rows(1))["reason"] == "baseline"
    assert evaluate(rows(1), rows(1.09))["continue"] is False
    assert evaluate(rows(1), rows(1.11))["continue"] is True
    assert evaluate(rows(10), rows(10.4))["continue"] is False
    assert evaluate(rows(1), rows(2, trades=1))["continue"] is False
    assert evaluate(rows(1), rows(2, state="failed"))["reason"] == "technical_failure"
    assert evaluate(rows(1) + rows(2), rows(1.3) + rows(1.8))["continue"] is False


@pytest.fixture
async def loop(tmp_path, intake, database):
    discovery, brief = intake
    calls = []
    service = StrategyDevelopment(
        tmp_path / "development", discovery, database, runner=runner_factory(calls)
    )
    identity = await service.prepare(
        brief["id"], Replies(definition().model_dump(mode="json")), "fixture", continuous=True
    )
    return service, identity, calls


async def run(service, identity, *actions):
    return await service.run(identity, Replies(*actions), owner=service.plan(identity)["owner"])


async def test_dynamic_tools_stop_on_first_insufficient_revision_and_reuse(loop):
    service, identity, calls = loop
    report = await run(
        service,
        identity,
        action("inspect_brief"),
        action("test_program"),
        action("backtest"),
        action("compare_results"),
        action("propose_program", specification=specification(fast=4)),
        action("test_program"),
        action("backtest"),
    )
    assert report["stop_reason"] == "no_significant_progress"
    assert len(report["programs"]) == 2
    assert report["best_experiment_id"] == report["programs"][0]["experiment_id"]
    assert report["native_starts"] == report["native_reserved"] == 4
    assert report["model_calls"] == 8
    assert service.report(identity) == report
    before = len(calls)
    assert await run(service, identity) == report
    assert len(calls) == before


async def test_pending_inference_resumes_without_resetting_model_slots(loop):
    service, identity, calls = loop
    entered = asyncio.Event()

    class Waiting:
        async def stream(self, messages):
            entered.set()
            await asyncio.Event().wait()
            yield ""

    task = asyncio.create_task(
        service.run(identity, Waiting(), owner=service.plan(identity)["owner"])
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert service.store.state(identity) == "paused"
    report = await run(
        service, identity, action("test_program"), action("backtest"), action("finish")
    )
    assert report["model_calls"] == 5  # initial definition + consumed lost turn + three decisions
    assert report["native_starts"] == 2
    assert len(calls) == 3


async def test_completed_native_child_reconciles_without_replay(loop, monkeypatch):
    service, identity, calls = loop
    from stratosphere.development.runner import ContinuousRunner

    original = ContinuousRunner.commit_turn
    interrupted = False

    def crash(self, turn, result, elapsed):
        nonlocal interrupted
        if "experiment_id" in result and not interrupted:
            interrupted = True
            raise asyncio.CancelledError
        original(self, turn, result, elapsed)

    monkeypatch.setattr(ContinuousRunner, "commit_turn", crash)
    with pytest.raises(asyncio.CancelledError):
        await run(service, identity, action("test_program"), action("backtest"))
    assert len(calls) == 3
    report = await run(service, identity, action("finish"))
    assert report["native_reserved"] == report["native_starts"] == 2
    assert len(calls) == 3


async def test_unresolved_native_attempt_is_never_resubmitted(loop, monkeypatch):
    service, identity, calls = loop
    from stratosphere.development.tools import Tools

    async def interrupted(*args):
        raise asyncio.CancelledError

    monkeypatch.setattr(Tools, "backtest", interrupted)
    with pytest.raises(asyncio.CancelledError):
        await run(service, identity, action("test_program"), action("backtest"))
    report = await run(service, identity)
    assert report["stop_reason"] == "unresolved_native_attempt"
    assert report["native_reserved"] == 2 and report["native_starts"] == 0
    assert len(calls) == 1


async def test_frozen_execution_changes_and_early_backtests_are_tool_errors(loop):
    service, identity, calls = loop
    report = await run(
        service, identity, action("backtest"), action("backtest"), action("backtest")
    )
    assert report["stop_reason"] == "technical_error_budget"
    assert report["native_starts"] == 0 and len(calls) == 1


async def test_checkpoint_tampering_is_rejected(loop):
    service, identity, calls = loop
    await run(service, identity, action("test_program"), action("backtest"), action("finish"))
    runner = service.continuous
    turn = runner.turn_id(identity, 0)
    path = runner.turns.directory(turn) / "action.json"
    value = json.loads(path.read_text())
    value["reason"] = "tampered"
    path.write_text(json.dumps(value))
    with pytest.raises(ResearchError, match="action changed"):
        service.report(identity)


async def test_more_than_two_revisions_when_each_makes_progress(loop, monkeypatch):
    service, identity, calls = loop
    from stratosphere.development import runner

    monkeypatch.setattr(
        runner,
        "evaluate",
        lambda best, candidate: {
            "continue": True,
            "reason": "baseline" if best is None else "significant_progress",
            "measurement": {"score": "1"},
        },
    )
    actions = [action("test_program"), action("backtest")]
    for fast in (4, 5, 6):
        actions.extend(
            [
                action("propose_program", specification=specification(fast=fast)),
                action("test_program"),
                action("backtest"),
            ]
        )
    report = await run(service, identity, *actions, action("finish"))
    assert len(report["programs"]) == 4 and report["native_starts"] == 8
    assert report["best_experiment_id"] == report["programs"][-1]["experiment_id"]


async def test_repeated_inspection_cannot_refill_the_model_budget(loop):
    service, identity, calls = loop
    from stratosphere.development.runner import LIMITS

    report = await run(
        service, identity, *[action("inspect_brief") for _ in range(LIMITS["max_model_calls"] - 1)]
    )
    assert report["stop_reason"] == "model_budget"
    assert report["model_calls"] == LIMITS["max_model_calls"]
    assert report["native_starts"] == 0 and len(calls) == 1


async def test_identical_program_stops_without_another_backtest(loop):
    service, identity, calls = loop
    report = await run(
        service,
        identity,
        action("test_program"),
        action("backtest"),
        action("propose_program", specification=specification()),
    )
    assert report["stop_reason"] == "duplicate_program"
    assert report["native_starts"] == 2 and len(calls) == 3


async def test_technical_test_failure_allows_a_bounded_repair(loop, monkeypatch):
    service, identity, calls = loop
    from stratosphere.development import runner

    original = runner.test_program
    attempts = 0

    def fail_once(*args):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("Authored synthetic runtime failure")
        return original(*args)

    monkeypatch.setattr(runner, "test_program", fail_once)
    report = await run(
        service,
        identity,
        action("test_program"),
        action("propose_program", specification=specification(fast=4)),
        action("test_program"),
        action("backtest"),
        action("finish"),
    )
    assert report["stop_reason"] == "agent_finished" and report["native_starts"] == 2
    assert attempts == 2 and report["model_calls"] == 6


async def test_frozen_sizing_cannot_change_inside_a_revision(loop):
    service, identity, calls = loop
    changed = specification().model_dump(mode="json")
    changed["position_sizing"]["amount"] = "2"
    proposal = action("propose_program", specification=changed)
    report = await run(
        service, identity, action("test_program"), action("backtest"), proposal, proposal, proposal
    )
    assert report["stop_reason"] == "technical_error_budget"
    assert len(report["programs"]) == 1 and report["native_starts"] == 2


async def test_reservation_tampering_is_rejected(loop):
    service, identity, calls = loop
    await run(service, identity, action("test_program"), action("backtest"), action("finish"))
    runner = service.continuous
    path = runner.turns.directory(runner.turn_id(identity, 1)) / "native_reservation.json"
    path.write_text('{"slots": 0}')
    with pytest.raises(ResearchError, match="reservation changed"):
        service.report(identity)


async def test_model_cannot_finish_on_a_structural_check_only(loop):
    service, identity, calls = loop
    runner = service.continuous
    assert "finish" not in runner.available_tools(runner.state(identity))
    report = await run(
        service,
        identity,
        action("finish"),
        action("test_program"),
        action("backtest"),
        action("finish"),
    )
    assert report["native_starts"] == 2 and report["model_calls"] == 5
    assert report["stop_reason"] == "agent_finished"


async def test_provider_schema_exposes_only_currently_available_tools():
    from stratosphere.development.agent import decide
    from stratosphere.models.streaming import ModelError

    schemas = []

    class InvalidChoice:
        async def stream_json(self, messages, schema):
            schemas.append(schema)
            yield json.dumps(action("finish"))

    with pytest.raises(ModelError):
        await decide(InvalidChoice(), {"available_tools": ["test_program"]})
    assert [v["properties"]["tool"]["const"] for v in schemas[0]["anyOf"]] == ["test_program"]


async def test_tool_schemas_require_arguments_and_freeze_non_signal_behavior():
    from stratosphere.development.agent import decide

    schemas = []

    class Search:
        async def stream_json(self, messages, schema):
            schemas.append(schema)
            yield json.dumps(action("search_sources", query="Wilder RSI research"))

    baseline = specification().model_dump(mode="json")
    await decide(
        Search(),
        {
            "available_tools": ["search_sources", "propose_program"],
            "definition": {"specification": baseline},
        },
    )
    search, proposal = schemas[0]["anyOf"]
    assert "query" in search["required"]
    assert search["properties"]["query"]["type"] == "string"
    assert "specification" in proposal["required"]
    program = schemas[0]["$defs"]["StrategySpec"]["properties"]
    assert program["position_sizing"] == {"const": baseline["position_sizing"]}
    assert program["assumptions"] == {"const": baseline["assumptions"]}
    assert program["data_requirements"]["properties"]["warmup_bars"]["maximum"] == 1024


async def test_complete_research_tool_surface_and_deterministic_tuning(loop):
    service, identity, calls = loop
    indicator = specification().indicators[0].id
    report = await run(
        service,
        identity,
        action("research_capabilities"),
        action("inspect_program"),
        action("inspect_data"),
        action("test_program"),
        action("backtest"),
        action("analyze_results"),
        action("analyze_conditions"),
        action("build_report"),
        action("tune_indicator", indicator_id=indicator, period=4),
        action("inspect_program"),
        action("test_program"),
        action("backtest"),
    )
    assert len(report["programs"]) == 2 and report["native_starts"] == 4
    assert report["stop_reason"] == "no_significant_progress"
    assert service.report(identity) == report
    history = service.continuous.history(identity)
    observations = {h["action"]["tool"]: h["outcome"]["result"] for h in history}
    assert observations["inspect_data"]["targets"][0]["bars"] > 8
    assert observations["build_report"]["execution"] == "blocked"
    assert "trades" not in observations["analyze_results"]["results"][0]["metrics"]["base"]
    assert observations["tune_indicator"]["specification"]["indicators"][0]["period"] == 4
    assert "application_generated_python" in observations["inspect_program"]
    assert len(calls) == 5


async def test_tune_requires_evaluation_and_existing_indicator(loop):
    service, identity, calls = loop
    invalid = action("tune_indicator", indicator_id="unknown", period=4)
    report = await run(
        service, identity, invalid, action("test_program"), action("backtest"), invalid, invalid
    )
    assert report["stop_reason"] == "technical_error_budget"
    assert len(report["programs"]) == 1 and report["native_starts"] == 2


def test_indicator_tuning_preserves_scope_and_expands_warmup():
    from stratosphere.development.research_tools import tune_indicator

    original = specification()
    revised = tune_indicator(original, original.indicators[0].id, 50)
    assert revised.data_requirements.warmup_bars == 51
    assert revised.position_sizing == original.position_sizing
    assert revised.instrument_constraints == original.instrument_constraints
    assert original.indicators[0].period == 3


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tool": "tune_indicator", "indicator_id": "fast"},
        {"tool": "tune_indicator", "period": 3},
        {"tool": "inspect_program", "indicator_id": "fast"},
        {"tool": "tune_indicator", "indicator_id": "fast", "period": True},
        {"tool": "tune_indicator", "indicator_id": "fast", "period": 520},
    ],
)
def test_tool_argument_boundaries(kwargs):
    with pytest.raises(ValueError):
        Action(reason="Authored malformed arguments", **kwargs)


async def test_tuning_schema_requires_both_arguments():
    from stratosphere.development.agent import decide

    captured = []

    class Tuner:
        async def stream_json(self, messages, schema):
            captured.append(schema)
            yield json.dumps(action("tune_indicator", indicator_id="fast", period=4))

    await decide(Tuner(), {"available_tools": ["tune_indicator"]})
    schema = captured[0]["anyOf"][0]
    assert set(schema["required"]) == {"tool", "reason", "indicator_id", "period"}


async def test_specialist_source_tools_use_audited_fallback_without_changing_frozen_rules(loop):
    import httpx

    from stratosphere.sources.fallback import ResearchWeb

    service, identity, calls = loop
    sources = service.discovery.sources

    def response(request):
        if request.url.host == "en.wikipedia.org":
            return httpx.Response(200, json={"query": {"search": []}})
        if request.url.host == "api.crossref.org":
            return httpx.Response(200, json={"message": {"items": []}})
        return httpx.Response(
            200, text="<script>authored empty app</script>", headers={"content-type": "text/html"}
        )

    sources.transport = httpx.MockTransport(response)

    async def public(host):
        return ["93.184.215.14"]

    class PublicReader:
        async def call(self, name, operation, value):
            assert name == "agent-reach"
            if operation == "search":
                return {
                    "ok": True,
                    "items": [{"title": "Authored paper", "url": "https://example.com/strategy"}],
                }
            assert value == "https://example.com/strategy"
            return {
                "ok": True,
                "items": [
                    {
                        "text": "Authored public explanation of a crossover. "
                        "This is research evidence, never executable code."
                    }
                ],
            }

    service.discovery.web = ResearchWeb(sources, clis=PublicReader(), resolver=public)
    report = await run(
        service,
        identity,
        action("search_sources", query="moving average research"),
        action("read_source", source_index=0),
        action("test_program"),
        action("backtest"),
        action("finish"),
    )
    assert report["stop_reason"] == "agent_finished" and report["native_starts"] == 2
    history = service.continuous.history(identity)
    source = sources.store.snapshot(history[1]["outcome"]["result"]["source_id"])
    assert source.access_state == "acquired" and source.normalization == "research-fallback/1"
    assert "Authored public explanation" in source.pieces[0].text
    state = service.continuous.state(identity)
    assert state["searches"] == 1
    context = service.continuous.context(identity, state)
    original_attempts = history[0]["outcome"]["result"]["attempts"]
    assert "records" in original_attempts[-1]
    assert "records" not in context["observations"][0]["result"]["attempts"][-1]
    assert state["current_specification"] == service.plan(identity)["definition"]["specification"]
