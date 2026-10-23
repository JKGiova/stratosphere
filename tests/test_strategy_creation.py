"""Idea provenance, stable ownership and human approval boundaries."""

import asyncio
import json

import pytest
from test_research_tui import ui_service as ui_service

from stratosphere.agents.creation import PROMPT, creation_request
from stratosphere.agents.orchestrator import Orchestrator
from stratosphere.models.streaming import ModelError
from stratosphere.registry.workspace import WorkspaceDatabase
from stratosphere.research.service import ResearchService
from stratosphere.research.store import ResearchError, ResearchStore


class Provider:
    def __init__(self):
        self.proposal = {
            "status": "ready",
            "name": "Hourly crossover",
            "hypothesis": "Costs may erase returns.",
            "parameters": {"fast_period": 3, "slow_period": 8, "quantity": "0.01"},
            "questions": [],
        }
        self.requests = []
        self.fail_analysis = False
        self.wait = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def stream(self, messages):
        self.requests.append(messages)
        if messages[0]["content"] == PROMPT:
            self.entered.set()
            if self.wait:
                await self.release.wait()
            yield json.dumps(self.proposal)
        else:
            if self.fail_analysis:
                raise ModelError("Artificial failure")
            yield "Untested. Compare turnover and costs before drawing conclusions."


@pytest.fixture
def app(tmp_path):
    async def online():
        pass

    provider = Provider()
    database = WorkspaceDatabase(tmp_path / "workspace.sqlite3")
    research = ResearchService(ResearchStore(tmp_path / "jobs"), online_check=online)
    return Orchestrator(provider, database, research, "cheaperinference/qwen3.7-flash")


async def respond(app, text="/create an hourly SMA candidate"):
    return "".join([chunk async for chunk in app.respond(text)])


async def test_creation_agent_handoff_duplicate_restart_and_approval(app):
    reply = await respond(app)
    assert "No backtest has started" in reply
    identity = app.active_strategy
    first = app.database.strategy(identity)
    job = app.last_action["job_id"]
    assert app.research.store.state(job) == "awaiting_approval"
    origin = app.research.store.read(job, "source.json")
    assert origin["kind"] == "orchestrator_idea/1"
    assert origin["interpretation_status"] == "unverified_model_proposal"
    assert "not an acquired source" in origin["text"]
    assert len(app.database.history(identity)) == 2
    assert not (app.research.store.directory(job) / "approval.json").exists()
    with pytest.raises(ResearchError):
        await app.research.run(job)
    await respond(app)
    assert len(app.database.catalog()) == 1
    assert app.database.strategy(identity)["agent_id"] == first["agent_id"]
    assert len(app.database.jobs(identity)) == 2
    assert app.last_action["job_id"] != job
    replay = Orchestrator(app.provider, app.database, app.research)
    replay.sync_research()
    assert len(replay.agent(identity).session.history) == 4
    plan = app.research.store.plan(job)
    with pytest.raises(ResearchError):
        app.research.approve(job, "0" * 64)
    app.research.approve(job, plan.digest)
    assert app.research.store.state(job) == "paused"


async def test_distinct_parameters_get_isolated_agent(app):
    await respond(app)
    original = app.active_strategy
    app.provider.proposal["parameters"]["fast_period"] = 4
    await respond(app)
    assert app.active_strategy != original
    assert len(app.database.history(original)) == 2
    assert len(app.database.history(app.active_strategy)) == 2
    assert not app.general.history


@pytest.mark.parametrize(
    "brief",
    ["https://example.com/code", "def strategy(): pass", "quantity 0.02 BTC", "fast 4 slow 8"],
)
async def test_source_conversion_or_changed_explicit_parameters_blocked(app, brief):
    with pytest.raises(ResearchError):
        await respond(app, "/create " + brief)
    assert not app.database.catalog()
    assert not app.research.store.jobs()


@pytest.mark.parametrize("status", ["unsupported", "needs_clarification"])
async def test_unresolved_proposal_cannot_register_or_run(app, status):
    app.provider.proposal.update(status=status, parameters=None, questions=["Choose SMA rules."])
    assert "Choose SMA rules" in await respond(app)
    assert not app.database.catalog()
    assert app.last_action is None


async def test_failed_analysis_keeps_review_without_partial_history(app):
    app.provider.fail_analysis = True
    assert "analysis failed" in await respond(app)
    assert app.last_action["action"] == "review"
    assert app.research.store.state(app.last_action["job_id"]) == "awaiting_approval"
    assert not app.database.history(app.active_strategy)


async def test_canceled_proposal_retains_attempt_without_registration(app):
    app.provider.wait = True
    task = asyncio.create_task(respond(app))
    await asyncio.wait_for(app.provider.entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not app.busy and not app.research.preparing
    assert not app.database.catalog()
    attempts = app.creator.store.jobs()
    assert len(attempts) == 1 and app.creator.store.state(attempts[0]) == "canceled"


async def test_private_key_shaped_input_rejected_before_model_or_disk(app):
    artificial = "ci_live_" + "z" * 40
    with pytest.raises(ResearchError):
        await respond(app, "/create " + artificial)
    assert not app.provider.requests
    assert not app.creator.store.root.exists()


def test_explicit_creation_detection():
    assert creation_request("Create a new strategy for BTC")
    assert creation_request("What does create a strategy mean?") is None


async def test_invalid_or_secret_model_output_cannot_register(app):
    app.provider.proposal["sql"] = "DROP TABLE strategies"
    with pytest.raises(ModelError):
        await respond(app)
    assert not app.database.catalog()
    del app.provider.proposal["sql"]
    app.provider.proposal["hypothesis"] = "ci_live_" + "z" * 40
    with pytest.raises(ResearchError):
        await respond(app)
    assert not app.database.catalog()
    assert not list(app.creator.store.root.glob("*/proposal.json"))


async def test_navigation_during_proposal_does_not_change_handoff_owner(app):
    await respond(app)
    old = app.active_strategy
    app.provider.proposal["parameters"]["fast_period"] = 4
    app.provider.wait = True
    app.provider.entered.clear()
    task = asyncio.create_task(respond(app))
    await asyncio.wait_for(app.provider.entered.wait(), 2)
    app.select(None)
    app.provider.release.set()
    await task
    assert app.active_strategy is None
    assert app.responding_strategy != old
    assert len(app.database.history(old)) == 2


async def test_registration_transaction_rollback_and_exact_replay(app, monkeypatch):
    original = app.database._register

    def fail(*args, **kwargs):
        original(*args, **kwargs)
        raise ResearchError("Artificial database failure")

    monkeypatch.setattr(app.database, "_register", fail)
    with pytest.raises(ResearchError):
        await respond(app)
    assert not app.database.catalog()
    monkeypatch.setattr(app.database, "_register", original)
    # A complete retained plan can be recovered, but never executed automatically.
    app.sync_research()
    identity = app.database.catalog()[0]["public_id"]
    job = app.database.jobs(identity)[0]["job_id"]
    plan = app.research.store.plan(job)
    source = app.research.store.read(job, "source.json")
    record = app.database.register_created_job(
        plan.specification, plan.draft.name, source, job, plan.digest
    )
    assert record["resolution"] == "retained_creation"
    assert len(app.database.jobs(identity)) == 1
    assert app.research.store.state(job) == "awaiting_approval"


async def test_tui_create_shows_review_and_no_runner_calls(ui_service):
    from test_research_tui import connect, send

    from stratosphere.research.widgets import ResearchReview
    from stratosphere.tui import StratosphereApp

    provider = Provider()
    tui = StratosphereApp(provider_factory=lambda settings: provider, research_service=ui_service)
    async with tui.run_test(size=(144, 40)) as pilot:
        await connect(tui, pilot)
        await send(tui, pilot, "/create an hourly SMA candidate")
        await tui.workers.wait_for_complete()
        await pilot.pause()
        assert tui.query(ResearchReview)
        assert ui_service.calls == []
        job = tui.session.last_action["job_id"]
        assert ui_service.store.state(job) == "awaiting_approval"


@pytest.mark.engine
async def test_created_candidate_approved_native_runs_match_independent_oracle(
    app, research_snapshot
):
    from decimal import Decimal

    from test_research_engine import oracle

    from stratosphere.research.worker import execute

    starts = []

    async def fetch():
        return research_snapshot

    async def runner(store, job, multiplier):
        starts.append(multiplier)
        return execute(store.read(job, "request.json"), multiplier)

    app.research.fetcher, app.research.runner = fetch, runner
    await respond(app)
    assert starts == []
    job = app.last_action["job_id"]
    plan = app.research.store.plan(job)
    app.research.approve(job, plan.digest)
    await app.agent().run_job(job)
    assert starts == [1, 2]
    for name, multiplier in (("base", 1), ("stress", 2)):
        result = app.research.store.read(job, name + ".json")["result"]
        signals, fills, curve, _, _ = oracle(plan, research_snapshot.bars, multiplier)
        assert result["signals"] == signals
        assert len(result["fills"]) == len(fills) > 0
        for actual, expected in zip(result["fills"], fills, strict=True):
            assert actual["side"] == expected["side"]
            assert Decimal(actual["price"]) == Decimal(expected["price"])
            assert Decimal(actual["commission"]) == Decimal(expected["commission"])
        assert [Decimal(row["equity"]) for row in result["equity_curve"]] == curve
    assert app.database.reports(app.active_strategy)
    with pytest.raises(ResearchError):
        await app.agent().run_job(job)
    assert starts == [1, 2]


@pytest.mark.parametrize(
    "brief",
    ["a variant of my SMA strategy", "a derived strategy", "a strategy based on my strategy"],
)
async def test_variants_require_explicit_lineage_workflow(app, brief):
    with pytest.raises(ResearchError, match="/study"):
        await respond(app, "/create " + brief)
    assert not app.provider.requests
    assert not app.creator.store.root.exists()
