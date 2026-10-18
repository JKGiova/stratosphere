"""Stable agent identity, exact registration, isolation, operation gates and restart evidence."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest

from stratosphere.agents.orchestrator import ROUTER_PROMPT, Orchestrator
from stratosphere.models.streaming import ModelError
from stratosphere.registry.workspace import RegistryError, WorkspaceDatabase
from stratosphere.research.contracts import ResearchConfig, build_spec
from stratosphere.research.service import ResearchService
from stratosphere.research.store import ResearchStore


@pytest.fixture
def database(tmp_path):
    return WorkspaceDatabase(tmp_path / "workspace.sqlite3")


def register(database, draft, source="private source", parent=None):
    return database.register(
        build_spec(draft), draft.name, {"kind": "pasted_text", "text": source}, parent=parent
    )


class AgentProvider:
    def __init__(self):
        self.requests = []
        self.route = {"action": "analyze"}
        self.fail = False
        self.wait = False
        self.entered = asyncio.Event()
        self.extraction = None

    async def stream(self, messages):
        self.requests.append(messages)
        if messages[0]["content"] == ROUTER_PROMPT:
            yield json.dumps(self.route)
        elif self.extraction and "Extract a research strategy" in messages[0]["content"]:
            yield self.extraction
        else:
            yield "Measured analysis"
            self.entered.set()
            if self.wait:
                await asyncio.Event().wait()
            if self.fail:
                raise ModelError("Model unavailable")


def bridge(database, tmp_path, provider=None):
    async def online():
        pass

    research = ResearchService(ResearchStore(tmp_path / "jobs"), online_check=online)
    return Orchestrator(provider or AgentProvider(), database, research, "test/model")


def test_registration_exact_dedup_source_accumulation_and_immutable_behavior(
    database, research_draft
):
    a = register(database, research_draft)
    b = register(database, research_draft, "second independent source")
    assert a["public_id"] == b["public_id"] and a["agent_id"] == b["agent_id"]
    assert b["resolution"] == "exact_duplicate"
    assert a["public_id"].isdigit() and len(a["public_id"]) == 24
    with database.connect() as db:
        assert db.execute("SELECT count(*) FROM submissions").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM strategy_sources").fetchone()[0] == 2
    changed = research_draft.model_copy(update={"fast_period": 4})
    variant = register(database, changed, parent=a["public_id"])
    assert variant["public_id"] != a["public_id"] and variant["agent_id"] != a["agent_id"]
    assert variant["parent_public_id"] == a["public_id"] and variant["resolution"] == "variant"
    with pytest.raises(RegistryError):
        with database.connect() as db:
            db.execute("UPDATE strategies SET name='overwrite'")
    assert database.strategy(a["public_id"])["name"] == research_draft.name
    with pytest.raises(RegistryError):
        register(database, research_draft, parent=a["public_id"])


def test_concurrent_same_spec_one_strategy_and_agent(database, research_draft):
    def submit(index):
        return register(database, research_draft, f"source {index}")

    with ThreadPoolExecutor(max_workers=8) as workers:
        rows = list(workers.map(submit, range(16)))
    assert len({r["public_id"] for r in rows}) == len({r["agent_id"] for r in rows}) == 1
    with database.connect() as db:
        assert db.execute("SELECT count(*) FROM submissions").fetchone()[0] == 16
        assert db.execute("SELECT count(*) FROM agents").fetchone()[0] == 1


def test_public_projection_never_contains_private_turns_or_source(database, research_draft):
    record = register(database, research_draft, "private source")
    database.append_turn(record["public_id"], "private chat", "private answer", "test/model")
    public = json.dumps(database.catalog())
    assert not any(
        value in public for value in ["private source", "private chat", "private answer"]
    )
    assert "registration_does_not_imply_validation" in public
    assert database.path.stat().st_mode & 0o777 == 0o600


async def test_orchestrator_is_bridge_with_distinct_strategy_agents_and_restart(
    database,
    tmp_path,
    research_draft,
):
    a = register(database, research_draft)
    b = register(database, research_draft.model_copy(update={"fast_period": 4}), "other source")
    provider = AgentProvider()
    coordinator = bridge(database, tmp_path, provider)
    coordinator.select(a["public_id"])
    assert "".join([d async for d in coordinator.respond("A question")]) == "Measured analysis"
    assert len(provider.requests) == 2
    assert len(database.history(a["public_id"])) == 2 and not database.history(b["public_id"])
    coordinator.select(b["public_id"])
    _ = [d async for d in coordinator.respond("B question")]
    last = provider.requests[-1]
    assert "A question" not in json.dumps(last)
    assert a["public_id"] not in json.dumps(last)
    assert b["public_id"] in last[0]["content"]
    restarted = bridge(WorkspaceDatabase(database.path), tmp_path, provider)
    restarted.select(a["public_id"])
    assert restarted.agent().identity["agent_id"] == a["agent_id"]
    assert restarted.history[-2]["content"] == "A question"
    assert coordinator.agent(a["public_id"]) is coordinator.agent(a["public_id"])
    assert coordinator.agent(a["public_id"]) is not coordinator.agent(b["public_id"])


@pytest.mark.parametrize(
    "route",
    [
        {"action": "approve"},
        {"action": "analyze", "strategy_id": "9" * 24},
        {"action": "new", "source": "changed source"},
        {"action": "analyze", "sql": "drop table strategies"},
    ],
)
async def test_invalid_routes_cannot_execute_or_invent_approval(
    database,
    tmp_path,
    research_draft,
    route,
):
    a = register(database, research_draft)
    provider = AgentProvider()
    provider.route = route
    coordinator = bridge(database, tmp_path, provider)
    coordinator.select(a["public_id"])
    with pytest.raises(ModelError):
        _ = [p async for p in coordinator.respond("unchanged user text")]
    assert database.history(a["public_id"]) == []
    with database.connect() as db:
        assert db.execute("SELECT count(*) FROM operations").fetchone()[0] == 0


@pytest.mark.parametrize("cancel", [False, True])
async def test_partial_failed_or_canceled_agent_turn_never_enters_database(
    database,
    tmp_path,
    research_draft,
    cancel,
):
    record = register(database, research_draft)
    provider = AgentProvider()
    provider.wait, provider.fail = cancel, not cancel
    coordinator = bridge(database, tmp_path, provider)
    coordinator.select(record["public_id"])

    async def respond():
        return [p async for p in coordinator.respond("private incomplete question")]

    task = asyncio.create_task(respond())
    if cancel:
        await provider.entered.wait()
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else ModelError):
        await task
    assert not database.history(record["public_id"])
    assert not coordinator.history and not coordinator.busy
    with database.connect() as db:
        row = db.execute("SELECT * FROM operations").fetchone()
        assert row["status"] == ("canceled" if cancel else "failed")


async def test_strategy_test_request_requires_plan_approval_and_sync_is_idempotent(
    database,
    tmp_path,
    research_draft,
    research_source,
):
    provider = AgentProvider()
    provider.extraction = research_draft.model_dump_json()
    provider.route = {"action": "tests"}
    coordinator = bridge(database, tmp_path, provider)
    job_id, _, plan = await coordinator.research.prepare(
        research_source, ResearchConfig(), provider
    )
    record = coordinator.register_job(job_id)
    coordinator.select(record["public_id"])
    for _ in range(3):
        coordinator.sync_research()
    with database.connect() as db:
        assert db.execute("SELECT count(*) FROM submissions").fetchone()[0] == 1
    _ = [p async for p in coordinator.respond("Run tests")]
    assert coordinator.last_action == {"action": "review", "job_id": job_id, "digest": plan.digest}
    assert coordinator.research.store.state(job_id) == "awaiting_approval"
    assert not (coordinator.research.store.directory(job_id) / "approval.json").exists()
    coordinator.research.approve(job_id, plan.digest)
    coordinator.sync_research()
    _ = [p async for p in coordinator.respond("Run the approved tests")]
    assert coordinator.last_action["action"] == "resume"
    assert coordinator.research.active_job is None  # TUI schedules the same approved operation.


def test_report_plan_identity_and_immutable_results(database, research_draft):
    a = register(database, research_draft)
    b = register(database, research_draft.model_copy(update={"fast_period": 4}))
    database.attach_job(a["public_id"], "job", "a" * 64, "completed")
    report = {"plan_sha256": "a" * 64, "strategy_sha256": a["spec_sha256"], "metrics": {"pnl": "1"}}
    digest = database.record_report("job", report)
    assert database.record_report("job", report) == digest
    assert len(database.reports(a["public_id"])) == 1 and not database.reports(b["public_id"])
    with pytest.raises(RegistryError):
        database.record_report("job", report | {"metrics": {"pnl": "999"}})
    with pytest.raises(RegistryError):
        database.attach_job(b["public_id"], "job", "a" * 64, "completed")
    with pytest.raises(RegistryError):
        database.attach_job(a["public_id"], "job", "b" * 64, "completed")
    assert Decimal(database.reports()[0]["report"]["metrics"]["pnl"]) == 1


async def test_router_cannot_switch_to_an_unmentioned_existing_strategy(
    database,
    tmp_path,
    research_draft,
):
    a = register(database, research_draft)
    b = register(database, research_draft.model_copy(update={"fast_period": 4}))
    provider = AgentProvider()
    provider.route = {"action": "analyze", "strategy_id": b["public_id"]}
    coordinator = bridge(database, tmp_path, provider)
    coordinator.select(a["public_id"])
    with pytest.raises(ModelError):
        _ = [p async for p in coordinator.respond("Analyze the selected strategy")]
    assert coordinator.active_strategy == a["public_id"]
    assert not database.history(a["public_id"]) and not database.history(b["public_id"])


def test_corrupted_cached_report_is_not_supplied_as_evidence(database, research_draft):
    record = register(database, research_draft)
    database.attach_job(record["public_id"], "job", "a" * 64, "completed")
    database.record_report(
        "job", {"plan_sha256": "a" * 64, "strategy_sha256": record["spec_sha256"], "metrics": {}}
    )
    with database.connect() as db:
        db.execute("UPDATE reports SET report_json='{}'")
    with pytest.raises(RegistryError, match="integrity"):
        database.reports()


def test_duplicate_variant_retains_multiple_parent_links(database, research_draft):
    a = register(database, research_draft)
    b = register(
        database, research_draft.model_copy(update={"fast_period": 4}), parent=a["public_id"]
    )
    c = register(database, research_draft.model_copy(update={"fast_period": 5}))
    reused = register(
        database, research_draft.model_copy(update={"fast_period": 4}), parent=c["public_id"]
    )
    assert reused["public_id"] == b["public_id"]
    assert set(reused["parent_public_ids"]) == {a["public_id"], c["public_id"]}
    assert reused["agent_id"] == b["agent_id"]


def test_corrupted_source_is_not_used_by_agent(database, research_draft):
    record = register(database, research_draft)
    with database.connect() as db:
        db.execute("UPDATE sources SET payload='{}'")
    with pytest.raises(RegistryError, match="integrity"):
        database.sources(record["public_id"])


async def test_specialist_cannot_run_another_strategy_job(database, tmp_path, research_draft):
    a = register(database, research_draft)
    b = register(database, research_draft.model_copy(update={"fast_period": 4}))
    database.attach_job(a["public_id"], "foreign-job", "a" * 64, "paused")
    coordinator = bridge(database, tmp_path)
    with pytest.raises(RegistryError, match="another strategy"):
        await coordinator.agent(b["public_id"]).run_job("foreign-job")
    with database.connect() as db:
        assert db.execute("SELECT count(*) FROM operations").fetchone()[0] == 0
