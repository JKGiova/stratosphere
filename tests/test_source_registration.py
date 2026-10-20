"""Whole native source identity, atomic receipts, restart and unchanged execution gates."""

import asyncio
import json

import pytest
from test_bounded_studies import studies as studies

from stratosphere.agents.orchestrator import Orchestrator
from stratosphere.phase0.contracts import fingerprint
from stratosphere.registry.workspace import RegistryError, WorkspaceDatabase
from stratosphere.research.contracts import SmaParameters, compile_sma_spec
from stratosphere.research.service import ResearchService
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.sources.captions import CaptionRequest
from stratosphere.sources.contracts import SuppliedRequest
from stratosphere.sources.registration import (
    RegistrationRequest,
    SourceRegistrationService,
)
from stratosphere.sources.service import SourceService
from stratosphere.sources.store import SourceStore


async def online():
    pass


@pytest.fixture
async def imports(tmp_path):
    sources = SourceService(SourceStore(tmp_path / "sources"), online_check=online)
    db = WorkspaceDatabase(tmp_path / "workspace.sqlite3")
    service = SourceRegistrationService(sources, db)
    spec = compile_sma_spec(SmaParameters(fast_period=3, slow_period=8, quantity="0.01"))
    snapshot = await sources.supply(
        SuppliedRequest(label="Authored native definition"), spec.model_dump_json(indent=2).encode()
    )
    return service, spec, snapshot


def counts(db):
    with db.connect() as connection:
        return {
            table: connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
            for table in (
                "strategies",
                "agents",
                "sources",
                "strategy_sources",
                "submissions",
                "strategy_lineage",
                "source_registrations",
            )
        }


async def test_explicit_registration_exact_binding_restart_and_auxiliary_event_loss(
    imports, monkeypatch
):
    service, spec, snapshot = imports
    before = counts(service.database)
    assert before["strategies"] == 0
    original = service.store.event

    def fail_completion(identity, state, detail=""):
        if state == "completed":
            raise OSError("Simulated marker disk failure after database commit")
        original(identity, state, detail)

    monkeypatch.setattr(service.store, "event", fail_completion)
    receipt = await service.register(
        RegistrationRequest(source_id=snapshot.id, name="Imported fixture")
    )
    record = service.database.strategy(receipt.strategy_id)
    assert record["spec_sha256"] == fingerprint(spec) == receipt.proof.strategy_sha256
    assert record["agent_id"] == receipt.agent_id and record["specification"] == spec.model_dump(
        mode="json"
    )
    assert service.store.state(receipt.id) == "validating"
    assert receipt.proof.snapshot.digest == snapshot.digest
    assert not receipt.proof.executable_attestation
    assert receipt.execution == "definition_registered_no_approval_or_test"
    restarted = SourceRegistrationService(service.sources, WorkspaceDatabase(service.database.path))
    assert await restarted.open(receipt.id) == receipt
    assert (await restarted.attempts())[0]["state"] == "registered"
    assert counts(restarted.database)["submissions"] == 1
    assert not restarted.database.jobs(receipt.strategy_id) and not restarted.database.reports()
    # Retry the same immutable transaction inputs: return the receipt, never append again.
    assert (
        restarted.database.register_source(
            receipt.id, receipt.request, receipt.proof, receipt.requested_at
        )
        == receipt
    )
    assert counts(restarted.database)["submissions"] == 1
    with pytest.raises(ResearchError, match="different inputs"):
        restarted.database.register_source(
            receipt.id,
            receipt.request.model_copy(update={"name": "Other"}),
            receipt.proof,
            receipt.requested_at,
        )


async def test_duplicate_reuses_agent_accumulates_provenance_and_preserves_history(
    imports,
):
    service, _, source = imports
    first = await service.register(RegistrationRequest(source_id=source.id, name="Original name"))
    service.database.append_turn(first.strategy_id, "PRIVATE_HISTORY", "Private answer", "fixture")
    before = service.database.history(first.strategy_id)
    second = await service.register(
        RegistrationRequest(source_id=source.id, name="Different supplied name")
    )
    assert second.strategy_id == first.strategy_id and second.agent_id == first.agent_id
    assert second.resolution == "exact_duplicate" and second.registered_name == "Original name"
    assert second.request.name == "Different supplied name" and second.id != first.id
    assert service.database.history(first.strategy_id) == before
    assert len(service.database.sources(first.strategy_id)) == 2
    assert counts(service.database)["strategies"] == counts(service.database)["agents"] == 1
    assert counts(service.database)["submissions"] == 2


async def test_behavioral_variant_parent_and_cycle_checks_are_atomic(imports):
    service, _, source = imports
    first = await service.register(RegistrationRequest(source_id=source.id))
    spec = compile_sma_spec(SmaParameters(fast_period=4, slow_period=10, quantity="0.01"))
    variant = await service.sources.supply(
        SuppliedRequest(label="Changed native definition"), spec.model_dump_json().encode()
    )
    second = await service.register(
        RegistrationRequest(source_id=variant.id, parent_strategy_id=first.strategy_id)
    )
    assert second.resolution == "variant" and second.strategy_id != first.strategy_id
    assert second.agent_id != first.agent_id
    assert service.database.strategy(second.strategy_id)["parent_public_ids"] == [first.strategy_id]
    before = counts(service.database)
    with pytest.raises(ResearchError, match="cycle"):
        await service.register(
            RegistrationRequest(source_id=source.id, parent_strategy_id=second.strategy_id)
        )
    with pytest.raises(ResearchError, match="change behavior"):
        await service.register(
            RegistrationRequest(source_id=source.id, parent_strategy_id=first.strategy_id)
        )
    assert counts(service.database) == before


@pytest.mark.parametrize(
    "mutation",
    ["missing", "unknown", "changed_exit", "coerced", "duplicate", "nonfinite", "python", "pine"],
)
async def test_incomplete_unsupported_or_ambiguous_source_never_registers(imports, mutation):
    service, spec, _ = imports
    data = spec.model_dump(mode="json")
    if mutation == "missing":
        del data["assumptions"]
    elif mutation == "unknown":
        data["execute_sql"] = "Untrusted, never executed"
    elif mutation == "changed_exit":
        data["execution_requirements"]["session_end"] = "close"
    elif mutation == "coerced":
        data["indicators"][0]["period"] = 3.0
    text = json.dumps(data)
    if mutation == "duplicate":
        text = text.replace(
            '"schema_version": "strategy/1"',
            '"schema_version":"strategy/1","schema_version":"strategy/1"',
        )
    elif mutation == "nonfinite":
        text = text.replace('"period": 3', '"period": NaN')
    elif mutation == "python":
        text = 'raise RuntimeError("Acquired code must never execute")'
    elif mutation == "pine":
        text = '//@version=6\nstrategy("Not equivalent")\n'
    source = await service.sources.supply(
        SuppliedRequest(label="Unverified input"), text.encode(), kind="code"
    )
    with pytest.raises(ResearchError, match="complete JSON"):
        await service.register(RegistrationRequest(source_id=source.id))
    assert (
        counts(service.database)["strategies"] == 0
        and counts(service.database)["source_registrations"] == 0
    )
    (attempt,) = await service.attempts()
    assert attempt["state"] == "failed"


async def test_caption_payload_is_not_silently_reinterpreted_as_native_file(imports):
    service, spec, _ = imports
    text = "WEBVTT\n\n00:00.000 --> 00:01.000\n" + spec.model_dump_json() + "\n"
    source = await service.sources.supply_captions(
        CaptionRequest(label="Not a definition file", format="webvtt"), text.encode()
    )
    with pytest.raises(ResearchError):
        await service.register(RegistrationRequest(source_id=source.id))
    assert counts(service.database)["strategies"] == 0


async def test_receipt_insert_failure_rolls_back_strategy_agent_source_submission_and_lineage(
    imports,
):
    service, _, source = imports
    with service.database.connect() as db:
        db.execute(
            "CREATE TRIGGER reject_receipt BEFORE INSERT ON source_registrations "
            "BEGIN SELECT RAISE(ABORT,'fixture failure'); END"
        )
    before = counts(service.database)
    with pytest.raises(RegistryError, match="no partial"):
        await service.register(RegistrationRequest(source_id=source.id))
    assert counts(service.database) == before
    assert (await service.attempts())[0]["state"] == "failed"


async def test_cancellation_during_online_source_check_never_reaches_registry(imports):
    service, _, source = imports
    reached, hold = asyncio.Event(), asyncio.Event()

    async def pause(identity):
        reached.set()
        await hold.wait()

    service.sources.open = pause
    task = asyncio.create_task(service.register(RegistrationRequest(source_id=source.id)))
    await reached.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert counts(service.database)["strategies"] == 0
    assert (await service.attempts())[0]["state"] == "canceled"


async def test_changed_source_before_transaction_and_offline_or_secret_request_are_blocked(imports):
    service, _, source = imports
    service.sources.store.known_secrets = ("fixture-key",)
    with pytest.raises(ResearchError, match="Credential"):
        await service.register(RegistrationRequest(source_id=source.id, name="fixture-key"))
    assert not service.store.jobs()

    async def offline():
        raise ValueError("offline")

    service.sources.online_check = offline
    with pytest.raises(ResearchError, match="online"):
        await service.register(RegistrationRequest(source_id=source.id))
    assert not service.store.jobs()
    service.sources.online_check = online
    original = service.sources.open

    async def changed(identity):
        value = await original(identity)
        (raw,) = source.raw_materials
        (service.sources.store.directory(identity) / ("blob_" + raw.sha256 + ".bin")).write_text(
            "changed"
        )
        return value

    service.sources.open = changed
    with pytest.raises(ResearchError, match="integrity"):
        await service.register(RegistrationRequest(source_id=source.id))
    assert counts(service.database)["strategies"] == 0


async def test_receipts_cannot_be_updated_or_deleted_and_damaged_source_cannot_be_opened(imports):
    service, _, source = imports
    receipt = await service.register(RegistrationRequest(source_id=source.id))
    for statement in (
        "UPDATE source_registrations SET payload='{}' WHERE id=?",
        "DELETE FROM source_registrations WHERE id=?",
    ):
        with pytest.raises(RegistryError):
            with service.database.connect() as db:
                db.execute(statement, (receipt.id,))
    (raw,) = source.raw_materials
    (service.sources.store.directory(source.id) / ("blob_" + raw.sha256 + ".bin")).write_text(
        "changed"
    )
    with pytest.raises(ResearchError, match="integrity"):
        await service.open(receipt.id)
    assert service.database.source_registration(receipt.id) == receipt


async def test_explicit_bridge_registration_makes_no_model_request_and_preserves_active_history(
    imports, tmp_path
):
    service, _, source = imports
    research = ResearchService(ResearchStore(tmp_path / "research"), online_check=online)
    bridge = Orchestrator(None, service.database, research, sources=service.sources)
    reply = "".join([p async for p in bridge.respond("Register source " + source.id)])
    registration = await bridge.source_registrations.open(bridge.last_action["id"])
    assert registration.strategy_id in reply and bridge.active_strategy is None
    assert not bridge.general.history and not bridge.database.history(registration.strategy_id)
    assert not bridge.research.store.jobs() and not bridge.studies.store.jobs()


async def test_duplicate_nested_keys_and_missing_defaulted_nodes_are_not_normalized_away(imports):
    service, spec, _ = imports
    payload = spec.model_dump(mode="json")
    del payload["entry_rules"][0]["condition"]["value"]
    cases = [
        json.dumps(payload),
        spec.model_dump_json().replace(
            '"algorithm_version":1', '"algorithm_version":1,"algorithm_version":1', 1
        ),
    ]
    for text in cases:
        source = await service.sources.supply(
            SuppliedRequest(label="Unnormalized source"), text.encode()
        )
        with pytest.raises(ResearchError):
            await service.register(RegistrationRequest(source_id=source.id))
    assert counts(service.database)["strategies"] == 0


async def test_concurrent_same_receipt_delivery_is_idempotent_across_connections(imports):
    import uuid

    service, _, source = imports
    first = await service.register(RegistrationRequest(source_id=source.id))
    identity = uuid.uuid7().hex
    database = WorkspaceDatabase(service.database.path)

    def deliver(db):
        return db.register_source(identity, first.request, first.proof, first.requested_at)

    values = await asyncio.gather(
        asyncio.to_thread(deliver, service.database), asyncio.to_thread(deliver, database)
    )
    assert values[0] == values[1] and values[0].strategy_id == first.strategy_id
    assert counts(database)["submissions"] == 2 and counts(database)["source_registrations"] == 2


async def test_committed_receipt_survives_missing_auxiliary_attempt_directory(imports):
    import shutil

    service, _, source = imports
    value = await service.register(RegistrationRequest(source_id=source.id))
    shutil.rmtree(service.store.directory(value.id))
    assert await service.open(value.id) == value
    assert (await service.attempts()) == [
        {"id": value.id, "state": "registered", "strategy_id": value.strategy_id}
    ]


@pytest.mark.engine
async def test_imported_definition_flows_to_reviewed_native_study_and_exact_report(
    studies, tmp_path
):
    from test_research_engine import oracle

    from stratosphere.research.service import run_worker
    from stratosphere.sources.registration import SourceRegistrationService
    from stratosphere.studies.worker import CellRequest, cell_bars

    sources = SourceService(SourceStore(tmp_path / "native-sources"), online_check=online)
    registration = SourceRegistrationService(sources, studies.database)
    spec = compile_sma_spec(SmaParameters(fast_period=3, slow_period=8, quantity="0.02"))
    source = await sources.supply(
        SuppliedRequest(label="Authored imported native fixture"), spec.model_dump_json().encode()
    )
    receipt = await registration.register(RegistrationRequest(source_id=source.id))
    studies.runner = run_worker
    anchor = studies.registry_anchor(receipt.strategy_id)
    job, protocol = await studies.prepare_registered(
        receipt.strategy_id,
        anchor,
        "Longer periods may reduce validation cost sensitivity; reject otherwise.",
        [{"fast": 3, "slow": 8}, {"fast": 4, "slow": 10}],
        "2025-01",
        10,
    )
    assert studies.store.state(job) == "awaiting_approval"
    assert not any(c[0] in {"fetch", "run"} for c in studies.test_calls)
    with pytest.raises(ResearchError):
        await studies.run(job)
    studies.approve(job, protocol.digest)
    report = await studies.run(job)
    assert report["native_attempts"] == 10 and report["holdout_status"] == "completed_once"
    assert report["parent_strategy_id"] == receipt.strategy_id
    assert report["strategy_sha256"] == receipt.proof.strategy_sha256
    directory = studies.store.directory(job)
    started = studies.store.read(job, "cell_0_development_1_started.json")
    run = directory / "runs" / started["worker_attempt"]
    request = CellRequest.model_validate_json((run / "request.json").read_text())
    plan, bars, start = cell_bars(request, run)
    signals, fills, curve, held, pending = oracle(plan, bars, 1, trade_start_ns=start)
    actual = report["attempts"][0]["cells"]["development_1"]["response"]["result"]
    assert actual["engine"] == "nautilus_trader"
    from decimal import Decimal

    assert actual["signals"] == signals and actual["pending_signal"] == pending
    assert len(actual["fills"]) == len(fills)
    for actual_fill, expected_fill in zip(actual["fills"], fills, strict=True):
        for key, value in expected_fill.items():
            if key in {"price", "commission"}:
                assert Decimal(actual_fill[key]) == Decimal(value)
            else:
                assert actual_fill[key] == value
    assert [Decimal(row["equity"]) for row in actual["equity_curve"]] == curve
    assert Decimal(actual["balances"]["BTC"]) == held
    assert await registration.open(receipt.id) == receipt
    selected = report["selection"]["selected_index"]
    assert report["metrics_strategy_id"] == report["attempts"][selected]["strategy_id"]
    assert await studies.open_report(job) == report


async def test_acquisition_request_bytes_remain_bound_even_if_normalized_snapshot_is_unchanged(
    imports,
):
    import hashlib

    service, _, source = imports
    receipt = await service.register(RegistrationRequest(source_id=source.id))
    directory = service.sources.store.directory(source.id)
    request_path = directory / "request.json"
    # Harmless whitespace is still a change to the exact retained acquisition request bytes.
    request_path.write_text(request_path.read_text() + "\n")
    manifest = service.sources.store.read(source.id, "manifest.json")
    manifest["request_sha256"] = hashlib.sha256(request_path.read_bytes()).hexdigest()
    (directory / "manifest.json").write_text(json.dumps(manifest))
    assert service.sources.store.snapshot(source.id).digest == source.digest
    with pytest.raises(ResearchError, match="differs"):
        await service.open(receipt.id)


@pytest.mark.parametrize("registered", [False, True])
async def test_model_generated_registration_route_is_not_authority(imports, tmp_path, registered):
    from stratosphere.models.streaming import ModelError

    service, spec, source = imports
    if registered:
        service.database.register(spec, "Existing fixture", {"kind": "authored_fixture"})

    class Untrusted:
        async def stream_json(self, *args):
            yield json.dumps({"action": "source_register", "source": source.id})

        async def stream(self, *args):
            yield json.dumps({"action": "source_register", "source": source.id})

    bridge = Orchestrator(
        Untrusted(),
        service.database,
        ResearchService(ResearchStore(tmp_path / "research"), online_check=online),
        sources=service.sources,
    )
    if registered:
        with pytest.raises(ModelError):
            _ = [p async for p in bridge.respond("Tell me what this source describes")]
    else:
        _ = [p async for p in bridge.respond("Tell me what this source describes")]
        assert bridge.last_action is None
    assert not bridge.source_registrations.store.jobs()
    assert counts(service.database)["strategies"] == int(registered)


async def test_pinned_github_native_file_retains_original_request_tree_and_blob_refs(imports):
    import hashlib

    import httpx
    from test_sources import COMMIT, github_handler

    from stratosphere.sources.contracts import SourceRequest
    from stratosphere.sources.registration import native_definition

    service, spec, _ = imports
    raw = spec.model_dump_json(indent=2).encode()
    blob = hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
    service.sources.transport = httpx.MockTransport(
        lambda request: github_handler(request, body=raw, blob=blob)
    )
    url = "https://github.com/owner/repo/blob/main/strategy.py?example=1#L1"
    source = await service.sources.acquire(SourceRequest(url=url))
    receipt = await service.register(RegistrationRequest(source_id=source.id))
    assert receipt.proof.snapshot.repository_commit == COMMIT
    assert receipt.proof.snapshot.repository_blob == blob
    assert receipt.proof.source_request.submitted_url == url
    assert receipt.proof.source_request.url == "https://github.com/owner/repo/blob/main/strategy.py"
    assert native_definition(receipt.proof.snapshot) == spec


async def test_registration_source_and_private_context_stay_fixed_across_navigation(
    imports, tmp_path
):
    service, spec, source = imports
    other_spec = compile_sma_spec(SmaParameters(fast_period=5, slow_period=12, quantity="0.02"))
    other = service.database.register(other_spec, "Other active definition", {"kind": "authored"})
    service.database.append_turn(
        other["public_id"], "PRIVATE_OTHER_CONTEXT", "Private reply", "fixture"
    )
    research = ResearchService(ResearchStore(tmp_path / "research"), online_check=online)
    bridge = Orchestrator(None, service.database, research, sources=service.sources)
    reached, release = asyncio.Event(), asyncio.Event()
    original = service.sources.open

    async def pause(identity):
        reached.set()
        await release.wait()
        return await original(identity)

    service.sources.open = pause

    async def run():
        return "".join([p async for p in bridge.respond("Register source " + source.id)])

    task = asyncio.create_task(run())
    await reached.wait()
    bridge.select(other["public_id"])
    release.set()
    reply = await task
    receipt = await bridge.source_registrations.open(bridge.last_action["id"])
    assert receipt.proof.strategy_sha256 == fingerprint(spec)
    assert (
        receipt.strategy_id != other["public_id"] and bridge.active_strategy == other["public_id"]
    )
    assert "PRIVATE_OTHER_CONTEXT" not in reply
    assert len(service.database.history(other["public_id"])) == 2
    assert not service.database.history(receipt.strategy_id)


async def test_committed_native_example_is_a_complete_supported_input(imports):
    from pathlib import Path

    service, _, _ = imports
    path = Path(__file__).parents[1] / "examples/strategies/sma-close-cross.json"
    source = await service.sources.supply(SuppliedRequest(label=path.name), path.read_bytes())
    receipt = await service.register(RegistrationRequest(source_id=source.id))
    assert receipt.proof.strategy_sha256 == fingerprint(
        compile_sma_spec(SmaParameters(fast_period=3, slow_period=8, quantity="0.01"))
    )


async def test_imported_variant_study_can_revisit_ancestor_without_reversing_lineage(
    studies, tmp_path
):
    sources = SourceService(SourceStore(tmp_path / "sources"), online_check=online)
    importer = SourceRegistrationService(sources, studies.database)
    ancestor = studies.test_parent["public_id"]
    spec = compile_sma_spec(SmaParameters(fast_period=4, slow_period=10, quantity="0.01"))
    source = await sources.supply(
        SuppliedRequest(label="Descendant definition"), spec.model_dump_json().encode()
    )
    receipt = await importer.register(
        RegistrationRequest(source_id=source.id, parent_strategy_id=ancestor)
    )
    job, protocol = await studies.prepare_registered(
        receipt.strategy_id,
        studies.registry_anchor(receipt.strategy_id),
        "Compare the existing ancestor against the descendant at fixed costs.",
        [{"fast": 4, "slow": 10}, {"fast": 3, "slow": 8}],
        "2025-01",
        10,
    )
    studies.approve(job, protocol.digest)
    report = await studies.run(job)
    assert report["attempts"][1]["strategy_id"] == ancestor
    assert report["attempts"][1]["status"] == "eligible"
    assert studies.database.strategy(ancestor)["parent_public_ids"] == []
    assert studies.database.strategy(receipt.strategy_id)["parent_public_ids"] == [ancestor]
    assert any(
        row["source"].get("comparison_parent_strategy_id") == receipt.strategy_id
        for row in studies.database.sources(ancestor)
    )
    assert await importer.open(receipt.id) == receipt
