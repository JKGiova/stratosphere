"""Actual process ownership/crash recovery and durable authored service controls."""

import asyncio
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from pydantic import ValidationError
from test_paper_ledger import intent
from test_paper_runtime import approve, observation
from test_paper_runtime import stream as stream

from stratosphere.paper import service as module
from stratosphere.paper.inspection import JournalInspector
from stratosphere.paper.ledger import PaperError
from stratosphere.paper.runtime import ClosedBarMandate
from stratosphere.paper.service import (
    ObservationWorker,
    QueuedObservation,
    ServiceJournal,
    ServicePlan,
    StreamPin,
    launch,
    probe_owner,
    service_digest,
)


async def online():
    pass


@pytest.fixture
def service(stream):
    runtime, mandate, account, registry = stream
    approve(stream)
    now = [10**18]
    plan = ServicePlan(
        account=account,
        account_journal=str(runtime.ledger.path),
        registry_database=str(registry.path),
        streams=(
            StreamPin(instance_id=mandate.instance.instance_id, mandate_sha256=mandate.digest),
        ),
        implementation_sha256=service_digest(),
        poll_interval_ns=50_000_000,
        online_timeout_ns=10**9,
        max_silence_ns=10 * 10**9,
    )
    journal = ServiceJournal.create(plan, clock=lambda: now[0])
    journal.approve(plan.digest)
    return journal, plan, now, runtime


def delivery(plan, bar, *, identity=None, **changes):
    value = observation(bar)
    return QueuedObservation(
        delivery_id=identity or uuid.uuid7().hex,
        instance_id=plan.streams[0].instance_id,
        observation=value,
        received_ns=value.available_ns,
        **changes,
    )


async def test_reviewed_queue_crossings_and_account_economics_survive_reopen(service, market_bars):
    journal, plan, now, runtime = service
    before = runtime.ledger.snapshot()
    for bar in market_bars[:30]:
        journal.enqueue(delivery(plan, bar))
    with ObservationWorker(journal, online_check=online).own() as worker:
        assert probe_owner(plan)
        for _ in market_bars[:30]:
            now[0] += 1_000_000
            assert await worker.tick() == "accepted"
        assert await worker.tick() == "idle"
    assert not probe_owner(plan)
    state = ServiceJournal(plan.directory, clock=lambda: now[0]).snapshot()
    assert len(state["queue"]) == len(state["results"]) == 30
    assert state["run_id"] is None and not state["faults"]
    accepted = runtime.inspect(plan.streams[0].instance_id)
    assert accepted["accepted_count"] == 30
    assert [row["signal_sha256"] for row in state["results"].values() if row["signal_sha256"]] == [
        row["signal_sha256"] for row in accepted["signals"]
    ]
    after = runtime.ledger.snapshot()
    for name in ("cash", "reserved_cash", "orders", "fills", "instances", "decisions"):
        assert before[name] == after[name]


async def test_queue_replay_aliases_and_conflicts_are_retained(service, market_bars):
    journal, plan, now, runtime = service
    first = delivery(plan, market_bars[0])
    assert journal.enqueue(first) == "queued"
    digest = journal.snapshot()["journal_sha256"]
    assert journal.enqueue(first) == "duplicate"
    assert journal.snapshot()["journal_sha256"] == digest
    alias = delivery(plan, market_bars[0])
    journal.enqueue(alias)
    with ObservationWorker(journal, online_check=online).own() as worker:
        assert await worker.tick() == "accepted"
        prior = journal.snapshot()["progress"]
        now[0] += 10**9
        assert await worker.tick() == "duplicate"
        assert journal.snapshot()["progress"] == prior
        changed = first.model_copy(update={"received_ns": first.received_ns + 1})
        assert journal.enqueue(changed) == "conflict"
        assert await worker.tick() == "halted"
    assert runtime.inspect(first.instance_id)["accepted_count"] == 1
    state = journal.snapshot()
    assert state["faults"][0]["delivery"] == changed.model_dump(mode="json")
    with pytest.raises(PaperError, match="durable halt"):
        with ObservationWorker(journal, online_check=online).own():
            pass


async def test_silence_includes_downtime_and_cannot_be_reset(service, market_bars):
    journal, plan, now, runtime = service
    with ObservationWorker(journal, online_check=online).own() as worker:
        assert await worker.tick() == "idle"
    now[0] += plan.max_silence_ns + 1
    journal.enqueue(delivery(plan, market_bars[0]))
    with ObservationWorker(journal, online_check=online).own() as worker:
        assert await worker.tick() == "halted"
    assert journal.snapshot()["faults"][-1]["reason"] == "silence_exceeded"
    assert not journal.snapshot()["results"]
    assert runtime.inspect(plan.streams[0].instance_id)["accepted_count"] == 0
    with pytest.raises(FileExistsError):
        ServiceJournal.create(plan, clock=lambda: now[0])


async def test_clock_regression_is_durable_and_releases_owner(service):
    journal, plan, now, _ = service
    with ObservationWorker(journal, online_check=online).own() as worker:
        now[0] -= 1
        assert await worker.tick() == "halted"
    state = journal.snapshot()
    assert state["faults"][-1]["reason"] == "clock_regression"
    assert state["faults"][-1]["observed_clock_ns"] == now[0]
    assert not probe_owner(plan)


def test_startup_clock_regression_is_retained_without_an_owner(service):
    journal, plan, now, _ = service
    now[0] -= 1
    with pytest.raises(PaperError, match="clock regressed"):
        with ObservationWorker(journal).own():
            pass
    assert journal.snapshot()["faults"][-1]["reason"] == "clock_regression"
    assert not probe_owner(plan)


async def test_idle_polling_does_not_append_a_heartbeat_every_poll(service):
    journal, plan, now, _ = service
    with ObservationWorker(journal, online_check=online).own() as worker:
        before = journal.snapshot()["journal_sha256"]
        for _ in range(5):
            assert await worker.tick() == "idle"
        assert journal.snapshot()["journal_sha256"] == before
        now[0] += plan.heartbeat_interval_ns
        assert await worker.tick() == "idle"
        assert journal.snapshot()["last_heartbeat_ns"] == now[0]
        assert journal.snapshot()["journal_sha256"] != before


@pytest.mark.parametrize(
    "failure", ["offline", "timeout", "swallowed_timeout", "clock", "silence", "stop", "cancel"]
)
async def test_await_boundary_never_processes_after_failed_gate(service, market_bars, failure):
    journal, plan, now, runtime = service
    journal.enqueue(delivery(plan, market_bars[0]))
    stop = asyncio.Event()

    async def gate():
        if failure == "offline":
            raise ValueError("private untrusted network detail")
        if failure == "timeout":
            raise TimeoutError
        if failure == "swallowed_timeout":
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                pass
        if failure == "clock":
            now[0] -= 1
        if failure == "silence":
            now[0] += plan.max_silence_ns + 1
        if failure == "stop":
            stop.set()
        if failure == "cancel":
            asyncio.current_task().cancel()
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                pass

    with ObservationWorker(journal, online_check=gate).own() as worker:
        if failure == "cancel":
            task = asyncio.create_task(worker.tick(stop))
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            assert await worker.tick(stop) == ("stopped" if failure == "stop" else "halted")
    assert runtime.inspect(plan.streams[0].instance_id)["accepted_count"] == 0
    assert "private untrusted" not in journal.path.read_bytes().decode(errors="ignore")


async def test_removed_account_is_not_recreated_after_online_wait(service, market_bars):
    journal, plan, _, runtime = service
    journal.enqueue(delivery(plan, market_bars[0]))

    async def gate():
        runtime.ledger.path.unlink()

    with ObservationWorker(journal, online_check=gate).own() as worker:
        assert await worker.tick() == "halted"
    assert not runtime.ledger.path.exists()
    assert journal.snapshot()["faults"][-1]["reason"] == "processing_failed"


async def test_changed_code_registry_and_runtime_fault_block_processing(
    service, market_bars, monkeypatch
):
    journal, plan, _, runtime = service
    journal.enqueue(delivery(plan, market_bars[0]))
    with ObservationWorker(journal, online_check=online).own() as worker:
        monkeypatch.setattr(module, "service_digest", lambda: "f" * 64)
        assert await worker.tick() == "halted"
    assert runtime.inspect(plan.streams[0].instance_id)["accepted_count"] == 0
    assert journal.snapshot()["faults"][-1]["reason"] == "processing_failed"


async def test_stream_gap_retains_rejection_and_pending_capital(service, market_bars, stream):
    journal, plan, _, runtime = service
    instance = stream[1].instance
    pending = intent(instance, quantity="0.5", price_bound="100", fee_allowance="0.01")
    assert runtime.ledger.reserve(pending, now_ns=100) is None
    before = runtime.ledger.snapshot()
    journal.enqueue(delivery(plan, market_bars[1]))
    with ObservationWorker(journal, online_check=online).own() as worker:
        assert await worker.tick() == "halted"
    result = next(iter(journal.snapshot()["results"].values()))
    assert result["reason"] == "gap_or_out_of_order"
    rejected = runtime.inspect(instance.instance_id)["faults"][0]
    assert rejected["observation"]["bar"] == market_bars[1].model_dump()
    after = runtime.ledger.snapshot()
    for field in ("orders", "reserved_cash", "cash", "instances", "fills", "decisions"):
        assert before[field] == after[field]
    assert after["reserved_cash"] == "50.01"


async def test_two_streams_have_separate_silence_deadlines(stream, market_bars):
    runtime, first, account, registry = stream
    approve(stream)
    second_instance = first.instance.model_copy(update={"instance_id": "c" * 32})
    runtime.ledger.add_instance(second_instance, registry)
    second = ClosedBarMandate.model_validate(first.model_dump() | {"instance": second_instance})
    runtime.prepare(second)
    runtime.approve(second_instance.instance_id, second.digest)
    now = [10**18]
    plan = ServicePlan(
        account=account,
        account_journal=str(runtime.ledger.path),
        registry_database=str(registry.path),
        implementation_sha256=service_digest(),
        streams=tuple(
            StreamPin(instance_id=m.instance.instance_id, mandate_sha256=m.digest)
            for m in (first, second)
        ),
        poll_interval_ns=50_000_000,
        online_timeout_ns=10**9,
        max_silence_ns=10**10,
    )
    journal = ServiceJournal.create(plan, clock=lambda: now[0])
    journal.approve(plan.digest)
    journal.enqueue(delivery(plan, market_bars[0]))
    with ObservationWorker(journal, online_check=online).own() as worker:
        now[0] += 5 * 10**9
        assert await worker.tick() == "accepted"
        progress = journal.snapshot()["progress"]
        assert progress[first.instance.instance_id] > progress[second.instance.instance_id]
        now[0] += 5 * 10**9 + 1
        assert await worker.tick() == "halted"
    assert journal.snapshot()["faults"][-1]["reason"] == "silence_exceeded"


async def test_explicit_stop_releases_lock_and_health_is_read_only(service, market_bars):
    journal, plan, _, runtime = service
    inspector = JournalInspector(runtime.ledger.path, online_check=online)
    journal.enqueue(delivery(plan, market_bars[0]))
    stop = asyncio.Event()

    async def gate():
        capture = await inspector.capture()
        row = next(row for row in capture.health if row["kind"] == "authored_observation_service")
        assert row["owner_lock_held_at_capture"]
        assert row["authority"] == "authored_observations_only_no_orders"
        stop.set()

    before = runtime.ledger.path.read_bytes()
    await ObservationWorker(journal, online_check=gate).run(stop)
    assert not probe_owner(plan)
    assert not journal.snapshot()["results"]
    assert runtime.ledger.path.read_bytes() == before
    service_before = journal.path.read_bytes()
    capture = await inspector.capture()
    row = next(row for row in capture.health if row["kind"] == "authored_observation_service")
    assert not row["owner_lock_held_at_capture"] and not row["halted"]
    assert journal.path.read_bytes() == service_before
    assert "not connected" in capture.summary["feed_service"]


async def test_damaged_service_is_unavailable_in_health_and_does_not_mutate_account(service):
    journal, _, _, runtime = service
    journal.path.chmod(0o644)
    before = runtime.ledger.path.read_bytes()
    capture = await JournalInspector(runtime.ledger.path, online_check=online).capture()
    assert capture.health[-1]["kind"] == "authored_observation_service_unavailable"
    assert runtime.ledger.path.read_bytes() == before


async def test_changed_registry_after_online_wait_is_rejected(service, market_bars):
    journal, plan, _, runtime = service
    journal.enqueue(delivery(plan, market_bars[0]))

    async def gate():
        with sqlite3.connect(plan.registry_database) as db:
            db.execute("DROP TRIGGER immutable_strategy")
            db.execute("UPDATE strategies SET spec_sha256=?", ("f" * 64,))

    with ObservationWorker(journal, online_check=gate).own() as worker:
        assert await worker.tick() == "halted"
    assert runtime.inspect(plan.streams[0].instance_id)["accepted_count"] == 0
    assert journal.snapshot()["faults"][-1]["reason"] == "processing_failed"


async def test_malformed_registry_after_online_wait_retains_a_halt(service, market_bars):
    journal, plan, _, runtime = service
    journal.enqueue(delivery(plan, market_bars[0]))

    async def gate():
        with sqlite3.connect(plan.registry_database) as db:
            db.execute("DROP TRIGGER immutable_strategy")
            db.execute("UPDATE strategies SET spec_json=?", ("{}",))

    with ObservationWorker(journal, online_check=gate).own() as worker:
        assert await worker.tick() == "halted"
    assert runtime.inspect(plan.streams[0].instance_id)["accepted_count"] == 0
    assert journal.snapshot()["faults"][-1]["reason"] == "processing_failed"


def test_empty_existing_service_cannot_appear_as_an_unconfigured_owner(service):
    journal, *_ = service
    with sqlite3.connect(journal.path) as db:
        db.execute("DROP TRIGGER retain_delete")
        db.execute("DELETE FROM events")
    with pytest.raises(PaperError, match="plan"):
        journal.snapshot()


@pytest.mark.parametrize("available", [True, False])
def test_status_cli_requires_online_before_reading_and_never_mutates(
    service, monkeypatch, capsys, available
):
    journal, plan, *_ = service
    before = journal.path.read_bytes()
    calls = []

    async def gate():
        calls.append("online")
        if not available:
            raise ValueError("private network detail")

    monkeypatch.setattr(module, "require_online", gate)
    monkeypatch.setattr(sys, "argv", ["service", "status", str(plan.directory)])
    if available:
        module.main()
        status = json.loads(capsys.readouterr().out)
        assert status["approved"] and not status["owner_lock_held_now"]
        assert status["acknowledged"] == 0
    else:

        def forbidden(*args):
            raise AssertionError("No service snapshot is allowed before the online gate")

        monkeypatch.setattr(ServiceJournal, "snapshot", forbidden)
        with pytest.raises(SystemExit) as caught:
            module.main()
        assert caught.value.code == 1
        assert "private network detail" not in capsys.readouterr().err
    assert calls == ["online"] and journal.path.read_bytes() == before


def test_approval_exact_target_and_existing_state_only(stream, market_bars):
    runtime, mandate, account, registry = stream
    approve(stream)
    plan = ServicePlan(
        account=account,
        account_journal=str(runtime.ledger.path),
        registry_database=str(registry.path),
        streams=(
            StreamPin(instance_id=mandate.instance.instance_id, mandate_sha256=mandate.digest),
        ),
        implementation_sha256=service_digest(),
        poll_interval_ns=50_000_000,
        online_timeout_ns=10**9,
        max_silence_ns=10**10,
    )
    journal = ServiceJournal.create(plan)
    before = journal.snapshot()["journal_sha256"]
    with pytest.raises(PaperError, match="exactly"):
        journal.approve("f" * 64)
    with pytest.raises(PaperError, match="Unreviewed"):
        journal.enqueue(delivery(plan, market_bars[0]))
    with pytest.raises(PaperError, match="approval"):
        with ObservationWorker(journal).own():
            pass
    assert journal.snapshot()["journal_sha256"] == before
    journal.approve(plan.digest)
    with pytest.raises(PaperError, match="exactly"):
        journal.approve(plan.digest)
    with pytest.raises(PaperError, match="Unreviewed"):
        journal.enqueue(delivery(plan, market_bars[0]).model_copy(update={"instance_id": "f" * 32}))


@pytest.mark.parametrize(
    "changes",
    [
        {"poll_interval_ns": True},
        {"max_silence_ns": 1},
        {"account_journal": "../account"},
        {"registry_database": "/private/tmp/../wrong"},
        {"streams": ()},
        {"authority": "orders"},
        {"recovery": "backfill"},
    ],
)
def test_invalid_plan_cannot_grant_runtime_authority(service, changes):
    _, plan, *_ = service
    with pytest.raises(ValidationError):
        ServicePlan.model_validate(plan.model_dump() | changes)


def test_journal_tampering_and_private_paths(service, tmp_path):
    journal, plan, *_ = service
    link = tmp_path / "service-link"
    link.symlink_to(plan.directory, target_is_directory=True)
    with pytest.raises(PaperError, match="symbolic"):
        ServiceJournal(link)
    journal.path.chmod(0o644)
    with pytest.raises(PaperError, match="private"):
        journal.snapshot()
    journal.path.chmod(0o600)
    with sqlite3.connect(journal.path) as db:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            db.execute("DELETE FROM events")
        db.execute("DROP TRIGGER retain_update")
        db.execute("UPDATE events SET digest=? WHERE seq=2", ("0" * 64,))
    with pytest.raises(PaperError, match="integrity"):
        journal.snapshot()


def test_launcher_excludes_all_parent_credentials_and_detaches(service, monkeypatch):
    journal, plan, *_ = service
    monkeypatch.setenv("NVIDIA_API_KEY", "must-not-enter-worker")
    monkeypatch.setenv("LIQUID_ACCESS_TOKEN", "paper-key-must-not-enter-worker")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "service-key-must-not-enter-worker")
    calls = []
    monkeypatch.setattr(
        module.subprocess, "Popen", lambda *args, **kwargs: calls.append((args, kwargs))
    )
    launch(journal)
    args, options = calls[0]
    assert args[0] == [
        sys.executable,
        "-I",
        "-m",
        "stratosphere.paper.service",
        "run",
        str(plan.directory),
    ]
    assert options["env"] == {"PATH": os.defpath, "LANG": "C.UTF-8", "PYTHONUNBUFFERED": "1"}
    assert options["stdin"] == subprocess.DEVNULL
    assert options["close_fds"] and options["start_new_session"]
    assert Path(options["cwd"]) == plan.directory


def test_direct_worker_cli_rejects_inherited_credentials_before_starting(
    service, monkeypatch, capsys
):
    journal, plan, *_ = service
    before = journal.path.read_bytes()
    monkeypatch.setenv("LIQUID_ACCESS_TOKEN", "must-not-start-a-worker")
    monkeypatch.setattr(sys, "argv", ["service", "run", str(plan.directory)])

    def forbidden(*args):
        raise AssertionError("No worker can start with inherited credentials")

    monkeypatch.setattr(module, "_run", forbidden)
    with pytest.raises(SystemExit) as caught:
        module.main()
    assert caught.value.code == 1
    assert "must-not-start-a-worker" not in capsys.readouterr().err
    assert journal.path.read_bytes() == before and not probe_owner(plan)


def _spawn(code, *arguments):
    return subprocess.Popen(
        [sys.executable, "-I", "-c", code, *map(str, arguments)],
        env={"PATH": os.defpath, "LANG": "C.UTF-8"},
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _ready(process):
    # Readiness is emitted only after acquiring the actual OS lock.
    import select

    readable, _, _ = select.select([process.stdout], [], [], 15)
    assert readable, "The actual child did not report readiness"
    assert process.stdout.readline().strip() == "owned"
    assert process.poll() is None


def test_two_real_processes_sigkill_recovery_and_status_probe(service):
    journal, plan, *_ = service
    code = """
import sys,time
from stratosphere.paper.service import ServiceJournal,ObservationWorker
with ObservationWorker(ServiceJournal(sys.argv[1])).own():
 print('owned',flush=True)
 while True: time.sleep(1)
"""
    owner = _spawn(code, plan.directory)
    try:
        _ready(owner)
        assert probe_owner(plan)
        contender = _spawn(code, plan.directory)
        _, stderr = contender.communicate(timeout=15)
        assert contender.returncode != 0 and "Another observation worker" in stderr
        owner.kill()
        owner.communicate(timeout=15)
        assert owner.returncode == -signal.SIGKILL
        assert not probe_owner(plan)
        # Neither a retained run ID nor a recent timestamp proves a live owner.
        assert journal.snapshot()["run_id"] is not None
        replacement = _spawn(code, plan.directory)
        try:
            _ready(replacement)
            state = journal.snapshot()
            assert len(state["interrupted_runs"]) == 1
            assert probe_owner(plan)
        finally:
            replacement.kill()
            replacement.communicate(timeout=15)
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.communicate(timeout=15)


def test_real_crash_after_account_commit_replays_without_another_crossing(service, market_bars):
    journal, plan, now, runtime = service
    # Align the service clock with child wall clocks; candle times remain authored evidence.
    now[0] = time.time_ns()
    queued = [delivery(plan, bar) for bar in market_bars[:24]]
    for item in queued:
        journal.enqueue(item)
    code = """
import asyncio,os,sys
from stratosphere.paper.service import ServiceJournal,ObservationWorker
async def online(): pass
worker=ObservationWorker(ServiceJournal(sys.argv[1]),online_check=online)
def crash(): os._exit(73)
worker._after_ingest=crash
with worker.own(): asyncio.run(worker.tick())
"""
    crash = _spawn(code, plan.directory)
    _, stderr = crash.communicate(timeout=20)
    assert crash.returncode == 73, stderr
    assert not probe_owner(plan)
    assert runtime.inspect(plan.streams[0].instance_id)["accepted_count"] == 1
    assert not journal.snapshot()["results"]
    code = """
import asyncio,sys
from stratosphere.paper.service import ServiceJournal,ObservationWorker
async def online(): pass
async def work():
 with ObservationWorker(ServiceJournal(sys.argv[1]),online_check=online).own() as worker:
  for _ in range(24): print(await worker.tick(),flush=True)
asyncio.run(work())
"""
    restarted = _spawn(code, plan.directory)
    stdout, stderr = restarted.communicate(timeout=30)
    assert restarted.returncode == 0, stderr
    assert stdout.splitlines() == ["duplicate"] + ["accepted"] * 23
    state = journal.snapshot()
    stream = runtime.inspect(plan.streams[0].instance_id)
    assert stream["accepted_count"] == len(state["results"]) == 24
    assert len({row["signal_sha256"] for row in stream["signals"]}) == len(stream["signals"])
    assert not state["faults"] and len(state["interrupted_runs"]) == 1
    assert state["progress"][plan.streams[0].instance_id] > now[0]


def test_real_worker_loop_heartbeats_and_halts_on_wall_clock_silence(service):
    journal, plan, *_ = service
    code = """
import asyncio,sys
from stratosphere.paper.service import ServiceJournal,ObservationWorker
async def online(): pass
async def work():
 journal=ServiceJournal(sys.argv[1])
 # Plan creation time is historical in this fixture, but the first start sets the grace clock.
 await ObservationWorker(journal,online_check=online).run(asyncio.Event())
asyncio.run(work())
"""
    child = _spawn(code, plan.directory)
    _, stderr = child.communicate(timeout=20)
    assert child.returncode == 0, stderr
    state = journal.snapshot()
    assert state["faults"][-1]["reason"] == "silence_exceeded"
    assert state["last_heartbeat_ns"] > next(iter(state["progress"].values()))
    assert state["run_id"] is None and not probe_owner(plan)
    assert not state["results"]
    # Polling at 20 Hz retained roughly one heartbeat per second, not one per poll.
    with sqlite3.connect(journal.path) as db:
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] < 20
