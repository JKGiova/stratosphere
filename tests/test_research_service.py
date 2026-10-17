import asyncio
import json

import pytest

from stratosphere.models.streaming import ModelError
from stratosphere.research.service import ResearchService
from stratosphere.research.store import ResearchError, ResearchStore


def fake_native(plan, bars, multiplier):
    # No performance is inferred by the service. A controlled fake isolates job lifecycle tests.
    return {
        "engine_version": "1.231.0",
        "cost_multiplier": multiplier,
        "signals": [],
        "fills": [],
        "equity_curve": [
            {
                "timestamp_ns": bar.close_ns,
                "cash": "10000",
                "base": "0",
                "close": bar.close,
                "equity": "10000",
            }
            for bar in bars
        ],
        "pending_signal": None,
        "open_positions": 0,
        "balances": {"USDT": "10000", "BTC": "0"},
    }


@pytest.fixture
def service(tmp_path, research_snapshot):
    calls = []

    async def fetcher():
        calls.append("fetch")
        return research_snapshot

    async def runner(store, job_id, multiplier):
        calls.append(multiplier)
        plan = store.plan(job_id)
        return {
            "plan_sha256": plan.digest,
            "normalized_sha256": research_snapshot.normalized_sha256,
            "result": fake_native(plan, research_snapshot.bars, multiplier),
        }

    result = ResearchService(
        ResearchStore(tmp_path / "private-research"), fetcher=fetcher, runner=runner
    )
    result.test_calls = calls
    return result


async def prepare(service, source, provider, config=None):
    from stratosphere.research.contracts import ResearchConfig

    return await service.prepare(source, config or ResearchConfig(), provider)


@pytest.mark.asyncio
async def test_approval_is_required_exact_idempotent_and_evidence_is_private_immutable(
    service,
    research_source,
    research_provider,
):
    job, draft, plan = await prepare(service, research_source, research_provider)
    assert service.store.state(job) == "awaiting_approval" and service.test_calls == []
    with pytest.raises(ResearchError):
        await service.run(job)
    with pytest.raises(ResearchError):
        service.approve(job, "0" * 64)
    service.approve(job, plan.digest)
    with pytest.raises(ResearchError):
        service.approve(job, plan.digest)
    report = await service.run(job)
    assert service.test_calls == ["fetch", 1, 2]
    assert service.store.state(job) == "completed"
    assert service.report(job) == report
    assert report["plan_sha256"] == plan.digest
    assert service.store.read(job, "source.json")["text"] == research_source
    assert len(research_provider.requests) == 1
    with pytest.raises(ResearchError):
        await service.run(job)
    with pytest.raises(FileExistsError):
        service.store.write(job, "plan.json", {"overwritten": True})
    for path in service.store.directory(job).iterdir():
        if path.is_file():
            assert path.stat().st_mode & 0o077 == 0
    assert service.store.directory(job).stat().st_mode & 0o077 == 0
    with pytest.raises(ResearchError):
        service.store.directory("../outside")


@pytest.mark.asyncio
async def test_questions_and_model_failures_retain_attempts_without_running(
    service,
    research_source,
    research_provider,
    research_draft,
):
    payload = research_draft.model_dump(mode="json")
    payload.update(status="needs_clarification", quantity=None, questions=["What BTC quantity?"])
    research_provider.reply = json.dumps(payload)
    job, draft, plan = await prepare(service, research_source, research_provider)
    assert plan is None and service.store.state(job) == "needs_clarification"
    with pytest.raises(ResearchError):
        service.approve(job, "0" * 64)
    research_provider.reply = "not JSON"
    with pytest.raises(ModelError):
        await prepare(service, research_source, research_provider)
    assert len(service.store.jobs()) == 2 and service.test_calls == []
    assert all((service.store.directory(j) / "source.json").exists() for j in service.store.jobs())


@pytest.mark.asyncio
async def test_pause_resume_keeps_completed_checkpoint_and_does_not_rerun_model_or_base(
    service,
    research_source,
    research_provider,
):
    job, _, plan = await prepare(service, research_source, research_provider)
    service.approve(job, plan.digest)
    original = service.runner
    entered = asyncio.Event()

    async def slow_runner(store, job_id, multiplier):
        if multiplier == 2:
            entered.set()
            await asyncio.Event().wait()
        return await original(store, job_id, multiplier)

    service.runner = slow_runner
    task = asyncio.create_task(service.run(job))
    await asyncio.wait_for(entered.wait(), 2)
    service.stop_state = "paused"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert service.store.state(job) == "paused"
    base = (service.store.directory(job) / "base.json").read_bytes()
    service.runner = original
    await service.run(job)
    assert service.test_calls == ["fetch", 1, "fetch", 2]
    assert len(research_provider.requests) == 1
    assert (service.store.directory(job) / "base.json").read_bytes() == base


@pytest.mark.asyncio
async def test_cancel_and_download_failure_never_create_reports(
    service, research_source, research_provider
):
    job, _, plan = await prepare(service, research_source, research_provider)
    service.approve(job, plan.digest)
    entered = asyncio.Event()

    async def download_wait():
        entered.set()
        await asyncio.Event().wait()

    service.fetcher = download_wait
    task = asyncio.create_task(service.run(job))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert service.store.state(job) == "canceled"
    assert not (service.store.directory(job) / "report.json").exists()
    next_job, _, next_plan = await prepare(service, research_source, research_provider)
    service.approve(next_job, next_plan.digest)

    async def fail():
        raise ValueError("Offline")

    service.fetcher = fail
    with pytest.raises(ValueError):
        await service.run(next_job)
    assert service.store.state(next_job) == "failed"
    assert service.test_calls == []


@pytest.mark.asyncio
async def test_plan_source_and_checkpoint_tampering_fail_closed(
    service, research_source, research_provider
):
    job, _, plan = await prepare(service, research_source, research_provider)
    directory = service.store.directory(job)
    (directory / "source.json").write_text(json.dumps({"text": "changed source"}))
    with pytest.raises(ResearchError, match="Source integrity"):
        service.approve(job, plan.digest)
    (directory / "source.json").write_text(json.dumps({"text": research_source}))
    service.approve(job, plan.digest)
    (directory / "approval.json").write_text(json.dumps({"plan_sha256": "0" * 64}))
    with pytest.raises(ResearchError, match="integrity"):
        await service.run(job)
    assert service.test_calls == []


@pytest.mark.asyncio
async def test_lock_and_recovery_do_not_duplicate_an_approved_run(
    service, research_source, research_provider
):
    job, _, plan = await prepare(service, research_source, research_provider)
    service.approve(job, plan.digest)
    service.store.event(job, "running")
    with service.store.lock(job):
        service.recover()
        assert service.store.state(job) == "running"
        with pytest.raises(ResearchError, match="another client"):
            with service.store.lock(job):
                pass
    service.recover()
    assert service.store.state(job) == "paused"
    await service.run(job)
    assert service.test_calls == ["fetch", 1, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["success", "cancel", "timeout"])
async def test_subprocess_receives_no_credentials_and_is_killed_on_interruption(
    service,
    research_source,
    research_provider,
    monkeypatch,
    ending,
):
    from stratosphere.research.service import run_worker

    job, _, plan = await prepare(service, research_source, research_provider)
    captured = {}
    entered = asyncio.Event()

    class Process:
        returncode = None
        killed = False
        released = asyncio.Event()

        async def wait(self):
            entered.set()
            if ending == "success":
                self.returncode = 0
            elif self.returncode is None:
                await self.released.wait()
            return self.returncode

        def kill(self):
            self.killed = True
            self.returncode = -9
            self.released.set()

    process = Process()

    async def create(*args, **kwargs):
        captured.update(args=args, options=kwargs)
        output = args[args.index("--result") + 1]
        if ending == "success":
            from pathlib import Path

            Path(output).write_text(json.dumps({"plan_sha256": plan.digest}))
        return process

    monkeypatch.setenv("OPENROUTER_API_KEY", "must-not-reach-worker")
    monkeypatch.setenv("NVIDIA_API_KEY", "also-private")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "private-service-credential")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    if ending == "timeout":
        original = asyncio.timeout
        monkeypatch.setattr(asyncio, "timeout", lambda _: original(0.01))
    task = asyncio.create_task(run_worker(service.store, job, 1))
    await entered.wait()
    if ending == "cancel":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    elif ending == "timeout":
        with pytest.raises(ResearchError, match="technical timeout"):
            await task
    else:
        assert (await task)["plan_sha256"] == plan.digest
    assert all(
        key not in captured["options"]["env"]
        for key in (
            "OPENROUTER_API_KEY",
            "NVIDIA_API_KEY",
            "SUPABASE_SERVICE_ROLE_KEY",
            "PYTHONPATH",
        )
    )
    assert not any(
        secret in json.dumps(captured["options"]["env"])
        for secret in ("must-not-reach-worker", "also-private", "private-service-credential")
    )
    assert "stratosphere.research.worker" in captured["args"]
    if ending != "success":
        assert process.killed and process.returncode == -9


@pytest.mark.asyncio
async def test_completed_report_integrity_changes_are_detected(
    service, research_source, research_provider
):
    job, _, plan = await prepare(service, research_source, research_provider)
    service.approve(job, plan.digest)
    await service.run(job)
    directory = service.store.directory(job)
    (directory / "report.json").write_text(json.dumps({"fabricated": "result"}))
    with pytest.raises(ResearchError, match="integrity"):
        service.report(job)


@pytest.mark.asyncio
async def test_preparation_holds_a_lock_and_cancellation_keeps_the_source(
    service, research_source, research_provider
):
    entered = asyncio.Event()

    async def waiting(messages):
        entered.set()
        await asyncio.Event().wait()
        yield ""

    research_provider.stream = waiting
    task = asyncio.create_task(prepare(service, research_source, research_provider))
    await entered.wait()
    job = service.store.jobs()[0]
    service.recover()
    assert service.store.state(job) == "extracting"
    with pytest.raises(ResearchError, match="active"):
        await prepare(service, research_source, research_provider)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert service.store.state(job) == "canceled"
    assert service.store.read(job, "source.json")["text"] == research_source
