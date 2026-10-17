"""Shared operations for research commands: propose, approve, pause/resume, cancel and report."""

import asyncio
import hashlib
import json
import os
import sys
import uuid

from stratosphere.research.contracts import (
    ResearchConfig,
    ResearchPlan,
    build_spec,
    implementation_digest,
    implementation_sources,
)
from stratosphere.research.data import bars_digest, fetch_snapshot, require_online
from stratosphere.research.extraction import extract_strategy
from stratosphere.research.metrics import build_report, report_markdown
from stratosphere.research.store import ResearchError, ResearchStore


async def run_worker(store: ResearchStore, job_id: str, multiplier: int) -> dict:
    directory = store.directory(job_id).resolve()
    # Each subprocess attempt has a new output path; failures/cancellation never replace artifacts.
    output = directory / f"worker-{uuid.uuid7().hex}.json"
    environment = {
        "PATH": os.defpath,
        "HOME": str(directory),
        "PYTHONUTF8": "1",
        "PYTHONNOUSERSITE": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
    }
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "stratosphere.research.worker",
        "--request",
        str(directory / "request.json"),
        "--result",
        str(output),
        "--multiplier",
        str(multiplier),
        cwd=directory,
        env=environment,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        async with asyncio.timeout(120):
            await process.wait()
        if not output.exists():
            raise ResearchError("The research worker stopped without a complete result.")
        response = json.loads(output.read_text())
        if process.returncode or "error" in response:
            if response.get("error") == "engine_unavailable":
                raise ResearchError(
                    "Install the pinned NautilusTrader engine in this Python environment."
                )
            raise ResearchError(
                "The simulation failed. Check sizing/capital and the pinned engine setup."
            )
        return response
    except TimeoutError:
        raise ResearchError(
            "The worker exceeded its 120-second technical timeout; no complete result."
        ) from None
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


class ResearchService:
    def __init__(
        self,
        store: ResearchStore,
        *,
        fetcher=fetch_snapshot,
        runner=run_worker,
        online_check=require_online,
    ):
        self.store = store
        self.fetcher = fetcher
        self.runner = runner
        self.online_check = online_check
        self.active_job: str | None = None
        self.stop_state = "canceled"
        self.preparing = False

    def recover(self) -> None:
        for job_id in self.store.jobs():
            if self.store.state(job_id) in {"running", "extracting"}:
                try:
                    with self.store.lock(job_id):
                        approved = (self.store.directory(job_id) / "approval.json").exists()
                        self.store.event(
                            job_id, "paused" if approved else "failed", "Client interrupted"
                        )
                except ResearchError:
                    continue

    async def prepare(self, source: str, config: ResearchConfig, provider):
        if self.active_job or self.preparing:
            raise ResearchError("A research operation is already active.")
        if not source.strip():
            raise ResearchError("Paste a strategy description before starting research.")
        job_id = self.store.create(source)
        self.preparing = True
        try:
            with self.store.lock(job_id):
                return await self._prepare(job_id, source, config, provider)
        finally:
            self.preparing = False

    async def _prepare(self, job_id, source, config, provider):
        try:
            draft = await extract_strategy(provider, source)
            self.store.write(job_id, "draft.json", draft.model_dump(mode="json"))
            if draft.status != "ready":
                self.store.event(job_id, draft.status)
                return job_id, draft, None
            plan = ResearchPlan(
                source_sha256=hashlib.sha256(source.encode()).hexdigest(),
                draft=draft,
                specification=build_spec(draft),
                config=config,
                implementation_sha256=implementation_digest(),
            )
            self.store.write(job_id, "plan.json", plan.model_dump(mode="json"))
            self.store.write(
                job_id,
                "implementation.json",
                {
                    "sha256": plan.implementation_sha256,
                    "modules": implementation_sources(),
                    "engine_version": plan.engine_version,
                },
            )
            self.store.event(job_id, "awaiting_approval")
            return job_id, draft, plan
        except asyncio.CancelledError:
            self.store.event(job_id, "canceled", "Extraction canceled")
            raise
        except Exception:
            self.store.event(job_id, "failed", "Specification preparation failed")
            raise

    def approve(self, job_id: str, plan_sha256: str) -> None:
        with self.store.lock(job_id):
            plan = self.store.plan(job_id)
            if self.store.state(job_id) != "awaiting_approval":
                raise ResearchError(
                    "This plan is not awaiting approval; duplicate starts are blocked."
                )
            if plan.digest != plan_sha256 or plan.implementation_sha256 != implementation_digest():
                raise ResearchError(
                    "The plan or implementation changed; prepare and review a new plan."
                )
            self.store.write(job_id, "approval.json", {"plan_sha256": plan.digest})
            self.store.event(job_id, "paused", "Approved; ready to execute")

    async def run(self, job_id: str, *, progress=None) -> dict:
        if self.active_job or self.preparing:
            raise ResearchError("A research job is already active. Pause or cancel it first.")
        with self.store.lock(job_id):
            plan = self.store.plan(job_id)
            if self.store.state(job_id) != "paused":
                raise ResearchError("Only an approved paused job can run or resume.")
            if self.store.read(job_id, "approval.json")["plan_sha256"] != plan.digest:
                raise ResearchError("Approved plan integrity check failed.")
            if plan.implementation_sha256 != implementation_digest():
                raise ResearchError("Implementation changed; create and approve a new plan.")
            self.active_job, self.stop_state = job_id, "canceled"
            self.store.event(job_id, "running", "Downloading and validating the approved archive")

            async def update(message):
                if progress:
                    await progress(job_id, message)

            try:
                await update("Downloading the approved BTC/USDT archive")
                snapshot = await self.fetcher()
                if hashlib.sha256(snapshot.archive).hexdigest() != plan.archive_sha256:
                    raise ResearchError("Downloaded snapshot differs from the approved archive.")
                if bars_digest(snapshot.bars) != snapshot.normalized_sha256:
                    raise ResearchError("Normalized input integrity check failed.")
                request = {
                    "plan": plan.model_dump(mode="json"),
                    "bars": [bar.model_dump(mode="json") for bar in snapshot.bars],
                    "normalized_sha256": snapshot.normalized_sha256,
                }
                directory = self.store.directory(job_id)
                if (directory / "request.json").exists():
                    if self.store.read(job_id, "request.json") != request:
                        raise ResearchError("Downloaded data differs from the retained checkpoint.")
                else:
                    self.store.write(job_id, "archive.zip", snapshot.archive)
                    self.store.write(job_id, "request.json", request)
                results = []
                for name, multiplier in [("base", 1), ("stress", 2)]:
                    await update(f"Running {name} simulation in NautilusTrader")
                    if (directory / f"{name}.json").exists():
                        response = self.store.read(job_id, f"{name}.json")
                    else:
                        response = await self.runner(self.store, job_id, multiplier)
                        self.store.write(job_id, f"{name}.json", response)
                    if response["plan_sha256"] != plan.digest or response[
                        "normalized_sha256"
                    ] != bars_digest(snapshot.bars):
                        raise ResearchError("Worker result integrity mismatch.")
                    results.append(response["result"])
                report = build_report(plan, snapshot.bars, snapshot.normalized_sha256, *results)
                if (directory / "report.json").exists():
                    if self.store.read(job_id, "report.json") != report:
                        raise ResearchError("Retained report integrity mismatch.")
                else:
                    self.store.write(job_id, "report.json", report)
                if not (directory / "report.md").exists():
                    self.store.write(job_id, "report.md", report_markdown(report))
                self.store.seal(job_id)
                self.store.event(job_id, "completed")
                await update("Completed; report and immutable evidence saved")
                return report
            except asyncio.CancelledError:
                self.store.event(job_id, self.stop_state, "Completed checkpoints retained")
                raise
            except Exception:
                self.store.event(job_id, "failed", "Run failed; completed evidence retained")
                raise
            finally:
                self.active_job = None

    async def require_online(self) -> None:
        try:
            await self.online_check()
        except ValueError:
            raise ResearchError(
                "Research inspection requires online access; retry when connected."
            ) from None

    async def open_report(self, job_id: str) -> dict:
        await self.require_online()
        return self.report(job_id)

    def report(self, job_id: str) -> dict:
        if self.store.state(job_id) != "completed":
            raise ResearchError("This job has no completed report.")
        self.store.verify_seal(job_id)
        return self.store.read(job_id, "report.json")


def plan_markdown(job_id: str, plan: ResearchPlan) -> str:
    draft, config = plan.draft, plan.config
    lines = [
        f"# Review research plan: {draft.name}",
        "",
        f"Job `{job_id}`",
        "",
        f"BTCUSDT spot · 1-hour candles · SMA {draft.fast_period}/{draft.slow_period}",
        f"Buy the fast/slow cross above; close on the cross below; fixed {draft.quantity} BTC.",
        "",
        "January 1–31, 2025 · 744 candles · Binance public archive",
        "",
        f"Starting balance: {config.initial_balance} USDT. "
        f"Fee: {config.fee_rate} per filled order.",
        f"Spread: {config.spread_bps} bps. Slippage: {config.slippage_bps} bps per side.",
        "",
        "## Source evidence",
        "",
    ]
    lines += [f"- **{item.field}**: {item.quote}" for item in draft.evidence]
    lines += ["", "## Assumptions and limits", ""]
    lines += [f"- {item}" for item in plan.assumptions]
    lines += [
        "",
        "The source and local draft remain private; shared registration is not implemented.",
        "",
        f"Approve this exact plan once: `/approve {job_id} {plan.digest}`",
        "Or revise it with `/new`, or cancel it with `/cancel " + job_id + "`.",
    ]
    return "\n".join(lines)
