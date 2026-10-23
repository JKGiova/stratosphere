"""Acquire once, review exact dataset/rules, then consume two finite native starts."""

import asyncio
import hashlib
import json
import os
import sys
import tempfile
import uuid
from datetime import UTC, datetime

import httpx

from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.yahoo.contracts import (
    YahooPlan,
    YahooRequest,
    YahooSnapshot,
    implementation_digest,
)
from stratosphere.yahoo.data import YahooDataError, YahooValidationError, encoded, normalize
from stratosphere.yahoo.metrics import summarize_yahoo


async def require_online():
    """Connectivity only; never a quote or replacement historical snapshot."""
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=False, trust_env=False) as client:
            response = await client.get("https://finance.yahoo.com/robots.txt")
            response.raise_for_status()
    except httpx.HTTPError:
        raise ResearchError("Online access is required for Yahoo research.") from None


async def worker(directory, action, input_name, *, multiplier=1):
    # The parent owns cleanup, including when acquisition is canceled and the child is killed.
    with tempfile.TemporaryDirectory(prefix="stratosphere-yahoo-") as cache:
        return await _worker(directory, action, input_name, multiplier, cache)


async def _worker(directory, action, input_name, multiplier, cache):
    output = directory / ("worker_" + uuid.uuid7().hex + ".json")
    # Deliberately do not inherit model keys, Liquid grants, proxies or user Python configuration.
    env = {
        "PATH": os.defpath,
        "HOME": cache,
        "TMPDIR": cache,
        "PYTHONUTF8": "1",
        "PYTHONNOUSERSITE": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
    }
    spawning = asyncio.create_task(
        asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "stratosphere.yahoo.worker",
            action,
            "--input",
            str(directory / input_name),
            "--output",
            str(output),
            "--multiplier",
            str(multiplier),
            "--cache",
            cache,
            cwd=directory,
            env=env,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
    )
    try:
        process = await asyncio.shield(spawning)
    except asyncio.CancelledError:
        # Even cancellation during spawn must acquire and terminate the child before cache cleanup.
        while not spawning.done():
            try:
                await asyncio.shield(spawning)
            except asyncio.CancelledError:
                continue
        process = spawning.result()
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise
    try:
        async with asyncio.timeout(180):
            await process.wait()
        if process.returncode or not output.is_file():
            raise ResearchError("Yahoo operation failed. Check history access, data and sizing.")
        return json.loads(output.read_text())
    except TimeoutError:
        raise ResearchError("Yahoo operation exceeded 180 seconds; no automatic retry.") from None
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


class YahooBacktests:
    def __init__(self, root, database, *, runner=worker, online_check=require_online):
        self.store = ResearchStore(root)
        self.database, self.runner, self.online_check = database, runner, online_check
        self.active_job = None
        self.preparing = False
        self.stop_state = "canceled"

    async def require_online(self):
        await self.online_check()

    def recover(self):
        for job in self.store.jobs():
            try:
                with self.store.lock(job):
                    if self.store.state(job) in {"fetching", "running"}:
                        self.store.event(
                            job, "interrupted", "No automatic download or native replay"
                        )
            except ResearchError:
                continue  # Another client still owns the operation; startup cannot interrupt it.

    async def prepare(self, request: YahooRequest):
        if self.active_job or self.preparing:
            raise ResearchError("A Yahoo operation is already active.")
        self.store.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        job = uuid.uuid7().hex
        self.store.directory(job).mkdir(mode=0o700)
        self.store.write(job, "request.json", request.model_dump(mode="json"))
        self.store.event(job, "fetching")
        self.preparing = True
        try:
            with self.store.lock(job):
                raw = await self.runner(self.store.directory(job), "fetch", "request.json")
                self.store.write(job, "returned_frame.json", raw)
                snapshot = normalize(raw, request)
                self.store.write(job, "snapshot.json", snapshot.model_dump(mode="json"))
                record = self.database.register(
                    request.specification(),
                    f"Yahoo {request.symbol} SMA {request.fast}/{request.slow}",
                    {
                        "kind": "yahoo-price-research/1",
                        "snapshot_sha256": snapshot.digest,
                        "request": request.model_dump(mode="json"),
                    },
                )
                plan = YahooPlan(
                    request=request,
                    snapshot_sha256=snapshot.digest,
                    strategy_sha256=record["spec_sha256"],
                    strategy_id=record["public_id"],
                    implementation_sha256=implementation_digest(),
                )
                self.store.write(job, "plan.json", plan.model_dump(mode="json"))
                self.store.write(
                    job,
                    "simulation.json",
                    {
                        "plan": plan.model_dump(mode="json"),
                        "snapshot": snapshot.model_dump(mode="json"),
                    },
                )
                self.store.event(job, "awaiting_approval")
                return job, plan
        except asyncio.CancelledError:
            self.store.event(job, "canceled", "Retained acquisition was not retried")
            raise
        except YahooDataError as error:
            self.store.write(
                job,
                "quality.json",
                {
                    "code": "invalid_ohlcv",
                    "timestamp_ns": error.timestamp_ns,
                },
            )
            self.store.event(job, "failed", "Invalid Yahoo OHLCV; no price repair or simulation")
            raise ResearchError(
                "Yahoo returned invalid OHLCV observations. See retained quality.json; "
                "no backtest was started."
            ) from None
        except YahooValidationError as error:
            self.store.write(
                job, "quality.json", {"code": "validation_blocked", "detail": str(error)}
            )
            self.store.event(job, "failed", "Yahoo validation blocked this window")
            raise ResearchError(str(error)) from None
        except Exception:
            self.store.event(job, "failed", "Acquisition/validation failed; evidence retained")
            raise ResearchError(
                "Yahoo acquisition or validation failed; evidence retained."
            ) from None
        finally:
            self.preparing = False

    def plan(self, job):
        directory = self.store.directory(job)
        if directory.is_symlink() or any(p.is_symlink() for p in directory.iterdir()):
            raise ResearchError("Yahoo evidence contains a redirected path")
        plan = YahooPlan.model_validate(self.store.read(job, "plan.json"))
        raw = self.store.read(job, "returned_frame.json")
        snapshot = YahooSnapshot.model_validate(self.store.read(job, "snapshot.json"))
        if (
            snapshot.digest != plan.snapshot_sha256
            or (hashlib.sha256(encoded(raw)).hexdigest() != snapshot.returned_frame_sha256)
            or normalize(raw, plan.request) != snapshot
        ):
            raise ResearchError("Yahoo retained dataset integrity check failed")
        if self.database.strategy(plan.strategy_id)["spec_sha256"] != plan.strategy_sha256:
            raise ResearchError("Yahoo owner identity changed")
        expected = {
            "plan": plan.model_dump(mode="json"),
            "snapshot": snapshot.model_dump(mode="json"),
        }
        if self.store.read(job, "simulation.json") != expected:
            raise ResearchError("Yahoo simulation input changed")
        return plan

    def approve(self, job, digest):
        with self.store.lock(job):
            plan = self.plan(job)
            if (
                self.store.state(job) != "awaiting_approval"
                or digest != plan.digest
                or (plan.implementation_sha256 != implementation_digest())
            ):
                raise ResearchError("Review the unchanged, pending Yahoo plan before approval")
            self.store.write(job, "approval.json", {"plan_sha256": digest})
            self.store.event(job, "approved")

    async def run(self, job):
        if self.active_job or self.preparing:
            raise ResearchError("A Yahoo operation is already active")
        with self.store.lock(job):
            plan = self.plan(job)
            if (
                self.store.state(job) != "approved"
                or (self.store.read(job, "approval.json")["plan_sha256"] != plan.digest)
                or plan.implementation_sha256 != implementation_digest()
            ):
                raise ResearchError("The exact Yahoo plan must be approved once before execution")
            self.active_job = job
            self.store.event(job, "running")
            try:
                await self.require_online()
                results = {}
                for name, multiplier in (("base", 1), ("stress", 2)):
                    # Consumption precedes dispatch; interruption cannot replay this native start.
                    self.store.write(job, name + "_started.json", {"plan_sha256": plan.digest})
                    result = await self.runner(
                        self.store.directory(job),
                        "simulate",
                        "simulation.json",
                        multiplier=multiplier,
                    )
                    if (
                        result.get("cost_multiplier") != multiplier
                        or result.get("engine_version") != plan.engine_version
                        or result.get("plan_sha256") != plan.digest
                        or result.get("snapshot_sha256") != plan.snapshot_sha256
                    ):
                        raise ResearchError("Native Yahoo response identity mismatch")
                    self.store.write(job, name + ".json", result)
                    results[name] = summarize_yahoo(result, plan.request.config.initial_balance)
                snapshot = YahooSnapshot.model_validate(self.store.read(job, "snapshot.json"))
                report = {
                    "schema_version": "yahoo-backtest-report/1",
                    "currency": snapshot.currency,
                    "observations": len(snapshot.bars),
                    "gap_count": len(snapshot.gaps),
                    "job_id": job,
                    "plan_sha256": plan.digest,
                    "snapshot_sha256": plan.snapshot_sha256,
                    "strategy_id": plan.strategy_id,
                    "strategy_sha256": plan.strategy_sha256,
                    "request": plan.request.model_dump(mode="json"),
                    "metrics": results,
                    "limitations": plan.limitations,
                    "created_at": datetime.now(UTC).isoformat(),
                }
                self.store.write(job, "report.json", report)
                self.store.write(job, "report.md", report_markdown(report))
                files = (
                    "request.json",
                    "returned_frame.json",
                    "snapshot.json",
                    "plan.json",
                    "simulation.json",
                    "approval.json",
                    "base_started.json",
                    "stress_started.json",
                    "base.json",
                    "stress.json",
                    "report.json",
                    "report.md",
                )
                self.store.write(
                    job,
                    "manifest.json",
                    {
                        name: hashlib.sha256(
                            (self.store.directory(job) / name).read_bytes()
                        ).hexdigest()
                        for name in files
                    },
                )
                self.store.event(job, "completed")
                return self.report(job)
            except asyncio.CancelledError:
                self.store.event(job, "canceled", "Consumed starts cannot replay")
                raise
            except Exception:
                self.store.event(job, "failed", "Consumed starts cannot replay")
                raise
            finally:
                self.active_job = None

    def report(self, job):
        if self.store.state(job) != "completed":
            raise ResearchError("No completed Yahoo report is available")
        self.plan(job)
        manifest = self.store.read(job, "manifest.json")
        if set(manifest) != {
            "request.json",
            "returned_frame.json",
            "snapshot.json",
            "plan.json",
            "simulation.json",
            "approval.json",
            "base_started.json",
            "stress_started.json",
            "base.json",
            "stress.json",
            "report.json",
            "report.md",
        }:
            raise ResearchError("Yahoo evidence manifest is incomplete")
        for name, digest in manifest.items():
            if name not in {
                "request.json",
                "returned_frame.json",
                "snapshot.json",
                "plan.json",
                "simulation.json",
                "approval.json",
                "base_started.json",
                "stress_started.json",
                "base.json",
                "stress.json",
                "report.json",
                "report.md",
            }:
                raise ResearchError("Invalid Yahoo evidence manifest")
            path = self.store.directory(job) / name
            if path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                raise ResearchError("Yahoo report evidence changed")
        report = self.store.read(job, "report.json")
        plan = self.plan(job)
        if report["plan_sha256"] != plan.digest or (
            report["snapshot_sha256"] != plan.snapshot_sha256
            or report["strategy_sha256"] != plan.strategy_sha256
            or report["strategy_id"] != plan.strategy_id
            or report["request"] != plan.request.model_dump(mode="json")
            or tuple(report["limitations"]) != plan.limitations
        ):
            raise ResearchError("Yahoo report ownership mismatch")
        return report


def plan_markdown(job, plan, snapshot):
    request = plan.request
    first = datetime.fromtimestamp(snapshot.bars[0].open_ns / 1e9, UTC).isoformat()
    last = datetime.fromtimestamp(snapshot.bars[-1].open_ns / 1e9, UTC).isoformat()
    return (
        f"# Yahoo backtest review: {request.symbol}\n\nJob `{job}` · strategy `{plan.strategy_id}`"
        f"\n\n{request.asset_class} · {request.interval} · "
        f"{request.start} to {request.end} (exclusive)"
        f"\n\n{len(snapshot.bars)} observations · {len(snapshot.gaps)} retained gaps · "
        f"{snapshot.currency}"
        f"\n\nObserved labels: {first} to {last}."
        " Session-calendar completeness is unverified."
        f"\n\nSMA {request.fast}/{request.slow}, fixed {request.quantity} units."
        f"\n\nCapital/costs: `{request.config.model_dump_json()}`"
        f"\n\nSnapshot `{plan.snapshot_sha256}` · two native starts."
        + "\n\n"
        + "\n".join("- " + item for item in plan.limitations)
        + f"\n\nApprove: `/yahoo-approve {job} {plan.digest}`"
    )


def report_markdown(report):
    request = report["request"]
    lines = [
        f"# Yahoo backtest: {request['symbol']}",
        f"Strategy `{report['strategy_id']}` · job `{report['job_id']}`",
        f"{request['start']} to {request['end']} (exclusive) · {request['interval']}",
        f"{report['observations']} observations · {report['gap_count']} retained gaps · "
        f"{report['currency']}",
        "| Scenario | Final equity | Net P&L | Return | Maximum drawdown | Filled orders | Fees |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, metrics in report["metrics"].items():
        lines.append(
            f"| {name} | {metrics['final_equity']} | {metrics['net_pnl']} | "
            f"{metrics['net_return_pct']}% | {metrics['max_drawdown_pct']}% | "
            f"{metrics['filled_orders']} | {metrics['fees']} |"
        )
    return (
        "\n\n".join(lines[:4])
        + "\n\n"
        + "\n".join(lines[4:])
        + "\n\n"
        + "\n".join("- " + item for item in report["limitations"])
    )
