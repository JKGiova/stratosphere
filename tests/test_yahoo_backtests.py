"""Historical-only Yahoo acquisition, exact approval and native-accounting checks."""

import asyncio
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal

import pytest

from stratosphere.phase0.contracts import fingerprint
from stratosphere.registry.workspace import WorkspaceDatabase
from stratosphere.research.store import ResearchError
from stratosphere.yahoo.contracts import YahooPlan, YahooRequest, implementation_digest
from stratosphere.yahoo.data import normalize
from stratosphere.yahoo.engine import run_simulation
from stratosphere.yahoo.service import YahooBacktests


@pytest.fixture
def req():
    return YahooRequest(
        symbol="BTC-USD",
        asset_class="crypto_spot",
        start="2025-01-01",
        end="2025-01-03",
        fast=3,
        slow=8,
        quantity="1",
    )


def raw_frame(req):
    start = datetime(2025, 1, 1, tzinfo=UTC)
    rows = []
    for i in range(48):
        close = 100 + ((i % 16) if i % 32 < 16 else 31 - i % 32)
        rows.append(
            {
                "timestamp_ns": int(
                    (start + timedelta(seconds=i * req.seconds)).timestamp() * 10**9
                ),
                "open": str(close),
                "high": str(close + 1),
                "low": str(close - 1),
                "close": str(close),
                "volume": "10",
                "dividends": "0",
                "splits": "0",
                "capital_gains": "0",
            }
        )
    return {
        "schema_version": "yfinance-returned-frame/1",
        "yfinance_version": "1.7.0",
        "request": req.model_dump(mode="json"),
        "observed_at_ns": int(
            datetime.combine(req.end + timedelta(days=1), datetime.min.time(), UTC).timestamp()
            * 10**9
        ),
        "metadata": {
            "symbol": req.symbol,
            "currency": "JPY" if req.symbol == "USDJPY=X" else "USD",
            "exchange": "fixture",
            "instrument_type": {
                "crypto_spot": "CRYPTOCURRENCY",
                "equity": "ETF" if req.symbol == "GLD" else "EQUITY",
                "forex": "CURRENCY",
            }[req.asset_class],
            "timezone": "UTC",
        },
        "rows": rows,
    }


async def online():
    pass


@pytest.fixture
def service(tmp_path, req):
    calls = []

    async def runner(directory, action, input_name, *, multiplier=1):
        import json

        calls.append(action)
        if action == "fetch":
            return raw_frame(req)
        payload = json.loads((directory / input_name).read_text())
        from stratosphere.yahoo.contracts import YahooSnapshot

        return run_simulation(
            YahooPlan.model_validate(payload["plan"]),
            YahooSnapshot.model_validate(payload["snapshot"]),
            multiplier=multiplier,
        )

    svc = YahooBacktests(
        tmp_path / "yahoo",
        WorkspaceDatabase(tmp_path / "library.sqlite3"),
        runner=runner,
        online_check=online,
    )
    svc.calls = calls
    return svc


@pytest.mark.parametrize(
    "mutation",
    ["duplicate", "reverse", "gap", "nan", "ohlc", "type", "action", "forming", "truncated"],
)
def test_bad_data_never_becomes_backtest_prices(req, mutation):
    raw = raw_frame(req)
    if mutation == "duplicate":
        raw["rows"][2] = raw["rows"][1]
    elif mutation == "reverse":
        raw["rows"].reverse()
    elif mutation == "gap":
        del raw["rows"][10]
    elif mutation == "nan":
        raw["rows"][0]["close"] = "nan"
    elif mutation == "ohlc":
        raw["rows"][0]["high"] = "1"
    elif mutation == "type":
        raw["metadata"]["instrument_type"] = "FUTURE"
    elif mutation == "action":
        raw["rows"][0]["dividends"] = "1"
    elif mutation == "forming":
        raw["observed_at_ns"] = raw["rows"][-1]["timestamp_ns"]
    else:
        del raw["rows"][:2]
    with pytest.raises((ValueError, ArithmeticError)):
        normalize(raw, req)


@pytest.mark.engine
@pytest.mark.parametrize("interval", ["1h", "1d"])
@pytest.mark.parametrize(
    "asset,symbol",
    [
        ("crypto_spot", "BTC-USD"),
        ("equity", "AAPL"),
        ("equity", "GLD"),
        ("forex", "EURUSD=X"),
        ("forex", "USDJPY=X"),
    ],
)
def test_native_prices_fills_and_equity_match_independent_ledger(req, asset, symbol, interval):
    req = YahooRequest.model_validate(
        req.model_dump()
        | {
            "symbol": symbol,
            "asset_class": asset,
            "interval": interval,
            "end": "2025-02-18" if interval == "1d" else "2025-01-03",
        }
    )
    snapshot = normalize(raw_frame(req), req)
    plan = YahooPlan(
        request=req,
        snapshot_sha256=snapshot.digest,
        strategy_id="1" * 24,
        strategy_sha256=fingerprint(req.specification()),
        implementation_sha256=implementation_digest(),
    )
    for multiplier in (1, 2):
        result = run_simulation(plan, snapshot, multiplier=multiplier)
        closes = [Decimal(bar.close) for bar in snapshot.bars]
        cash, held, pending = Decimal("10000"), Decimal(0), None
        previous = None
        curve, fills = [], []
        quantity = Decimal(req.quantity)
        cash_tick = Decimal("1") if snapshot.currency == "JPY" else Decimal("0.01")
        impact = Decimal(req.config.spread_bps) / 2 + Decimal(req.config.slippage_bps)
        impact = impact * multiplier / 10000
        for i, bar in enumerate(snapshot.bars):
            if pending:
                buying = pending == "BUY"
                price = (Decimal(bar.open) * (1 + impact if buying else 1 - impact)).quantize(
                    Decimal("0.00000001"), rounding=ROUND_CEILING if buying else ROUND_FLOOR
                )
                fee = (price * quantity * Decimal(req.config.fee_rate) * multiplier).quantize(
                    cash_tick, rounding=ROUND_HALF_UP
                )
                notional = (price * quantity).quantize(cash_tick, rounding=ROUND_HALF_UP)
                cash += -notional - fee if buying else notional - fee
                held += quantity if buying else -quantity
                fills.append((pending, price, fee))
                pending = None
            if i >= req.slow - 1:
                difference = (
                    sum(closes[i - req.fast + 1 : i + 1]) / req.fast
                    - sum(closes[i - req.slow + 1 : i + 1]) / req.slow
                )
                if previous is not None:
                    if not held and previous <= 0 < difference:
                        pending = "BUY"
                    elif held and previous >= 0 > difference:
                        pending = "SELL"
                previous = difference
            curve.append(cash + held * Decimal(bar.close))
        assert fills
        assert [
            (f["side"], Decimal(f["price"]), Decimal(f["commission"])) for f in result["fills"]
        ] == fills
        assert [Decimal(r["equity"]) for r in result["equity_curve"]] == curve


@pytest.mark.engine
async def test_review_native_report_reopen_and_no_replay(service, req):
    job, plan = await service.prepare(req)
    assert service.calls == ["fetch"]
    with pytest.raises(ResearchError):
        await service.run(job)
    with pytest.raises(ResearchError):
        service.approve(job, "0" * 64)
    service.approve(job, plan.digest)
    await service.run(job)
    assert service.calls == ["fetch", "simulate", "simulate"]
    reopened = YahooBacktests(service.store.root, service.database, online_check=online)
    assert reopened.report(job)["snapshot_sha256"] == plan.snapshot_sha256
    with pytest.raises(ResearchError):
        await service.run(job)
    assert len(service.database.catalog()) == 1
    path = service.store.directory(job) / "base.json"
    path.write_text("{}")
    with pytest.raises(ResearchError):
        service.report(job)


async def test_native_failure_consumes_slot_and_retains_evidence(service, req):
    job, plan = await service.prepare(req)

    async def failure(*args, **kwargs):
        raise ResearchError("Artificial native failure")

    service.runner = failure
    service.approve(job, plan.digest)
    with pytest.raises(ResearchError):
        await service.run(job)
    assert (service.store.directory(job) / "base_started.json").exists()
    assert service.store.state(job) == "failed"
    with pytest.raises(ResearchError):
        await service.run(job)


async def test_canceled_acquisition_retained_without_registering(service, req):
    entered = asyncio.Event()

    async def waiting(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    service.runner = waiting
    task = asyncio.create_task(service.prepare(req))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not service.database.catalog()
    assert service.store.state(service.store.jobs()[0]) == "canceled"
    assert not service.preparing


def test_gap_findings_preserve_original_rows(req):
    req = req.model_copy(update={"symbol": "AAPL", "asset_class": "equity"})
    raw = raw_frame(req)
    del raw["rows"][20]
    original = deepcopy(raw)
    snapshot = normalize(raw, req)
    assert raw == original and len(snapshot.gaps) == 1


@pytest.mark.engine
async def test_tui_yahoo_form_review_execution_and_restart(service, req, tmp_path):
    from test_research_tui import send
    from textual.widgets import Input

    from stratosphere.research.service import ResearchService
    from stratosphere.research.store import ResearchStore
    from stratosphere.tui import StratosphereApp
    from stratosphere.yahoo.widgets import YahooReview

    research = ResearchService(ResearchStore(tmp_path / "legacy"), online_check=online)
    app = StratosphereApp(research_service=research, yahoo_service=service)
    service.database = app.database
    async with app.run_test(size=(144, 40)) as pilot:
        await pilot.press("escape")
        await pilot.pause()
        await send(app, pilot, "/yahoo")
        assert app.research_form is not None
        for key, value in {
            "symbol": req.symbol,
            "start": str(req.start),
            "end": str(req.end),
            "fast": str(req.fast),
            "slow": str(req.slow),
            "quantity": req.quantity,
        }.items():
            app.query_one("#yahoo-" + key, Input).value = value
        app.query_one("#yahoo-prepare").focus()
        await pilot.press("enter")
        await app.workers.wait_for_complete()
        await pilot.pause()
        review = app.query_one(YahooReview)
        assert service.calls == ["fetch"]
        assert service.store.state(review.job) == "awaiting_approval"
        review.query_one("#yahoo-approve").focus()
        await pilot.press("enter")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert service.store.state(review.job) == "completed"
        await send(app, pilot, "/yahoo-show " + review.job)
        assert service.calls == ["fetch", "simulate", "simulate"]


@pytest.mark.engine
async def test_real_subprocess_does_not_inherit_credentials(service, req, monkeypatch):
    from stratosphere.yahoo.service import worker

    job, plan = await service.prepare(req)
    monkeypatch.setenv("STRATOSPHERE_TEST_SECRET", "artificial-secret-never-forward")
    original = asyncio.create_subprocess_exec
    environments = []

    async def checked(*args, **kwargs):
        environments.append(kwargs["env"])
        assert "STRATOSPHERE_TEST_SECRET" not in kwargs["env"]
        assert not any(name.endswith("API_KEY") for name in kwargs["env"])
        return await original(*args, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", checked)
    service.runner = worker
    service.approve(job, plan.digest)
    report = await service.run(job)
    assert len(environments) == 2
    assert report["metrics"]["base"]["filled_orders"] > 0


async def test_invalid_provider_frame_retained_with_safe_quality_diagnostic(service, req):
    raw = raw_frame(req)
    raw["rows"][3]["close"] = "NaN"

    async def invalid(*args, **kwargs):
        return raw

    service.runner = invalid
    with pytest.raises(ResearchError, match="invalid OHLCV"):
        await service.prepare(req)
    job = service.store.jobs()[0]
    assert service.store.read(job, "returned_frame.json") == raw
    assert service.store.read(job, "quality.json") == {
        "code": "invalid_ohlcv",
        "timestamp_ns": raw["rows"][3]["timestamp_ns"],
    }
    assert not service.database.catalog()


@pytest.mark.parametrize("target", ["returned_frame.json", "plan.json", "simulation.json"])
async def test_changed_or_redirected_pending_data_cannot_be_approved(
    service, req, target, tmp_path
):
    job, plan = await service.prepare(req)
    path = service.store.directory(job) / target
    copy = tmp_path / "copy.json"
    copy.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(copy)
    with pytest.raises(ResearchError):
        service.approve(job, plan.digest)
    assert not (service.store.directory(job) / "approval.json").exists()


async def test_restart_marks_interrupted_native_work_without_replay(service, req):
    job, plan = await service.prepare(req)
    service.approve(job, plan.digest)
    entered = asyncio.Event()

    async def blocked(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    service.runner = blocked
    task = asyncio.create_task(service.run(job))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (service.store.directory(job) / "base_started.json").exists()
    service.recover()
    with pytest.raises(ResearchError):
        await service.run(job)


@pytest.mark.engine
async def test_full_year_of_hourly_fixture_observations_runs_without_monthly_limit(service, req):
    import json

    req = YahooRequest.model_validate(
        req.model_dump() | {"start": "2025-01-01", "end": "2026-01-01", "quantity": "0.01"}
    )
    raw = raw_frame(req)
    pattern = deepcopy(raw["rows"])
    raw["rows"] = []
    start = datetime(2025, 1, 1, tzinfo=UTC)
    for index in range(8760):
        row = deepcopy(pattern[index % len(pattern)])
        row["timestamp_ns"] = int((start + timedelta(hours=index)).timestamp() * 10**9)
        raw["rows"].append(row)

    async def runner(directory, action, input_name, *, multiplier=1):
        if action == "fetch":
            return raw
        payload = json.loads((directory / input_name).read_text())
        from stratosphere.yahoo.contracts import YahooSnapshot

        return run_simulation(
            YahooPlan.model_validate(payload["plan"]),
            YahooSnapshot.model_validate(payload["snapshot"]),
            multiplier=multiplier,
        )

    service.runner = runner
    job, plan = await service.prepare(req)
    service.approve(job, plan.digest)
    report = await service.run(job)
    assert report["observations"] == 8760
    assert report["metrics"]["base"]["filled_orders"] > 0
    for case in ("base", "stress"):
        native = service.store.read(job, case + ".json")
        assert len(native["equity_curve"]) == 8760
        completed = []
        for fill in native["fills"]:
            if fill["side"] == "BUY":
                opening = Decimal(fill["quote_cash_delta"])
            else:
                completed.append(opening + Decimal(fill["quote_cash_delta"]))
        assert [Decimal(t["net_pnl"]) for t in report["metrics"][case]["trades"]] == completed


async def test_another_client_recovery_does_not_interrupt_owned_native_work(service, req):
    job, _ = await service.prepare(req)
    service.store.event(job, "running")
    with service.store.lock(job):
        other = YahooBacktests(service.store.root, service.database, online_check=online)
        other.recover()
        assert other.store.state(job) == "running"
    other.recover()
    assert other.store.state(job) == "interrupted"


async def test_canceled_acquisition_worker_removes_temporary_cookie_storage(tmp_path, monkeypatch):
    from pathlib import Path

    from stratosphere.yahoo.service import worker

    entered = asyncio.Event()
    caches = []

    class Process:
        returncode = None

        async def wait(self):
            if self.returncode is None:
                entered.set()
                await asyncio.Event().wait()

        def kill(self):
            self.returncode = -9

    async def launch(*args, **kwargs):
        cache = Path(kwargs["env"]["HOME"])
        caches.append(cache)
        assert kwargs["env"]["TMPDIR"] == str(cache)
        (cache / "anonymous-cookie.sqlite").write_text("artificial-cookie")
        return Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
    task = asyncio.create_task(worker(tmp_path, "fetch", "request.json"))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert caches and not caches[0].exists()


async def test_corporate_actions_explain_the_block_without_provider_error_bodies(service, req):
    raw = raw_frame(req)
    raw["rows"][5]["dividends"] = "0.25"

    async def returned(*args, **kwargs):
        return raw

    service.runner = returned
    with pytest.raises(ResearchError, match="corporate actions"):
        await service.prepare(req)
    job = service.store.jobs()[0]
    assert service.store.read(job, "quality.json")["code"] == "validation_blocked"
    assert not service.database.catalog()


def test_sparse_equity_history_cannot_silently_shorten_requested_year(req):
    req = YahooRequest.model_validate(
        req.model_dump() | {"symbol": "AAPL", "asset_class": "equity", "start": "2024-01-01"}
    )
    with pytest.raises(ValueError, match="coverage"):
        normalize(raw_frame(req), req)


async def test_cancel_during_spawn_waits_for_child_and_cleans_cache(tmp_path, monkeypatch):
    from pathlib import Path

    from stratosphere.yahoo.service import worker

    entered, release = asyncio.Event(), asyncio.Event()
    caches = []

    class Process:
        returncode = None
        killed = False

        def kill(self):
            self.returncode, self.killed = -9, True

        async def wait(self):
            return self.returncode

    process = Process()

    async def launch(*args, **kwargs):
        caches.append(Path(kwargs["env"]["HOME"]))
        entered.set()
        await release.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
    task = asyncio.create_task(worker(tmp_path, "fetch", "request.json"))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.killed and not caches[0].exists()


@pytest.mark.parametrize("asset,symbol", [("crypto_spot", "BTC-USD"), ("forex", "EURUSD=X")])
def test_etf_metadata_cannot_be_reclassified_as_crypto_or_fx(req, asset, symbol):
    req = YahooRequest.model_validate(req.model_dump() | {"asset_class": asset, "symbol": symbol})
    raw = raw_frame(req)
    raw["metadata"]["instrument_type"] = "ETF"
    with pytest.raises(ValueError, match="different instrument or asset class"):
        normalize(raw, req)
