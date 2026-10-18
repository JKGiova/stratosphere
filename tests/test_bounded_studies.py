"""Authored monthly archives and lifecycle fakes, plus explicitly marked native tests."""

import asyncio
import hashlib
import io
import json
import zipfile
from decimal import Decimal

import httpx
import pytest

from stratosphere.registry.workspace import RegistryError, WorkspaceDatabase
from stratosphere.research.contracts import MarketBar
from stratosphere.research.store import ResearchError
from stratosphere.studies.contracts import HOUR_NS, ArchivePin
from stratosphere.studies.data import fetch_month, normalize_month, pin_month
from stratosphere.studies.service import StudyService
from stratosphere.studies.store import StudyStore
from stratosphere.studies.worker import CellRequest, cell_bars, execute_cell


def monthly(month, prices=(100, 105, 110, 98, 85, 90, 100, 101)):
    pin = ArchivePin(month=month, sha256="0" * 64)
    rows = []
    for index in range(pin.count):
        opened = pin.first_ns + index * HOUR_NS
        opening, close = prices[(index - 1) % len(prices)], prices[index % len(prices)]
        rows.append(
            f"{opened // 1000},{opening},{max(opening, close) + 1},"
            f"{min(opening, close) - 1},{close},100,{(opened + HOUR_NS - 1000) // 1000},"
            "10000,10,50,5000,0"
        )
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(pin.filename.removesuffix(".zip") + ".csv", "\n".join(rows) + "\n")
    content = output.getvalue()
    pin = ArchivePin(month=month, sha256=hashlib.sha256(content).hexdigest())
    return pin, normalize_month(content, pin)


@pytest.fixture
def studies(tmp_path, research_plan, research_source):
    database = WorkspaceDatabase(tmp_path / "registry.sqlite3")
    parent = database.register(research_plan.specification, "Parent", {"text": research_source})
    snapshots = {month: monthly(month) for month in ("2025-01", "2025-02", "2025-03")}
    calls = []

    async def pinner(month):
        calls.append(("pin", month))
        return snapshots[month][0]

    async def fetcher(pin):
        calls.append(("fetch", pin.month))
        if pin.month == "2025-02":
            selection = service.store.read(service.active_job, "selection.json")
            assert selection["selected_index"] is not None
        return snapshots[pin.month][1]

    async def runner(store, cell_id, multiplier):
        request = CellRequest.model_validate(store.read(cell_id, "request.json"))
        calls.append(("run", request.candidate_index, request.window, multiplier))
        plan, bars, start = cell_bars(request, store.directory(cell_id))
        curve = [
            {
                "timestamp_ns": b.close_ns,
                "cash": "10000",
                "base": "0",
                "close": b.close,
                "equity": "10000",
            }
            for b in bars
            if b.open_ns >= start
        ]
        # Lifecycle fixture only: candidate 1 wins validation; holdout cannot reverse selection.
        pnl = (request.candidate_index + 1) * 10
        if request.window == "holdout":
            pnl = -100
        curve[-1]["equity"] = str(10000 + pnl)
        return {
            "protocol_sha256": request.protocol.digest,
            "candidate_index": request.candidate_index,
            "window": request.window,
            "cost_multiplier": multiplier,
            "result": {
                "engine_version": "1.231.0",
                "cost_multiplier": multiplier,
                "signals": [],
                "fills": [],
                "equity_curve": curve,
                "pending_signal": None,
                "open_positions": 0,
                "balances": {"USDT": str(10000 + pnl), "BTC": "0"},
            },
        }

    async def online():
        calls.append(("online",))

    service = StudyService(
        StudyStore(tmp_path / "studies"),
        database,
        pinner=pinner,
        fetcher=fetcher,
        runner=runner,
        online_check=online,
    )
    service.test_parent = parent
    service.test_plan = research_plan
    service.test_calls = calls
    service.test_snapshots = snapshots
    return service


async def prepare(studies, candidates=None, **kwargs):
    return await studies.prepare(
        studies.test_parent["public_id"],
        studies.test_plan,
        "Slower periods may reduce cost sensitivity",
        candidates or [{"fast": 3, "slow": 8}, {"fast": 4, "slow": 10}],
        kwargs.get("month", "2025-01"),
        kwargs.get("budget", 10),
    )


@pytest.mark.asyncio
async def test_freeze_approval_all_candidates_and_validation_selection_before_holdout(studies):
    job, protocol = await prepare(studies)
    assert studies.test_calls == [("pin", "2025-01"), ("pin", "2025-02")]
    with pytest.raises(ResearchError):
        await studies.run(job)
    with pytest.raises(ResearchError):
        studies.approve(job, "0" * 64)
    studies.approve(job, protocol.digest)
    with pytest.raises(ResearchError):
        studies.approve(job, protocol.digest)
    report = await studies.run(job)
    assert report["selection"]["selected_index"] == 1
    assert report["metrics"]["net_pnl"] == "-100.00000000"
    assert report["holdout_status"] == "completed_once"
    assert report["attempted_candidates"] == 2 and report["native_attempts"] == 10
    assert len([c for c in studies.test_calls if c[0] == "run"]) == protocol.max_simulations
    assert all(a["strategy_id"] for a in report["attempts"])
    assert report == await studies.open_report(job)
    assert len(studies.database.catalog()) == 2
    assert studies.database.reports(studies.test_parent["public_id"])[0]["kind"] == "study"
    with pytest.raises(ResearchError):
        await studies.run(job)


@pytest.mark.asyncio
async def test_invalid_failed_candidates_retained_and_never_selected(studies):
    job, protocol = await prepare(
        studies,
        [
            {"fast": 8, "slow": 3},
            {"fast": 3, "slow": 8},
            {"fast": 4, "slow": 10},
        ],
        budget=14,
    )
    original = studies.runner

    async def fail_one(store, cell, multiplier):
        if store.read(cell, "request.json")["candidate_index"] == 2:
            raise ResearchError("Authored failure")
        return await original(store, cell, multiplier)

    studies.runner = fail_one
    studies.approve(job, protocol.digest)
    report = await studies.run(job)
    assert [a["status"] for a in report["attempts"]] == ["invalid", "eligible", "ineligible"]
    assert report["selection"]["selected_index"] == 1
    assert report["native_attempts"] == 10 and report["attempted_candidates"] == 3


@pytest.mark.asyncio
async def test_all_invalid_produces_no_holdout_or_fabricated_metrics(studies):
    job, protocol = await prepare(studies, [{"fast": -1, "slow": 5000}], budget=6)
    studies.approve(job, protocol.digest)
    report = await studies.run(job)
    assert report["metrics"] == {} and report["holdout_status"] == "not_evaluated"
    assert report["native_attempts"] == 0 and report["invalid_candidates"] == 1
    assert not any(c[0] == "fetch" and c[1] == "2025-02" for c in studies.test_calls)


@pytest.mark.asyncio
async def test_pause_resume_never_reruns_consumed_cell_or_reselects_on_holdout(studies):
    job, protocol = await prepare(studies)
    studies.approve(job, protocol.digest)
    original, entered = studies.runner, asyncio.Event()

    async def waiting(store, cell, multiplier):
        request = store.read(cell, "request.json")
        if request["candidate_index"] == 0 and request["window"] == "validation":
            entered.set()
            await asyncio.Event().wait()
        return await original(store, cell, multiplier)

    studies.runner = waiting
    task = asyncio.create_task(studies.run(job))
    await asyncio.wait_for(entered.wait(), 5)
    studies.stop_state = "paused"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert studies.store.state(job) == "paused"
    studies.runner = original
    report = await studies.run(job)
    assert report["attempts"][0]["status"] == "ineligible"
    assert report["attempts"][0]["cells"]["validation_1"]["status"] == "canceled"
    assert report["selection"]["selected_index"] == 1
    assert studies.test_calls.count(("run", 0, "development", 1)) == 1
    assert len(list(studies.store.directory(job).glob("cell_*_started.json"))) == 10


@pytest.mark.asyncio
async def test_local_holdout_reuse_and_later_optimization_are_blocked_before_download(studies):
    job, protocol = await prepare(studies)
    studies.approve(job, protocol.digest)
    await studies.run(job)
    other, plan = await prepare(studies)
    studies.approve(other, plan.digest)
    before = studies.test_calls.count(("fetch", "2025-02"))
    with pytest.raises(RegistryError, match="prior local exposure"):
        await studies.run(other)
    assert studies.test_calls.count(("fetch", "2025-02")) == before == 1
    later, plan = await prepare(studies, month="2025-02")
    studies.approve(later, plan.digest)
    with pytest.raises(RegistryError, match="prior local exposure"):
        await studies.run(later)
    assert studies.test_calls.count(("fetch", "2025-02")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "candidates,budget",
    [
        ([{"fast": 3, "slow": 8}], 5),
        ([{"fast": 3, "slow": 8}, {"fast": 3, "slow": 8}], 10),
        ([{"fast": 3.0, "slow": 8}], 6),
    ],
)
async def test_invalid_proposals_keep_input_and_do_not_acquire_prices(studies, candidates, budget):
    with pytest.raises(ValueError):
        await prepare(studies, candidates, budget=budget)
    jobs = studies.store.jobs()
    assert len(jobs) == 1 and studies.store.state(jobs[0]) == "failed"
    assert studies.store.read(jobs[0], "source.json")["text"]
    assert not any(c[0] in {"fetch", "run"} for c in studies.test_calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "artifact", ["protocol.json", "approval.json", "cell_0_validation_2.json", "report.json"]
)
async def test_pending_and_completed_artifact_tampering_is_detected(studies, artifact):
    job, protocol = await prepare(studies)
    if artifact != "protocol.json":
        studies.approve(job, protocol.digest)
    if artifact.startswith("cell") or artifact == "report.json":
        await studies.run(job)
    (studies.store.directory(job) / artifact).write_text("{}")
    with pytest.raises(ResearchError, match="integrity"):
        if artifact == "protocol.json":
            studies.approve(job, protocol.digest)
        elif artifact == "approval.json":
            await studies.run(job)
        else:
            studies.report(job)


def test_monthly_adapter_validates_calendar_windows_units_and_pin():
    for month, count in [("2025-02", 672), ("2028-02", 696), ("2025-12", 744)]:
        pin, snapshot = monthly(month)
        assert len(snapshot.bars) == count
        assert snapshot.bars[0].open_ns == pin.first_ns
        assert snapshot.bars[-1].close_ns == pin.first_ns + count * HOUR_NS - 1000
        with pytest.raises(ValueError, match="checksum"):
            normalize_month(snapshot.archive + b"changed", pin)
    with pytest.raises(ValueError):
        ArchivePin(month="2024-12", sha256="0" * 64)
    with pytest.raises(ValueError):
        ArchivePin(month="2025-13", sha256="0" * 64)


@pytest.mark.asyncio
async def test_checksum_only_pinning_redirect_failure_and_changed_archive():
    pin, snapshot = monthly("2025-02")
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path.endswith(".CHECKSUM"):
            return httpx.Response(200, text=pin.sha256 + "  " + pin.filename)
        return httpx.Response(200, content=snapshot.archive)

    transport = httpx.MockTransport(handler)
    assert await pin_month(pin.month, transport=transport) == pin
    assert len(calls) == 1 and calls[0].url.path.endswith(".CHECKSUM")
    assert await fetch_month(pin, transport=transport) == snapshot
    assert not any("authorization" in call.headers for call in calls)
    with pytest.raises(ValueError):
        await pin_month(pin.month, transport=httpx.MockTransport(lambda r: httpx.Response(302)))
    with pytest.raises(ValueError, match="revised"):
        await fetch_month(ArchivePin(month=pin.month, sha256="0" * 64), transport=transport)


@pytest.mark.asyncio
async def test_worker_window_warmup_and_frozen_holdout_guard(studies):
    job, protocol = await prepare(studies)
    directory = studies.store.directory(job)
    directory.joinpath("development.zip").write_bytes(studies.test_snapshots["2025-01"][1].archive)
    directory.joinpath("holdout.zip").write_bytes(studies.test_snapshots["2025-02"][1].archive)
    request = CellRequest(protocol=protocol, candidate_index=0, window="validation")
    plan, bars, start = cell_bars(request, directory)
    assert start == protocol.development.first_ns + protocol.split * HOUR_NS
    assert sum(b.open_ns < start for b in bars) == plan.draft.slow_period + 1
    invalid = CellRequest(protocol=protocol, candidate_index=0, window="holdout", selected_index=1)
    with pytest.raises(ValueError, match="frozen selection"):
        cell_bars(invalid, directory)
    with pytest.raises(ValueError):
        execute_cell(request.model_dump() | {"candidate_index": 200}, 1, directory)


@pytest.mark.engine
@pytest.mark.asyncio
async def test_native_full_study_preserves_all_fills_windows_and_report(studies):
    from stratosphere.research.service import run_worker

    studies.runner = run_worker
    job, protocol = await prepare(studies)
    studies.approve(job, protocol.digest)
    report = await studies.run(job)
    assert report["holdout_status"] == "completed_once"
    assert report["native_attempts"] == 10
    assert report["metrics"]["filled_orders"] > 0
    for attempt in report["attempts"]:
        for key, cell in attempt["cells"].items():
            window = key.split("_")[0]
            result = cell["response"]["result"]
            boundary = protocol.development.first_ns + (
                protocol.split * HOUR_NS if window == "validation" else 0
            )
            assert result["engine"] == "nautilus_trader"
            assert all(s["signal_ns"] >= boundary for s in result["signals"])
            assert all(f["fill_ns"] > f["signal_ns"] for f in result["fills"])
            assert Decimal(result["equity_curve"][0]["equity"]) == Decimal("10000")
    assert Decimal(report["metrics"]["max_drawdown_pct"]) >= 0
    assert studies.report(job) == report


@pytest.mark.engine
def test_native_warmup_only_matches_independent_batch_sma_and_resets_flat(
    research_plan, market_bars
):
    from test_research_engine import oracle

    from stratosphere.research.engine import run_simulation

    start = market_bars[100].open_ns
    result = run_simulation(research_plan, market_bars, trade_start_ns=start)
    signals, fills, curve, held, pending = oracle(research_plan, market_bars, trade_start_ns=start)
    assert result["signals"] == signals
    assert [Decimal(p["equity"]) for p in result["equity_curve"]] == curve
    assert Decimal(result["balances"]["BTC"]) == held
    assert result["pending_signal"] == pending
    assert len(result["fills"]) == len(fills)
    for actual, expected in zip(result["fills"], fills, strict=True):
        assert all(actual[key] == value for key, value in expected.items())
    changed = tuple(
        MarketBar.model_validate(
            b.model_dump() | {"open": "1", "high": "1", "low": "1", "close": "1"}
        )
        if i >= 150
        else b
        for i, b in enumerate(market_bars)
    )
    mutated = run_simulation(research_plan, changed, trade_start_ns=start)
    assert result["equity_curve"][:50] == mutated["equity_curve"][:50]


@pytest.mark.asyncio
async def test_inline_study_form_approval_agent_execution_reports_and_resize(
    studies,
    tmp_path,
    research_source,
    research_provider,
):
    from test_research_tui import connect, send
    from textual.widgets import DataTable, Input, TextArea

    from stratosphere.research.contracts import ResearchConfig
    from stratosphere.research.service import ResearchService
    from stratosphere.research.store import ResearchStore
    from stratosphere.research.widgets import ResearchReview
    from stratosphere.studies.widgets import StudyForm
    from stratosphere.tui import StratosphereApp

    research = ResearchService(ResearchStore(tmp_path / "ui"), online_check=studies.online_check)
    parent_job, _, _ = await research.prepare(research_source, ResearchConfig(), research_provider)
    app = StratosphereApp(
        provider_factory=lambda _: research_provider,
        research_service=research,
        study_service=studies,
    )
    studies.database = app.database
    app.workspace_bridge.register_job(parent_job)
    async with app.run_test(size=(144, 40)) as pilot:
        await connect(app, pilot)
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        await send(app, pilot, f"/study {parent_job}")
        assert isinstance(app.research_form, StudyForm)
        app.query_one("#study-budget", Input).focus()
        await pilot.pause()
        budget = app.query_one("#study-budget", Input)
        assert app.query_one("#conversation").scrollable_content_region.contains_region(
            budget.region
        )
        assert not app.query_one("#composer").display
        await pilot.resize_terminal(144, 40)
        await pilot.pause()
        app.query_one("#study-hypothesis", TextArea).text = "Compare small SMA neighbors"
        app.query_one("#study-candidates", Input).value = "3/8,4/10,8/3"
        app.query_one("#study-budget", Input).value = "14"
        app.query_one("#prepare-study").focus()
        await pilot.press("enter")
        await app.research_worker.wait()
        await pilot.pause()
        assert not app.research_form
        job = studies.store.jobs()[-1]
        protocol = studies.store.protocol(job)
        assert studies.store.state(job) == "awaiting_approval"
        assert not any(c[0] == "run" for c in studies.test_calls)
        review = next(r for r in app.query(ResearchReview) if r.job_id == job)
        assert review.simulations == 14
        await send(app, pilot, f"/approve {job} {protocol.digest}")
        await app.research_worker.wait()
        await pilot.pause()
        assert studies.store.state(job) == "completed"
        assert app.query_one("#reports", DataTable).row_count == 1
        await send(app, pilot, f"/report {job}")
        assert app.active_view == "reports"
        assert "holdout" in app.query_one("#report-preview").content.markup.lower()
        for size in ((45, 12), (70, 50), (144, 40), (210, 60)):
            await pilot.resize_terminal(*size)
            await pilot.pause()
            assert app.query_one("#reports-pane").display
        assert len(research_provider.requests) == 1
        app.session.sync_research()
        assert app.database.reports()[0]["report"]["attempted_candidates"] == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["valid", "single", "duplicate", "unsupported", "no_baseline", "empty_hypothesis", "sql"],
)
async def test_specialist_drafts_bounded_proposal_without_approval_or_download(
    studies, tmp_path, research_provider, research_source, case
):
    from stratosphere.agents.orchestrator import ROUTER_PROMPT, Orchestrator
    from stratosphere.models.streaming import ModelError
    from stratosphere.research.contracts import ResearchConfig
    from stratosphere.research.service import ResearchService
    from stratosphere.research.store import ResearchStore

    proposal = {
        "hypothesis": "Slower pairs reduce cost sensitivity",
        "month": "2025-01",
        "candidates": [{"fast": 3, "slow": 8}, {"fast": 4, "slow": 10}],
    }
    if case == "single":
        proposal["candidates"] = proposal["candidates"][:1]
    elif case == "duplicate":
        proposal["candidates"] = [proposal["candidates"][0]] * 2
    elif case == "unsupported":
        proposal["candidates"][1] = {"fast": 100, "slow": 5}
    elif case == "no_baseline":
        proposal["candidates"] = [{"fast": 5, "slow": 12}, {"fast": 6, "slow": 15}]
    elif case == "empty_hypothesis":
        proposal["hypothesis"] = "  "
    elif case == "sql":
        proposal["sql"] = "SELECT private_turns FROM unrelated_strategy"

    class Provider:
        requests = []

        async def stream(self, messages):
            self.requests.append(messages)
            yield json.dumps(
                {"action": "study"} if messages[0]["content"] == ROUTER_PROMPT else proposal
            )

    provider = Provider()
    research = ResearchService(ResearchStore(tmp_path / "jobs"), online_check=studies.online_check)
    job, _, _ = await research.prepare(research_source, ResearchConfig(), research_provider)
    bridge = Orchestrator(provider, studies.database, research, studies=studies)
    identity = bridge.register_job(job)
    bridge.select(identity["public_id"])
    if case == "valid":
        _ = [s async for s in bridge.respond("Propose a bounded comparison")]
        assert bridge.last_action == {
            "action": "study",
            "job_id": job,
            "suggestion": proposal | {"max_simulations": 10},
        }
        assert (
            json.loads(studies.database.history(identity["public_id"])[-1]["content"]) == proposal
        )
    else:
        with pytest.raises(ModelError):
            _ = [s async for s in bridge.respond("Propose a bounded comparison")]
        assert not bridge.last_action and not studies.database.history(identity["public_id"])
    assert len(provider.requests) == 2
    assert not studies.store.jobs() and not any(c[0] == "fetch" for c in studies.test_calls)


@pytest.mark.asyncio
async def test_interrupted_native_start_is_not_retried_and_operation_is_reconciled(studies):
    job, protocol = await prepare(studies)
    studies.approve(job, protocol.digest)
    op = studies.database.operation(studies.test_parent["public_id"], "study_cell", job_id=job)
    studies.store.write(job, "cell_0_development_1_started.json", {"operation_id": op})
    studies.store.event(job, "running", "Authored crash before response")
    studies.recover()
    assert studies.store.state(job) == "paused"
    report = await studies.run(job)
    assert report["attempts"][0]["cells"]["development_1"]["status"] == "interrupted"
    assert report["selection"]["selected_index"] == 1
    assert ("run", 0, "development", 1) not in studies.test_calls
    with studies.database.connect() as db:
        assert (
            db.execute("SELECT status FROM operations WHERE id=?", (op,)).fetchone()[0] == "failed"
        )


@pytest.mark.asyncio
async def test_native_holdout_failure_is_retained_without_success_or_reselection(studies):
    job, protocol = await prepare(studies)
    studies.approve(job, protocol.digest)
    original = studies.runner

    async def failing(store, cell, multiplier):
        if store.read(cell, "request.json")["window"] == "holdout":
            raise ResearchError("Authored holdout failure")
        return await original(store, cell, multiplier)

    studies.runner = failing
    report = await studies.run(job)
    assert report["selection"]["selected_index"] == 1
    assert report["holdout_status"] == "incomplete" and report["metrics"] == {}
    assert report["native_attempts"] == 10 and studies.report(job) == report


def test_atomic_holdout_claims_allow_only_one_study(studies):
    from concurrent.futures import ThreadPoolExecutor

    pin = studies.test_snapshots["2025-02"][0]

    def claim(number):
        try:
            studies.database.claim_dataset(pin.url, pin.sha256, str(number), "holdout")
            return "claimed"
        except RegistryError:
            return "blocked"

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(claim, range(2))) == ["blocked", "claimed"]


@pytest.mark.parametrize(
    "change",
    [
        lambda rows: rows[:-1],
        lambda rows: [rows[0], *rows],
        lambda rows: [rows[1], rows[0], *rows[2:]],
        lambda rows: [rows[0].replace("1738368000000000", "1738368000000"), *rows[1:]],
        lambda rows: [rows[0] + ",extra", *rows[1:]],
    ],
)
def test_new_months_reject_incomplete_duplicate_unordered_wrong_unit_and_malformed_bars(change):
    pin, snapshot = monthly("2025-02")
    member = pin.filename.removesuffix(".zip") + ".csv"
    with zipfile.ZipFile(io.BytesIO(snapshot.archive)) as archive:
        rows = archive.read(member).decode().splitlines()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(member, "\n".join(change(rows)))
    content = output.getvalue()
    pin = ArchivePin(month=pin.month, sha256=hashlib.sha256(content).hexdigest())
    with pytest.raises(ValueError):
        normalize_month(content, pin)
