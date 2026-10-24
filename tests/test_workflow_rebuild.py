"""New bounded research handoff, owner isolation and fail-closed lifecycle contracts."""

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from stratosphere.phase0.contracts import fingerprint
from stratosphere.registry.workspace import WorkspaceDatabase
from stratosphere.research.store import ResearchError
from stratosphere.workflow.contracts import PortfolioDefinition, ResearchProposal, SignalVerdict
from stratosphere.workflow.research import ResearchCampaigns
from stratosphere.yahoo.contracts import YahooPlan, YahooRequest, YahooSnapshot
from stratosphere.yahoo.engine import run_simulation
from stratosphere.yahoo.service import YahooBacktests


@pytest.fixture
def request_spec():
    return YahooRequest(
        symbol="BTC-USD",
        asset_class="crypto_spot",
        interval="1h",
        start="2025-01-01",
        end="2025-01-03",
        fast=3,
        slow=8,
        quantity="1",
    )


def raw_frame(request):
    start = datetime(2025, 1, 1, tzinfo=UTC)
    rows = []
    for index in range(48):
        price = 100 + (index % 16 if index % 32 < 16 else 31 - index % 32)
        rows.append(
            {
                "timestamp_ns": int((start + timedelta(hours=index)).timestamp() * 1e9),
                "open": str(price),
                "high": str(price + 1),
                "low": str(price - 1),
                "close": str(price),
                "volume": "10",
                "dividends": "0",
                "splits": "0",
                "capital_gains": "0",
            }
        )
    return {
        "schema_version": "yfinance-returned-frame/1",
        "yfinance_version": "1.7.0",
        "request": request.model_dump(mode="json"),
        "observed_at_ns": int(datetime(2025, 1, 4, tzinfo=UTC).timestamp() * 1e9),
        "metadata": {
            "symbol": request.symbol,
            "currency": "USD",
            "instrument_type": "CRYPTOCURRENCY",
            "exchange": "fixture",
            "timezone": "UTC",
        },
        "rows": rows,
    }


@pytest.fixture
def campaigns(tmp_path):
    async def online():
        pass

    calls = []

    async def runner(directory, action, name, *, multiplier=1):
        payload = json.loads((directory / name).read_text())
        calls.append(action)
        if action == "fetch":
            req = YahooRequest.model_validate(payload)
            raw = raw_frame(req)
            if req.symbol == "ETH-USD":
                raw["rows"][0]["open"] = "nan"
            return raw
        return run_simulation(
            YahooPlan.model_validate(payload["plan"]),
            YahooSnapshot.model_validate(payload["snapshot"]),
            multiplier=multiplier,
        )

    db = WorkspaceDatabase(tmp_path / "library.sqlite3")
    yahoo = YahooBacktests(tmp_path / "yahoo", db, runner=runner, online_check=online)
    svc = ResearchCampaigns(tmp_path / "workflow", yahoo, db)
    svc.calls = calls
    return svc


def proposal(*candidates):
    return ResearchProposal(
        status="ready",
        name="Authored comparison",
        hypothesis="Slower crossings may reduce turnover.",
        candidates=candidates,
        questions=(),
    )


@pytest.mark.engine
async def test_single_approval_covers_exact_candidates_and_cannot_replay(campaigns, request_spec):
    alternative = request_spec.model_copy(update={"fast": 4})
    identity, plan = await campaigns.prepare("Compare periods", proposal(request_spec, alternative))
    assert plan.native_starts == 4
    assert campaigns.calls == ["fetch", "fetch"]
    with pytest.raises(ResearchError, match="human approval"):
        await campaigns.run(identity)
    with pytest.raises(ResearchError):
        campaigns.approve(identity, "0" * 64)
    campaigns.approve(identity, plan.digest)
    report = await campaigns.run(identity)
    assert len(report["results"]) == 2
    assert campaigns.calls.count("simulate") == 4
    assert report["deployment_status"] == "research_only"
    assert all(
        r["strategy_id"] == p.strategy_id
        for r, p in zip(report["results"], plan.candidates, strict=True)
    )
    assert campaigns.report(identity) == report
    with pytest.raises(ResearchError):
        await campaigns.run(identity)
    assert campaigns.calls.count("simulate") == 4
    first, second = (campaigns.database.strategy(p.strategy_id) for p in plan.candidates)
    assert first["agent_id"] != second["agent_id"]


@pytest.mark.engine
async def test_blocked_candidates_remain_in_report_and_do_not_consume_budget(
    campaigns, request_spec
):
    bad = request_spec.model_copy(update={"symbol": "ETH-USD"})
    identity, plan = await campaigns.prepare("Compare assets", proposal(request_spec, bad))
    assert len(plan.candidates) == 1 and plan.native_starts == 2
    campaigns.approve(identity, plan.digest)
    report = await campaigns.run(identity)
    assert report["blocked"][0]["request"]["symbol"] == "ETH-USD"
    assert len(report["results"]) == 1 and campaigns.calls.count("simulate") == 2


async def test_exact_duplicate_reuses_agent_and_variant_has_lineage(campaigns, request_spec):
    a, first = await campaigns.prepare("First", proposal(request_spec))
    b, second = await campaigns.prepare("Duplicate", proposal(request_spec))
    assert a != b and first.candidates[0].strategy_id == second.candidates[0].strategy_id
    owner = campaigns.database.strategy(first.candidates[0].strategy_id)
    _, changed = await campaigns.prepare(
        "Variant", proposal(request_spec.model_copy(update={"fast": 4})), parent=owner["public_id"]
    )
    variant = campaigns.database.strategy(changed.candidates[0].strategy_id)
    assert variant["agent_id"] != owner["agent_id"]
    assert variant["parent_public_id"] == owner["public_id"]


async def test_candidate_or_blocked_tamper_prevents_approval(campaigns, request_spec):
    identity, plan = await campaigns.prepare("One", proposal(request_spec))
    path = campaigns.store.directory(identity) / "blocked.json"
    path.write_text('{"attempts":[{"index":1}]}')
    with pytest.raises(ResearchError, match="Blocked"):
        campaigns.approve(identity, plan.digest)
    assert "simulate" not in campaigns.calls


async def test_independent_candidate_approval_cannot_be_stolen(campaigns, request_spec):
    identity, plan = await campaigns.prepare("One", proposal(request_spec))
    pin = plan.candidates[0]
    campaigns.yahoo.approve(pin.job_id, pin.plan_sha256)
    with pytest.raises(ResearchError, match="independently approved"):
        campaigns.approve(identity, plan.digest)
    assert campaigns.yahoo.store.state(pin.job_id) == "approved"


async def test_recovery_does_not_interrupt_another_owner(campaigns):
    identity = "a" * 32
    campaigns.store.root.mkdir(parents=True)
    campaigns.store.directory(identity).mkdir()
    campaigns.store.event(identity, "running")
    with campaigns.store.lock(identity):
        campaigns.recover()
        assert campaigns.store.state(identity) == "running"
    campaigns.recover()
    assert campaigns.store.state(identity) == "interrupted"


async def test_cancellation_preserves_attempts_and_blocks_resume(campaigns, request_spec):
    identity, plan = await campaigns.prepare("One", proposal(request_spec))
    campaigns.approve(identity, plan.digest)

    async def canceled(*args, **kwargs):
        raise asyncio.CancelledError

    campaigns.yahoo.run = canceled
    with pytest.raises(asyncio.CancelledError):
        await campaigns.run(identity)
    assert campaigns.store.state(identity) == "canceled"
    assert campaigns.store.read(identity, "partial.json") == {"results": []}
    with pytest.raises(ResearchError):
        await campaigns.run(identity)


@pytest.mark.parametrize(
    "mutation",
    [
        {"max_position_fraction": "0.5"},
        {"max_risk_fraction": "NaN"},
        {"provider": "ftmo"},
        {"mode": "evaluation"},
    ],
)
def test_portfolio_limits_and_provider_modes_fail_closed(mutation):
    base = {
        "name": "Test",
        "provider": "alpaca",
        "mode": "paper",
        "currency": "USD",
        "max_strategy_fraction": "0.4",
        "max_position_fraction": "0.1",
        "max_risk_fraction": "0.005",
        "max_daily_loss_fraction": "0.02",
    }
    with pytest.raises(ValueError):
        PortfolioDefinition.model_validate(base | mutation)


def test_signal_is_binary_and_bound_to_owner_and_context():
    data = {
        "strategy_id": "1" * 24,
        "signal_sha256": "a" * 64,
        "context_sha256": "b" * 64,
        "verdict": "reliable",
        "explanation": "Authored evidence only",
        "evidence": ["bar:1"],
    }
    assert SignalVerdict.model_validate(data).verdict == "reliable"
    with pytest.raises(ValueError):
        SignalVerdict.model_validate(data | {"verdict": "maybe"})
    with pytest.raises(ValueError):
        SignalVerdict.model_validate(data | {"approved": True})
    assert fingerprint(SignalVerdict.model_validate(data))


@pytest.fixture
def portfolio_basis():
    from stratosphere.workflow.contracts import RiskSnapshot

    definition = PortfolioDefinition(
        name="Authored portfolio",
        provider="alpaca",
        mode="paper",
        currency="USD",
        max_strategy_fraction="0.5",
        max_position_fraction="0.1",
        max_risk_fraction="0.005",
        max_daily_loss_fraction="0.02",
    )
    snapshot = RiskSnapshot(
        portfolio_id="a" * 32,
        currency="USD",
        equity="10000",
        free_cash="8000",
        daily_loss="0",
        strategy_capital={"1" * 24: "4000", "2" * 24: "3000"},
        committed_capital={"1" * 24: "0", "2" * 24: "0"},
        as_of_ns=100,
        expires_at_ns=200,
        reconciled=True,
    )
    return definition, snapshot


@pytest.mark.parametrize(
    "allocations",
    [
        {"1" * 24: "6000", "2" * 24: "3000"},
        {"1" * 24: "3000"},
        {"1" * 24: "3000", "3" * 24: "3000"},
    ],
)
def test_wallet_agent_cannot_overallocate_or_cross_owners(portfolio_basis, allocations):
    from stratosphere.workflow.contracts import CapitalProposal
    from stratosphere.workflow.risk import validate_capital

    definition, snapshot = portfolio_basis
    p = CapitalProposal(
        portfolio_id=snapshot.portfolio_id,
        snapshot_sha256=snapshot.digest,
        allocations=allocations,
        explanation="Authored proposal",
    )
    with pytest.raises(ResearchError):
        validate_capital(definition, snapshot, p, at_ns=150)


def test_committed_capital_locks_allocation(portfolio_basis):
    from stratosphere.workflow.contracts import CapitalProposal, RiskSnapshot
    from stratosphere.workflow.risk import validate_capital

    definition, snapshot = portfolio_basis
    snapshot = RiskSnapshot.model_validate(
        snapshot.model_dump() | {"committed_capital": {"1" * 24: "500", "2" * 24: "0"}}
    )
    p = CapitalProposal(
        portfolio_id=snapshot.portfolio_id,
        snapshot_sha256=snapshot.digest,
        allocations={"1" * 24: "3500", "2" * 24: "3000"},
        explanation="Authored proposal",
    )
    with pytest.raises(ResearchError, match="lock"):
        validate_capital(definition, snapshot, p, at_ns=150)


def test_reliable_signal_is_only_a_preview_not_order_authority(portfolio_basis):
    from stratosphere.workflow.contracts import PositionSizingProposal
    from stratosphere.workflow.risk import position_preview

    definition, snapshot = portfolio_basis
    verdict = SignalVerdict(
        strategy_id="1" * 24,
        signal_sha256="b" * 64,
        context_sha256="c" * 64,
        verdict="reliable",
        explanation="Authored signal",
        evidence=("bar:1",),
    )
    p = PositionSizingProposal(
        portfolio_id=snapshot.portfolio_id,
        strategy_id="1" * 24,
        snapshot_sha256=snapshot.digest,
        signal_sha256="b" * 64,
        requested_notional="999",
        explanation="Within limits",
    )
    kwargs = dict(
        context_sha256="c" * 64,
        entry_price="100",
        stop_price="99",
        quantity_increment="1",
        at_ns=150,
    )
    result = position_preview(definition, snapshot, verdict, p, **kwargs)
    assert result["quantity"] == "9" and result["execution_authorized"] is False
    with pytest.raises(ResearchError, match="rejected"):
        position_preview(
            definition,
            snapshot,
            verdict.model_copy(update={"verdict": "not_reliable"}),
            p,
            **kwargs,
        )
    with pytest.raises(ResearchError, match="risk limits"):
        position_preview(
            definition,
            snapshot,
            verdict,
            p.model_copy(update={"requested_notional": "1001"}),
            **kwargs,
        )
    with pytest.raises(ResearchError, match="risk limits"):
        position_preview(definition, snapshot, verdict, p, **(kwargs | {"stop_price": "90"}))
    with pytest.raises(ResearchError, match="current reconciled"):
        position_preview(definition, snapshot, verdict, p, **(kwargs | {"at_ns": 200}))


async def test_active_bridge_creates_yahoo_campaign_not_retired_creator(
    campaigns, request_spec, tmp_path
):
    from stratosphere.research.service import ResearchService
    from stratosphere.research.store import ResearchStore
    from stratosphere.workflow.runtime import Orchestrator

    class Provider:
        async def stream(self, messages):
            yield proposal(request_spec).model_dump_json()

    async def online():
        pass

    research = ResearchService(ResearchStore(tmp_path / "legacy"), online_check=online)
    bridge = Orchestrator(
        Provider(), campaigns.database, research, yahoo=campaigns.yahoo, campaigns=campaigns
    )
    text = "".join([delta async for delta in bridge.respond("/create an SMA strategy")])
    assert "Strategy-agent research plan" in text
    assert bridge.last_action["action"] == "flow_review"
    assert bridge.active_strategy is not None
    assert not bridge.creator.store.jobs()
    assert campaigns.calls == ["fetch"]
    assert bridge.agent().public_id == bridge.active_strategy


async def test_model_route_cannot_invent_creation_or_approval(campaigns, tmp_path):
    from stratosphere.models.streaming import ModelError
    from stratosphere.research.service import ResearchService
    from stratosphere.research.store import ResearchStore
    from stratosphere.workflow.runtime import Orchestrator

    class Provider:
        async def stream(self, messages):
            yield '{"action":"create","strategy_id":null}'

    async def online():
        pass

    research = ResearchService(ResearchStore(tmp_path / "legacy"), online_check=online)
    bridge = Orchestrator(
        Provider(), campaigns.database, research, yahoo=campaigns.yahoo, campaigns=campaigns
    )
    # Ensure routing has a catalog, rather than the intentional no-catalog chat path.
    from stratosphere.research.contracts import SmaParameters, compile_sma_spec

    campaigns.database.register(
        compile_sma_spec(SmaParameters(fast_period=3, slow_period=8, quantity="1")), "Fixture", {}
    )
    with pytest.raises(ModelError):
        await bridge.route("Hello")
    assert campaigns.calls == []


@pytest.mark.engine
async def test_tui_new_campaign_approval_report_and_restart(campaigns, request_spec, tmp_path):
    from test_research_tui import connect, send
    from textual.widgets import DataTable

    from stratosphere.research.service import ResearchService
    from stratosphere.research.store import ResearchStore
    from stratosphere.tui import StratosphereApp
    from stratosphere.workflow.widgets import CampaignReview

    class Provider:
        async def stream(self, messages):
            yield proposal(request_spec).model_dump_json()

    async def online():
        pass

    research = ResearchService(ResearchStore(tmp_path / "ui"), online_check=online)
    app = StratosphereApp(provider_factory=lambda settings: Provider(), research_service=research)
    app.yahoo.runner = campaigns.yahoo.runner
    app.yahoo.online_check = online
    async with app.run_test(size=(144, 40)) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/create an hourly crossover research comparison")
        await app.workers.wait_for_complete()
        await pilot.pause()
        review = app.query_one(CampaignReview)
        identity = review.identity
        assert app.campaigns.store.state(identity) == "awaiting_approval"
        assert campaigns.calls == ["fetch"]
        review.query_one("#flow-approve").focus()
        await pilot.press("enter")
        await pilot.pause()
        await app.workers.wait_for_complete()
        assert app.campaigns.store.state(identity) == "completed"
        assert campaigns.calls.count("simulate") == 2
        assert app.query_one("#reports", DataTable).row_count >= 1
        report = app.campaigns.report(identity)
    reopened = StratosphereApp(
        provider_factory=lambda settings: Provider(), research_service=research
    )
    reopened.yahoo.online_check = online
    async with reopened.run_test(size=(90, 28)) as pilot:
        await reopened.handle_flow_command("flow-show", (identity,))
        assert reopened.campaigns.report(identity) == report
        assert campaigns.calls.count("simulate") == 2


async def test_risk_agent_rechecks_expiry_after_inference(portfolio_basis):
    from stratosphere.workflow.risk import AdvisoryAgents

    definition, snapshot = portfolio_basis

    class Provider:
        async def stream(self, messages):
            yield json.dumps(
                {
                    "portfolio_id": snapshot.portfolio_id,
                    "snapshot_sha256": snapshot.digest,
                    "allocations": snapshot.strategy_capital,
                    "explanation": "Authored current proposal",
                }
            )

    agent = AdvisoryAgents(Provider(), clock_ns=lambda: 201)
    with pytest.raises(ResearchError, match="current reconciled"):
        await agent.capital(definition, snapshot, at_ns=150)


async def test_signal_agent_rejects_invented_facts_without_calling_wallet():
    from stratosphere.workflow.risk import AdvisoryAgents

    class Provider:
        async def stream(self, messages):
            yield json.dumps(
                {
                    "strategy_id": "1" * 24,
                    "signal_sha256": "a" * 64,
                    "context_sha256": "b" * 64,
                    "verdict": "reliable",
                    "explanation": "Fixture",
                    "evidence": ["invented"],
                }
            )

    with pytest.raises(ResearchError, match="invalid evidence"):
        await AdvisoryAgents(Provider()).review_signal(
            "1" * 24, "a" * 64, "b" * 64, {"bar:1": "closed"}
        )


async def test_bridge_cannot_choose_between_duplicate_display_names(
    campaigns, request_spec, tmp_path
):
    from stratosphere.models.streaming import ModelError
    from stratosphere.research.service import ResearchService
    from stratosphere.research.store import ResearchStore
    from stratosphere.workflow.runtime import Orchestrator

    _, plan = await campaigns.prepare(
        "Compare", proposal(request_spec, request_spec.model_copy(update={"fast": 4}))
    )

    class Provider:
        async def stream(self, messages):
            yield json.dumps({"action": "delegate", "strategy_id": plan.candidates[1].strategy_id})

    async def online():
        pass

    bridge = Orchestrator(
        Provider(),
        campaigns.database,
        ResearchService(ResearchStore(tmp_path / "legacy"), online_check=online),
        yahoo=campaigns.yahoo,
        campaigns=campaigns,
    )
    with pytest.raises(ModelError, match="unambiguous"):
        await bridge.route("Analyze Authored comparison")
    assert bridge.active_strategy is None
