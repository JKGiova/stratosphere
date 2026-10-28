"""Authored paper accounts, models and HTTP responses; no real keys or broker calls."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
from pydantic import SecretStr

from stratosphere.legacy.alpaca_paper import DATA, PAPER, AlpacaPaper, LiquidPaper, PaperKeys
from stratosphere.registry.workspace import WorkspaceDatabase
from stratosphere.research.store import ResearchError
from stratosphere.workflow.execution import PaperRunner
from stratosphere.workflow.portfolios import PaperPortfolios, package_markdown
from stratosphere.workflow.research import payload_digest
from stratosphere.yahoo.contracts import YahooRequest

ACCOUNT = "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa"
STEP = 3600 * 10**9
START = int(datetime(2025, 1, 1, tzinfo=UTC).timestamp() * 1e9)


class Evidence:
    def __init__(self, report):
        self.value = report

    def report(self, identity):
        assert identity == "a" * 32
        return self.value


@pytest.fixture
def portfolio(tmp_path):
    db = WorkspaceDatabase(tmp_path / "library.sqlite3")
    request = YahooRequest(
        symbol="BTC-USD",
        asset_class="crypto_spot",
        interval="1h",
        start="2025-01-01",
        end="2025-01-03",
        fast=3,
        slow=8,
        quantity="1",
    )
    owner = db.register(request.specification(), "Authored SMA", {})
    report = {"strategy_sha256": owner["spec_sha256"], "request": request.model_dump(mode="json")}
    campaigns = Evidence(
        {
            "results": [
                {
                    "state": "completed",
                    "strategy_id": owner["public_id"],
                    "job_id": "b" * 32,
                    "report": report,
                    "report_sha256": payload_digest(report),
                }
            ]
        }
    )
    service = PaperPortfolios(tmp_path / "portfolios.sqlite3", campaigns=campaigns, database=db)
    service.bind("alpaca", ACCOUNT)
    package = service.prepare("a" * 32, owner["public_id"], "alpaca")
    return service, package


class Models:
    def __init__(self, *, verdict="reliable", size="50", invented=False):
        self.calls = []
        self.verdict, self.size, self.invented = verdict, size, invented

    async def stream(self, messages):
        v = json.loads(messages[-1]["content"])
        self.calls.append(v)
        if "facts" in v:
            reply = {
                "strategy_id": v["strategy_id"],
                "signal_sha256": v["signal_sha256"],
                "context_sha256": v["context_sha256"],
                "verdict": self.verdict,
                "explanation": "Authored decision",
                "evidence": ["unknown" if self.invented else "trigger"],
            }
        elif "definition" in v:
            reply = {
                "portfolio_id": v["snapshot"]["portfolio_id"],
                "snapshot_sha256": v["snapshot_sha256"],
                "allocations": {s: "2500" for s in v["snapshot"]["strategy_capital"]},
                "explanation": "Authored capital proposal",
            }
        else:
            reply = {
                "portfolio_id": v["portfolio_id"],
                "strategy_id": v["strategy_id"],
                "snapshot_sha256": v["snapshot_sha256"],
                "signal_sha256": v["signal_sha256"],
                "requested_notional": self.size,
                "explanation": "Authored size proposal",
            }
        yield json.dumps(reply)


class Adapter:
    provider, mode, supports_automatic_orders = "alpaca", "paper", True

    def __init__(self):
        self.now = START + 10 * STEP + 5 * 10**9
        self.values = ["100"] * 10
        self.account_value = {
            "account_id": ACCOUNT,
            "currency": "USD",
            "equity": "10000",
            "cash": "10000",
            "last_equity": "10000",
        }
        self.orders, self.positions_value, self.posts = {}, {}, []
        self.timeout = False
        self.quote_old = False

    async def account(self):
        return self.account_value.copy()

    async def asset(self, symbol):
        return {"symbol": symbol, "increment": "0.01", "minimum": "0.01", "price_increment": "0.01"}

    async def positions(self):
        return self.positions_value.copy()

    async def open_orders(self):
        return [
            o.copy()
            for o in self.orders.values()
            if o["status"] not in {"filled", "canceled", "expired", "rejected"}
        ]

    async def find_order(self, identity):
        return self.orders.get(identity)

    async def bars(self, symbol, seconds, count, now):
        closed = (self.now // STEP) * STEP
        return [
            {"open_ns": closed - (count - i) * STEP, "close": p} for i, p in enumerate(self.values)
        ]

    async def quote(self, symbol):
        return {
            "bid": "100",
            "ask": "100",
            "as_of_ns": self.now - (60 * 10**9 if self.quote_old else 0),
        }

    async def submit(self, body):
        self.posts.append(body.copy())
        if self.timeout:
            raise ResearchError("Authored timeout after dispatch")
        observation = body | {
            "id": "cccccccc-cccc-4ccc-cccc-cccccccccccc",
            "status": "new",
            "filled_qty": "0",
        }
        self.orders[body["client_order_id"]] = observation
        return observation.copy()

    def next(self, price):
        self.now += STEP
        self.values = self.values[1:] + [price]

    def fill(self, *, partial=False):
        order = next(reversed(self.orders.values()))
        qty = Decimal(order["qty"]) / (2 if partial else 1)
        order.update(status="partially_filled" if partial else "filled", filled_qty=str(qty))
        old = Decimal(self.positions_value.get(order["symbol"], "0"))
        owned = old + qty if order["side"] == "buy" else old - qty
        if owned:
            self.positions_value[order["symbol"]] = str(owned)
        else:
            self.positions_value.pop(order["symbol"], None)


async def warmup(portfolio, model=None):
    service, package = portfolio
    service.approve(package.package_id, package.digest)
    adapter = Adapter()
    model = model or Models()
    runner = PaperRunner(service, adapter, model, clock_ns=lambda: adapter.now)
    assert await runner.tick() == []
    assert adapter.posts == [] and model.calls == []
    adapter.next("110")
    return runner, adapter, model


def test_both_portfolios_have_separate_stable_risk_agents_and_liquid_blocker(portfolio):
    service, _ = portfolio
    rows = service.portfolios()
    assert {r["provider"] for r in rows} == {"alpaca", "liquid"}
    assert len({r["id"] for r in rows}) == len({r["agent_id"] for r in rows}) == 2
    liquid = next(r for r in rows if r["provider"] == "liquid")
    assert liquid["execution_available"] is False and "paper-only" in liquid["blocker"]
    with pytest.raises(ResearchError):
        service.bind("liquid", ACCOUNT)


async def test_liquid_adapter_cannot_dispatch_or_reuse_global_paper_toggle():
    with pytest.raises(ResearchError, match="paper-only"):
        await LiquidPaper().submit({})


def test_one_final_approval_is_exact_and_survives_restart(portfolio):
    service, package = portfolio
    with pytest.raises(ResearchError):
        service.approve(package.package_id, "0" * 64)
    assert service.packages(approved=True) == []
    assert "single approval" in package_markdown(package)
    service.approve(package.package_id, package.digest)
    reopened = PaperPortfolios(service.path, campaigns=service.campaigns, database=service.database)
    assert reopened.packages(approved=True) == [package]
    with pytest.raises(ResearchError):
        reopened.approve(package.package_id, package.digest)
    with pytest.raises(ResearchError):
        reopened.bind("alpaca", "different-account")


def test_changed_report_or_package_cannot_be_approved(portfolio):
    service, package = portfolio
    service.campaigns.value["results"][0]["state"] = "failed"
    with pytest.raises(ResearchError, match="report changed"):
        service.approve(package.package_id, package.digest)


def test_package_tamper_is_rejected(portfolio):
    service, package = portfolio
    with service.transaction() as db:
        payload = package.model_dump(mode="json")
        payload["limits"]["max_entries_per_day"] = 100
        db.execute(
            "UPDATE paper_packages SET payload=? WHERE id=?",
            (json.dumps(payload), package.package_id),
        )
    with pytest.raises(ResearchError, match="package changed"):
        service.package(package.package_id)


async def test_automatic_entry_and_deterministic_exit_need_no_extra_approval(portfolio):
    runner, adapter, models = await warmup(portfolio)
    result = await runner.tick()
    assert len(result) == len(adapter.posts) == 1 and len(models.calls) == 3
    body = adapter.posts[0]
    assert body["side"] == "buy" and body["type"] == "limit" and body["time_in_force"] == "ioc"
    assert Decimal(body["qty"]) < 1
    assert Decimal(body["qty"]) * Decimal(body["limit_price"]) * Decimal("1.01") <= 50
    adapter.fill()
    assert await runner.tick() == []
    adapter.next("80")
    # A model outage cannot block an owned exit.
    result = await runner.tick()
    assert len(result) == 1 and adapter.posts[-1]["side"] == "sell"
    assert len(models.calls) == 3
    adapter.fill()
    assert await runner.tick() == []


@pytest.mark.parametrize(
    "models", [Models(verdict="not_reliable"), Models(size="101"), Models(invented=True)]
)
async def test_negative_invented_or_over_budget_reviews_never_submit(portfolio, models):
    runner, adapter, _ = await warmup(portfolio, models)
    assert await runner.tick() == [] and adapter.posts == []
    assert await runner.tick() == []  # The denied trigger stays consumed.
    assert len(models.calls) <= 3


async def test_stale_quote_denies_without_order_or_retry(portfolio):
    runner, adapter, _ = await warmup(portfolio)
    adapter.quote_old = True
    assert await runner.tick() == [] and adapter.posts == []
    adapter.quote_old = False
    assert await runner.tick() == [] and adapter.posts == []


async def test_timeout_retains_reservation_and_restart_never_resubmits(portfolio):
    runner, adapter, models = await warmup(portfolio)
    adapter.timeout = True
    assert await runner.tick() == []
    assert len(adapter.posts) == 1
    with runner.store.transaction() as db:
        row = db.execute("SELECT * FROM paper_orders").fetchone()
        assert row["state"] == "ambiguous" and Decimal(row["reserved"]) > 0
    reopened = PaperRunner(runner.store, adapter, models, clock_ns=lambda: adapter.now)
    with pytest.raises(ResearchError, match="provider-confirmed"):
        await reopened.tick()
    assert len(adapter.posts) == 1
    assert runner.store.portfolios()[0]["halted"]


async def test_partial_fill_remains_reserved_and_pending(portfolio):
    runner, adapter, _ = await warmup(portfolio)
    await runner.tick()
    adapter.fill(partial=True)
    account, quantities, committed, pending, _ = await runner.reconcile()
    assert pending > 0 and sum(committed.values()) > 0 and sum(quantities.values()) > 0
    assert len(adapter.posts) == 1


async def test_external_exposure_latches_halt_and_no_orders(portfolio):
    runner, adapter, _ = await warmup(portfolio)
    adapter.positions_value["ETH/USD"] = "1"
    with pytest.raises(ResearchError, match="authoritative"):
        await runner.tick()
    assert not adapter.posts
    assert runner.store.portfolios()[0]["halted"]


async def test_changed_account_during_inference_denies(portfolio):
    class ChangingModels(Models):
        async def stream(self, messages):
            async for text in super().stream(messages):
                adapter.account_value["cash"] = "9999"
                yield text

    runner, adapter, _ = await warmup(portfolio, ChangingModels())
    assert await runner.tick() == [] and not adapter.posts


async def test_model_delay_cannot_use_expired_snapshot(portfolio):
    class SlowModels(Models):
        async def stream(self, messages):
            async for text in super().stream(messages):
                adapter.now += 61 * 10**9
                yield text

    runner, adapter, _ = await warmup(portfolio, SlowModels())
    assert await runner.tick() == [] and not adapter.posts


async def test_missing_bar_or_revised_warmup_never_backfills(portfolio):
    runner, adapter, _ = await warmup(portfolio)
    adapter.next("120")
    with pytest.raises(ResearchError, match="no backfill"):
        await runner.tick()
    assert not adapter.posts


async def test_account_lease_excludes_second_client(portfolio):
    runner, adapter, _ = await warmup(portfolio)
    with runner.store.lease("alpaca"):
        with pytest.raises(ResearchError, match="another client"):
            await runner.tick()
    assert not adapter.posts


async def test_daily_loss_is_latched_and_cannot_reset_with_recovery(portfolio):
    runner, adapter, _ = await warmup(portfolio)
    adapter.account_value.update(equity="9700", cash="9700")
    assert await runner.tick() == []
    adapter.account_value.update(equity="10000", cash="10000")
    assert "loss" in runner.store.portfolios()[0]["halted"]
    assert not adapter.posts


@pytest.mark.parametrize("bad", ["https://api.alpaca.markets", "https://evil.invalid"])
async def test_transport_is_paper_only_and_never_follows_redirects(bad):
    calls = []

    def handle(request):
        calls.append(str(request.url))
        return httpx.Response(302, headers={"location": bad + "/v2/account"})

    adapter = AlpacaPaper(
        PaperKeys(key=SecretStr("fixture-key"), secret=SecretStr("fixture-secret")),
        transport=httpx.MockTransport(handle),
    )
    with pytest.raises(ResearchError):
        await adapter.account()
    assert calls == [PAPER + "/v2/account"]


async def test_credential_echo_and_ambiguous_post_are_sanitized():
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, json={"secret": "fixture-secret"})

    adapter = AlpacaPaper(
        PaperKeys(key=SecretStr("fixture-key"), secret=SecretStr("fixture-secret")),
        transport=httpx.MockTransport(handle),
    )
    with pytest.raises(ResearchError) as error:
        await adapter.account()
    assert "fixture-secret" not in str(error.value)
    assert len(calls) == 1


async def test_http_order_readback_and_submission_use_exact_fixed_endpoints():
    calls = []
    body = {
        "client_order_id": "d" * 32,
        "symbol": "BTC/USD",
        "side": "buy",
        "qty": "0.01",
        "type": "limit",
        "time_in_force": "ioc",
        "limit_price": "100",
    }

    def handle(request):
        calls.append(request)
        return httpx.Response(200, json=body | {"id": ACCOUNT, "status": "new", "filled_qty": "0"})

    adapter = AlpacaPaper(
        PaperKeys(key=SecretStr("fixture-key"), secret=SecretStr("fixture-secret")),
        transport=httpx.MockTransport(handle),
    )
    await adapter.submit(body)
    await adapter.find_order(body["client_order_id"])
    assert [r.url.host for r in calls] == ["paper-api.alpaca.markets"] * 2
    assert calls[0].method == "POST" and json.loads(calls[0].content) == body
    assert calls[1].url.path == "/v2/orders:by_client_order_id"
    assert DATA not in str(calls[0].url)


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_strategy_fraction", "0"),
        ("max_position_fraction", "0.5"),
        ("max_risk_fraction", "NaN"),
        ("max_daily_loss_fraction", "-0.1"),
        ("max_quote_age_ns", 61_000_000_000),
        ("max_entries_per_day", 101),
        ("fee_buffer_fraction", "0"),
        ("max_slippage_fraction", "0.06"),
    ],
)
def test_paper_limits_reject_unsafe_or_unbounded_values(field, value):
    from stratosphere.workflow.portfolios import PaperLimits

    with pytest.raises(ValueError):
        PaperLimits(**{field: value})


async def test_tampered_order_and_provider_evidence_fail_closed(portfolio):
    runner, adapter, _ = await warmup(portfolio)
    await runner.tick()
    adapter.fill()
    await runner.reconcile()
    with runner.store.transaction() as db:
        row = db.execute("SELECT id,observation FROM paper_orders").fetchone()
        observation = json.loads(row["observation"])
        observation["filled_qty"] = "0"
        db.execute(
            "UPDATE paper_orders SET observation=? WHERE id=?", (json.dumps(observation), row["id"])
        )
    with pytest.raises(ResearchError, match="confirmation changed"):
        await runner.reconcile()
    assert len(adapter.posts) == 1


async def test_cancellation_after_dispatch_is_uncertain_and_never_replayed(portfolio):
    import asyncio

    runner, adapter, models = await warmup(portfolio)

    async def canceled(body):
        adapter.posts.append(body)
        raise asyncio.CancelledError

    adapter.submit = canceled
    with pytest.raises(asyncio.CancelledError):
        await runner.tick()
    with runner.store.transaction() as db:
        assert db.execute("SELECT state FROM paper_orders").fetchone()[0] == "ambiguous"
    with pytest.raises(ResearchError):
        await PaperRunner(runner.store, adapter, models, clock_ns=lambda: adapter.now).tick()
    assert len(adapter.posts) == 1


async def test_late_fill_or_terminal_reopened_order_stops_account(portfolio):
    runner, adapter, _ = await warmup(portfolio)
    await runner.tick()
    adapter.fill()
    await runner.reconcile()
    order = next(iter(adapter.orders.values()))
    order["status"] = "new"
    with pytest.raises(ResearchError, match="authoritative"):
        await runner.reconcile()
    assert len(adapter.posts) == 1


async def test_final_quote_expiry_during_reconciliation_denies(portfolio):
    runner, adapter, _ = await warmup(portfolio)
    original = runner.reconcile
    calls = 0

    async def slow():
        nonlocal calls
        calls += 1
        result = await original()
        if calls == 2:
            adapter.now += 31 * 10**9
        return result

    runner.reconcile = slow
    assert await runner.tick() == [] and not adapter.posts


async def test_real_http_shapes_for_account_instrument_bars_and_quotes():
    now = START + 10 * STEP + 5 * 10**9

    def handle(request):
        if request.url.path == "/v2/account":
            return httpx.Response(
                200,
                json={
                    "id": ACCOUNT,
                    "currency": "USD",
                    "status": "ACTIVE",
                    "trading_blocked": False,
                    "account_blocked": False,
                    "equity": "10000",
                    "cash": "10000",
                    "last_equity": "10000",
                },
            )
        if request.url.path.startswith("/v2/assets/"):
            return httpx.Response(
                200,
                json={
                    "symbol": "BTC/USD",
                    "class": "crypto",
                    "status": "active",
                    "tradable": True,
                    "min_trade_increment": "0.01",
                    "min_order_size": "0.01",
                    "price_increment": "0.01",
                },
            )
        if request.url.path.endswith("/bars"):
            bars = [
                {"t": datetime.fromtimestamp((START + i * STEP) / 1e9, UTC).isoformat(), "c": 100}
                for i in range(11)
            ]
            return httpx.Response(200, json={"bars": {"BTC/USD": bars}, "next_page_token": None})
        if request.url.path.endswith("/quotes"):
            return httpx.Response(
                200,
                json={
                    "quotes": {
                        "BTC/USD": {
                            "bp": 99,
                            "ap": 100,
                            "t": datetime.fromtimestamp(now / 1e9, UTC).isoformat(),
                        }
                    }
                },
            )
        raise AssertionError("Unexpected operation")

    adapter = AlpacaPaper(
        PaperKeys(key=SecretStr("fixture-key"), secret=SecretStr("fixture-secret")),
        transport=httpx.MockTransport(handle),
    )
    assert (await adapter.account())["account_id"] == ACCOUNT
    assert (await adapter.asset("BTC/USD"))["minimum"] == "0.01"
    bars = await adapter.bars("BTC/USD", 3600, 10, now)
    assert len(bars) == 10 and bars[-1]["open_ns"] == START + 9 * STEP
    assert (await adapter.quote("BTC/USD"))["as_of_ns"] == now


async def test_equity_mark_moves_recheck_limits_without_false_account_conflict(portfolio):
    class MarkingModels(Models):
        async def stream(self, messages):
            async for text in super().stream(messages):
                adapter.account_value["equity"] = "10001"
                yield text

    runner, adapter, _ = await warmup(portfolio, MarkingModels())
    assert len(await runner.tick()) == 1 and len(adapter.posts) == 1
    with runner.store.transaction() as db:
        receipt = json.loads(
            db.execute("SELECT payload FROM paper_receipts WHERE stage='final_gate'").fetchone()[0]
        )
    assert receipt["fresh_equity"] == "10001"


async def test_equity_drop_during_inference_rechecks_daily_limit(portfolio):
    class FallingModels(Models):
        async def stream(self, messages):
            async for text in super().stream(messages):
                adapter.account_value["equity"] = "9700"
                yield text

    runner, adapter, _ = await warmup(portfolio, FallingModels())
    assert await runner.tick() == [] and not adapter.posts
    assert "loss" in runner.store.portfolios()[0]["halted"]


async def test_daily_entry_budget_counts_entries_and_does_not_block_owned_exits(portfolio):
    from stratosphere.workflow.portfolios import PaperLimits

    service, original = portfolio
    package = service.prepare(
        "a" * 32, original.strategy_id, "alpaca", limits=PaperLimits(max_entries_per_day=2)
    )
    runner, adapter, _ = await warmup((service, package))
    for price in ("110", "80", "130", "50"):
        if price != "110":
            adapter.next(price)
        assert len(await runner.tick()) == 1
        adapter.fill()
        assert await runner.tick() == []
    adapter.next("180")
    assert await runner.tick() == []
    assert [o["side"] for o in adapter.posts] == ["buy", "sell", "buy", "sell"]


async def test_daily_loss_halt_still_allows_deterministic_owned_exit(portfolio):
    runner, adapter, models = await warmup(portfolio)
    await runner.tick()
    adapter.fill()
    await runner.tick()
    adapter.account_value.update(equity="9700", cash="9700")
    adapter.next("80")
    assert len(await runner.tick()) == 1
    assert adapter.posts[-1]["side"] == "sell" and len(models.calls) == 3
    assert "loss" in runner.store.portfolios()[0]["halted"]


@pytest.mark.parametrize("damage", ["schema", "portfolio", "version"])
def test_damaged_journal_never_recreates_authority_or_clears_halts(portfolio, damage):
    service, _ = portfolio
    service.halt("alpaca", "Retained risk stop")
    with service.transaction() as db:
        if damage == "schema":
            db.execute("DROP TABLE paper_orders")
        elif damage == "portfolio":
            db.execute("DELETE FROM paper_portfolios WHERE provider='alpaca'")
        else:
            db.execute("PRAGMA user_version=0")
    with pytest.raises(ResearchError, match="damaged|implicit migration"):
        PaperPortfolios(service.path, campaigns=service.campaigns, database=service.database)
