"""Reviewed, authored signal-to-reservation gates; no real provider or model authority."""

import hashlib
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest
from pydantic import ValidationError
from test_paper_ledger import fill
from test_paper_runtime import approve, push
from test_paper_runtime import stream as stream
from test_research_engine import native_plan as native_plan

from stratosphere.paper import decisions as module
from stratosphere.paper.decisions import (
    AuthoredQuote,
    DecisionPlan,
    SignalDecisions,
    StreamPin,
    implementation_digest,
)
from stratosphere.paper.ledger import Intent, PaperError, PaperLedger
from stratosphere.paper.runtime import HOUR_NS


def build_plan(stream, **changes):
    runtime, mandate, *_ = stream
    return DecisionPlan.model_validate(
        dict(
            account_id=mandate.instance.account_id,
            implementation_sha256=implementation_digest(),
            streams=[
                StreamPin(instance_id=mandate.instance.instance_id, stream_sha256=mandate.digest)
            ],
            max_gross_notional="200",
            max_concentration_pct="30",
            max_loss_quote="100",
            max_drawdown_quote="100",
            max_spread_bps="200",
            slippage_bps="0",
            fee_rate_cap="0.001",
            max_quote_age_ns=2 * 10**9,
            signal_ttl_ns=10**9,
            intent_ttl_ns=5 * 10**8,
        )
        | changes
    )


@pytest.fixture
def decision(stream, market_bars):
    approve(stream)
    for bar in market_bars[:12]:
        push(stream, bar)
    runtime, mandate, *_ = stream
    service = SignalDecisions(runtime.ledger, runtime.registry)
    plan = build_plan(stream)
    signal = runtime.inspect(mandate.instance.instance_id)["signals"][-1]
    assert signal["side"] == "BUY"
    stamp = signal["received_ns"] + 10
    quote = AuthoredQuote(
        quote_id="e" * 64,
        bid="100",
        ask="101",
        bid_size="1",
        ask_size="1",
        event_ns=stamp,
        available_ns=stamp + 10,
    )
    return service, plan, signal, quote, stamp + 30


def activate(decision, **changes):
    service, plan, signal, quote, now = decision
    if changes:
        plan = DecisionPlan.model_validate(plan.model_dump() | changes)
    service.prepare(plan)
    service.approve(plan.digest)
    assert service.observe_quote(quote, received_ns=now) is None
    return service, plan, signal, quote, now


def evaluate(decision, *, now=None):
    service, plan, signal, quote, stamp = decision
    return service.evaluate(
        plan.streams[0].instance_id, signal["signal_sha256"], now_ns=stamp if now is None else now
    )


def test_exact_review_and_no_legacy_bypass(decision):
    service, plan, signal, quote, now = decision
    with pytest.raises(PaperError, match="Prepare"):
        evaluate(decision)
    service.prepare(plan)
    with pytest.raises(PaperError, match="not approved"):
        service.observe_quote(quote, received_ns=now)
    with pytest.raises(PaperError, match="exactly"):
        service.approve("f" * 64)
    service.approve(plan.digest)
    with pytest.raises(PaperError, match="exactly"):
        service.approve(plan.digest)
    service.observe_quote(quote, received_ns=now)
    result = evaluate(decision)
    assert result["status"] == "reserved" and result["intent"]["quantity"] == "0.01"
    order = Intent.model_validate(result["intent"])
    assert order.price_bound == "101" and order.fee_allowance == "0.00101"
    assert service.ledger.snapshot()["reserved_cash"] == "1.01101"
    arbitrary = order.model_copy(update={"intent_id": "a" * 32})
    assert service.ledger.reserve(arbitrary, now_ns=now) == "reviewed_signal_required"
    with pytest.raises(PaperError, match="reviewed signal gateway"):
        service.ledger.begin_delivery(order.intent_id, now_ns=now)
    service.begin_delivery(order.intent_id, now_ns=now)
    with pytest.raises(PaperError, match="already claimed"):
        service.begin_delivery(order.intent_id, now_ns=now)


def test_signal_consumption_restart_and_changed_quote_never_resubmits(decision, stream):
    service, plan, signal, quote, now = activate(decision)
    result = evaluate(decision)
    before = service.ledger.snapshot()
    runtime, _, account, registry = stream
    other = SignalDecisions(PaperLedger(runtime.ledger.path, account), registry)
    retained = other.evaluate(plan.streams[0].instance_id, signal["signal_sha256"], now_ns=now + 1)
    assert retained == {"status": "duplicate", "retained": result}
    assert other.ledger.snapshot() == before
    assert len(before["orders"]) == 1


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"max_gross_notional": "1"}, "gross_exposure_limit"),
        ({"max_concentration_pct": "0.01"}, "concentration_limit"),
        ({"max_spread_bps": "99"}, "spread_limit"),
    ],
)
def test_account_limits_are_explicit_and_block_before_reservation(decision, changes, reason):
    ready = activate(decision, **changes)
    result = evaluate(ready)
    assert result["status"] == "blocked" and result["reason"] == reason
    assert result["intent"] is None and not ready[0].ledger.snapshot()["orders"]
    assert evaluate(ready)["status"] == "duplicate"


@pytest.mark.parametrize(
    "patch,reason",
    [
        ({"ask_size": "0.001"}, "displayed_liquidity_limit"),
        ({"event_ns": -1}, "quote_precedes_signal_availability"),
    ],
)
def test_liquidity_and_causality(decision, patch, reason):
    service, plan, signal, quote, now = decision
    service.prepare(plan)
    service.approve(plan.digest)
    if patch.get("event_ns") == -1:
        patch = {"event_ns": signal["received_ns"] - 1, "available_ns": signal["received_ns"]}
    quote = AuthoredQuote.model_validate(quote.model_dump() | patch)
    service.observe_quote(quote, received_ns=now)
    result = evaluate((service, plan, signal, quote, now))
    assert result["reason"] == reason and not service.ledger.snapshot()["orders"]


def test_stale_signal_cannot_revive_on_a_fresh_quote(decision):
    service, plan, signal, quote, now = activate(decision)
    at = signal["received_ns"] + plan.signal_ttl_ns
    assert evaluate(decision, now=at)["reason"] == "signal_expired_or_future"
    assert evaluate(decision, now=now)["status"] == "duplicate"
    assert not service.ledger.snapshot()["orders"]


def test_unseen_or_cross_instance_signals_cannot_be_invented(decision):
    service, plan, *_ = activate(decision)
    with pytest.raises(PaperError, match="owned by this instance"):
        service.evaluate(plan.streams[0].instance_id, "0" * 64, now_ns=decision[-1])
    with pytest.raises(PaperError, match="retained stream"):
        service.evaluate("c" * 32, decision[2]["signal_sha256"], now_ns=decision[-1])
    assert not service.ledger.snapshot()["signal_plan"]["consumed"]


def test_atomic_consumption_and_reservation_crash_retry(decision, monkeypatch):
    service, *_ = activate(decision)
    before = service.ledger.snapshot()
    append = service.ledger._append

    def crash(db, chain, event):
        append(db, chain, event)
        raise RuntimeError("Before commit")

    with monkeypatch.context() as p:
        p.setattr(service.ledger, "_append", crash)
        with pytest.raises(RuntimeError, match="Before commit"):
            evaluate(decision)
    assert service.ledger.snapshot() == before
    assert evaluate(decision)["status"] == "reserved"


def test_concurrent_delivery_of_same_signal_reserves_once(decision, stream):
    service, plan, signal, quote, now = activate(decision)
    other = SignalDecisions(PaperLedger(service.ledger.path, stream[2]), stream[3])
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda s: s.evaluate(
                    plan.streams[0].instance_id, signal["signal_sha256"], now_ns=now
                ),
                [service, other],
            )
        )
    assert sorted(r["status"] for r in results) == ["duplicate", "reserved"]
    assert len(service.ledger.snapshot()["orders"]) == 1


def test_partial_fill_exit_uses_actual_attributed_quantity(decision, stream, market_bars):
    service, plan, signal, quote, now = activate(decision)
    order = Intent.model_validate(evaluate(decision)["intent"])
    service.begin_delivery(order.intent_id, now_ns=now)
    assert service.ledger.observe_fill(fill(order, "0.004", "101", "0.000404"))
    assert service.ledger.settle(order.intent_id, "canceled", confirmed_filled="0.004")
    for bar in market_bars[12:16]:
        push(stream, bar)
    runtime, mandate, *_ = stream
    exit_signal = runtime.inspect(mandate.instance.instance_id)["signals"][-1]
    assert exit_signal["side"] == "SELL"
    stamp = exit_signal["received_ns"] + 10
    next_quote = quote.model_copy(
        update={"quote_id": "f" * 64, "event_ns": stamp, "available_ns": stamp}
    )
    service.observe_quote(next_quote, received_ns=stamp)
    result = service.evaluate(
        mandate.instance.instance_id, exit_signal["signal_sha256"], now_ns=stamp
    )
    assert result["status"] == "reserved" and result["intent"]["side"] == "SELL"
    assert result["intent"]["quantity"] == "0.004"
    # Reductions remain possible under an entry-only control.
    service.ledger.block_entries()
    service.begin_delivery(result["intent"]["intent_id"], now_ns=stamp)


@pytest.mark.parametrize("fault", ["stale", "price", "liquidity", "stream", "risk"])
def test_delivery_rechecks_current_state_before_claim(decision, stream, market_bars, fault):
    ready = activate(decision, **({"max_loss_quote": "0.02"} if fault == "risk" else {}))
    service, plan, signal, quote, now = ready
    order = Intent.model_validate(evaluate(ready)["intent"])
    at = now
    if fault == "stale":
        at += plan.intent_ttl_ns
    elif fault == "stream":
        push(stream, market_bars[13])
    else:
        later = quote.model_copy(
            update={
                "quote_id": "f" * 64,
                "event_ns": now + 1,
                "available_ns": now + 1,
                **(
                    {"ask": "101.5"}
                    if fault == "price"
                    else {"bid": "50", "ask": "51"}
                    if fault == "risk"
                    else {"ask_size": "0.001"}
                ),
            }
        )
        service.observe_quote(later, received_ns=now + 1)
        at += 1
    before = service.ledger.snapshot()
    with pytest.raises(PaperError, match="blocked"):
        service.begin_delivery(order.intent_id, now_ns=at)
    assert service.ledger.snapshot() == before
    assert service.ledger.snapshot()["orders"][order.intent_id]["status"] == "reserved"


def test_account_loss_latches_survive_recovery_and_restart(decision, stream):
    ready = activate(decision, max_loss_quote="0.02", max_drawdown_quote="0.02")
    service, plan, signal, quote, now = ready
    order = Intent.model_validate(evaluate(ready)["intent"])
    service.begin_delivery(order.intent_id, now_ns=now)
    assert not service.ledger.observe_fill(fill(order, "0.01", "103", "0.00103"))
    state = service.ledger.snapshot()["signal_plan"]
    assert state["risk_latches"] == ["account_loss_limit", "account_drawdown_limit"]
    assert state["risk_findings"][0]["origin"].startswith("fill:")
    recovered = quote.model_copy(
        update={
            "quote_id": "f" * 64,
            "bid": "110",
            "ask": "111",
            "event_ns": now + 1,
            "available_ns": now + 1,
        }
    )
    service.observe_quote(recovered, received_ns=now + 1)
    other = SignalDecisions(PaperLedger(service.ledger.path, stream[2]), stream[3])
    assert other.ledger.snapshot()["signal_plan"]["risk_latches"] == state["risk_latches"]
    assert Decimal(other.ledger.snapshot()["signal_plan"]["peak_equity"]) > 1000


@pytest.mark.parametrize(
    "patch",
    [
        {"purpose": "live"},
        {"max_concentration_pct": "101"},
        {"max_loss_quote": "0"},
        {"fee_rate_cap": "1"},
        {"slippage_bps": "10000"},
        {"signal_ttl_ns": True},
        {"intent_ttl_ns": HOUR_NS},
        {"max_quote_age_ns": 0},
        {"approved": True},
    ],
)
def test_model_or_caller_cannot_expand_the_contract(decision, patch):
    with pytest.raises(ValidationError):
        DecisionPlan.model_validate(decision[1].model_dump() | patch)


def test_changed_code_blocks_new_decisions_and_delivery(decision, monkeypatch):
    service, *_ = activate(decision)
    order = Intent.model_validate(evaluate(decision)["intent"])
    monkeypatch.setattr(module, "implementation_digest", lambda: "0" * 64)
    with pytest.raises(PaperError, match="implementation changed"):
        service.begin_delivery(order.intent_id, now_ns=decision[-1])
    assert service.ledger.snapshot()["orders"][order.intent_id]["status"] == "reserved"


def add_second_stream(stream, market_bars):
    from stratosphere.paper.runtime import ClosedBarMandate
    from stratosphere.research.contracts import SmaParameters, compile_sma_spec

    runtime, mandate, account, registry = stream
    spec = compile_sma_spec(SmaParameters(fast_period=4, slow_period=10, quantity="0.01"))
    identity = registry.register(spec, "Second authored strategy", {"fixture": True})
    instance = mandate.instance.model_copy(
        update={
            "instance_id": "c" * 32,
            "strategy_id": identity["public_id"],
            "strategy_sha256": identity["spec_sha256"],
        }
    )
    runtime.ledger.add_instance(instance, registry)
    other = ClosedBarMandate.model_validate(
        mandate.model_dump()
        | {"instance": instance.model_dump(), "specification": spec.model_dump()}
    )
    runtime.prepare(other)
    runtime.approve(instance.instance_id, other.digest)
    for bar in market_bars[:12]:
        push((runtime, other, account, registry), bar)
    return other, runtime.inspect(instance.instance_id)["signals"][-1]


@pytest.mark.parametrize(
    "limit,reason",
    [
        ({"max_gross_notional": "1.5"}, "gross_exposure_limit"),
        ({"max_concentration_pct": "0.15"}, "concentration_limit"),
        ({"max_loss_quote": "0.02"}, "account_risk_limit"),
    ],
)
def test_shared_limits_include_other_strategy_pending_reservations(
    decision, stream, market_bars, limit, reason
):
    service, plan, signal, quote, now = decision
    other, other_signal = add_second_stream(stream, market_bars)
    plan = DecisionPlan.model_validate(
        plan.model_dump()
        | limit
        | {
            "streams": [
                *plan.streams,
                StreamPin(instance_id=other.instance.instance_id, stream_sha256=other.digest),
            ]
        }
    )
    activate((service, plan, signal, quote, now))
    second = SignalDecisions(PaperLedger(service.ledger.path, stream[2]), stream[3])
    tasks = [
        (service, plan.streams[0].instance_id, signal),
        (second, other.instance.instance_id, other_signal),
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda v: v[0].evaluate(v[1], v[2]["signal_sha256"], now_ns=now), tasks)
        )
    assert sorted(r["status"] for r in results) == ["blocked", "reserved"]
    assert next(r for r in results if r["status"] == "blocked")["reason"] == reason
    assert len(service.ledger.snapshot()["orders"]) == 1
    assert service.ledger.snapshot()["reserved_cash"] == "1.01101"


def test_quote_alias_cannot_replenish_already_filled_displayed_liquidity(
    decision, stream, market_bars
):
    service, plan, signal, quote, now = decision
    other, second_signal = add_second_stream(stream, market_bars)
    plan = DecisionPlan.model_validate(
        plan.model_dump()
        | {
            "streams": [
                *plan.streams,
                StreamPin(instance_id=other.instance.instance_id, stream_sha256=other.digest),
            ]
        }
    )
    quote = quote.model_copy(update={"ask_size": "0.015"})
    ready = activate((service, plan, signal, quote, now))
    order = Intent.model_validate(evaluate(ready)["intent"])
    service.begin_delivery(order.intent_id, now_ns=now)
    assert service.ledger.observe_fill(fill(order, "0.01", "101", "0.00101"))
    alias = quote.model_copy(update={"quote_id": "a" * 64})
    assert service.observe_quote(alias, received_ns=now + 1) == "duplicate"
    result = service.evaluate(
        other.instance.instance_id, second_signal["signal_sha256"], now_ns=now + 1
    )
    assert result["reason"] == "displayed_liquidity_limit"
    assert service.ledger.snapshot()["signal_plan"]["latest_quote"] == quote.quote_id


def test_projected_cost_loss_blocks_before_fills(decision):
    ready = activate(decision, max_loss_quote="0.005")
    result = evaluate(ready)
    assert result["reason"] == "account_risk_limit"
    assert "account_loss_limit" in result["metrics"]["projected_breaches"]
    assert not ready[0].ledger.snapshot()["orders"]
    assert not ready[0].ledger.snapshot()["signal_plan"]["risk_latches"]


@pytest.mark.parametrize("kind", ["id", "event"])
def test_quote_conflicts_are_retained_and_do_not_clear_on_recovery(decision, stream, kind):
    service, plan, signal, quote, now = activate(decision)
    revised = quote.model_copy(
        update={"ask": "101.5", **({"quote_id": "a" * 64} if kind == "event" else {})}
    )
    assert service.observe_quote(revised, received_ns=now + 1) == "conflicting_quote_" + kind
    fresh = quote.model_copy(
        update={"quote_id": "f" * 64, "event_ns": now + 2, "available_ns": now + 2}
    )
    service.observe_quote(fresh, received_ns=now + 2)
    assert evaluate(decision, now=now + 2)["reason"] == "quote_reconciliation_required"
    other = SignalDecisions(PaperLedger(service.ledger.path, stream[2]), stream[3])
    retained = other.ledger.snapshot()["signal_plan"]
    assert retained["quote_faults"][0]["quote"] == revised.model_dump()
    assert retained["quotes"][quote.quote_id] == quote.model_dump()


def test_rejected_quote_cannot_be_cleared_by_an_old_duplicate(decision):
    service, plan, signal, quote, now = activate(decision)
    future = quote.model_copy(
        update={"quote_id": "f" * 64, "event_ns": now + 2, "available_ns": now + 2}
    )
    assert service.observe_quote(future, received_ns=now) == "future_quote"
    assert service.observe_quote(quote, received_ns=now) == "duplicate"
    assert evaluate(decision)["reason"] == "no_current_quote"


def test_peak_drawdown_latch_allows_fresh_attributed_reduction(decision, stream, market_bars):
    ready = activate(decision, max_drawdown_quote="0.1")
    service, plan, signal, quote, now = ready
    order = Intent.model_validate(evaluate(ready)["intent"])
    service.begin_delivery(order.intent_id, now_ns=now)
    service.ledger.observe_fill(fill(order, "0.01", "101", "0.00101"))
    for tick, bid, ask in [(1, "200", "201"), (2, "185", "186")]:
        value = quote.model_copy(
            update={
                "quote_id": str(tick) * 64,
                "bid": bid,
                "ask": ask,
                "event_ns": now + tick,
                "available_ns": now + tick,
            }
        )
        service.observe_quote(value, received_ns=now + tick)
    state = service.ledger.snapshot()["signal_plan"]
    assert state["risk_latches"] == ["account_drawdown_limit"]
    assert Decimal(state["risk_findings"][0]["metrics"]["loss_quote"]) == 0
    for bar in market_bars[12:16]:
        push(stream, bar)
    closing = stream[0].inspect(plan.streams[0].instance_id)["signals"][-1]
    at = closing["received_ns"] + 10
    value = quote.model_copy(
        update={
            "quote_id": "f" * 64,
            "bid": "185",
            "ask": "186",
            "event_ns": at,
            "available_ns": at,
        }
    )
    service.observe_quote(value, received_ns=at)
    result = service.evaluate(plan.streams[0].instance_id, closing["signal_sha256"], now_ns=at)
    assert result["status"] == "reserved" and result["intent"]["side"] == "SELL"
    service.begin_delivery(result["intent"]["intent_id"], now_ns=at)


def test_registry_is_rechecked_at_delivery(decision, monkeypatch):
    from stratosphere.registry.workspace import RegistryError

    service, *_ = activate(decision)
    order = Intent.model_validate(evaluate(decision)["intent"])

    def missing(identity):
        raise RegistryError("Fixture identity no longer readable")

    monkeypatch.setattr(service.registry, "strategy", missing)
    with pytest.raises(RegistryError, match="identity"):
        service.begin_delivery(order.intent_id, now_ns=decision[-1])
    assert service.ledger.snapshot()["orders"][order.intent_id]["status"] == "reserved"


@pytest.mark.engine
def test_reviewed_signal_reservation_native_fill_and_restart_match_account_cache(
    decision, stream, native_plan
):
    from importlib.metadata import version

    from nautilus_trader.backtest.engine import BacktestEngine
    from nautilus_trader.config import BacktestEngineConfig, LoggingConfig
    from nautilus_trader.model.currencies import BTC, USDT
    from nautilus_trader.model.data import QuoteTick
    from nautilus_trader.model.enums import AccountType, OmsType, OrderSide, TimeInForce
    from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, Symbol, Venue
    from nautilus_trader.model.instruments import CurrencyPair
    from nautilus_trader.model.objects import Money, Price, Quantity
    from nautilus_trader.trading.strategy import Strategy

    from stratosphere.paper.ledger import Fill

    assert version("nautilus_trader") == "1.231.0"
    service, plan, signal, quote, now = activate(decision)
    order = Intent.model_validate(evaluate(decision)["intent"])
    instrument_id = InstrumentId.from_str("BTCUSDT.SIM")
    instrument = CurrencyPair(
        instrument_id=instrument_id,
        raw_symbol=Symbol("BTCUSDT"),
        base_currency=BTC,
        quote_currency=USDT,
        price_precision=8,
        size_precision=8,
        price_increment=Price.from_str("0.00000001"),
        size_increment=Quantity.from_str("0.00000001"),
        min_quantity=Quantity.from_str("0.00000001"),
        maker_fee=Decimal("0.001"),
        taker_fee=Decimal("0.001"),
        ts_event=0,
        ts_init=0,
    )

    class Fixture(Strategy):
        def on_start(self):
            self.subscribe_quote_ticks(instrument_id)

        def on_quote_tick(self, tick):
            service.begin_delivery(order.intent_id, now_ns=tick.ts_init)
            self.submit_order(
                self.order_factory.market(
                    instrument_id=instrument_id,
                    order_side=OrderSide.BUY,
                    quantity=Quantity.from_str(order.quantity),
                    time_in_force=TimeInForce.IOC,
                    client_order_id=ClientOrderId("reviewed-" + order.intent_id),
                )
            )

        def on_event(self, event):
            if type(event).__name__ == "OrderFilled":
                assert service.ledger.observe_fill(
                    Fill(
                        event_id=hashlib.sha256(str(event.trade_id).encode()).hexdigest(),
                        intent_id=order.intent_id,
                        quantity=str(event.last_qty),
                        price=str(event.last_px),
                        notional=str(
                            instrument.notional_value(event.last_qty, event.last_px).as_decimal()
                        ),
                        fee=str(event.commission.as_decimal()),
                    )
                )

    engine = BacktestEngine(config=BacktestEngineConfig(logging=LoggingConfig(bypass_logging=True)))
    try:
        engine.add_venue(
            venue=Venue("SIM"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=None,
            starting_balances=[Money.from_decimal(Decimal("1000"), USDT), Money(0, BTC)],
            use_message_queue=False,
            use_random_ids=False,
            bar_execution=False,
            trade_execution=False,
            allow_cash_borrowing=False,
            liquidity_consumption=True,
        )
        engine.add_instrument(instrument)
        engine.add_strategy(Fixture())
        engine.add_data(
            [
                QuoteTick(
                    instrument_id,
                    Price.from_str("100.00000000"),
                    Price.from_str("101.00000000"),
                    Quantity.from_str("1.00000000"),
                    Quantity.from_str("1.00000000"),
                    now + 1,
                    now + 1,
                )
            ]
        )
        engine.run()
        state = service.ledger.snapshot()
        balances = engine.cache.accounts()[0].balances_total()
        assert Decimal(state["cash"]) == balances[USDT].as_decimal() == Decimal("998.98899")
        assert (
            Decimal(state["instances"][plan.streams[0].instance_id]["base"])
            == balances[BTC].as_decimal()
            == Decimal("0.01")
        )
        assert (
            state["orders"][order.intent_id]["status"] == "filled" and state["reserved_cash"] == "0"
        )
        restarted = SignalDecisions(PaperLedger(service.ledger.path, stream[2]), stream[3])
        assert restarted.ledger.snapshot() == state
        assert (
            restarted.evaluate(
                plan.streams[0].instance_id, signal["signal_sha256"], now_ns=now + 2
            )["status"]
            == "duplicate"
        )
        with pytest.raises(PaperError, match="already claimed"):
            restarted.begin_delivery(order.intent_id, now_ns=now + 2)
    finally:
        engine.dispose()


def test_pending_exit_floor_includes_its_worst_slippage_and_fee(decision, stream, market_bars):
    ready = activate(decision, slippage_bps="500")
    service, plan, signal, quote, now = ready
    opening = Intent.model_validate(evaluate(ready)["intent"])
    service.begin_delivery(opening.intent_id, now_ns=now)
    assert service.ledger.observe_fill(fill(opening, "0.01", "101", "0.00101"))
    for bar in market_bars[12:16]:
        push(stream, bar)
    closing = stream[0].inspect(plan.streams[0].instance_id)["signals"][-1]
    at = closing["received_ns"] + 10
    newquote = quote.model_copy(update={"quote_id": "f" * 64, "event_ns": at, "available_ns": at})
    service.observe_quote(newquote, received_ns=at)
    result = service.evaluate(plan.streams[0].instance_id, closing["signal_sha256"], now_ns=at)
    assert result["status"] == "reserved"
    exit_order = Intent.model_validate(result["intent"])
    assert exit_order.price_bound == "95" and exit_order.fee_allowance == "0.00095"
    # Execute the reviewed worst price and fee; actual cash must match the prior floor.
    service.begin_delivery(exit_order.intent_id, now_ns=at)
    assert service.ledger.observe_fill(fill(exit_order, "0.01", "95", "0.00095", event="exit"))
    assert (
        service.ledger.snapshot()["cash"]
        == result["metrics"]["projected_equity_floor"]
        == "999.93804"
    )


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"bid": "10000", "ask": "10001"}, "instance_size_limit"),
        ({"bid": "0.00000001", "ask": "0.00000001"}, None),
    ],
)
def test_extreme_quote_bounds_are_retained_or_blocked_before_intent_construction(
    decision, changes, reason
):
    service, plan, signal, quote, now = decision
    quote = quote.model_copy(update=changes)
    ready = activate((service, plan, signal, quote, now))
    result = evaluate(ready)
    assert result["reason"] == reason
    assert result["status"] == ("blocked" if reason else "reserved")


def test_previously_denied_intent_identity_cannot_revive_through_the_signal_gateway(decision):
    service, plan, signal, quote, now = decision
    identity = plan.streams[0].instance_id
    intent_id = hashlib.sha256(
        (plan.digest + identity + signal["signal_sha256"]).encode()
    ).hexdigest()[:32]
    denied = Intent(
        intent_id=intent_id,
        instance_id=identity,
        side="BUY",
        quantity="10",
        price_bound="101",
        fee_allowance="0",
        created_ns=now,
        expires_ns=now + 100,
        quote_ns=quote.event_ns,
        signal_sha256=signal["signal_sha256"],
        proposal_sha256=plan.digest,
    )
    assert service.ledger.reserve(denied, now_ns=now) == "instance_size_limit"
    ready = activate(decision)
    assert evaluate(ready)["reason"] == "intent_id_already_retained"
    state = service.ledger.snapshot()
    assert state["decisions"][intent_id]["intent"] == denied.model_dump()
    assert not state["orders"]


def test_preparing_another_stream_cannot_downgrade_the_reviewed_journal(
    decision, stream, market_bars
):
    import sqlite3

    service, plan, signal, quote, now = activate(decision)
    other, other_signal = add_second_stream(stream, market_bars)
    with sqlite3.connect(service.ledger.path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 3
    result = service.evaluate(other.instance.instance_id, other_signal["signal_sha256"], now_ns=now)
    assert result["reason"] == "stream_not_reviewed"
    assert not service.ledger.snapshot()["orders"]
