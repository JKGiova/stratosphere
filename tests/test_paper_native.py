"""Actual pinned native IOC events exercise the durable journal using authored quotes."""

import hashlib
from decimal import Decimal
from importlib.metadata import version

import pytest
from test_paper_ledger import intent
from test_paper_ledger import paper as paper
from test_research_engine import native_plan as native_plan

from stratosphere.paper.ledger import Fill, PaperLedger


@pytest.mark.engine
@pytest.mark.parametrize(
    "bid,ask", [("100", "101"), ("100.12345678", "101.12345678"), ("0.00000001", "0.00000002")]
)
def test_native_partial_ioc_stream_batches_and_reopened_journal(paper, native_plan, bid, ask):
    from nautilus_trader.backtest.engine import BacktestEngine
    from nautilus_trader.config import BacktestEngineConfig, LoggingConfig
    from nautilus_trader.model.currencies import BTC, USDT
    from nautilus_trader.model.data import QuoteTick
    from nautilus_trader.model.enums import AccountType, OmsType, OrderSide, TimeInForce
    from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, Symbol, Venue
    from nautilus_trader.model.instruments import CurrencyPair
    from nautilus_trader.model.objects import Money, Price, Quantity
    from nautilus_trader.trading.strategy import Strategy

    assert version("nautilus_trader") == "1.231.0"
    ledger, account, instance, _ = paper
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
        maker_fee=Decimal(0),
        taker_fee=Decimal(0),
        ts_event=0,
        ts_init=0,
    )

    class NativeFixture(Strategy):
        def __init__(self):
            super().__init__()
            self.ledger = ledger
            self.orders = {}
            self.ticks = self.starts = self.stops = 0
            self.fills = 0

        def on_start(self):
            self.starts += 1
            self.subscribe_quote_ticks(instrument_id)

        def on_stop(self):
            self.stops += 1

        def on_quote_tick(self, tick):
            self.ticks += 1
            opening = self.ticks == 1
            proposal = intent(
                instance,
                self.ticks,
                side="BUY" if opening else "SELL",
                quantity="0.7" if opening else "0.1",
                price_bound=ask if opening else bid,
                created_ns=tick.ts_init,
                expires_ns=tick.ts_init + 10,
                quote_ns=tick.ts_init,
            )
            assert self.ledger.reserve(proposal, now_ns=tick.ts_init) is None
            self.ledger.begin_delivery(proposal.intent_id, now_ns=tick.ts_init)
            client_id = ClientOrderId("paper-" + proposal.intent_id)
            self.orders[str(client_id)] = proposal
            order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=OrderSide.BUY if opening else OrderSide.SELL,
                quantity=Quantity.from_str("0.70000000" if opening else "0.10000000"),
                time_in_force=TimeInForce.IOC,
                client_order_id=client_id,
            )
            self.submit_order(order)

        def on_event(self, event):
            kind = type(event).__name__
            if kind == "OrderFilled":
                self.fills += 1
                proposal = self.orders[str(event.client_order_id)]
                assert self.ledger.observe_fill(
                    Fill(
                        event_id=hashlib.sha256(str(event.trade_id).encode()).hexdigest(),
                        intent_id=proposal.intent_id,
                        quantity=str(event.last_qty),
                        price=str(event.last_px),
                        notional=format(
                            instrument.notional_value(event.last_qty, event.last_px).as_decimal(),
                            "f",
                        ),
                        fee=str(event.commission.as_decimal()),
                    )
                )
            elif kind == "OrderCanceled":
                proposal = self.orders[str(event.client_order_id)]
                order = self.cache.order(event.client_order_id)
                assert self.ledger.settle(
                    proposal.intent_id, "canceled", confirmed_filled=str(order.filled_qty)
                )

    engine = BacktestEngine(config=BacktestEngineConfig(logging=LoggingConfig(bypass_logging=True)))
    try:
        engine.add_venue(
            venue=Venue("SIM"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=None,
            starting_balances=[Money(1000, USDT), Money(0, BTC)],
            use_message_queue=False,
            use_random_ids=False,
            bar_execution=False,
            trade_execution=False,
            allow_cash_borrowing=False,
            liquidity_consumption=True,
        )
        engine.add_instrument(instrument)
        strategy = NativeFixture()
        engine.add_strategy(strategy)
        snapshots = []
        for index in range(2):
            stamp = (1735689600 + index) * 10**9
            engine.add_data(
                [
                    QuoteTick(
                        instrument_id,
                        Price(float(bid), precision=8),
                        Price(float(ask), precision=8),
                        Quantity.from_str("0.40000000"),
                        Quantity.from_str("0.40000000"),
                        stamp,
                        stamp,
                    )
                ]
            )
            engine.run(streaming=True)
            engine.clear_data()
            state = strategy.ledger.snapshot()
            balances = engine.cache.accounts()[0].balances_total()
            assert Decimal(state["cash"]) == balances[USDT].as_decimal()
            assert (
                Decimal(state["instances"][instance.instance_id]["base"])
                == balances[BTC].as_decimal()
            )
            assert state["reserved_cash"] == "0" and not state["blocks"]
            snapshots.append(state)
            # Reopen only the journal. The native process remains active across batches.
            strategy.ledger = PaperLedger(ledger.path, account)
            assert strategy.ledger.snapshot() == state
        assert strategy.starts == 1 and strategy.stops == 0 and strategy.fills == 2
        if bid == "100":
            assert snapshots[0]["cash"] == "959.6" and snapshots[1]["cash"] == "969.6"
        assert snapshots[0]["instances"][instance.instance_id]["base"] == "0.4"
        assert snapshots[1]["instances"][instance.instance_id]["base"] == "0.3"
        assert snapshots[0]["orders"][f"{1:032x}"]["status"] == "canceled"
    finally:
        engine.dispose()
