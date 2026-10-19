"""Trusted SMA implementation on the pinned native NautilusTrader engine."""

import platform
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal
from importlib.metadata import version

from stratosphere.phase0.signals import SmaCross
from stratosphere.research.contracts import (
    ENGINE_VERSION,
    MarketBar,
    ResearchPlan,
    SimulationPlan,
)

TICK = Decimal("0.00000001")


def execution_quote(reference: str, plan: ResearchPlan | SimulationPlan, multiplier: int = 1):
    half_spread = Decimal(plan.config.spread_bps) / 2
    impact = (half_spread + Decimal(plan.config.slippage_bps)) * multiplier / 10000
    price = Decimal(reference)
    return (
        (price * (1 - impact)).quantize(TICK, rounding=ROUND_FLOOR),
        (price * (1 + impact)).quantize(TICK, rounding=ROUND_CEILING),
    )


def run_simulation(
    plan: ResearchPlan | SimulationPlan,
    bars: tuple[MarketBar, ...],
    *,
    multiplier: int = 1,
    trade_start_ns: int | None = None,
) -> dict:
    """No arbitrary user/model Python, broker credentials or live venue exists here."""
    if multiplier not in (1, 2):
        raise ValueError("Only approved base and doubled-cost runs are supported")
    if version("nautilus_trader") != ENGINE_VERSION:
        raise ValueError("The pinned NautilusTrader engine is required")
    if len(bars) <= plan.draft.slow_period:
        raise ValueError("Not enough observations for SMA warmup")
    for previous, current in zip(bars, bars[1:], strict=False):
        if current.open_ns - previous.open_ns != 3600 * 10**9:
            raise ValueError("Missing, duplicate or unordered bars")
    if trade_start_ns is not None and trade_start_ns not in {bar.open_ns for bar in bars}:
        raise ValueError("Trading must start at an observed opening boundary")
    measured_count = sum(trade_start_ns is None or bar.open_ns >= trade_start_ns for bar in bars)
    from nautilus_trader.backtest.engine import BacktestEngine
    from nautilus_trader.backtest.models.fee import FeeModel
    from nautilus_trader.config import BacktestEngineConfig, LoggingConfig
    from nautilus_trader.model.currencies import BTC, USDT
    from nautilus_trader.model.data import Bar, BarType, QuoteTick
    from nautilus_trader.model.enums import AccountType, OmsType, OrderSide
    from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
    from nautilus_trader.model.instruments import CurrencyPair
    from nautilus_trader.model.objects import Money, Price, Quantity
    from nautilus_trader.trading.strategy import Strategy

    instrument_id = InstrumentId.from_str("BTCUSDT.SIM")
    bar_type = BarType.from_str("BTCUSDT.SIM-1-HOUR-LAST-EXTERNAL")
    fee_rate = Decimal(plan.config.fee_rate) * multiplier
    native_instrument = CurrencyPair(
        instrument_id=instrument_id,
        raw_symbol=Symbol("BTCUSDT"),
        base_currency=BTC,
        quote_currency=USDT,
        price_precision=8,
        size_precision=8,
        price_increment=Price.from_str("0.00000001"),
        size_increment=Quantity.from_str("0.00000001"),
        min_quantity=Quantity.from_str("0.00000001"),
        maker_fee=fee_rate,
        taker_fee=fee_rate,
        ts_event=0,
        ts_init=0,
    )
    opens = {bar.open_ns: bar.open for bar in bars}

    class DecimalResearchFees(FeeModel):
        def get_commission(self, order, fill_qty, fill_px, instrument):
            # Quantize before constructing native money: Money(Decimal, currency) takes
            # a float path which can move an exact half-quantum below its rounding tie.
            amount = (fill_px.as_decimal() * fill_qty.as_decimal() * fee_rate).quantize(
                TICK, rounding=ROUND_HALF_UP
            )
            return Money.from_decimal(amount, instrument.quote_currency)

    class SmaStrategy(Strategy):
        def __init__(self):
            super().__init__()
            self.cross = SmaCross(plan.draft.fast_period, plan.draft.slow_period)
            self.pending = None
            self.held = False
            self.signals = []
            self.fills = []
            self.curve = []
            self.active_signal_ns = None
            self.active_reference = None

        def on_start(self):
            self.subscribe_quote_ticks(instrument_id)
            self.subscribe_bars(bar_type)

        def on_quote_tick(self, tick):
            if self.pending is None:
                return
            side, signal_ns = self.pending
            self.pending = None
            if tick.ts_event <= signal_ns:
                raise RuntimeError("A fill must follow its closed-candle signal")
            self.active_signal_ns = signal_ns
            self.active_reference = opens[tick.ts_event]
            order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=OrderSide.BUY if side == "BUY" else OrderSide.SELL,
                quantity=Quantity.from_str(f"{Decimal(plan.draft.quantity):.8f}"),
            )
            before = len(self.fills)
            self.submit_order(order)
            if len(self.fills) != before + 1:
                raise RuntimeError("Expected one full synthetic quote fill")

        def on_bar(self, bar):
            signal = self.cross.push(str(bar.close))
            # Prior observations warm indicators only. Each measured window starts flat;
            # a pre-window crossing cannot create an order at the first measured open.
            if trade_start_ns is not None and bar.ts_event < trade_start_ns:
                return
            if (signal == "BUY" and not self.held) or (signal == "SELL" and self.held):
                self.signals.append({"side": signal, "signal_ns": bar.ts_event})
                self.pending = signal, bar.ts_event
            balances = self.cache.accounts()[0].balances_total()
            cash = balances.get(USDT, Money(0, USDT)).as_decimal()
            base = balances.get(BTC, Money(0, BTC)).as_decimal()
            self.curve.append(
                {
                    "timestamp_ns": bar.ts_event,
                    "cash": str(cash),
                    "base": str(base),
                    "close": str(bar.close),
                    "equity": str(cash + base * bar.close.as_decimal()),
                }
            )

        def on_order_filled(self, event):
            self.held = event.order_side == OrderSide.BUY
            self.fills.append(
                {
                    "side": "BUY" if self.held else "SELL",
                    "signal_ns": self.active_signal_ns,
                    "fill_ns": event.ts_event,
                    "quantity": str(event.last_qty),
                    "price": str(event.last_px),
                    "reference_open": self.active_reference,
                    "commission": str(event.commission.as_decimal()),
                    "commission_currency": event.commission.currency.code,
                }
            )

        def on_order_denied(self, event):
            raise RuntimeError(
                "The simulated order was denied; check quantity and starting capital"
            )

        def on_order_rejected(self, event):
            raise RuntimeError("The simulated order was rejected; this run has no complete report")

    engine = BacktestEngine(config=BacktestEngineConfig(logging=LoggingConfig(bypass_logging=True)))
    try:
        engine.add_venue(
            venue=Venue("SIM"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=None,
            starting_balances=[Money(Decimal(plan.config.initial_balance), USDT), Money(0, BTC)],
            use_message_queue=False,
            use_random_ids=False,
            bar_execution=False,
            trade_execution=False,
            allow_cash_borrowing=False,
            fee_model=DecimalResearchFees(),
        )
        engine.add_instrument(native_instrument)
        quotes, candles = [], []
        # Synthetic execution quotes are distinct from indicator candles. No OHLC path is invented.
        liquidity = Quantity.from_str(f"{Decimal(plan.draft.quantity) * 10:.8f}")
        for bar in bars:
            bid, ask = execution_quote(bar.open, plan, multiplier)
            quotes.append(
                QuoteTick(
                    instrument_id,
                    Price.from_str(f"{bid:.8f}"),
                    Price.from_str(f"{ask:.8f}"),
                    liquidity,
                    liquidity,
                    bar.open_ns,
                    bar.open_ns,
                )
            )
            candles.append(
                Bar(
                    bar_type,
                    *(
                        Price.from_str(f"{Decimal(getattr(bar, key)):.8f}")
                        for key in ("open", "high", "low", "close")
                    ),
                    Quantity.from_str(f"{Decimal(bar.volume):.8f}"),
                    bar.close_ns,
                    bar.close_ns,
                )
            )
        engine.add_data(quotes)
        engine.add_data(candles)
        strategy = SmaStrategy()
        engine.add_strategy(strategy)
        engine.run()
        if engine.cache.orders_open():
            raise RuntimeError("Unfilled native orders invalidate this full-fill simulation")
        if len(strategy.curve) != measured_count:
            raise RuntimeError("Native engine did not process every observation")
        balances = engine.cache.accounts()[0].balances_total()
        return {
            "engine": "nautilus_trader",
            "engine_version": ENGINE_VERSION,
            "runtime_versions": {
                "python": platform.python_version(),
                **{
                    name: version(name)
                    for name in ("nautilus_trader", "numpy", "pandas", "pyarrow")
                },
            },
            "cost_multiplier": multiplier,
            "signals": strategy.signals,
            "fills": strategy.fills,
            "equity_curve": strategy.curve,
            "pending_signal": strategy.pending,
            "open_positions": len(engine.cache.positions_open()),
            "balances": {
                currency.code: str(amount.as_decimal())
                for currency, amount in sorted(balances.items(), key=lambda p: p[0].code)
            },
        }
    finally:
        engine.dispose()
