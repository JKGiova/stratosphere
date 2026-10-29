"""Separate Yahoo price-research engine; legacy Binance implementation stays unchanged."""

import platform
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal
from importlib.metadata import version

from stratosphere.phase0.signals import SmaCross
from stratosphere.yahoo.contracts import YahooPlan, YahooSnapshot, implementation_digest

ENGINE_VERSION = "1.231.0"

TICK = Decimal("0.00000001")


def execution_quote(reference: str, plan: YahooPlan, multiplier: int = 1):
    half_spread = Decimal(plan.request.config.spread_bps) / 2
    impact = (half_spread + Decimal(plan.request.config.slippage_bps)) * multiplier / 10000
    price = Decimal(reference)
    return (
        (price * (1 - impact)).quantize(TICK, rounding=ROUND_FLOOR),
        (price * (1 + impact)).quantize(TICK, rounding=ROUND_CEILING),
    )


def run_simulation(plan: YahooPlan, snapshot: YahooSnapshot, *, multiplier=1):
    """Native price-series simulation; never a broker or Liquid execution model."""
    if (
        plan.implementation_sha256 != implementation_digest()
        or snapshot.digest != plan.snapshot_sha256
    ):
        raise ValueError("Approved Yahoo input/implementation changed")
    if snapshot.request != plan.request:
        raise ValueError("Snapshot request differs from approved simulation rules")

    class SmaSignals:
        def __init__(self):
            self.cross = SmaCross(plan.request.fast, plan.request.slow)

        def push(self, bar, held):
            return self.cross.push(bar["close"])

    if len(snapshot.bars) <= plan.request.slow:
        raise ValueError("Not enough observations for SMA warmup")
    return run_price_simulation(plan, snapshot, SmaSignals, multiplier=multiplier)


def run_price_simulation(plan, snapshot, signals_factory, *, multiplier=1):
    """Trusted shared native adapter; callers own exact program/input authorization."""
    bars = snapshot.bars
    request = plan.request
    config = request.config
    if multiplier not in (1, 2):
        raise ValueError("Only base and doubled-cost runs are supported")
    if version("nautilus_trader") != ENGINE_VERSION:
        raise ValueError("The pinned NautilusTrader engine is required")
    measured_count = len(bars)
    from nautilus_trader.backtest.engine import BacktestEngine
    from nautilus_trader.backtest.models.fee import FeeModel
    from nautilus_trader.config import BacktestEngineConfig, LoggingConfig
    from nautilus_trader.model.data import Bar, BarType, QuoteTick
    from nautilus_trader.model.enums import AccountType, OmsType, OrderSide
    from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
    from nautilus_trader.model.instruments import CurrencyPair, Equity
    from nautilus_trader.model.objects import Currency, Money, Price, Quantity
    from nautilus_trader.trading.strategy import Strategy

    quote = Currency.from_str(snapshot.currency, strict=True)
    base = (
        Currency.from_str(snapshot.base_currency, strict=True) if snapshot.base_currency else None
    )
    if quote is None or (snapshot.base_currency and base is None):
        raise ValueError("Unsupported native currency")
    cash_tick = Decimal(1).scaleb(-quote.precision)
    instrument_id = InstrumentId.from_str(request.symbol + ".YAHOOSIM")
    unit = "HOUR" if request.interval == "1h" else "DAY"
    bar_type = BarType.from_str(str(instrument_id) + "-1-" + unit + "-LAST-EXTERNAL")
    fee_rate = Decimal(config.fee_rate) * multiplier
    common = dict(
        instrument_id=instrument_id,
        raw_symbol=Symbol(request.symbol),
        price_precision=8,
        price_increment=Price.from_str("0.00000001"),
        maker_fee=fee_rate,
        taker_fee=fee_rate,
        ts_event=0,
        ts_init=0,
    )
    if request.asset_class == "equity":
        native_instrument = Equity(**common, currency=quote, lot_size=Quantity.from_int(1))
    else:
        native_instrument = CurrencyPair(
            **common,
            base_currency=base,
            quote_currency=quote,
            size_precision=8,
            size_increment=Quantity.from_str("0.00000001"),
            min_quantity=Quantity.from_str("0.00000001"),
        )

    def units(value):
        return (
            Quantity.from_int(int(Decimal(value)))
            if request.asset_class == "equity"
            else (Quantity.from_str(f"{Decimal(value):.8f}"))
        )

    opens = {bar.open_ns: bar.open for bar in bars}

    class DecimalResearchFees(FeeModel):
        def get_commission(self, order, fill_qty, fill_px, instrument):
            # Quantize before constructing native money: Money(Decimal, currency) takes
            # a float path which can move an exact half-quantum below its rounding tie.
            amount = (fill_px.as_decimal() * fill_qty.as_decimal() * fee_rate).quantize(
                cash_tick, rounding=ROUND_HALF_UP
            )
            return Money.from_decimal(amount, instrument.quote_currency)

    class SmaStrategy(Strategy):
        def __init__(self):
            super().__init__()
            self.cross = signals_factory()
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
                quantity=units(request.quantity),
            )
            before = len(self.fills)
            cash_before = self.cache.accounts()[0].balances_total()[quote].as_decimal()
            self.submit_order(order)
            if len(self.fills) != before + 1:
                raise RuntimeError("Expected one full synthetic quote fill")
            cash_after = self.cache.accounts()[0].balances_total()[quote].as_decimal()
            self.fills[-1]["quote_cash_delta"] = str(cash_after - cash_before)
            fee = Decimal(self.fills[-1]["commission"])
            self.fills[-1]["execution_notional"] = str(abs(cash_after - cash_before + fee))

        def on_bar(self, bar):
            signal = self.cross.push(
                {key: str(getattr(bar, key)) for key in ("open", "high", "low", "close", "volume")},
                self.held,
            )
            if (signal == "BUY" and not self.held) or (signal == "SELL" and self.held):
                self.signals.append({"side": signal, "signal_ns": bar.ts_event})
                self.pending = signal, bar.ts_event
            balances = self.cache.accounts()[0].balances_total()
            cash = balances.get(quote, Money(0, quote)).as_decimal()
            held = sum(
                (position.quantity.as_decimal() for position in self.cache.positions_open()),
                Decimal(0),
            )
            self.curve.append(
                {
                    "timestamp_ns": bar.ts_event,
                    "cash": str(cash),
                    "base": str(held),
                    "close": str(bar.close),
                    "equity": str(cash + held * bar.close.as_decimal()),
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
            venue=Venue("YAHOOSIM"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=None,
            starting_balances=[Money.from_decimal(Decimal(config.initial_balance), quote)]
            + ([Money(0, base)] if base else []),
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
        liquidity = units(str(Decimal(request.quantity) * 10))
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
                    Quantity.from_str(f"{Decimal(bar.volume or '0'):.8f}"),
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
            "plan_sha256": plan.digest,
            "snapshot_sha256": snapshot.digest,
            "engine": "nautilus_trader",
            "cash_precision": quote.precision,
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
