"""Real NautilusTrader simulation proof; no model code, broker or live operation."""

import argparse
import hashlib
import json
import platform
import uuid
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path

from stratosphere.phase0.contracts import (
    DatasetManifest,
    ExperimentProtocol,
    InstrumentManifest,
    StrategyDocument,
    fingerprint,
    load_contract,
)
from stratosphere.phase0.data_proof import EXAMPLES, Observation, normalize_ecb, observation_digest
from stratosphere.phase0.signals import SmaCross

SUPPORTED_SPEC = "b155e4f23654825e82ef2d9634262fead388f3e0fd6e1379bc93e530bfff4617"


def validate_bindings(document, dataset, instrument, protocol):
    if fingerprint(document.formal_spec) != SUPPORTED_SPEC:
        raise ValueError("Adapter supports only the complete authored Phase 0 SMA contract")
    if (
        protocol.strategy_sha256,
        protocol.dataset_manifest_sha256,
        protocol.instrument_manifest_sha256,
    ) != (
        fingerprint(document.formal_spec),
        fingerprint(dataset),
        fingerprint(instrument),
    ):
        raise ValueError("Protocol references do not match the supplied contracts")
    if instrument.symbol != dataset.symbol or instrument.asset_class != dataset.asset_class:
        raise ValueError("Dataset/instrument mismatch")
    if (
        fingerprint(instrument)
        != "46243e16993d2d47b6667ef2a71fb826e5487d5c5163c56929e024172bf1218d"
    ):
        raise ValueError("Adapter supports only the declared SIM instrument")
    if protocol.spread_bps != "0" or protocol.slippage_bps != "0":
        raise ValueError("Nonzero spread/slippage requires a different verified adapter")
    if protocol.initial_base_balance != "0":
        raise ValueError("This proof requires initial flat exposure")


def run_engine(
    observations: tuple[Observation, ...],
    document: StrategyDocument,
    dataset: DatasetManifest,
    instrument: InstrumentManifest,
    protocol: ExperimentProtocol,
) -> dict:
    validate_bindings(document, dataset, instrument, protocol)
    if tuple(row.date for row in observations) != dataset.expected_dates:
        raise ValueError("Engine input dates do not match the snapshot")
    if observation_digest(observations) != dataset.content_sha256:
        raise ValueError("Engine input values do not match the snapshot")
    if version("nautilus_trader") != protocol.engine_version:
        raise ValueError("Pinned engine version required")
    # Imports remain here so core contracts can be checked on hosts without the engine wheel.
    from nautilus_trader.backtest.engine import BacktestEngine
    from nautilus_trader.config import BacktestEngineConfig, LoggingConfig
    from nautilus_trader.model.currencies import EUR, USD
    from nautilus_trader.model.data import TradeTick
    from nautilus_trader.model.enums import AccountType, AggressorSide, OmsType, OrderSide
    from nautilus_trader.model.identifiers import InstrumentId, Symbol, TradeId, Venue
    from nautilus_trader.model.instruments import CurrencyPair
    from nautilus_trader.model.objects import Money, Price, Quantity
    from nautilus_trader.trading.strategy import Strategy

    if not observations:
        raise ValueError("No observations")
    for previous, current in zip(observations, observations[1:], strict=False):
        lag = current.available_ns - previous.available_ns
        if not 0 < lag <= document.formal_spec.missing_data_policy.max_staleness_seconds * 10**9:
            raise ValueError("Invalid ordering or stale observations")
    if any(Decimal(row.rate) % Decimal(instrument.price_increment) for row in observations):
        raise ValueError("Reference price violates the declared instrument increment")

    instrument_id = InstrumentId.from_str("EURUSD.SIM")
    native_instrument = CurrencyPair(
        instrument_id=instrument_id,
        raw_symbol=Symbol(instrument.symbol),
        base_currency=EUR,
        quote_currency=USD,
        price_precision=4,
        size_precision=0,
        price_increment=Price.from_str("0.0001"),
        size_increment=Quantity.from_int(1),
        min_quantity=Quantity.from_int(1),
        min_notional=Money(1, USD),
        maker_fee=Decimal(protocol.taker_fee_rate),
        taker_fee=Decimal(protocol.taker_fee_rate),
        ts_event=0,
        ts_init=0,
    )

    class ReferenceStrategy(Strategy):
        def __init__(self):
            super().__init__()
            self.cross = SmaCross()
            self.pending = None
            self.held = False
            self.signals = []
            self.fills = []
            self.active_signal_ns = None

        def on_start(self):
            self.subscribe_trade_ticks(instrument_id)

        def on_trade_tick(self, tick):
            # The engine updates its simulated exchange before publishing the data callback.
            if self.pending is not None:
                side, signal_ns = self.pending
                self.pending = None
                self.active_signal_ns = signal_ns
                order = self.order_factory.market(
                    instrument_id=instrument_id,
                    order_side=OrderSide.BUY if side == "BUY" else OrderSide.SELL,
                    quantity=Quantity.from_int(100),
                )
                before = len(self.fills)
                self.submit_order(order)
                if len(self.fills) != before + 1:
                    raise RuntimeError("Expected one synchronous full fill in this proof")
            signal = self.cross.push(str(tick.price))
            if (signal == "BUY" and not self.held) or (signal == "SELL" and self.held):
                self.signals.append({"side": signal, "signal_ns": tick.ts_event})
                self.pending = (signal, tick.ts_event)

        def on_order_filled(self, event):
            self.held = event.order_side == OrderSide.BUY
            self.fills.append(
                {
                    "side": "BUY" if self.held else "SELL",
                    "signal_ns": self.active_signal_ns,
                    "fill_ns": event.ts_event,
                    "quantity": str(event.last_qty),
                    "price": str(event.last_px),
                    "commission": str(event.commission.as_decimal()),
                    "commission_currency": event.commission.currency.code,
                }
            )

        def on_order_denied(self, event):
            raise RuntimeError(f"Order denied: {event.reason}")

        def on_order_rejected(self, event):
            raise RuntimeError(f"Order rejected: {event.reason}")

    engine = BacktestEngine(config=BacktestEngineConfig(logging=LoggingConfig(bypass_logging=True)))
    try:
        engine.add_venue(
            venue=Venue("SIM"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=None,
            starting_balances=[Money(Decimal(protocol.initial_quote_balance), USD), Money(0, EUR)],
            use_message_queue=False,
            use_random_ids=False,
            bar_execution=False,
            trade_execution=True,
            allow_cash_borrowing=False,
        )
        engine.add_instrument(native_instrument)
        ticks = [
            TradeTick(
                instrument_id=instrument_id,
                price=Price.from_str(f"{Decimal(row.rate):.4f}"),
                size=Quantity.from_int(1_000_000),
                aggressor_side=AggressorSide.BUYER,
                trade_id=TradeId(str(index)),
                ts_event=row.available_ns,
                ts_init=row.available_ns,
            )
            for index, row in enumerate(observations, 1)
        ]
        engine.add_data(ticks)
        strategy = ReferenceStrategy()
        engine.add_strategy(strategy)
        engine.run()
        accounts = engine.cache.accounts()
        if len(accounts) != 1:
            raise RuntimeError("Unexpected simulated account count")
        balances = accounts[0].balances_total()
        payload = {
            "schema_version": "engine-proof/1",
            "engine_version": version("nautilus_trader"),
            "strategy_sha256": fingerprint(document.formal_spec),
            "dataset_manifest_sha256": fingerprint(dataset),
            "input_event_sha256": hashlib.sha256(
                json.dumps(
                    [row.__dict__ for row in observations],
                    sort_keys=True,
                ).encode()
            ).hexdigest(),
            "instrument_manifest_sha256": fingerprint(instrument),
            "protocol_sha256": fingerprint(protocol),
            "implementation_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "signals_implementation_sha256": hashlib.sha256(
                Path(__file__).with_name("signals.py").read_bytes()
            ).hexdigest(),
            "lock_sha256": hashlib.sha256(
                (EXAMPLES.parents[1] / "uv.lock").read_bytes()
            ).hexdigest(),
            "observations": len(observations),
            "signals": strategy.signals,
            "fills": strategy.fills,
            "pending_signal": strategy.pending,
            "open_positions": len(engine.cache.positions_open()),
            "balances": {
                currency.code: str(balance.as_decimal())
                for currency, balance in sorted(balances.items(), key=lambda p: p[0].code)
            },
            "attribution": dataset.attribution,
            "limitations": [
                "Reference rates are not executable quotes.",
                "Synthetic large trade sizes and zero spread/slippage assume full fills.",
                "Availability is assumed, not verified point-in-time evidence.",
                "No profitability, holdout, paper, broker or live verification.",
            ],
        }
        if engine.cache.orders_open():
            raise RuntimeError("Unexpected unfilled native order")
        return payload
    finally:
        engine.dispose()


def example_inputs():
    return (
        load_contract(EXAMPLES / "strategy.json", StrategyDocument),
        load_contract(EXAMPLES / "dataset.json", DatasetManifest),
        load_contract(EXAMPLES / "instrument.json", InstrumentManifest),
        load_contract(EXAMPLES / "experiment.json", ExperimentProtocol),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("reports/engine"))
    args = parser.parse_args()
    document, dataset, instrument, protocol = example_inputs()
    observations = normalize_ecb((EXAMPLES / "ecb-reference.csv").read_bytes(), dataset)
    report = {
        "run_id": str(uuid.uuid7()),
        "host": platform.platform(),
        "deterministic": run_engine(observations, document, dataset, instrument, protocol),
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / f"{report['run_id']}.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(path)


if __name__ == "__main__":
    main()
