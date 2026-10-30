"""Application-owned XAUUSD D1 signal program and protective execution profile."""

import hashlib
import json
from decimal import Decimal
from fractions import Fraction

from stratosphere.mt5.bridge import BridgeError, MT5Bridge
from stratosphere.phase0.contracts import StrategyDocument, StrategySpec, fingerprint
from stratosphere.research.contracts import SmaParameters, compile_sma_spec

SYMBOL = "XAUUSD"
FRAME = "PERIOD_D1"
BAR_COUNT = 260
NOTIONAL_FRACTION = Decimal("0.10")
PROFILE = {
    "schema_version": "mt5-gold-golden-profile/1",
    "symbol": SYMBOL,
    "frame": FRAME,
    "fast": 50,
    "slow": 200,
    "initialization": "Enter a bullish closed-bar regime when flat; never invent past fills",
    "desired_position": "Long if SMA50 > SMA200; otherwise flat; no shorts",
    "stop": "Initial bid minus twice the 14-bar simple mean true range, rounded down to tick",
    "notional_fraction_ceiling": "0.10",
    "sizing": "One isolated advisory call; application and native finite limits are authoritative",
    "entry_budget": 1,
    "account_mode": "demo_only",
    "evidence": "Operator-defined connection; no faithful CFD backtest or FTMO certification",
}


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def text(value):
    return format(value, "f")


def document():
    value = compile_sma_spec(
        SmaParameters(fast_period=50, slow_period=200, quantity="1")
    ).model_dump(mode="json")
    for indicator in value["indicators"]:
        indicator["timeframe_seconds"] = 86400
    for rules, op in (("entry_rules", "gt"), ("exit_rules", "le")):
        value[rules][0]["condition"]["op"] = op
    value["position_sizing"] = {"mode": "fraction_equity", "amount": text(NOTIONAL_FRACTION)}
    value["signal_timing"]["submit"] = "after_signal"
    value["timeframes"] = [86400]
    value["data_requirements"]["warmup_bars"] = BAR_COUNT
    value["instrument_constraints"] = {
        "asset_classes": ["commodity_cfd"],
        "fixed_symbols": ["MT5:XAUUSD"],
        "selection": "fixed",
    }
    value["missing_data_policy"] = {"action": "suspend", "max_staleness_seconds": 345600}
    # The full broker overlay participates in the immutable strategy identity, not just metadata.
    value["assumptions"] = [json.dumps(PROFILE, sort_keys=True)]
    return StrategyDocument(
        name="Gold Golden Cross 50/200 D1",
        description="Closed daily XAUUSD CFD regime with startup catch-up, one entry and native SL",
        formal_spec=StrategySpec.model_validate(value),
        source_refs=(
            "Explicit operator request; application-owned golden-cross execution profile",
        ),
    )


def compiled_script():
    pin = fingerprint(document().formal_spec)
    return (
        '"""Application-generated gold strategy entry point; approval is separate."""\n'
        "from stratosphere.mt5.gold import main\n\n"
        f"SPECIFICATION_SHA256 = {pin!r}\n"
        "if __name__ == '__main__':\n"
        "    main(expected_specification=SPECIFICATION_SHA256)\n"
    ).encode()


def evaluate(rates, quote):
    MT5Bridge.validate_data("rates", rates, {"count": BAR_COUNT})
    MT5Bridge.validate_data("quote", quote, {})
    if int(rates["count"]) != BAR_COUNT:
        raise BridgeError("Gold strategy needs exactly 260 closed daily bars; no substitute window")
    if not -2000 <= int(quote["age_ms"]) <= 5000:
        raise BridgeError("Gold strategy quote is stale or clock-inconsistent")
    bars = [
        {
            k: rates[f"b.{i}.{k}"]
            for k in ("time", "open", "high", "low", "close", "tick_volume", "spread")
        }
        for i in range(1, BAR_COUNT + 1)
    ]
    if any(int(b["time"]) - int(a["time"]) > 345600 for a, b in zip(bars, bars[1:], strict=False)):
        raise BridgeError("Gold daily history contains an unsupported gap longer than four days")
    last = int(bars[-1]["time"])
    now = int(quote["server_time_ms"]) // 1000
    if not 86400 <= now - last <= 345600:
        raise BridgeError("Gold daily window is forming, stale or outside the server-clock bound")
    closes = [Fraction(bar["close"]) for bar in bars]
    fast, slow = sum(closes[-50:]) / 50, sum(closes[-200:]) / 200
    previous_fast, previous_slow = sum(closes[-51:-1]) / 50, sum(closes[-201:-1]) / 200
    # This is explicitly a simple mean TR, not Wilder/MetaTrader iATR.
    ranges = []
    for i in range(BAR_COUNT - 14, BAR_COUNT):
        high, low, previous = Fraction(bars[i]["high"]), Fraction(bars[i]["low"]), closes[i - 1]
        ranges.append(max(high - low, abs(high - previous), abs(low - previous)))
    tr = sum(ranges) / 14

    def decimal(value):
        return text(Decimal(value.numerator) / Decimal(value.denominator))

    signal = {
        "schema_version": "mt5-gold-signal/1",
        "specification_sha256": fingerprint(document().formal_spec),
        "symbol": SYMBOL,
        "frame": FRAME,
        "bar_open_server_epoch": last,
        "window_sha256": digest(bars),
        "sma50": decimal(fast),
        "sma200": decimal(slow),
        "previous_sma50": decimal(previous_fast),
        "previous_sma200": decimal(previous_slow),
        "mean_true_range_14": decimal(tr),
        "desired_position": "long" if fast > slow else "flat",
        "new_golden_cross": previous_fast <= previous_slow and fast > slow,
        "new_death_cross": previous_fast > previous_slow and fast <= slow,
        "time_domain": "Raw MT5 server/chart epochs; historical UTC mapping unverified",
    }
    return signal | {"signal_sha256": digest(signal)}


def entry_bounds(signal, quote, symbol, account, deployment):
    """Anonymous sizing facts only. Exact native profit/margin checks still gate every order."""
    if symbol.get("symbol") != SYMBOL or (
        account["currency"] != "USD"
        or symbol["profit_currency"] != "USD"
        or symbol["margin_currency"] != "USD"
    ):
        raise BridgeError("Gold sizing currently requires USD account/profit/margin currencies")

    def positive(data, key):
        result = Decimal(data[key])
        if not result.is_finite() or result <= 0:
            raise BridgeError("Gold sizing requires positive finite broker metadata")
        return result

    tick, contract, step, minimum, broker_max, tick_value = [
        positive(symbol, key)
        for key in (
            "tick_size",
            "contract_size",
            "volume_step",
            "volume_min",
            "volume_max",
            "tick_value",
        )
    ]
    bid, ask = positive(quote, "bid"), positive(quote, "ask")
    if ask - bid > Decimal(deployment.max_spread):
        raise BridgeError("Gold quote exceeds the approved spread")
    distance = Fraction(signal["mean_true_range_14"]) * 2
    stop_ticks = (Fraction(bid) - distance) // Fraction(tick)
    stop = Decimal(stop_ticks) * tick
    if stop <= 0 or stop >= bid:
        raise BridgeError("Gold protective stop is not valid")
    risk_per_lot = (ask - stop) / tick * tick_value
    notional_per_lot = ask * contract
    equity, free_margin = positive(account, "equity"), positive(account, "free_margin")
    if equity < Decimal(deployment.min_equity):
        raise BridgeError("Gold equity floor breached")
    risk_budget = Decimal(deployment.max_order_risk) - Decimal(deployment.cost_buffer)
    if risk_budget <= 0:
        raise BridgeError("Gold risk buffer exhausts the approved risk bound")
    notional_budget = equity * NOTIONAL_FRACTION
    ceiling = min(
        Decimal(deployment.max_volume),
        broker_max,
        risk_budget / risk_per_lot,
        notional_budget / notional_per_lot,
    )
    maximum = (ceiling // step) * step
    if maximum < minimum:
        raise BridgeError("No broker-minimum gold position fits the approved bounds")
    return {
        "symbol": SYMBOL,
        "side": "buy",
        "stop_loss": text(stop),
        "min_lots": text(minimum),
        "max_lots": text(maximum),
        "lot_step": text(step),
        "risk_per_lot_estimate": text(risk_per_lot),
        "max_stop_risk_excluding_buffer": text(risk_budget),
        "max_margin": text(min(free_margin, Decimal(deployment.max_order_margin))),
        "max_notional": text(notional_budget),
        "notional_per_lot": text(notional_per_lot),
        "max_effective_notional_to_equity": text(NOTIONAL_FRACTION),
        "meaning": "Anonymous sizing facts; the agent cannot change broker account leverage",
    }


def validate_volume(proposal, bounds):
    volume = Decimal(proposal.volume)
    step = Decimal(bounds["lot_step"])
    if (
        not volume.is_finite()
        or not Decimal(bounds["min_lots"]) <= volume <= Decimal(bounds["max_lots"])
        or volume % step != 0
    ):
        raise BridgeError("Gold sizing agent proposed an invalid or excessive lot quantity")
    return volume
