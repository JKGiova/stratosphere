"""Application-owned, calendar-driven CFD adaptation of arXiv:2010.01727 Figure 1."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import lru_cache

import exchange_calendars as calendars
from pydantic import Field, model_validator

from stratosphere.mt5.bridge import BridgeError, MT5Bridge
from stratosphere.mt5.gold_strategy import digest, text, validate_volume
from stratosphere.mt5.overnight_markets import MARKETS, market
from stratosphere.phase0.contracts import Contract, StrategyDocument, StrategySpec, fingerprint

SOURCE = "https://arxiv.org/pdf/2010.01727"
SYMBOLS = tuple(MARKETS)
PROFILE = {
    "schema_version": "mt5-overnight-profile/1",
    "source": SOURCE,
    "attribution": "Bruce Knuteson, Strikingly Suspicious Overnight and Intraday Returns, Figure 1",
    "conversion": "Operational adaptation of return decomposition, not a paper trading system",
    "instrument": "One exact broker index CFD adaptation, with explicit benchmark/calendar mapping",
    "calendar": "Mapped cash session, exchange-calendars 4.13.2; frozen UTC entry/exit windows",
    "entry": "Buy in [regular close minus 300 seconds, regular close minus 30 seconds)",
    "exit": "Close the exact owned long at or after the next mapped regular cash-session open",
    "startup": "No late entry, intranight catch-up, reinforcement or shorting",
    "weekends": "Hold through non-session days until the next regular open, including weekends",
    "stop": "Initial bid minus 1 percent of bid, rounded down to the broker tick",
    "notional_fraction_ceiling": "0.10",
    "review": "One binary strategy review, then one risk-sizing call per session trigger",
    "execution": "Existing native OrderCheck/OrderSend gateway, mandatory SL, one account position",
    "scope": "Demo only; at most 5 entries, 10 model calls and 7 entry-days per deployment",
    "evidence": "No validated net-of-cost CFD profitability or FTMO compliance claim",
}


class Window(Contract):
    session: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    entry_start: int = Field(strict=True, gt=0)
    entry_end: int = Field(strict=True, gt=0)
    exit_at: int = Field(strict=True, gt=0)

    @model_validator(mode="after")
    def valid(self):
        if not self.entry_start < self.entry_end < self.exit_at:
            raise ValueError("Overnight window must have a bounded entry before its exit")
        return self


@lru_cache(maxsize=16)
def calendar(year, code="XNYS"):
    # Explicit range: do not depend on the library's moving default calendar window.
    return calendars.get_calendar(code, start=f"{year - 1}-01-01", end=f"{year + 1}-12-31")


def windows(start, expires, symbol="US100.cash"):
    day = datetime.fromtimestamp(start, UTC).date()
    last = datetime.fromtimestamp(expires, UTC).date()
    cal = calendar(day.year, market(symbol)["calendar"])
    result = []
    for session in cal.sessions_in_range(day.isoformat(), last.isoformat()):
        close = int(cal.session_close(session).timestamp())
        opening = int(cal.session_open(cal.next_session(session)).timestamp())
        if close - 30 > start and close - 300 < expires:
            result.append(
                Window(
                    session=str(session.date()),
                    entry_start=close - 300,
                    entry_end=close - 30,
                    exit_at=opening,
                )
            )
    return tuple(result)


def upcoming(now, symbol="US100.cash"):
    return windows(now, now + int(timedelta(days=7).total_seconds()), symbol)


def profile(symbol):
    return PROFILE | {"mapping": market(symbol), "symbol": symbol}


def document(symbol):
    if symbol not in SYMBOLS:
        raise BridgeError("Overnight adapter requires an exact reviewed index CFD mapping")
    spec = StrategySpec.model_validate(
        {
            "schema_version": "strategy/1",
            "indicators": [],
            "entry_rules": [
                {
                    "id": "regular_close_entry",
                    "action": "open_long",
                    "condition": {"op": "session", "value": "regular_close_window"},
                }
            ],
            "exit_rules": [
                {
                    "id": "next_regular_open_exit",
                    "action": "close_long",
                    "condition": {"op": "session", "value": "next_regular_open"},
                }
            ],
            "position_sizing": {"mode": "fraction_equity", "amount": "0.10"},
            "signal_timing": {
                "evaluate": "event",
                "submit": "after_signal",
                "precedence": "exit_before_entry",
            },
            "timeframes": [60],
            "market_conditions": [],
            "instrument_constraints": {
                "asset_classes": ["index_cfd"],
                "fixed_symbols": ["MT5:" + symbol],
                "selection": "fixed",
            },
            "data_requirements": {
                "input": "quotes",
                "warmup_bars": 1,
                "availability": "event_timestamp",
            },
            "execution_requirements": {
                "order_type": "market",
                "shorting": False,
                "max_positions": 1,
                "reinforcement": False,
                "session_end": "hold",
            },
            "missing_data_policy": {"action": "suspend", "max_staleness_seconds": 5},
            "assumptions": [json.dumps(profile(symbol), sort_keys=True)],
        }
    )
    return StrategyDocument(
        name=f"Overnight close-to-open — {symbol}",
        description="Calendar-driven long-only index CFD adaptation; broker fills, not auctions",
        formal_spec=spec,
        source_refs=(SOURCE,),
    )


def signal(symbol, window):
    value = {
        "schema_version": "mt5-overnight-signal/1",
        "symbol": symbol,
        "specification_sha256": fingerprint(document(symbol).formal_spec),
        "window": window.model_dump(mode="json"),
        "side": "buy",
    }
    return value | {"signal_sha256": digest(value)}


def compiled_script(symbol):
    pin = fingerprint(document(symbol).formal_spec)
    return (
        '"""Application-generated overnight agent; exact deployment approval is separate."""\n'
        "from stratosphere.mt5.overnight import main\n\n"
        f"SYMBOL = {symbol!r}\nSPECIFICATION_SHA256 = {pin!r}\n"
        "if __name__ == '__main__':\n"
        "    main(expected_symbol=SYMBOL, expected_specification=SPECIFICATION_SHA256)\n"
    ).encode()


def entry_bounds(symbol_name, quote, symbol, account, deployment):
    MT5Bridge.validate_data("quote", quote, {})
    if not -2000 <= int(quote["age_ms"]) <= 5000:
        raise BridgeError("Overnight quote is stale or clock-inconsistent")
    if symbol.get("symbol") != symbol_name or account["currency"] != "USD":
        raise BridgeError("Overnight sizing requires exact symbol and USD account")

    def positive(data, key):
        value = Decimal(data[key])
        if not value.is_finite() or value <= 0:
            raise BridgeError("Positive finite broker metadata required")
        return value

    tick, tick_value, contract, minimum, maximum, step = [
        positive(symbol, k)
        for k in (
            "tick_size",
            "tick_value",
            "contract_size",
            "volume_min",
            "volume_max",
            "volume_step",
        )
    ]
    bid, ask = positive(quote, "bid"), positive(quote, "ask")
    equity, margin = positive(account, "equity"), positive(account, "free_margin")
    if ask - bid > Decimal(deployment.max_spread) or equity < Decimal(deployment.min_equity):
        raise BridgeError("Overnight approved spread/equity gate failed")
    stop = ((bid * Decimal("0.99")) // tick) * tick
    if not 0 < stop < bid:
        raise BridgeError("Invalid mandatory overnight stop")
    risk = (ask - stop) / tick * tick_value
    risk_budget = Decimal(deployment.max_order_risk) - Decimal(deployment.cost_buffer)
    conversion = (
        Decimal("1") if symbol["profit_currency"] == "USD" else positive(quote, "conversion_to_usd")
    )
    notional = ask * contract * conversion
    cap = min(
        maximum,
        Decimal(deployment.max_volume),
        risk_budget / risk,
        equity * Decimal("0.10") / notional,
    )
    cap = (cap // step) * step
    if risk_budget <= 0 or cap < minimum:
        raise BridgeError("No broker-minimum overnight size fits all finite bounds")
    return {
        "symbol": symbol_name,
        "side": "buy",
        "stop_loss": text(stop),
        "min_lots": text(minimum),
        "max_lots": text(cap),
        "lot_step": text(step),
        "risk_per_lot_estimate": text(risk),
        "notional_per_lot": text(notional),
        "max_stop_risk_excluding_buffer": text(risk_budget),
        "max_margin": text(min(margin, Decimal(deployment.max_order_margin))),
        "max_notional": text(equity * Decimal("0.10")),
        "max_effective_notional_to_equity": "0.10",
        "broker_swap_long_raw": symbol.get("swap_long", "unavailable"),
        "broker_swap_mode": symbol.get("swap_mode", "unverified"),
        "broker_triple_swap_day": symbol.get("swap_triple_day", "unverified"),
        "costs": "Native SL risk plus fixed buffer; gaps, financing and slippage may exceed it",
    }


__all__ = [
    "PROFILE",
    "SYMBOLS",
    "Window",
    "compiled_script",
    "document",
    "entry_bounds",
    "signal",
    "upcoming",
    "validate_volume",
    "windows",
]
