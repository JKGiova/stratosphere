"""Versioned Yahoo snapshot and price-research approval contracts."""

import hashlib
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from stratosphere.phase0.contracts import Contract, Digest, StrategySpec, fingerprint
from stratosphere.research.contracts import ResearchConfig, SmaParameters, compile_sma_spec

VERSION = "1.7.0"
LIMITATIONS = (
    "SMA warmup uses the requested observations; initial alignment alone cannot enter a position.",
    "Equity/FX boundary coverage must fall within seven calendar days of each requested edge; "
    "this does not verify interior sessions.",
    "Personal historical price research only; Yahoo/yfinance access is unofficial and "
    "unverified rights are not granted by this application.",
    "One long position, fixed units, closed-observation SMA crosses, next available "
    "observation open; terminal exposure stays open.",
    "No leverage, shorts, reinforcement, optimization, holdout selection or live trading.",
    "Yahoo observation labels and interval ends are assumed timing, not archived "
    "publication/execution timestamps.",
    "Session calendars are not independently verified. All observed gaps are retained "
    "and disclosed; no missing price is invented.",
    "Raw returned frame values and corporate actions are retained. Execution uses "
    "provider split-adjusted OHLC, rounded explicitly to eight decimals; dividends are "
    "not reinvested or credited.",
    "Windows containing split or dividend events are blocked until corporate-action "
    "accounting is supported.",
    "Synthetic full fills, configured spread/slippage/fees, and quote-currency "
    "rounding; no venue liquidity, broker borrowing or FX rollover model.",
    "Yahoo spot/equity/FX identity is separate from Binance and Liquid perpetual "
    "contracts; no funding, margin or liquidation claim.",
    "Exactly two native starts (base and doubled costs). Failed or interrupted starts "
    "cannot silently replay; use a newly reviewed plan.",
)


class YahooRequest(Contract):
    symbol: str = Field(pattern=r"^[A-Z0-9][A-Z0-9.\-=]{0,19}$")
    asset_class: Literal["equity", "crypto_spot", "forex"]
    interval: Literal["1h", "1d"] = "1h"
    start: date
    end: date  # Exclusive, as in yfinance; never silently changed.
    fast: int = Field(default=20, strict=True, gt=0)
    slow: int = Field(default=50, strict=True, gt=0)
    quantity: str = "1"
    config: ResearchConfig = ResearchConfig()

    @model_validator(mode="after")
    def valid(self):
        SmaParameters(fast_period=self.fast, slow_period=self.slow, quantity=self.quantity)
        if self.end <= self.start or self.end > datetime.now(UTC).date():
            raise ValueError("Use a positive, fully historical start/end window; end is exclusive.")
        if self.asset_class == "equity" and Decimal(self.quantity) % 1:
            raise ValueError("The equity simulation supports whole shares only.")
        if self.asset_class == "forex" and Decimal(self.quantity) % Decimal("0.01"):
            raise ValueError("Fully funded FX simulation requires hundredths of a base unit.")
        return self

    @property
    def seconds(self):
        return 3600 if self.interval == "1h" else 86400

    def specification(self):
        spec = compile_sma_spec(
            SmaParameters(fast_period=self.fast, slow_period=self.slow, quantity=self.quantity)
        ).model_dump(mode="json")
        for indicator in spec["indicators"]:
            indicator["timeframe_seconds"] = self.seconds
        spec["timeframes"] = [self.seconds]
        spec["instrument_constraints"] = {
            "asset_classes": [self.asset_class],
            "fixed_symbols": ["YAHOO:" + self.symbol],
            "selection": "fixed",
        }
        spec["missing_data_policy"] = {"action": "skip", "max_staleness_seconds": self.seconds}
        spec["assumptions"] = list(LIMITATIONS)
        return StrategySpec.model_validate(spec)


class YahooBar(Contract):
    open_ns: int = Field(strict=True, gt=0)
    close_ns: int = Field(strict=True, gt=0)
    open: str
    high: str
    low: str
    close: str
    volume: str | None

    @model_validator(mode="after")
    def valid(self):
        numbers = {k: Decimal(getattr(self, k)) for k in ("open", "high", "low", "close")}
        if not all(v.is_finite() and v > 0 for v in numbers.values()):
            raise ValueError("Invalid Yahoo price")
        if (
            not numbers["low"]
            <= min(numbers["open"], numbers["close"])
            <= max(numbers["open"], numbers["close"])
            <= numbers["high"]
        ):
            raise ValueError("Yahoo OHLC range is inconsistent")
        if self.volume is not None and (
            not Decimal(self.volume).is_finite() or Decimal(self.volume) < 0
        ):
            raise ValueError("Invalid Yahoo volume")
        if self.close_ns <= self.open_ns:
            raise ValueError("Invalid observation boundaries")
        return self


class YahooSnapshot(Contract):
    schema_version: Literal["yahoo-snapshot/1"] = "yahoo-snapshot/1"
    request: YahooRequest
    returned_frame_sha256: Digest
    yfinance_version: Literal["1.7.0"] = VERSION
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    base_currency: str | None
    exchange: str
    timezone: str
    bars: tuple[YahooBar, ...] = Field(min_length=1)
    gaps: tuple[tuple[int, int], ...]
    observed_at_ns: int = Field(strict=True, gt=0)

    @model_validator(mode="after")
    def ordered(self):
        if len(self.bars) <= self.request.slow + 1:
            raise ValueError("Insufficient observations for SMA warmup and evaluation")
        for previous, current in zip(self.bars, self.bars[1:], strict=False):
            if current.open_ns <= previous.open_ns or current.open_ns <= previous.close_ns:
                raise ValueError("Duplicate, overlapping or unordered Yahoo observations")
        gaps = tuple(
            (p.open_ns, c.open_ns)
            for p, c in zip(self.bars, self.bars[1:], strict=False)
            if c.open_ns - p.open_ns > self.request.seconds * 10**9
        )
        if gaps != self.gaps:
            raise ValueError("Gap findings differ from retained observations")
        if self.request.asset_class == "crypto_spot" and gaps:
            raise ValueError("Missing continuous-market observations block crypto backtests")
        return self

    @property
    def digest(self):
        return fingerprint(self)


def implementation_digest():
    root = Path(__file__).parent
    sources = [*root.glob("*.py")]
    sources += [
        root.parent / name
        for name in (
            "phase0/signals.py",
            "phase0/contracts.py",
            "research/contracts.py",
            "research/metrics.py",
        )
    ]
    return hashlib.sha256(
        b"".join(p.name.encode() + b"\0" + p.read_bytes() for p in sorted(sources))
    ).hexdigest()


class YahooPlan(Contract):
    schema_version: Literal["yahoo-backtest-plan/1"] = "yahoo-backtest-plan/1"
    request: YahooRequest
    snapshot_sha256: Digest
    strategy_sha256: Digest
    strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    implementation_sha256: Digest
    engine_version: Literal["1.231.0"] = "1.231.0"
    native_starts: Literal[2] = 2
    limitations: tuple[str, ...] = LIMITATIONS

    @model_validator(mode="after")
    def valid(self):
        if self.strategy_sha256 != fingerprint(self.request.specification()):
            raise ValueError("Yahoo strategy identity differs from the supported implementation")
        if self.limitations != LIMITATIONS:
            raise ValueError("Unsupported Yahoo simulation assumptions")
        return self

    @property
    def digest(self):
        return fingerprint(self)
