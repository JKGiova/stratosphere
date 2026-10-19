"""Immutable research plans and a deliberately narrow implementation contract."""

import hashlib
import re
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator

from stratosphere.phase0.contracts import (
    Contract,
    Digest,
    StrategySpec,
    canonical_bytes,
    decimal_text,
    fingerprint,
)

ARCHIVE_URL = (
    "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2025-01.zip"
)
ARCHIVE_SHA256 = "e7f2aaa5396a8062a57ab29ce9fab2d76b2c84ec58815d4ab7b3fe16f78c7118"
PROFILE = "binance-btcusdt-1h-2025-01/1"
ENGINE_VERSION = "1.231.0"
EVIDENCE_FIELDS = {
    "symbol",
    "timeframe_seconds",
    "fast_period",
    "slow_period",
    "quantity",
    "entry",
    "exit",
}
BEHAVIOR_ASSUMPTIONS = (
    "Long-only fixed BTC quantity; no reinforcement, leverage, stops or other filters.",
    "SMA periods count closed hourly observations; exact rational arithmetic.",
    "Cross above: previous fast <= slow and current fast > slow; below reverses inequalities.",
    "Warmup needs the slow SMA plus a previous comparison; no entry from initial SMA alignment.",
    "Entry requires flat exposure; exit requires long exposure; end exposure is held.",
)
ASSUMPTIONS = BEHAVIOR_ASSUMPTIONS + (
    "BTC/USDT spot only; no shorting, leverage, reinforcement, stops or intrabar exits.",
    "Signals use closed hourly candles; orders fill at the next candle's open.",
    "Synthetic bid/ask quotes apply half the spread plus slippage on each side.",
    "Full fills and sufficient synthetic liquidity are assumed; no market-impact model.",
    "SIM uses eight-decimal prices/quantities, not historical Binance order filters.",
    "One open position; fixed BTC quantity; initial exposure is flat; end exposure is marked.",
    "Candle close timestamps are assumed availability, not archived publication evidence.",
    "Development/validation use chronological 70/30 segments with carried positions/indicators.",
    "The validation segment is not an independent final holdout or a promotion decision.",
    "Two simulations: base costs and doubled fee/spread/slippage; no parameter search.",
    "Costs are user-reviewed simulation assumptions, not verified account fee tiers.",
    "The future stratosphere opening-order commission is not collected or modeled here.",
)


class ResearchConfig(Contract):
    initial_balance: str = "10000"
    fee_rate: str = "0.001"
    spread_bps: str = "2"
    slippage_bps: str = "1"

    @model_validator(mode="after")
    def valid_costs(self) -> Self:
        for name in type(self).model_fields:
            object.__setattr__(self, name, decimal_text(getattr(self, name)))
        if Decimal(self.initial_balance) <= 0 or Decimal(self.initial_balance) % Decimal("0.01"):
            raise ValueError("Starting balance must be positive USDT with at most two decimals")
        if not 0 <= Decimal(self.fee_rate) < Decimal("0.5"):
            raise ValueError("Fee rate must allow a doubled-cost simulation below 100%")
        if min(Decimal(self.spread_bps), Decimal(self.slippage_bps)) < 0:
            raise ValueError("Execution costs cannot be negative")
        if Decimal(self.spread_bps) / 2 + Decimal(self.slippage_bps) >= 5000:
            raise ValueError("Doubled execution costs must preserve positive bid prices")
        return self


class RuleEvidence(Contract):
    field: Literal[
        "symbol", "timeframe_seconds", "fast_period", "slow_period", "quantity", "entry", "exit"
    ]
    quote: str = Field(min_length=1)


class Extraction(Contract):
    status: Literal["ready", "needs_clarification", "unsupported"]
    name: str
    symbol: str | None
    timeframe_seconds: int | None = Field(strict=True)
    fast_period: int | None = Field(strict=True)
    slow_period: int | None = Field(strict=True)
    quantity: str | None
    entry: Literal["fast_crosses_above_slow"] | None
    exit: Literal["fast_crosses_below_slow"] | None
    evidence: tuple[RuleEvidence, ...]
    questions: tuple[str, ...]

    def validate_source(self, source: str) -> None:
        for item in self.evidence:
            if item.quote not in source:
                raise ValueError("A model evidence quote is absent from the supplied text")
        if self.status != "ready":
            if not self.questions or any(not question.strip() for question in self.questions):
                raise ValueError("Unresolved extraction requires explicit questions/limitations")
            return
        self.validate_behavior()
        for field in ("fast_period", "slow_period", "quantity"):
            quotes = [item.quote for item in self.evidence if item.field == field]
            numbers = [
                Decimal(number)
                for quote in quotes
                for number in re.findall(r"(?<![\d.])\d+(?:\.\d+)?(?!\d|\.\d)", quote)
            ]
            if Decimal(str(getattr(self, field))) not in numbers:
                raise ValueError("A numeric parameter is absent from its quoted source evidence")

    def validate_behavior(self) -> None:
        if self.status != "ready":
            raise ValueError("Only a complete specification can be compiled")
        if self.questions:
            raise ValueError("An unresolved question blocks plan approval")
        if self.symbol != "BTCUSDT" or self.timeframe_seconds != 3600:
            raise ValueError("Only BTCUSDT spot hourly rules are supported by this implementation")
        if self.fast_period is None or self.slow_period is None:
            raise ValueError("Both SMA periods must be explicit")
        if not 0 < self.fast_period < self.slow_period < 744:
            raise ValueError("Require 0 < fast < slow < the 744-bar snapshot")
        if self.entry is None or self.exit is None or self.quantity is None:
            raise ValueError("Entry, exit and sizing must be explicit")
        quantity = decimal_text(self.quantity)
        if Decimal(quantity) <= 0 or Decimal(quantity) % Decimal("0.00000001"):
            raise ValueError("Quantity must be positive with at most eight decimal places")
        if {item.field for item in self.evidence} != EVIDENCE_FIELDS:
            raise ValueError("Every behavioral field needs a source quote")
        if not self.name.strip():
            raise ValueError("A strategy name is required")


def build_spec(draft: Extraction) -> StrategySpec:
    """Compile validated parameters into a complete canonical behavior contract."""

    draft.validate_behavior()
    return compile_sma_spec(
        SmaParameters(
            fast_period=draft.fast_period, slow_period=draft.slow_period, quantity=draft.quantity
        )
    )


def compile_sma_spec(parameters: SmaParameters) -> StrategySpec:
    """Compile trusted parameters without claiming a source extraction or its provenance."""
    parameters = SmaParameters.model_validate(parameters.model_dump())

    def reference(name):
        return {"op": "indicator", "value": name}

    return StrategySpec.model_validate(
        {
            "schema_version": "strategy/1",
            "indicators": [
                {
                    "id": name,
                    "algorithm": "sma",
                    "algorithm_version": 1,
                    "input": "close",
                    "period": period,
                    "timeframe_seconds": 3600,
                }
                for name, period in [
                    ("fast", parameters.fast_period),
                    ("slow", parameters.slow_period),
                ]
            ],
            "entry_rules": [
                {
                    "id": "enter_long",
                    "condition": {
                        "op": "crosses_above",
                        "args": [reference("fast"), reference("slow")],
                    },
                    "action": "open_long",
                }
            ],
            "exit_rules": [
                {
                    "id": "exit_long",
                    "condition": {
                        "op": "crosses_below",
                        "args": [reference("fast"), reference("slow")],
                    },
                    "action": "close_long",
                }
            ],
            "position_sizing": {"mode": "fixed_base", "amount": parameters.quantity},
            "signal_timing": {
                "evaluate": "bar_close",
                "submit": "next_bar_open",
                "precedence": "exit_before_entry",
            },
            "timeframes": [3600],
            "market_conditions": [],
            "instrument_constraints": {
                "asset_classes": ["crypto_spot"],
                "fixed_symbols": ["BTCUSDT"],
                "selection": "fixed",
            },
            "data_requirements": {
                "input": "ohlcv",
                "warmup_bars": parameters.slow_period + 1,
                "availability": "closed_bars_only",
            },
            "execution_requirements": {
                "order_type": "market",
                "shorting": False,
                "max_positions": 1,
                "reinforcement": False,
                "session_end": "hold",
            },
            "missing_data_policy": {"action": "abort", "max_staleness_seconds": 3600},
            "assumptions": BEHAVIOR_ASSUMPTIONS,
        }
    )


def sma_parameters(specification: StrategySpec) -> SmaParameters:
    try:
        indicators = {item.id: item for item in specification.indicators}
        parameters = SmaParameters(
            fast_period=indicators["fast"].period,
            slow_period=indicators["slow"].period,
            quantity=specification.position_sizing.amount,
        )
    except KeyError, ValueError:
        raise ValueError("The definition is outside the supported SMA implementation") from None
    if canonical_bytes(specification) != canonical_bytes(compile_sma_spec(parameters)):
        raise ValueError("The complete definition differs from the supported SMA implementation")
    return parameters


def implementation_sources() -> dict[str, str]:
    root = Path(__file__).parent
    sources = {f"research/{path.name}": path.read_text() for path in sorted(root.glob("*.py"))}
    for name in ("contracts.py", "signals.py"):
        sources[f"phase0/{name}"] = (root.parent / "phase0" / name).read_text()
    return sources


def implementation_digest() -> str:
    digest = hashlib.sha256()
    for name, source in sorted(implementation_sources().items()):
        digest.update(name.encode() + b"\0" + source.encode())
    return digest.hexdigest()


class ResearchPlan(Contract):
    schema_version: Literal["research-plan/1"] = "research-plan/1"
    source_sha256: Digest
    draft: Extraction
    specification: StrategySpec
    config: ResearchConfig
    dataset_profile: Literal["binance-btcusdt-1h-2025-01/1"] = PROFILE
    archive_sha256: Digest = Field(default_factory=lambda: ARCHIVE_SHA256)
    engine_version: Literal["1.231.0"] = ENGINE_VERSION
    implementation_sha256: Digest
    adapter_version: Literal["research-next-open/1"] = "research-next-open/1"
    normalization_version: Literal["binance-spot-microseconds-ohlcv/1"] = (
        "binance-spot-microseconds-ohlcv/1"
    )
    simulation_count: Literal[2] = 2
    assumptions: tuple[str, ...] = ASSUMPTIONS

    @model_validator(mode="after")
    def conforming_spec(self) -> Self:
        if self.assumptions != ASSUMPTIONS:
            raise ValueError("Unsupported research protocol assumptions")
        if self.archive_sha256 != ARCHIVE_SHA256:
            raise ValueError("Dataset snapshot is not supported by this implementation")
        if self.draft.status != "ready":
            raise ValueError("Only a complete extraction can become a research plan")
        if canonical_bytes(self.specification) != canonical_bytes(build_spec(self.draft)):
            raise ValueError("Specification does not match the supported compiled implementation")
        return self

    @property
    def digest(self) -> str:
        return fingerprint(self)


class MarketBar(Contract):
    open_ns: int = Field(strict=True, gt=0)
    close_ns: int = Field(strict=True, gt=0)
    open: str
    high: str
    low: str
    close: str
    volume: str

    @model_validator(mode="after")
    def valid_bar(self) -> Self:
        for name in ("open", "high", "low", "close", "volume"):
            value = decimal_text(getattr(self, name))
            object.__setattr__(self, name, value)
            if Decimal(value) % Decimal("0.00000001"):
                raise ValueError("Data precision exceeds the SIM instrument")
        if min(Decimal(self.open), Decimal(self.low), Decimal(self.close)) <= 0:
            raise ValueError("Prices must be positive")
        if Decimal(self.volume) < 0 or not (
            Decimal(self.low)
            <= min(Decimal(self.open), Decimal(self.close))
            <= max(Decimal(self.open), Decimal(self.close))
            <= Decimal(self.high)
        ):
            raise ValueError("Invalid OHLCV range")
        if self.close_ns - self.open_ns != 3600 * 10**9 - 1000:
            raise ValueError("Expected inclusive microsecond hourly close timestamp")
        return self


class SmaParameters(Contract):
    """Trusted parameter-only execution input; not a source extraction or approval."""

    fast_period: int = Field(strict=True, gt=0)
    slow_period: int = Field(strict=True, gt=0)
    quantity: str

    @model_validator(mode="after")
    def supported(self) -> Self:
        if not self.fast_period < self.slow_period < 744:
            raise ValueError("Require 0 < fast < slow < 744")
        quantity = decimal_text(self.quantity)
        if Decimal(quantity) <= 0 or Decimal(quantity) % Decimal("0.00000001"):
            raise ValueError("Quantity must be positive BTC with at most eight decimals")
        object.__setattr__(self, "quantity", quantity)
        return self


class SimulationPlan(Contract):
    """Packaged implementation parameters; the enclosing study supplies authorization."""

    draft: SmaParameters
    config: ResearchConfig
