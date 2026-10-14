"""Versioned Phase 0 wire contracts, not a persisted registry or strategy generator."""

import hashlib
import json
import re
import secrets
from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

PositiveInt = Annotated[int, Field(strict=True, gt=0)]
Digest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
Name = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$")]


def decimal_text(value: str) -> str:
    """Normalize decimal spelling without a Decimal context or float rounding."""
    if not isinstance(value, str) or not re.fullmatch(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?", value):
        raise ValueError("Use an exact decimal string without exponent notation")
    integer, dot, fraction = value.partition(".")
    fraction = fraction.rstrip("0")
    normalized = integer + (dot + fraction if fraction else "")
    return "0" if Decimal(normalized) == 0 else normalized


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Expression(Contract):
    op: Literal[
        "decimal",
        "boolean",
        "field",
        "indicator",
        "position",
        "add",
        "sub",
        "mul",
        "div",
        "gt",
        "ge",
        "lt",
        "le",
        "eq",
        "and",
        "or",
        "not",
        "crosses_above",
        "crosses_below",
    ]
    value: str | bool | None = None
    args: tuple[Expression, ...] = ()

    @model_validator(mode="after")
    def valid_node(self) -> Self:
        if self.op == "decimal":
            object.__setattr__(self, "value", decimal_text(self.value))
        elif self.op == "boolean":
            if type(self.value) is not bool:
                raise ValueError("Boolean literal required")
        elif self.op in {"field", "indicator", "position"}:
            if not isinstance(self.value, str):
                raise ValueError("Named reference required")
            if self.op == "field" and self.value not in {"open", "high", "low", "close", "volume"}:
                raise ValueError("Unknown bar field")
            if self.op == "position" and self.value not in {"flat", "long", "short"}:
                raise ValueError("Unknown position state")
        elif self.value is not None:
            raise ValueError("Operators cannot contain literal values")
        arity = 0 if self.op in {"decimal", "boolean", "field", "indicator", "position"} else 2
        if self.op == "not":
            arity = 1
        if len(self.args) != arity:
            raise ValueError(f"{self.op} requires {arity} arguments")
        return self


class Indicator(Contract):
    id: Name
    algorithm: Literal["sma", "ema", "rsi"]
    algorithm_version: Literal[1]
    input: Literal["open", "high", "low", "close", "volume"]
    period: PositiveInt
    timeframe_seconds: PositiveInt


class Rule(Contract):
    id: Name
    condition: Expression
    action: Literal["open_long", "open_short", "close_long", "close_short"]


class Sizing(Contract):
    mode: Literal["fixed_base", "fraction_equity"]
    amount: str

    @model_validator(mode="after")
    def positive_amount(self) -> Self:
        object.__setattr__(self, "amount", decimal_text(self.amount))
        if Decimal(self.amount) <= 0:
            raise ValueError("Sizing must be positive")
        if self.mode == "fraction_equity" and Decimal(self.amount) > 1:
            raise ValueError("Equity fraction must not exceed one")
        return self


class Timing(Contract):
    evaluate: Literal["bar_close", "intrabar", "event"]
    submit: Literal["next_bar_open", "next_observation", "after_signal"]
    precedence: Literal["exit_before_entry", "entry_before_exit"]


class InstrumentConstraints(Contract):
    asset_classes: tuple[Literal["crypto_spot", "crypto_perpetual", "equity", "etf", "forex"], ...]
    fixed_symbols: tuple[str, ...]
    selection: Literal["fixed", "deployment_choice"]


class DataRequirements(Contract):
    input: Literal["ohlcv", "trades", "quotes", "reference_rate"]
    warmup_bars: PositiveInt
    availability: Literal["closed_bars_only", "event_timestamp"]


class ExecutionRequirements(Contract):
    order_type: Literal["market", "limit"]
    shorting: bool = Field(strict=True)
    max_positions: PositiveInt
    reinforcement: bool = Field(strict=True)
    session_end: Literal["hold", "close"]


class MissingDataPolicy(Contract):
    action: Literal["abort", "suspend", "skip"]
    max_staleness_seconds: PositiveInt


class StrategySpec(Contract):
    schema_version: Literal["strategy/1"]
    indicators: tuple[Indicator, ...]
    entry_rules: tuple[Rule, ...]
    exit_rules: tuple[Rule, ...]
    position_sizing: Sizing
    signal_timing: Timing
    timeframes: tuple[PositiveInt, ...]
    market_conditions: tuple[Expression, ...]
    instrument_constraints: InstrumentConstraints
    data_requirements: DataRequirements
    execution_requirements: ExecutionRequirements
    missing_data_policy: MissingDataPolicy
    assumptions: tuple[str, ...]

    @model_validator(mode="after")
    def complete_and_consistent(self) -> Self:
        ids = [item.id for item in self.indicators]
        rule_ids = [rule.id for rule in (*self.entry_rules, *self.exit_rules)]
        if len(ids) != len(set(ids)) or len(rule_ids) != len(set(rule_ids)):
            raise ValueError("Indicator and rule IDs must be unique within their groups")
        if not self.entry_rules or not self.exit_rules or not self.timeframes:
            raise ValueError("Complete entry/exit/timeframe definitions required")
        if len(self.timeframes) != len(set(self.timeframes)):
            raise ValueError("Duplicate timeframe")
        if not self.instrument_constraints.asset_classes:
            raise ValueError("At least one explicit asset class required")
        fixed = self.instrument_constraints.selection == "fixed"
        if fixed != bool(self.instrument_constraints.fixed_symbols):
            raise ValueError("Fixed selection requires symbols; deployment choice excludes them")
        if any(item.timeframe_seconds not in self.timeframes for item in self.indicators):
            raise ValueError("Indicator timeframe is absent from requirements")
        if any(not rule.action.startswith("open_") for rule in self.entry_rules):
            raise ValueError("Entry rules must open positions")
        if any(not rule.action.startswith("close_") for rule in self.exit_rules):
            raise ValueError("Exit rules must close positions")
        if not self.execution_requirements.shorting and any(
            rule.action.endswith("short") for rule in (*self.entry_rules, *self.exit_rules)
        ):
            raise ValueError("Short rules require shorting capability")
        for item in self.indicators:
            if self.data_requirements.warmup_bars < item.period + 1:
                raise ValueError("Warmup must include the previous indicator observation")

        def expression_type(node: Expression) -> str:
            if node.op == "indicator" and node.value not in ids:
                raise ValueError("Unknown indicator reference")
            if node.op in {"boolean", "position"}:
                return "boolean"
            if node.op in {"decimal", "field", "indicator"}:
                return "numeric"
            types = [expression_type(arg) for arg in node.args]
            expected = "boolean" if node.op in {"and", "or", "not"} else "numeric"
            if any(kind != expected for kind in types):
                raise ValueError("Expression argument type mismatch")
            return "numeric" if node.op in {"add", "sub", "mul", "div"} else "boolean"

        for condition in [
            *(rule.condition for rule in (*self.entry_rules, *self.exit_rules)),
            *self.market_conditions,
        ]:
            if expression_type(condition) != "boolean":
                raise ValueError("A rule/filter must produce a boolean")
        if any(not assumption.strip() for assumption in self.assumptions):
            raise ValueError("Empty assumption")
        return self


class StrategyDocument(Contract):
    """Explanatory metadata is outside the immutable behavior fingerprint."""

    name: str
    description: str
    formal_spec: StrategySpec
    source_refs: tuple[str, ...]


class DatasetManifest(Contract):
    schema_version: Literal["dataset/1"]
    provider: str
    symbol: str
    asset_class: Literal["crypto_spot", "crypto_perpetual", "equity", "etf", "forex"]
    source_url: str
    content_sha256: Digest
    timestamp_unit: Literal["iso_date"]
    timezone: Literal["UTC"]
    interval_seconds: PositiveInt
    expected_rows: PositiveInt
    expected_dates: tuple[str, ...]
    availability: Literal["assume_17_utc_on_observation_date"]
    normalization_version: Literal["ecb-reference/1"]
    attribution: Literal["Source: European Central Bank; available free from the ECB."]
    usage: Literal["reference_rates_not_tradable_prices"]

    @model_validator(mode="after")
    def complete_dates(self) -> Self:
        for value in self.expected_dates:
            if date.fromisoformat(value).isoformat() != value:
                raise ValueError("Use ISO dates")
        if len(set(self.expected_dates)) != self.expected_rows:
            raise ValueError("Dataset date/row count mismatch")
        if self.expected_dates != tuple(sorted(set(self.expected_dates))):
            raise ValueError("Dataset dates must be unique and ordered")
        if self.provider != "ECB" or self.symbol != "EURUSD" or self.asset_class != "forex":
            raise ValueError("This normalization profile supports only ECB EURUSD references")
        if self.interval_seconds != 86400:
            raise ValueError("ECB reference profile requires daily observations")
        return self


class InstrumentManifest(Contract):
    schema_version: Literal["instrument/1"]
    symbol: str
    venue: str
    base_currency: str
    quote_currency: str
    asset_class: Literal["crypto_spot", "forex"]
    price_increment: str
    quantity_increment: str
    min_quantity: str
    min_notional: str
    metadata_basis: Literal["declared_simulation_not_historical_venue_rules"]

    @model_validator(mode="after")
    def exact_positive(self) -> Self:
        for field in ("price_increment", "quantity_increment", "min_quantity", "min_notional"):
            normalized = decimal_text(getattr(self, field))
            if Decimal(normalized) <= 0:
                raise ValueError("Instrument increments/minima must be positive")
            object.__setattr__(self, field, normalized)
        return self


class ExperimentProtocol(Contract):
    schema_version: Literal["experiment/1"]
    purpose: Literal["compatibility_and_conformance_only"]
    strategy_sha256: Digest
    dataset_manifest_sha256: Digest
    instrument_manifest_sha256: Digest
    engine: Literal["nautilus_trader"]
    engine_version: Literal["1.231.0"]
    adapter_version: Literal["phase0-next-reference/1"]
    initial_quote_balance: str
    initial_base_balance: str
    taker_fee_rate: str
    spread_bps: str
    slippage_bps: str
    fill_convention: Literal["synthetic_trade_at_next_reference"]
    last_signal: Literal["record_without_fill"]
    end_positions: Literal["leave_open_and_mark"]
    evaluation: Literal["whole_snapshot_no_profitability_or_holdout_claim"]
    seed: Literal[0]

    @model_validator(mode="after")
    def exact_costs(self) -> Self:
        for field in (
            "initial_quote_balance",
            "initial_base_balance",
            "taker_fee_rate",
            "spread_bps",
            "slippage_bps",
        ):
            value = decimal_text(getattr(self, field))
            if Decimal(value) < 0:
                raise ValueError("Balance/cost cannot be negative")
            object.__setattr__(self, field, value)
        if Decimal(self.initial_quote_balance) <= 0:
            raise ValueError("Initial quote balance must be positive")
        if Decimal(self.taker_fee_rate) >= 1:
            raise ValueError("Fee rate must be below one")
        return self


def canonical_bytes(document: Contract) -> bytes:
    return json.dumps(
        document.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def fingerprint(document: Contract) -> str:
    return hashlib.sha256(canonical_bytes(document)).hexdigest()


def same_behavior(left: StrategySpec, right: StrategySpec) -> bool:
    """Digest equality is not sufficient to deduplicate specifications."""
    if fingerprint(left) != fingerprint(right):
        return False
    if canonical_bytes(left) != canonical_bytes(right):
        raise ValueError("Strategy digest collision")
    return True


def load_contract(path, contract_type):
    """Reject duplicate JSON keys instead of silently retaining their last value."""

    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    data = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_pairs)
    return contract_type.model_validate(data)


def new_public_id() -> str:
    """Registry persistence must enforce uniqueness and retry collisions in Phase 1."""
    return f"{secrets.randbelow(10**24):024d}"
