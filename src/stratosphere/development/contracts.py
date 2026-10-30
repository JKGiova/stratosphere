"""Frozen research scope and a typed, finite signal language, never arbitrary model Python."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, model_validator

from stratosphere.phase0.contracts import Contract, StrategySpec, fingerprint
from stratosphere.research.contracts import ResearchConfig
from stratosphere.yahoo.contracts import YahooBar


class Market(Contract):
    symbol: str = Field(pattern=r"^[A-Z0-9][A-Z0-9.\-=]{0,19}$")
    asset_class: Literal["equity", "crypto_spot", "forex"]
    interval: Literal["1h", "1d"]
    start: date
    end: date
    quantity: str = "1"
    config: ResearchConfig = ResearchConfig()
    hypothesis: str = Field(
        default="Declared research target, not a measured suitability claim",
        min_length=1,
        max_length=1000,
    )

    @model_validator(mode="after")
    def valid(self):
        from stratosphere.phase0.contracts import decimal_text

        quantity = Decimal(decimal_text(self.quantity))
        object.__setattr__(self, "quantity", decimal_text(self.quantity))
        if quantity <= 0 or quantity % Decimal("0.00000001"):
            raise ValueError("Positive quantity with at most eight decimals required")
        if self.asset_class == "equity" and quantity % 1:
            raise ValueError("Equity research uses whole shares")
        if not self.start < self.end <= datetime.now(UTC).date():
            raise ValueError("Use a fully historical positive window; end is exclusive")
        return self

    @property
    def seconds(self):
        return 3600 if self.interval == "1h" else 86400


class PriceSnapshot(Contract):
    schema_version: Literal["development-price-snapshot/1"] = "development-price-snapshot/1"
    request: Market
    returned_frame_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    currency: str
    base_currency: str | None
    exchange: str
    timezone: str
    bars: tuple[YahooBar, ...] = Field(min_length=3)
    gaps: tuple[tuple[int, int], ...]
    observed_at_ns: int = Field(strict=True, gt=0)

    @model_validator(mode="after")
    def valid(self):
        gaps = []
        for a, b in zip(self.bars, self.bars[1:], strict=False):
            if b.open_ns <= a.close_ns:
                raise ValueError("Overlapping or unordered price observations")
            if b.open_ns - a.open_ns > self.request.seconds * 10**9:
                gaps.append((a.open_ns, b.open_ns))
        if tuple(gaps) != self.gaps or (gaps and self.request.asset_class == "crypto_spot"):
            raise ValueError("Unaccounted or continuous-market gaps")
        return self

    @property
    def digest(self):
        return fingerprint(self)


def validate_program(spec: StrategySpec):
    """Reject unsupported behavior before code generation or a native start."""
    if (
        len(spec.timeframes) != 1
        or spec.timeframes[0] not in {3600, 86400}
        or spec.signal_timing.evaluate != "bar_close"
        or spec.signal_timing.submit != "next_bar_open"
        or spec.signal_timing.precedence != "exit_before_entry"
        or spec.position_sizing.mode != "fixed_base"
        or spec.data_requirements.input != "ohlcv"
        or spec.data_requirements.availability != "closed_bars_only"
        or spec.execution_requirements.order_type != "market"
        or spec.execution_requirements.shorting
        or spec.execution_requirements.reinforcement
        or spec.execution_requirements.max_positions != 1
        or spec.execution_requirements.session_end != "hold"
        or spec.missing_data_policy.action not in {"skip", "abort"}
    ):
        raise ValueError("Unsupported timing, sizing, execution, shorting or missing-data behavior")
    if len(spec.indicators) > 12 or len(spec.entry_rules) + len(spec.exit_rules) > 16:
        raise ValueError("Program indicator/rule budget exceeded")
    if any(i.period > 519 for i in spec.indicators) or spec.data_requirements.warmup_bars > 1024:
        raise ValueError("Program warmup/period budget exceeded")
    count = 0

    def visit(node, depth=0):
        nonlocal count
        if node.op == "session":
            raise ValueError("Session events require the dedicated MT5 overnight adapter")
        count += 1
        if depth > 16 or count > 256:
            raise ValueError("Expression complexity budget exceeded")
        for child in node.args:
            visit(child, depth + 1)

    for rule in (*spec.entry_rules, *spec.exit_rules):
        visit(rule.condition)
    for condition in spec.market_conditions:
        visit(condition)
    if any(r.action != "open_long" for r in spec.entry_rules) or any(
        r.action != "close_long" for r in spec.exit_rules
    ):
        raise ValueError("Only long entry and full long exit are supported")
    return spec


class Definition(Contract):
    status: Literal["ready", "needs_clarification", "unsupported"]
    name: str = Field(min_length=1, max_length=160)
    hypothesis: str = Field(min_length=1, max_length=2000)
    specification: StrategySpec | None
    markets: tuple[Market, ...] = Field(max_length=4)
    # One application-owned intake-rule index per formal behavioral category.
    rule_indices: dict[str, Annotated[int, Field(strict=True, ge=0)]]
    questions: tuple[str, ...] = Field(max_length=12)

    @model_validator(mode="after")
    def coherent(self):
        if self.status == "ready":
            if self.specification is None or not self.markets or self.questions:
                raise ValueError(
                    "Ready definitions require complete rules, markets and no questions"
                )
            validate_program(self.specification)
            if len(
                {
                    (
                        m.symbol,
                        m.asset_class,
                        m.interval,
                        m.start,
                        m.end,
                        m.quantity,
                        m.config.model_dump_json(),
                    )
                    for m in self.markets
                }
            ) != len(self.markets):
                raise ValueError("Repeated markets are not additional evidence")
            if set(self.rule_indices) != {"entry", "exit", "market", "timing", "sizing", "risk"}:
                raise ValueError("Every formal category must map to the retained intake")
        elif self.specification is not None or self.markets or not self.questions:
            raise ValueError("Unresolved definitions cannot contain executable candidates")
        return self


class Revision(Contract):
    action: Literal["revise", "stop"]
    reason: str = Field(min_length=1, max_length=2000)
    specification: StrategySpec | None

    @model_validator(mode="after")
    def valid(self):
        if (self.action == "revise") != (self.specification is not None):
            raise ValueError("Only revisions contain specifications")
        if self.specification is not None:
            validate_program(self.specification)
        return self


def candidate_spec(spec, market):
    data = spec.model_dump(mode="json")
    data["timeframes"] = [market.seconds]
    for indicator in data["indicators"]:
        indicator["timeframe_seconds"] = market.seconds
    data["position_sizing"]["amount"] = market.quantity
    data["instrument_constraints"] = {
        "asset_classes": [market.asset_class],
        "fixed_symbols": ["YAHOO:" + market.symbol],
        "selection": "fixed",
    }
    data["missing_data_policy"]["max_staleness_seconds"] = market.seconds
    return validate_program(StrategySpec.model_validate(data))


def implementation_digest():
    import hashlib

    root = Path(__file__).parent
    files = [
        *root.glob("*.py"),
        *(root.parent / "sources").glob("*.py"),
        root.parent / "workflow/discovery.py",
        root.parent / "yahoo/data.py",
        root.parent / "yahoo/engine.py",
        root.parent / "yahoo/metrics.py",
        root.parent / "research/metrics.py",
        root.parent / "phase0/contracts.py",
        root.parent / "research/contracts.py",
    ]
    # Optional readers are installed separately; their declared dependencies are still frozen.
    manifests = root.parents[2] / "tools/research"
    files.extend(
        p
        for p in (
            manifests / "package.json",
            manifests / "package-lock.json",
            manifests / "agent-reach-requirements.txt",
        )
        if p.is_file()
    )
    return hashlib.sha256(
        b"".join(p.name.encode() + b"\0" + p.read_bytes() for p in sorted(files))
    ).hexdigest()


class CellPlan(Contract):
    schema_version: Literal["strategy-development-cell/1"] = "strategy-development-cell/1"
    campaign_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    campaign_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    specification: StrategySpec
    request: Market
    snapshot_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    code_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    implementation_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def coherent(self):
        validate_program(self.specification)
        if self.specification != candidate_spec(self.specification, self.request):
            raise ValueError("Concrete strategy differs from its exact market scope")
        return self

    @property
    def digest(self):
        return fingerprint(self)
