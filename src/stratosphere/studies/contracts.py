"""Immutable, explicit experimental protocols for the supported SMA implementation."""

import calendar
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator

from stratosphere.phase0.contracts import (
    Contract,
    Digest,
    StrategySpec,
    canonical_bytes,
    fingerprint,
)
from stratosphere.research.contracts import (
    Extraction,
    ResearchConfig,
    ResearchPlan,
    SimulationPlan,
    SmaParameters,
    build_spec,
    implementation_sources,
)

HOUR_NS = 3600 * 10**9


class ArchivePin(Contract):
    month: str = Field(pattern=r"^20[0-9]{2}-(0[1-9]|1[0-2])$")
    sha256: Digest
    provider: Literal["binance-spot-btcusdt-hourly-microseconds/1"] = (
        "binance-spot-btcusdt-hourly-microseconds/1"
    )

    @model_validator(mode="after")
    def valid_month(self) -> Self:
        if self.month < "2025-01":
            raise ValueError("This adapter verifies microsecond archives from January 2025")
        return self

    @property
    def first_ns(self) -> int:
        return (
            int(datetime.fromisoformat(self.month + "-01").replace(tzinfo=UTC).timestamp()) * 10**9
        )

    @property
    def count(self) -> int:
        year, month = map(int, self.month.split("-"))
        return calendar.monthrange(year, month)[1] * 24

    @property
    def filename(self) -> str:
        return f"BTCUSDT-1h-{self.month}.zip"

    @property
    def url(self) -> str:
        return f"https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1h/{self.filename}"


class Candidate(Contract):
    # Invalid proposals remain in the reviewed list and get an explicit retained rejection.
    fast: int = Field(strict=True)
    slow: int = Field(strict=True)

    def parameters(self, quantity: str) -> SmaParameters:
        return SmaParameters(fast_period=self.fast, slow_period=self.slow, quantity=quantity)


class StrategyAnchor(Contract):
    """The original behavior/source/cost references, independent of its earlier data profile."""

    original_plan_sha256: Digest
    source_sha256: Digest
    draft: Extraction
    specification: StrategySpec
    config: ResearchConfig
    engine_version: Literal["1.231.0"] = "1.231.0"

    @model_validator(mode="after")
    def compiled(self) -> Self:
        if canonical_bytes(self.specification) != canonical_bytes(build_spec(self.draft)):
            raise ValueError("The parent behavior differs from the trusted SMA implementation")
        return self

    @classmethod
    def from_plan(cls, plan: ResearchPlan):
        return cls(
            original_plan_sha256=plan.digest,
            source_sha256=plan.source_sha256,
            draft=plan.draft,
            specification=plan.specification,
            config=plan.config,
        )


ASSUMPTIONS = (
    "BTCUSDT spot hourly SMA only; candidate changes are exactly the listed SMA periods.",
    "Development is the first floor(70% * monthly bars); validation is the remaining bars.",
    "Each window starts flat with the reviewed initial cash; indicators warm on prior bars only.",
    "Crossings must occur at a measured closed candle; orders fill at its next open.",
    "No order from pre-window crossings; terminal exposure is marked, not closed.",
    "Every candidate has development and validation runs at base and doubled costs.",
    "Selection uses validation doubled-cost net P&L, then lower drawdown, then listed order.",
    "Development metrics are descriptive; holdout performance cannot change selection.",
    "A candidate is eligible only after all four runs complete; failures stay in the denominator.",
    "The selected candidate gets one base and one doubled-cost run on the next monthly archive.",
    "Acquire holdout bars after immutable selection; claim local exposure before download.",
    "Local tracking cannot prove data was unseen outside this workspace or on another device.",
    "Synthetic full fills, eight-decimal SIM values; account fees/liquidity are unverified.",
    "No profitability threshold, automatic promotion, paper/live activation or fee collection.",
    "Binance Vision commercial data rights are not granted by this protocol or application.",
)


def study_sources() -> dict[str, str]:
    sources = implementation_sources()
    sources.update(
        {f"studies/{p.name}": p.read_text() for p in sorted(Path(__file__).parent.glob("*.py"))}
    )
    return sources


def study_implementation_digest() -> str:
    digest = hashlib.sha256()
    for name, source in sorted(study_sources().items()):
        digest.update(name.encode() + b"\0" + source.encode())
    return digest.hexdigest()


class StudyProtocol(Contract):
    schema_version: Literal["bounded-study/1"] = "bounded-study/1"
    parent_strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    parent_plan: StrategyAnchor
    hypothesis: str = Field(min_length=1)
    candidates: tuple[Candidate, ...] = Field(min_length=1)
    development: ArchivePin
    holdout: ArchivePin
    max_simulations: int = Field(strict=True, gt=0)
    implementation_sha256: Digest
    selection: Literal["validation-stress-net-pnl-drawdown-order/1"] = (
        "validation-stress-net-pnl-drawdown-order/1"
    )
    benchmark: Literal["cash-and-same-quantity-buy-hold/1"] = "cash-and-same-quantity-buy-hold/1"
    data_entitlement: Literal["commercial-rights-unverified"] = "commercial-rights-unverified"
    assumptions: tuple[str, ...] = ASSUMPTIONS

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if not self.hypothesis.strip() or self.assumptions != ASSUMPTIONS:
            raise ValueError("An explicit hypothesis and the supported protocol are required")
        if len(set((c.fast, c.slow) for c in self.candidates)) != len(self.candidates):
            raise ValueError("Repeated candidates would conceal the effective search size")
        if self.development.first_ns + self.development.count * HOUR_NS != self.holdout.first_ns:
            raise ValueError("The final holdout must be the next separate monthly archive")
        if self.max_simulations < self.required_simulations:
            raise ValueError("Budget must cover four runs per candidate and two final holdout runs")
        return self

    @property
    def required_simulations(self) -> int:
        return 4 * len(self.candidates) + 2

    @property
    def split(self) -> int:
        return self.development.count * 7 // 10

    @property
    def digest(self) -> str:
        return fingerprint(self)

    def simulation(self, candidate: Candidate) -> SimulationPlan:
        parameters = candidate.parameters(self.parent_plan.draft.quantity)
        if parameters.slow_period >= self.split:
            raise ValueError("The development window cannot warm these SMA periods")
        return SimulationPlan(draft=parameters, config=self.parent_plan.config)

    def variant(self, candidate: Candidate):
        self.simulation(candidate)
        # Retain original source evidence in parent_plan. Explicit protocol parameters, rather
        # than invented source quotes, authorize this behavioral variant.
        payload = self.parent_plan.draft.model_dump(mode="json")
        payload.update(fast_period=candidate.fast, slow_period=candidate.slow)
        return build_spec(Extraction.model_validate(payload))
