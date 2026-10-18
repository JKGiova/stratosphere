"""Separate requested compatibility, observations, evidence and missing verification."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from stratosphere.phase0.contracts import Contract, Digest, decimal_text


class AssessmentTarget(Contract):
    asset_class: Literal["crypto_spot", "crypto_perpetual", "equity", "etf", "forex"]
    symbol: str = Field(min_length=1, pattern=r"^[A-Z0-9][A-Z0-9._/-]*$")
    timeframe_seconds: int = Field(strict=True, gt=0)
    allow_overnight: bool = Field(default=True, strict=True)
    max_drawdown_pct: str | None = None

    @model_validator(mode="after")
    def exact_limit(self) -> Self:
        if self.max_drawdown_pct is not None:
            value = decimal_text(self.max_drawdown_pct)
            if not 0 <= Decimal(value) <= 100:
                raise ValueError("A drawdown preference must be between zero and 100 percent")
            object.__setattr__(self, "max_drawdown_pct", value)
        return self


class MarketContext(Contract):
    """Only a trusted adapter can supply provider_snapshot; user notes stay unverified."""

    target: AssessmentTarget
    origin: Literal["unavailable", "user_supplied", "provider_snapshot"]
    as_of: AwareDatetime | None = None
    available_at: AwareDatetime | None = None
    source_refs: tuple[str, ...] = ()
    observations: tuple[str, ...] = ()
    snapshot_sha256: Digest | None = None
    missing_reason: str | None = None

    @model_validator(mode="after")
    def timestamps(self) -> Self:
        for key in ("as_of", "available_at"):
            if value := getattr(self, key):
                object.__setattr__(self, key, value.astimezone(UTC))
        if self.origin == "unavailable" and not self.missing_reason:
            raise ValueError("Unavailable current context requires an explicit reason")
        if self.origin == "provider_snapshot" and (
            not self.as_of
            or not self.available_at
            or not self.source_refs
            or not self.snapshot_sha256
        ):
            raise ValueError(
                "Provider context requires both timestamps, sources and snapshot digest"
            )
        return self

    def condition(self, assessed_at: datetime, max_age: int) -> str:
        if self.origin == "unavailable":
            return "unavailable"
        if self.origin == "user_supplied":
            return "user_supplied_unverified"
        if self.as_of > assessed_at or self.available_at > assessed_at:
            return "future_dated"
        if self.available_at < self.as_of:
            return "invalid_availability_order"
        if (assessed_at - self.as_of).total_seconds() > max_age:
            return "stale"
        return "current_snapshot"


class EvidenceReference(Contract):
    job_id: str
    artifact_sha256: Digest
    plan_sha256: Digest
    strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    strategy_sha256: Digest
    kind: Literal["single_research", "study_variant"]
    attempted_variants: int = Field(strict=True, ge=1)
    candidate_status: str
    final_holdout: Literal["completed_for_exact_variant", "incomplete", "not_evaluated"]
    development_month: str | None = None
    holdout_month: str | None = None
    metrics: dict
    limitations: tuple[str, ...]
    data_representativeness: Literal["not_reverified_for_this_assessment"] = (
        "not_reverified_for_this_assessment"
    )


class ApplicabilityAssessment(Contract):
    schema_version: Literal["applicability-assessment/1"] = "applicability-assessment/1"
    strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    strategy_sha256: Digest
    name: str
    assessed_at: AwareDatetime
    target: AssessmentTarget
    compatibility: dict[str, bool]
    data_capability: str
    implementation_evidence: Literal["retained_native_results", "not_verified"]
    current_context: MarketContext
    current_context_status: str
    evidence: tuple[EvidenceReference, ...]
    retained_jobs: dict[str, int]
    attempted_variants: int = Field(strict=True, ge=0)
    conclusion: Literal["incompatible_rules", "research_candidate_requires_verification"]
    missing_verification: tuple[str, ...]
    next_steps: tuple[str, ...]
    promotion: Literal["none"] = "none"
