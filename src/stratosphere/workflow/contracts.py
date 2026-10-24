"""Application-owned contracts for bounded research and portfolio decision stages."""

from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from stratosphere.phase0.contracts import Contract, Digest, decimal_text, fingerprint
from stratosphere.yahoo.contracts import YahooRequest


class ResearchProposal(Contract):
    schema_version: Literal["agent-research-proposal/1"] = "agent-research-proposal/1"
    status: Literal["ready", "needs_clarification", "unsupported"]
    name: str = Field(min_length=1, max_length=160)
    hypothesis: str = Field(min_length=1, max_length=2000)
    candidates: tuple[YahooRequest, ...] = Field(max_length=12)
    questions: tuple[str, ...] = Field(max_length=8)

    @model_validator(mode="after")
    def coherent(self):
        if self.status == "ready":
            if not self.candidates or self.questions:
                raise ValueError("A ready proposal needs candidates and no open questions")
            if len({fingerprint(c) for c in self.candidates}) != len(self.candidates):
                raise ValueError("Repeated research candidates are not additional evidence")
        elif self.candidates or not self.questions:
            raise ValueError("Unresolved proposals cannot contain runnable candidates")
        return self


class CandidatePin(Contract):
    index: int = Field(strict=True, ge=0, lt=12)
    job_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    plan_sha256: Digest
    implementation_sha256: Digest


class CampaignPlan(Contract):
    schema_version: Literal["agent-research-plan/1"] = "agent-research-plan/1"
    campaign_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    proposal_sha256: Digest
    implementation_sha256: Digest
    candidates: tuple[CandidatePin, ...] = Field(min_length=1, max_length=12)
    blocked_attempts_sha256: Digest
    native_starts: int = Field(strict=True, ge=2, le=24)
    selection_policy: Literal["human_review_all_attempts_no_holdout"] = (
        "human_review_all_attempts_no_holdout"
    )

    @model_validator(mode="after")
    def finite(self):
        if self.native_starts != 2 * len(self.candidates):
            raise ValueError("Each candidate consumes exactly base and stress starts")
        if len({c.job_id for c in self.candidates}) != len(self.candidates):
            raise ValueError("Candidate jobs must be distinct")
        if len({c.index for c in self.candidates}) != len(self.candidates):
            raise ValueError("Candidate indices must be distinct")
        return self

    @property
    def digest(self):
        return fingerprint(self)


class AutomaticCampaignPlan(CampaignPlan):
    """New authorization semantics; never reinterpret retained v1 human decisions."""

    schema_version: Literal["agent-research-plan/2"] = "agent-research-plan/2"
    authorization: Literal["application_research_policy/1"] = "application_research_policy/1"
    max_candidates: Literal[12] = 12
    max_native_starts: Literal[24] = 24
    selection_policy: Literal["review_all_attempts_no_holdout"] = "review_all_attempts_no_holdout"


class PortfolioDefinition(Contract):
    schema_version: Literal["portfolio-definition/1"] = "portfolio-definition/1"
    name: str = Field(min_length=1, max_length=100)
    provider: Literal["alpaca", "liquid", "ftmo"]
    mode: Literal["paper", "live", "evaluation"]
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    max_strategy_fraction: str
    max_position_fraction: str
    max_risk_fraction: str
    max_daily_loss_fraction: str
    profile: Literal["standard", "ftmo_2step_swing"] = "standard"

    @model_validator(mode="after")
    def limits(self):
        for name in (
            "max_strategy_fraction",
            "max_position_fraction",
            "max_risk_fraction",
            "max_daily_loss_fraction",
        ):
            value = decimal_text(getattr(self, name))
            object.__setattr__(self, name, value)
            if not 0 < Decimal(value) <= 1:
                raise ValueError("Explicit positive portfolio limits of at most one are required")
        if Decimal(self.max_position_fraction) > Decimal(self.max_strategy_fraction):
            raise ValueError("Position capital cannot exceed strategy capital")
        if self.provider == "ftmo":
            if self.mode != "evaluation" or self.profile != "ftmo_2step_swing":
                raise ValueError("FTMO is a separate two-step Swing evaluation profile")
        elif self.mode == "evaluation" or self.profile != "standard":
            raise ValueError("Broker paper/live portfolios use the standard profile")
        return self


class SignalVerdict(Contract):
    schema_version: Literal["strategy-signal-verdict/1"] = "strategy-signal-verdict/1"
    strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    signal_sha256: Digest
    context_sha256: Digest
    verdict: Literal["reliable", "not_reliable"]
    explanation: str = Field(min_length=1, max_length=2000)
    evidence: tuple[str, ...] = Field(min_length=1, max_length=20)


class CapitalProposal(Contract):
    schema_version: Literal["portfolio-capital-proposal/1"] = "portfolio-capital-proposal/1"
    portfolio_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    snapshot_sha256: Digest
    allocations: dict[str, str]
    explanation: str = Field(min_length=1, max_length=2000)

    @model_validator(mode="after")
    def exact_amounts(self):
        import re

        if not self.allocations or len(self.allocations) > 100:
            raise ValueError("Propose allocations for one to 100 assigned strategies")
        for strategy, value in self.allocations.items():
            if not re.fullmatch(r"[0-9]{24}", strategy):
                raise ValueError("Use exact strategy IDs")
            normalized = decimal_text(value)
            if Decimal(normalized) < 0:
                raise ValueError("Strategy capital cannot be negative")
        return self


class RiskSnapshot(Contract):
    """Application-supplied, minimal account aggregates; not a provider diagnostic."""

    schema_version: Literal["portfolio-risk-snapshot/1"] = "portfolio-risk-snapshot/1"

    portfolio_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    equity: str
    free_cash: str
    daily_loss: str
    strategy_capital: dict[str, str]
    committed_capital: dict[str, str]
    as_of_ns: int = Field(strict=True, gt=0)
    expires_at_ns: int = Field(strict=True, gt=0)
    reconciled: bool

    @model_validator(mode="after")
    def amounts(self):
        import re

        for name in ("equity", "free_cash", "daily_loss"):
            value = decimal_text(getattr(self, name))
            if Decimal(value) < 0:
                raise ValueError("Portfolio aggregates cannot be negative")
        if Decimal(self.equity) <= 0 or Decimal(self.free_cash) > Decimal(self.equity):
            raise ValueError("Use positive equity and bounded available cash")
        if self.expires_at_ns <= self.as_of_ns:
            raise ValueError("Snapshot validity must be finite")
        if set(self.strategy_capital) != set(self.committed_capital):
            raise ValueError("Every assigned strategy needs capital and committed exposure")
        for strategy in self.strategy_capital:
            if not re.fullmatch(r"[0-9]{24}", strategy):
                raise ValueError("Use exact assigned strategy identities")
            cap = Decimal(decimal_text(self.strategy_capital[strategy]))
            committed = Decimal(decimal_text(self.committed_capital[strategy]))
            if not 0 <= committed <= cap:
                raise ValueError("Committed and reserved exposure must fit strategy capital")
        if sum(map(Decimal, self.strategy_capital.values())) > Decimal(self.equity):
            raise ValueError("Assigned capital cannot exceed equity")
        return self

    @property
    def digest(self):
        return fingerprint(self)


class PositionSizingProposal(Contract):
    schema_version: Literal["portfolio-position-proposal/1"] = "portfolio-position-proposal/1"
    portfolio_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    snapshot_sha256: Digest
    signal_sha256: Digest
    requested_notional: str
    explanation: str = Field(min_length=1, max_length=2000)

    @model_validator(mode="after")
    def positive_notional(self):
        if Decimal(decimal_text(self.requested_notional)) <= 0:
            raise ValueError("Position capital must be positive")
        return self
