"""Explicit bar boundaries, availability provenance and exact market identities."""

from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from stratosphere.phase0.contracts import Contract, Digest, decimal_text, fingerprint


class MarketIdentity(Contract):
    asset_class: Literal["crypto_spot", "crypto_perpetual", "equity", "etf", "forex"]
    symbol: str = Field(pattern=r"^[A-Z0-9][A-Z0-9._/-]*$")
    venue: str = Field(min_length=1)
    base_asset: str = Field(pattern=r"^[A-Z0-9]+$")
    quote_currency: str = Field(pattern=r"^[A-Z0-9]+$")
    interval_seconds: int = Field(strict=True, gt=0)
    price_kind: Literal["venue_trade_ohlcv", "reference_rate", "authored_fixture"]

    @property
    def digest(self):
        return fingerprint(self)


class ClosedObservation(Contract):
    # end_ns is exclusive, unlike the legacy archive's inclusive close timestamp.
    open_ns: int = Field(strict=True, gt=0)
    end_ns: int = Field(strict=True, gt=0)
    available_ns: int = Field(strict=True, gt=0)
    received_ns: int = Field(strict=True, gt=0)
    open: str
    high: str
    low: str
    close: str
    volume: str | None
    complete: bool = Field(strict=True)

    @model_validator(mode="after")
    def numeric_shape(self):
        for key in ("open", "high", "low", "close", "volume"):
            value = getattr(self, key)
            if value is not None:
                object.__setattr__(self, key, decimal_text(value))
        if min(Decimal(self.open), Decimal(self.low), Decimal(self.close)) <= 0:
            raise ValueError("Market prices must be positive")
        if (
            not Decimal(self.low)
            <= min(Decimal(self.open), Decimal(self.close))
            <= max(Decimal(self.open), Decimal(self.close))
            <= Decimal(self.high)
        ):
            raise ValueError("Invalid OHLC range")
        if self.volume is not None and Decimal(self.volume) < 0:
            raise ValueError("Volume cannot be negative")
        if self.end_ns <= self.open_ns:
            raise ValueError("A bar must have a positive interval")
        return self


class DataBatch(Contract):
    schema_version: Literal["market-data-batch/1"] = "market-data-batch/1"
    provider: str = Field(min_length=1)
    provider_symbol: str = Field(min_length=1)
    identity: MarketIdentity
    raw_sha256: Digest
    normalizer_sha256: Digest
    source_refs: tuple[str, ...] = Field(min_length=1)
    availability: Literal[
        "provider_published", "observed_receipt", "assumed_close", "authored_fixture"
    ]
    rows: tuple[ClosedObservation, ...]
    session_calendar: Literal[
        "continuous", "verified_exchange_calendar", "reference_publication_calendar", "unverified"
    ]
    corporate_actions: Literal[
        "not_applicable",
        "unadjusted_with_verified_actions",
        "adjusted_with_verified_actions",
        "unverified",
    ]
    funding: Literal["not_applicable", "retained", "unverified"]
    # Honest provider-specific exclusions are inspectable, never inferred quality repair.
    exclusions: tuple[str, ...] = ()

    @property
    def digest(self):
        return fingerprint(self)


class QualityPolicy(Contract):
    max_age_ns: int = Field(strict=True, ge=0)
    max_receipt_lag_ns: int = Field(strict=True, ge=0)
    warmup_bars: int = Field(strict=True, ge=0)
    allow_assumed_availability: bool = Field(default=False, strict=True)
    alignment_origin_ns: int = Field(default=0, strict=True, ge=0)
    expected_start_ns: int | None = Field(default=None, strict=True, gt=0)
    expected_end_ns: int | None = Field(default=None, strict=True, gt=0)

    @model_validator(mode="after")
    def requested_window(self):
        if (self.expected_start_ns is None) != (self.expected_end_ns is None):
            raise ValueError("A bounded request needs both exclusive window boundaries")
        if self.expected_start_ns is not None and self.expected_end_ns <= self.expected_start_ns:
            raise ValueError("The requested window must be positive")
        return self


class QualityFinding(Contract):
    code: str
    indices: tuple[int, ...] = ()
    detail: str


class DataQuality(Contract):
    batch_sha256: Digest
    checked_at_ns: int = Field(strict=True, gt=0)
    findings: tuple[QualityFinding, ...]
    accepted_rows: int = Field(strict=True, ge=0)

    @property
    def valid(self):
        return not self.findings
