"""Packaged capability descriptions, not account entitlements or implied live adapters."""

from typing import Literal

from pydantic import Field

from stratosphere.market_data.contracts import MarketIdentity
from stratosphere.phase0.contracts import Contract
from stratosphere.research.store import ResearchError


class DataRequest(Contract):
    identity: MarketIdentity
    purpose: Literal["reference_proof", "historical_research", "paper", "software_verification"]
    provider: str


class CapabilityDecision(Contract):
    provider: str
    implementation: Literal["verified_bounded_historical", "fixture_only", "not_implemented"]
    technical_match: bool = Field(strict=True)
    eligible: bool = Field(strict=True)
    purpose: str
    blockers: tuple[str, ...]
    limitations: tuple[str, ...]
    references: tuple[str, ...]


def decide(request: DataRequest) -> CapabilityDecision:
    identity = request.identity
    profile = (
        identity.asset_class,
        identity.symbol,
        identity.venue,
        identity.interval_seconds,
        identity.price_kind,
        identity.base_asset,
        identity.quote_currency,
    )
    provider = request.provider
    eligible = False
    technical = False
    implementation = "not_implemented"
    blockers = []
    limitations = []
    references = []
    if provider == "ecb-reference-proof":
        implementation = "verified_bounded_historical"
        technical = profile == (
            "forex",
            "EURUSD",
            "ECB_REFERENCE",
            86400,
            "reference_rate",
            "EUR",
            "USD",
        )
        eligible = technical and request.purpose == "reference_proof"
        if not eligible:
            blockers.append(
                "The ECB integration is a bounded daily reference-rate proof, not tradable forex "
                "OHLCV or a paper feed."
            )
        limitations += [
            "Only the pinned January–March 2025 EURUSD proof is verified.",
            "17:00 UTC availability is an explicit assumption; no tradable quote or fill "
            "capability.",
        ]
        references += ["https://data.ecb.europa.eu/data/datasets/EXR/EXR.D.USD.EUR.SP00.A"]
    elif provider == "binance-archive":
        implementation = "verified_bounded_historical"
        technical = profile == (
            "crypto_spot",
            "BTCUSDT",
            "BINANCE",
            3600,
            "venue_trade_ohlcv",
            "BTC",
            "USDT",
        )
        blockers.append(
            "Commercial dataset entitlement is unverified; the archive cannot authorize "
            "commercial paper/live use."
        )
        if not technical:
            blockers.append(
                "The verified historical profile is BTCUSDT spot, hourly; no cross-market/venue "
                "substitution."
            )
        if request.purpose != "historical_research":
            blockers.append("This monthly historical adapter is not a real-time or execution feed.")
        limitations += [
            "Legacy January 2025 research and approved monthly SMA studies have separate reviewed "
            "protocols; this inspection does not execute or rewrite them.",
            "Archive close timestamps are assumed availability; historical exchange "
            "filters/liquidity are not verified.",
        ]
        references += [
            "https://github.com/binance/binance-public-data/blob/master/TERMS_AND_CONDITIONS.md"
        ]
    elif provider == "authored-fixture":
        implementation = "fixture_only"
        technical = identity.price_kind == "authored_fixture"
        eligible = technical and request.purpose == "software_verification"
        if not eligible:
            blockers.append(
                "Authored fixtures verify software only; they cannot establish market "
                "performance, current context or a real paper feed."
            )
        limitations += [
            "A matching decision does not verify fixture bytes; a caller must bind and validate "
            "its exact authored snapshot."
        ]
    elif provider == "kraken-public":
        blockers += [
            "No Kraken market-data adapter is implemented or verified in stratosphere.",
            "Prior permission for non-personal commercial use of public data has not been "
            "verified.",
        ]
        limitations += [
            "OHLC WebSocket updates are trade-driven and include running candles; no "
            "closed-bar/reconnect/catch-up behavior is implemented.",
            "REST OHLC is limited to the newest 720 entries, including the current uncommitted "
            "candle.",
        ]
        references += [
            "https://docs-legacy.kraken.com/api/docs/guides/global-intro/#notices",
            "https://docs.kraken.com/api-reference/market-data/get-ohlc-data",
            "https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ohlc",
        ]
    else:
        blockers.append("No implemented verified adapter matches this provider request.")
    if not technical and not blockers:
        blockers.append(
            "Requested market/venue/interval semantics do not match the bounded adapter."
        )
    if request.purpose == "paper":
        eligible = False
        if not any("paper" in text.lower() for text in blockers):
            blockers.append("No commercial-ready real-time paper-data capability is verified.")
    if blockers:
        eligible = False
    return CapabilityDecision(
        provider=provider,
        implementation=implementation,
        technical_match=technical,
        eligible=eligible,
        purpose=request.purpose,
        blockers=tuple(blockers),
        limitations=tuple(limitations),
        references=tuple(references),
    )


def require_capability(request: DataRequest):
    decision = decide(request)
    if not decision.eligible:
        raise ResearchError("Data request blocked: " + " ".join(decision.blockers))
    return decision


PROFILES = (
    "ecb-reference-proof",
    "binance-archive",
    "authored-fixture",
    "kraken-public",
)


def profile_request(provider, purpose=None):
    # These are requested bounded/exemplar profiles, not discovered account instruments.
    values = dict(
        asset_class="crypto_spot",
        symbol="BTCUSDT",
        venue="BINANCE",
        base_asset="BTC",
        quote_currency="USDT",
        interval_seconds=3600,
        price_kind="venue_trade_ohlcv",
    )
    default = "historical_research"
    if provider == "ecb-reference-proof":
        values.update(
            asset_class="forex",
            symbol="EURUSD",
            venue="ECB_REFERENCE",
            base_asset="EUR",
            quote_currency="USD",
            interval_seconds=86400,
            price_kind="reference_rate",
        )
        default = "reference_proof"
    elif provider == "authored-fixture":
        values.update(venue="SIM", price_kind="authored_fixture")
        default = "software_verification"
    elif provider == "kraken-public":
        values.update(venue="KRAKEN")
        default = "paper"
    elif provider != "binance-archive":
        raise ResearchError("Unknown data profile. Use /data to inspect the supported scope.")
    return DataRequest(
        identity=MarketIdentity.model_validate(values),
        provider=provider,
        purpose=purpose or default,
    )


def capabilities_markdown(provider=None, purpose=None):
    profiles = (provider,) if provider else PROFILES
    lines = [
        "# Data capabilities",
        "This is a read-only capability inspection. No prices were downloaded, account linked "
        "or order placed. Passing data quality alone does not grant usage rights or execution "
        "capability.",
    ]
    for name in profiles:
        request = profile_request(name, purpose)
        value = decide(request)
        lines += [
            f"## {name}",
            f"{value.implementation.replace('_', ' ')} · purpose: {value.purpose}",
            f"Requested profile: {request.identity.asset_class} {request.identity.symbol} at "
            f"{request.identity.venue}, {request.identity.interval_seconds}s.",
            f"Eligible for this bounded purpose: **{'yes' if value.eligible else 'no'}**. "
            f"Technical profile match: **{'yes' if value.technical_match else 'no'}**.",
        ]
        lines += ["- " + text for text in value.blockers + value.limitations]
        lines += ["Reference: " + url for url in value.references]
    return "\n\n".join(lines)
