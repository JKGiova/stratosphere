"""Routing and quality do not invent market semantics or repair provider input."""

import pytest

from stratosphere.market_data.contracts import (
    ClosedObservation,
    DataBatch,
    MarketIdentity,
    QualityPolicy,
)
from stratosphere.market_data.quality import inspect_batch
from stratosphere.market_data.routing import DataRequest, decide, require_capability
from stratosphere.research.store import ResearchError

SECOND = 10**9
START = 3600 * SECOND


def identity(**changes):
    return MarketIdentity.model_validate(
        dict(
            asset_class="crypto_spot",
            symbol="BTCUSDT",
            venue="BINANCE",
            base_asset="BTC",
            quote_currency="USDT",
            interval_seconds=3600,
            price_kind="venue_trade_ohlcv",
        )
        | changes
    )


def observation(index=0, **changes):
    opened = START + index * 3600 * SECOND
    return ClosedObservation.model_validate(
        dict(
            open_ns=opened,
            end_ns=opened + 3600 * SECOND,
            available_ns=opened + 3600 * SECOND + 2 * SECOND,
            received_ns=opened + 3600 * SECOND + 2 * SECOND,
            open="100",
            high="101",
            low="99",
            close="100.5",
            volume="1000",
            complete=True,
        )
        | changes
    )


def batch(**changes):
    return DataBatch.model_validate(
        dict(
            provider="authored-provider-shaped-fixture",
            provider_symbol="BTCUSDT",
            identity=identity(),
            raw_sha256="a" * 64,
            normalizer_sha256="b" * 64,
            source_refs=("authored fixture, no provider request",),
            availability="observed_receipt",
            rows=(observation(), observation(1)),
            session_calendar="continuous",
            corporate_actions="not_applicable",
            funding="not_applicable",
        )
        | changes
    )


def policy(**changes):
    return QualityPolicy.model_validate(
        dict(
            max_age_ns=5 * SECOND,
            max_receipt_lag_ns=3 * SECOND,
            warmup_bars=2,
            expected_start_ns=START,
            expected_end_ns=START + 7200 * SECOND,
        )
        | changes
    )


def codes(value, **kwargs):
    result = inspect_batch(value, policy(**kwargs), now_ns=START + 7200 * SECOND + 3 * SECOND)
    return result, {finding.code for finding in result.findings}


def test_well_shaped_closed_fixture_has_exact_identity_digest_and_no_quality_repair():
    value = batch()
    result, findings = codes(value)
    assert result.valid and not findings and result.accepted_rows == 2
    assert result.batch_sha256 == value.digest
    assert (
        value.rows[0].close == "100.5"
        and value.identity.digest != identity(quote_currency="USD").digest
    )
    # Passing quality is not a license, verified venue filters, feed or activation.
    decision = decide(
        DataRequest(identity=value.identity, purpose="paper", provider="binance-archive")
    )
    assert decision.technical_match and not decision.eligible


@pytest.mark.parametrize(
    "rows,expected",
    [
        ((observation(1), observation()), "unordered"),
        ((observation(), observation()), "duplicate"),
        ((observation(), observation(2)), "gap"),
        ((observation(), observation(1, complete=False)), "incomplete"),
        (
            (observation(), observation(1, available_ns=START + 7200 * SECOND - 1)),
            "availability_before_close",
        ),
        (
            (observation(), observation(1, received_ns=START + 7200 * SECOND)),
            "receipt_before_availability",
        ),
        (
            (observation(), observation(1, received_ns=START + 7200 * SECOND + 10 * SECOND)),
            "future_observation",
        ),
        ((observation(), observation(1, volume=None)), "missing_volume"),
        ((observation(),), "incomplete_requested_window"),
        ((), "empty"),
    ],
)
def test_bad_observations_are_retained_and_block_the_entire_batch(rows, expected):
    value = batch(rows=rows)
    before = value.model_dump_json()
    result, found = codes(value)
    assert expected in found and not result.valid and result.accepted_rows == 0
    assert value.model_dump_json() == before


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"end_ns": START + 3500 * SECOND}, "interval_mismatch"),
        ({"open_ns": START + SECOND}, "unaligned_interval"),
        ({"received_ns": START + 3600 * SECOND + 4 * SECOND}, "receipt_lag"),
    ],
)
def test_width_alignment_and_lag_are_explicit(changes, expected):
    _, found = codes(batch(rows=(observation(**changes), observation(1))))
    assert expected in found


def test_freshness_uses_observation_time_not_a_recent_network_receipt():
    value = batch()
    result = inspect_batch(value, policy(), now_ns=START + 7200 * SECOND + 100 * SECOND)
    assert "stale" in {f.code for f in result.findings}
    assumed = batch(
        availability="assumed_close",
        rows=tuple(observation(i, available_ns=START + (i + 1) * 3600 * SECOND) for i in range(2)),
    )
    _, found = codes(assumed)
    assert "unverified_availability" in found
    result, _ = codes(assumed, allow_assumed_availability=True)
    assert result.valid
    _, found = codes(batch(), warmup_bars=3)
    assert "insufficient_warmup" in found


def test_receipt_only_metadata_cannot_invent_an_earlier_publication_time():
    value = batch(rows=(observation(0, available_ns=START + 3600 * SECOND), observation(1)))
    _, found = codes(value)
    assert "receipt_provenance_mismatch" in found
    _, found = codes(batch(availability="assumed_close"), allow_assumed_availability=True)
    assert "close_assumption_mismatch" in found


@pytest.mark.parametrize("asset", ["equity", "etf"])
def test_session_and_corporate_action_labels_cannot_claim_a_verified_stock_adapter(asset):
    market = identity(
        asset_class=asset, symbol="AAPL", venue="IEX", base_asset="AAPL", quote_currency="USD"
    )
    _, found = codes(
        batch(
            identity=market,
            session_calendar="verified_exchange_calendar",
            corporate_actions="adjusted_with_verified_actions",
        )
    )
    assert "calendar_adapter_not_verified" in found
    _, found = codes(
        batch(identity=market, session_calendar="unverified", corporate_actions="unverified")
    )
    assert {"unverified_sessions", "unverified_corporate_actions"} <= found


def test_perpetual_funding_and_reference_only_data_are_not_spot_liquidity():
    _, found = codes(batch(identity=identity(asset_class="crypto_perpetual"), funding="unverified"))
    assert "missing_funding" in found
    _, found = codes(
        batch(
            identity=identity(
                asset_class="forex",
                symbol="EURUSD",
                venue="ECB_REFERENCE",
                base_asset="EUR",
                quote_currency="USD",
                price_kind="reference_rate",
            )
        )
    )
    assert "reference_only" in found


@pytest.mark.parametrize("purpose", ["historical_research", "paper", "software_verification"])
def test_ecb_proof_cannot_be_selected_as_a_tradable_forex_feed(purpose):
    market = identity(
        asset_class="forex",
        symbol="EURUSD",
        venue="ECB_REFERENCE",
        base_asset="EUR",
        quote_currency="USD",
        interval_seconds=86400,
        price_kind="reference_rate",
    )
    request = DataRequest(identity=market, provider="ecb-reference-proof", purpose=purpose)
    value = decide(request)
    assert value.technical_match and not value.eligible
    with pytest.raises(ResearchError, match="blocked"):
        require_capability(request)
    proof = DataRequest(identity=market, provider="ecb-reference-proof", purpose="reference_proof")
    assert require_capability(proof).eligible


@pytest.mark.parametrize(
    "changes",
    [
        {"asset_class": "crypto_perpetual"},
        {"symbol": "BTCUSD", "quote_currency": "USD"},
        {"base_asset": "ETH"},
        {"quote_currency": "USD"},
        {"interval_seconds": 60},
        {"venue": "KRAKEN"},
        {"price_kind": "authored_fixture"},
    ],
)
def test_same_looking_symbols_cannot_transfer_market_or_currency_capabilities(changes):
    value = decide(
        DataRequest(
            identity=identity(**changes), purpose="historical_research", provider="binance-archive"
        )
    )
    assert not value.technical_match and not value.eligible


@pytest.mark.parametrize("provider", ["kraken-public", "alpaca-iex", "unknown-provider"])
def test_documentation_or_endpoint_access_does_not_implement_a_provider(provider):
    value = decide(DataRequest(identity=identity(), purpose="paper", provider=provider))
    assert (
        value.implementation == "not_implemented"
        and not value.technical_match
        and not value.eligible
    )
    assert value.blockers


def test_authored_fixture_capability_cannot_promote_software_evidence_to_a_market_feed():
    market = identity(price_kind="authored_fixture")
    assert decide(
        DataRequest(identity=market, purpose="software_verification", provider="authored-fixture")
    ).eligible
    for purpose in ("paper", "historical_research", "reference_proof"):
        assert not decide(
            DataRequest(identity=market, purpose=purpose, provider="authored-fixture")
        ).eligible


@pytest.mark.parametrize(
    "fields",
    [
        {"close": "1e2"},
        {"volume": "-1"},
        {"open": 100.0},
        {"low": "102"},
        {"open_ns": True},
        {"available_ns": 1.1},
        {"complete": "true"},
    ],
)
def test_numeric_and_timestamp_wire_types_are_strict(fields):
    with pytest.raises(ValueError):
        observation(**fields)


def test_capability_view_reports_bounded_purposes_without_claiming_a_paper_adapter():
    from stratosphere.commands import route_command
    from stratosphere.market_data.routing import capabilities_markdown

    for text, args in [
        ("/data", ()),
        ("/data binance-archive", ("binance-archive",)),
        ("/data authored-fixture paper", ("authored-fixture", "paper")),
    ]:
        value = route_command(text)
        assert value.action == "data" and value.args == args
    view = capabilities_markdown()
    assert "not implemented" in view and "fixture only" in view and "ECB_REFERENCE" in view
    assert "prior permission" in view.lower() and "IEX" in view
    paper = capabilities_markdown("authored-fixture", "paper")
    assert "Eligible for this bounded purpose: **no**" in paper
    with pytest.raises(ResearchError):
        capabilities_markdown("unknown")
