"""Dated context, exact-variant evidence attribution and conservative local candidates."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from test_bounded_studies import prepare
from test_bounded_studies import studies as studies

from stratosphere.catalog.contracts import AssessmentTarget, MarketContext
from stratosphere.catalog.service import AssessmentService, assessment_markdown
from stratosphere.registry.workspace import RegistryError, WorkspaceDatabase
from stratosphere.research.service import ResearchService
from stratosphere.research.store import ResearchError, ResearchStore

NOW = datetime(2026, 10, 18, 12, tzinfo=UTC)


def target(**changes):
    return AssessmentTarget(
        asset_class="crypto_spot", symbol="BTCUSDT", timeframe_seconds=3600, **changes
    )


def assessments(studies, tmp_path, **kwargs):
    research = ResearchService(ResearchStore(tmp_path / "jobs"), online_check=studies.online_check)
    return AssessmentService(studies.database, research, studies, clock=lambda: NOW, **kwargs)


async def completed_study(studies):
    job, protocol = await prepare(studies)
    studies.approve(job, protocol.digest)
    return job, await studies.run(job)


async def test_empty_evidence_is_dated_unknown_and_never_a_validation_badge(studies, tmp_path):
    service = assessments(studies, tmp_path)
    identity = studies.test_parent["public_id"]
    record = await service.assess(identity, target())
    value = record["assessment"]
    assert value["assessed_at"] == "2026-10-18T12:00:00Z"
    assert all(value["compatibility"].values())
    assert value["implementation_evidence"] == "not_verified" and value["evidence"] == []
    assert value["current_context_status"] == "unavailable"
    assert value["conclusion"] == "research_candidate_requires_verification"
    assert value["promotion"] == "none"
    assert any("commercial" in item for item in value["missing_verification"])
    assert await service.open(record["id"]) == record
    assert "historical assessment snapshot" in assessment_markdown(record)


async def test_selected_variant_holdout_never_transfers_to_parent_or_sibling(studies, tmp_path):
    _, report = await completed_study(studies)
    service = assessments(studies, tmp_path)
    first, selected = report["attempts"]
    assert report["selection"]["selected_index"] == 1
    parent = (await service.assess(first["strategy_id"], target()))["assessment"]
    child = (await service.assess(selected["strategy_id"], target()))["assessment"]
    assert parent["evidence"][0]["final_holdout"] == "not_evaluated"
    assert "holdout_1" not in parent["evidence"][0]["metrics"]
    assert child["evidence"][0]["final_holdout"] == "completed_for_exact_variant"
    assert child["evidence"][0]["metrics"]["holdout_1"]["net_pnl"] == "-100.00000000"
    assert parent["attempted_variants"] == child["attempted_variants"] == 2
    assert parent["conclusion"] == child["conclusion"]
    assert all(e["strategy_id"] == selected["strategy_id"] for e in child["evidence"])
    assert child["evidence"][0]["data_representativeness"] == "not_reverified_for_this_assessment"


@pytest.mark.parametrize(
    "target_values",
    [
        {"asset_class": "crypto_perpetual", "symbol": "BTCUSDT", "timeframe_seconds": 3600},
        {"asset_class": "equity", "symbol": "AAPL", "timeframe_seconds": 3600},
        {"asset_class": "crypto_spot", "symbol": "ETHUSDT", "timeframe_seconds": 3600},
        {"asset_class": "crypto_spot", "symbol": "BTCUSDT", "timeframe_seconds": 300},
    ],
)
async def test_market_instrument_and_timeframe_mismatches_do_not_inherit_metrics(
    studies, tmp_path, target_values
):
    await completed_study(studies)
    service = assessments(studies, tmp_path)
    result = (
        await service.assess(studies.test_parent["public_id"], AssessmentTarget(**target_values))
    )["assessment"]
    assert result["conclusion"] == "incompatible_rules" and not all(
        result["compatibility"].values()
    )
    assert result["evidence"] == [] and result["attempted_variants"] == 0
    assert result["promotion"] == "none"


@pytest.mark.parametrize(
    "case,expected",
    [
        ("fresh", "current_snapshot"),
        ("stale", "stale"),
        ("future", "future_dated"),
        ("availability_order", "invalid_availability_order"),
        ("user_notes", "user_supplied_unverified"),
        ("wrong_target", "mismatched_target"),
        ("provider_error", "unavailable"),
    ],
)
async def test_current_context_provenance_time_and_provider_failure(
    studies, tmp_path, case, expected
):
    async def provider(request):
        if case == "provider_error":
            raise RuntimeError("provider body with sentinel-key must stay private")
        if case == "user_notes":
            return MarketContext(
                target=request, origin="user_supplied", observations=("Source claims bullish",)
            )
        date = NOW - timedelta(seconds=3601 if case == "stale" else 60)
        if case == "future":
            date = NOW + timedelta(seconds=1)
        available = date - timedelta(seconds=1) if case == "availability_order" else date
        chosen = (
            AssessmentTarget(asset_class="equity", symbol="AAPL", timeframe_seconds=3600)
            if case == "wrong_target"
            else request
        )
        return MarketContext(
            target=chosen,
            origin="provider_snapshot",
            as_of=date,
            available_at=available,
            source_refs=("https://authored-provider.invalid/snapshot",),
            snapshot_sha256="0" * 64,
            observations=("Authored test snapshot, not market data",),
        )

    service = assessments(studies, tmp_path, context_provider=provider)
    record = await service.assess(studies.test_parent["public_id"], target())
    assert record["assessment"]["current_context_status"] == expected
    assert record["assessment"]["promotion"] == "none"
    assert "sentinel-key" not in json.dumps(record)
    assert any("regime" in x for x in record["assessment"]["missing_verification"]) == (
        case == "fresh"
    )


async def test_assessments_are_immutable_restartable_and_identity_bound(studies, tmp_path):
    service = assessments(studies, tmp_path)
    parent = studies.test_parent["public_id"]
    first = await service.assess(parent, target())
    assert (await service.assess(parent, target()))["id"] == first["id"]
    assert len(studies.database.assessments(parent)) == 1
    restarted = WorkspaceDatabase(studies.database.path)
    assert restarted.assessment(first["id"]) == first
    with pytest.raises(RegistryError):
        with restarted.connect() as db:
            db.execute("UPDATE assessments SET assessed_at='tomorrow'")
    with pytest.raises(RegistryError):
        with restarted.connect() as db:
            db.execute("DELETE FROM assessments")
    with pytest.raises(RegistryError):
        restarted.record_assessment(parent, first["assessment"] | {"strategy_sha256": "0" * 64})


async def test_corrupted_report_cannot_become_applicability_evidence(studies, tmp_path):
    job, _ = await completed_study(studies)
    (studies.store.directory(job) / "report.json").write_text("{}")
    service = assessments(studies, tmp_path)
    with pytest.raises(ResearchError, match="integrity"):
        await service.assess(studies.test_parent["public_id"], target())
    assert not studies.database.assessments()


async def test_drawdown_preference_and_overnight_conflicts_do_not_transfer_between_variants(
    studies, tmp_path
):
    _, report = await completed_study(studies)
    service = assessments(studies, tmp_path)
    baseline, selected = report["attempts"]
    for candidate, should_breach in ((baseline, False), (selected, True)):
        record = await service.assess(candidate["strategy_id"], target(max_drawdown_pct="0.5"))
        assert (
            any("drawdown exceeds" in m for m in record["assessment"]["missing_verification"])
            == should_breach
        )
        assert "Drawdown preference: 0.5%" in assessment_markdown(record)
        assert record["assessment"]["promotion"] == "none"
    blocked = await service.assess(baseline["strategy_id"], target(allow_overnight=False))
    assert not blocked["assessment"]["compatibility"]["holding_preference"]
    assert blocked["assessment"]["conclusion"] == "incompatible_rules"


async def test_assessment_timestamp_follows_context_retrieval(studies, tmp_path):
    state = {"retrieved": False}

    async def provider(request):
        state["retrieved"] = True
        return MarketContext(
            target=request,
            origin="provider_snapshot",
            as_of=NOW + timedelta(seconds=2),
            available_at=NOW + timedelta(seconds=2),
            snapshot_sha256="0" * 64,
            source_refs=("https://authored.invalid/context",),
        )

    service = assessments(studies, tmp_path, context_provider=provider)
    service.clock = lambda: NOW + timedelta(seconds=3) if state["retrieved"] else NOW
    record = await service.assess(studies.test_parent["public_id"], target())
    assert record["assessment"]["current_context_status"] == "current_snapshot"
    assert record["assessed_at"] == "2026-10-18T12:00:03Z"
