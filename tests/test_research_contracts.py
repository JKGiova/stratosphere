import json
from decimal import Decimal

import pytest
from pydantic import ValidationError

from stratosphere.models.streaming import ModelError
from stratosphere.phase0.contracts import fingerprint
from stratosphere.research.contracts import Extraction, ResearchConfig, ResearchPlan, build_spec
from stratosphere.research.extraction import extract_strategy, parse_extraction


def test_complete_spec_identity_changes_with_behavior_not_explanatory_name(
    research_draft, research_source
):
    research_draft.validate_source(research_source)
    spec = build_spec(research_draft)
    changed = research_draft.model_dump()
    changed.update(fast_period=4, name="Different name")
    alternate = build_spec(Extraction.model_validate(changed))
    assert fingerprint(spec) != fingerprint(alternate)
    changed["fast_period"] = 3
    assert fingerprint(spec) == fingerprint(build_spec(Extraction.model_validate(changed)))
    assert spec.signal_timing.submit == "next_bar_open"
    assert spec.execution_requirements.session_end == "hold"
    assert spec.data_requirements.warmup_bars == 9
    assert not spec.execution_requirements.reinforcement


@pytest.mark.parametrize(
    "change",
    [
        {"fast_period": 8},
        {"slow_period": 744},
        {"symbol": "ETHUSDT"},
        {"quantity": "0"},
        {"quantity": "0.000000001"},
        {"timeframe_seconds": 60},
        {"exit": None},
        {"evidence": []},
        {"questions": ["Unanswered behavior"]},
    ],
)
def test_incomplete_or_incompatible_rules_cannot_be_executed(
    research_draft, research_source, change
):
    payload = research_draft.model_dump(mode="json") | change
    with pytest.raises((ValueError, ValidationError)):
        Extraction.model_validate(payload).validate_source(research_source)


def test_quotes_must_exist_and_unknown_fields_and_duplicate_keys_are_rejected(
    research_draft, research_source
):
    payload = research_draft.model_dump(mode="json")
    payload["evidence"][0]["quote"] = "An invented quotation"
    with pytest.raises(ModelError):
        parse_extraction(json.dumps(payload), research_source)
    with pytest.raises(ModelError):
        parse_extraction('{"status":"ready","status":"unsupported"}', research_source)
    payload = research_draft.model_dump(mode="json") | {"code": "import os"}
    with pytest.raises(ModelError):
        parse_extraction(json.dumps(payload), research_source)


@pytest.mark.asyncio
async def test_one_hosted_extraction_call_preserves_exact_fields_without_chat_history(
    research_provider,
    research_source,
    research_draft,
):
    draft = await extract_strategy(research_provider, research_source)
    assert draft == research_draft
    assert len(research_provider.requests) == 1
    request = research_provider.requests[0]
    assert request[1]["content"] == research_source
    assert len(request) == 2 and "untrusted" in request[0]["content"]


def test_questions_and_unsupported_results_do_not_become_plans(research_draft, research_source):
    payload = research_draft.model_dump(mode="json")
    payload.update(
        status="needs_clarification", quantity=None, questions=["What fixed BTC quantity?"]
    )
    draft = parse_extraction(json.dumps(payload), research_source)
    with pytest.raises(ValueError):
        build_spec(draft)
    payload.update(
        status="unsupported", questions=["RSI stops are not supported by this implementation."]
    )
    assert parse_extraction(json.dumps(payload), research_source).status == "unsupported"


@pytest.mark.parametrize(
    "change",
    [
        {"initial_balance": "0"},
        {"initial_balance": "1.001"},
        {"fee_rate": "0.5"},
        {"slippage_bps": "-1"},
        {"spread_bps": "10000"},
        {"fee_rate": "NaN"},
    ],
)
def test_invalid_cost_profiles_are_rejected(change):
    with pytest.raises(ValueError):
        ResearchConfig(**change)


def test_plan_tampering_and_wrong_snapshot_are_rejected(research_plan):
    payload = research_plan.model_dump(mode="json")
    payload["specification"]["execution_requirements"]["shorting"] = True
    with pytest.raises(ValueError, match="Specification"):
        ResearchPlan.model_validate(payload)
    payload = research_plan.model_dump(mode="json")
    payload["archive_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="snapshot"):
        ResearchPlan.model_validate(payload)
    assert Decimal(research_plan.config.fee_rate) == Decimal("0.001")


def test_model_cannot_invent_a_numeric_parameter_with_an_unrelated_quote(
    research_draft,
    research_source,
):
    payload = research_draft.model_dump(mode="json")
    payload["fast_period"] = 4
    with pytest.raises(ModelError):
        parse_extraction(json.dumps(payload), research_source)
