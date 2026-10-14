import copy
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from stratosphere.phase0 import contracts
from stratosphere.phase0.contracts import (
    DatasetManifest,
    ExperimentProtocol,
    InstrumentManifest,
    StrategyDocument,
    StrategySpec,
    canonical_bytes,
    decimal_text,
    fingerprint,
    load_contract,
    new_public_id,
    same_behavior,
)

EXAMPLES = Path(__file__).resolve().parents[1] / "examples/phase0"


@pytest.fixture
def document():
    return load_contract(EXAMPLES / "strategy.json", StrategyDocument)


def test_examples_and_published_schemas_match(document):
    dataset = load_contract(EXAMPLES / "dataset.json", DatasetManifest)
    instrument = load_contract(EXAMPLES / "instrument.json", InstrumentManifest)
    protocol = load_contract(EXAMPLES / "experiment.json", ExperimentProtocol)
    assert protocol.strategy_sha256 == fingerprint(document.formal_spec)
    assert protocol.dataset_manifest_sha256 == fingerprint(dataset)
    assert protocol.instrument_manifest_sha256 == fingerprint(instrument)
    for name, cls in (
        ("strategy", StrategyDocument),
        ("dataset", DatasetManifest),
        ("instrument", InstrumentManifest),
        ("experiment", ExperimentProtocol),
    ):
        schema = EXAMPLES.parents[1] / "schemas" / f"{name}-v1.json"
        assert json.loads(schema.read_text()) == cls.model_json_schema()


def test_metadata_ordering_and_decimal_spelling_do_not_change_identity(document):
    data = document.model_dump(mode="json")
    data.update(name="Another name", description="Another description", source_refs=["new-source"])
    data["formal_spec"]["position_sizing"]["amount"] = "100.00000"
    data["formal_spec"] = dict(reversed(list(data["formal_spec"].items())))
    other = StrategyDocument.model_validate(data)
    assert same_behavior(document.formal_spec, other.formal_spec)


@pytest.mark.parametrize(
    "group",
    [
        "indicators",
        "entry_rules",
        "exit_rules",
        "position_sizing",
        "signal_timing",
        "timeframes",
        "market_conditions",
        "instrument_constraints",
        "data_requirements",
        "execution_requirements",
        "missing_data_policy",
        "assumptions",
    ],
)
def test_every_behavior_group_participates_in_identity(document, group):
    data = document.formal_spec.model_dump(mode="json")
    match group:
        case "indicators":
            data[group][0]["period"] = 4
        case "entry_rules" | "exit_rules":
            data[group][0]["condition"]["op"] = "gt"
        case "position_sizing":
            data[group]["amount"] = "100.00000000000000000000000000000000001"
        case "signal_timing":
            data[group]["submit"] = "after_signal"
        case "timeframes":
            data[group].append(3600)
        case "market_conditions":
            data[group].append({"op": "boolean", "value": True})
        case "instrument_constraints":
            data[group]["fixed_symbols"] = ["GBPUSD"]
        case "data_requirements":
            data[group]["warmup_bars"] = 10
        case "execution_requirements":
            data[group]["session_end"] = "close"
        case "missing_data_policy":
            data[group]["max_staleness_seconds"] = 259200
        case "assumptions":
            data[group].append("Additional interpretation.")
    variant = StrategySpec.model_validate(data)
    assert not same_behavior(document.formal_spec, variant)


@pytest.mark.parametrize(
    "corruption",
    [
        "missing_group",
        "extra_group",
        "unknown_reference",
        "bad_type",
        "numeric_rule",
        "bad_arity",
        "duplicate_rule",
        "unresolved",
        "short_without_capability",
        "insufficient_warmup",
    ],
)
def test_reject_ambiguous_or_inconsistent_specs(document, corruption):
    data = copy.deepcopy(document.formal_spec.model_dump(mode="json"))
    match corruption:
        case "missing_group":
            del data["missing_data_policy"]
        case "extra_group":
            data["stop_loss"] = "ask later"
        case "unknown_reference":
            data["entry_rules"][0]["condition"]["args"][0]["value"] = "missing"
        case "bad_type":
            data["indicators"][0]["period"] = "3"
        case "numeric_rule":
            data["entry_rules"][0]["condition"] = {"op": "decimal", "value": "1"}
        case "bad_arity":
            data["exit_rules"][0]["condition"]["args"] = []
        case "duplicate_rule":
            data["entry_rules"].append(data["entry_rules"][0])
        case "unresolved":
            data["position_sizing"]["amount"] = "unknown"
        case "short_without_capability":
            data["entry_rules"][0]["action"] = "open_short"
        case "insufficient_warmup":
            data["data_requirements"]["warmup_bars"] = 8
    with pytest.raises(ValidationError):
        StrategySpec.model_validate(data)


def test_no_rounding_mutation_or_hash_only_equality(document, monkeypatch):
    value = "123456789012345678901234567890.00000000000000000000000100"
    assert decimal_text(value) == value[:-2]
    assert decimal_text("-0.000") == "0"
    with pytest.raises(ValidationError):
        document.formal_spec.position_sizing.amount = "5"
    data = document.formal_spec.model_dump(mode="json")
    data["position_sizing"]["amount"] = "101"
    other = StrategySpec.model_validate(data)
    monkeypatch.setattr(contracts, "fingerprint", lambda _: "0" * 64)
    with pytest.raises(ValueError, match="collision"):
        same_behavior(document.formal_spec, other)
    assert canonical_bytes(document.formal_spec) != canonical_bytes(other)


def test_duplicate_keys_rejected_and_numeric_ids_preserve_zeroes(tmp_path, monkeypatch):
    path = tmp_path / "duplicate.json"
    path.write_text('{"name":"one", "name":"two"}')
    with pytest.raises(ValueError, match="Duplicate JSON"):
        load_contract(path, StrategyDocument)
    monkeypatch.setattr(contracts.secrets, "randbelow", lambda _: 7)
    assert new_public_id() == "000000000000000000000007"


@pytest.mark.parametrize("value", [1.5, True, "NaN", "Infinity", "1e-3", "01", ""])
def test_unsafe_decimal_representations_rejected(value):
    with pytest.raises(ValueError):
        decimal_text(value)
