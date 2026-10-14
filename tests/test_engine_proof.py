import importlib.util
import json
import os
from dataclasses import replace
from decimal import ROUND_HALF_UP, Decimal
from fractions import Fraction
from pathlib import Path

import pytest

from stratosphere.phase0.contracts import DatasetManifest, ExperimentProtocol, fingerprint
from stratosphere.phase0.data_proof import EXAMPLES, normalize_ecb, observation_digest
from stratosphere.phase0.engine_proof import (
    example_inputs,
    run_engine,
    validate_bindings,
    validate_input_events,
)
from stratosphere.phase0.signals import SmaCross


def oracle(observations):
    """Batch calculation independent of the incremental engine strategy helper."""
    prices = [Fraction(row.rate) for row in observations]
    differences = {
        i: sum(prices[i - 2 : i + 1]) / 3 - sum(prices[i - 7 : i + 1]) / 8
        for i in range(7, len(prices))
    }
    held = False
    pending = None
    fills, signals = [], []
    for i, row in enumerate(observations):
        if pending is not None:
            side, signal_ns = pending
            fills.append(
                {
                    "side": side,
                    "signal_ns": signal_ns,
                    "fill_ns": row.available_ns,
                    "price": f"{Decimal(row.rate):.4f}",
                }
            )
            held = side == "BUY"
            pending = None
        if i - 1 not in differences:
            continue
        previous, current = differences[i - 1], differences[i]
        if not held and previous <= 0 < current:
            pending = ("BUY", row.available_ns)
        elif held and previous >= 0 > current:
            pending = ("SELL", row.available_ns)
        if pending:
            signals.append({"side": pending[0], "signal_ns": pending[1]})
    return signals, fills, held, pending


def test_incremental_signal_warmup_equality_and_future_independence():
    cross = SmaCross()
    assert [cross.push("1") for _ in range(8)] == [None] * 8
    assert cross.push("2") == "BUY"
    _, dataset, _, _ = example_inputs()
    rows = normalize_ecb((EXAMPLES / "ecb-reference.csv").read_bytes(), dataset)
    for boundary in (10, 25, 40):
        full, partial = SmaCross(), SmaCross()
        a = [full.push(row.rate) for row in rows]
        b = [partial.push(row.rate) for row in rows[:boundary]]
        assert a[:boundary] == b


def test_unsupported_rules_and_unbound_protocol_fail_before_engine():
    document, dataset, instrument, protocol = example_inputs()
    modified = document.model_dump(mode="json")
    modified["formal_spec"]["indicators"][0]["period"] = 4
    with pytest.raises(ValueError, match="supports only"):
        validate_bindings(type(document).model_validate(modified), dataset, instrument, protocol)
    data = protocol.model_dump(mode="json")
    data["dataset_manifest_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="references"):
        validate_bindings(document, dataset, instrument, ExperimentProtocol.model_validate(data))
    data = protocol.model_dump(mode="json")
    data["slippage_bps"] = "1"
    with pytest.raises(ValueError, match="spread/slippage"):
        validate_bindings(document, dataset, instrument, ExperimentProtocol.model_validate(data))


@pytest.fixture
def engine_inputs():
    if importlib.util.find_spec("nautilus_trader") is None:
        if os.environ.get("STRATOSPHERE_REQUIRE_ENGINE") == "1":
            pytest.fail("Engine is mandatory in this supported-host CI gate")
        pytest.skip("No engine wheel on this host; core checks do not verify engine behavior")
    document, dataset, instrument, protocol = example_inputs()
    rows = normalize_ecb((EXAMPLES / "ecb-reference.csv").read_bytes(), dataset)
    return rows, document, dataset, instrument, protocol


def test_forged_availability_and_changed_engine_inputs_are_rejected():
    document, dataset, instrument, _ = example_inputs()
    rows = normalize_ecb((EXAMPLES / "ecb-reference.csv").read_bytes(), dataset)
    altered = (replace(rows[0], available_ns=rows[0].available_ns - 10**9), *rows[1:])
    with pytest.raises(ValueError, match="availability"):
        validate_input_events(altered, dataset, document, instrument)
    with pytest.raises(ValueError, match="values"):
        validate_input_events(
            (replace(rows[0], rate="1.0000"), *rows[1:]), dataset, document, instrument
        )
    with pytest.raises(ValueError, match="dates"):
        validate_input_events(rows[1:], dataset, document, instrument)


@pytest.mark.engine
def test_real_engine_repeatability_oracle_timing_costs_and_balances(engine_inputs):
    rows, document, dataset, instrument, protocol = engine_inputs
    first = run_engine(*engine_inputs)
    second = run_engine(*engine_inputs)
    assert first == second
    signals, expected, held, pending = oracle(rows)
    assert first["signals"] == signals
    assert first["pending_signal"] == pending
    assert first["open_positions"] == int(held)
    assert len(first["fills"]) == len(expected) > 0
    quote, base = Decimal("10000"), Decimal(0)
    for native, calculated in zip(first["fills"], expected, strict=True):
        assert {key: native[key] for key in calculated} == calculated
        assert native["fill_ns"] > native["signal_ns"]
        assert Decimal(native["quantity"]) == 100
        assert native["commission_currency"] == "USD"
        notional = Decimal(native["price"]) * 100
        fee = (notional * Decimal("0.001")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        assert Decimal(native["commission"]) == fee
        quote += -notional - fee if native["side"] == "BUY" else notional - fee
        base += 100 if native["side"] == "BUY" else -100
    assert Decimal(first["balances"]["USD"]) == quote
    assert Decimal(first["balances"]["EUR"]) == base
    free = protocol.model_dump(mode="json")
    free["taker_fee_rate"] = "0"
    zero_fee = run_engine(
        rows, document, dataset, instrument, ExperimentProtocol.model_validate(free)
    )
    assert sum(Decimal(fill["commission"]) for fill in first["fills"]) > 0
    assert all(Decimal(fill["commission"]) == 0 for fill in zero_fee["fills"])
    assert Decimal(zero_fee["balances"]["USD"]) > quote
    Path("reports/engine").mkdir(parents=True, exist_ok=True)
    Path("reports/engine/repeatability.json").write_text(
        json.dumps(
            {
                "repeated_payloads_identical": True,
                "independent_oracle_matches": True,
                "timing_costs_balances_verified": True,
                "deterministic": first,
            },
            indent=2,
        )
        + "\n"
    )


@pytest.mark.engine
def test_prefix_is_causal_and_end_does_not_force_close(engine_inputs):
    rows, document, dataset, instrument, protocol = engine_inputs
    full = run_engine(*engine_inputs)
    first_buy = next(fill for fill in full["fills"] if fill["side"] == "BUY")
    boundary = next(i for i, row in enumerate(rows) if row.available_ns == first_buy["fill_ns"]) + 1
    prefix = rows[:boundary]
    manifest = dataset.model_dump(mode="json")
    manifest.update(
        expected_dates=[row.date for row in prefix],
        expected_rows=len(prefix),
        content_sha256=observation_digest(prefix),
    )
    subset = DatasetManifest.model_validate(manifest)
    config = protocol.model_dump(mode="json")
    config["dataset_manifest_sha256"] = fingerprint(subset)
    result = run_engine(
        prefix, document, subset, instrument, ExperimentProtocol.model_validate(config)
    )
    assert result["fills"] == [first_buy]
    assert result["open_positions"] == 1
    assert Decimal(result["balances"]["EUR"]) == 100
    assert len(result["signals"]) == 1
    assert result["fills"][0]["fill_ns"] > result["fills"][0]["signal_ns"]


@pytest.mark.engine
def test_native_high_precision_is_preserved(engine_inputs):
    from nautilus_trader.model.objects import Price, Quantity

    assert Price.from_str("1.0000000000000001").as_decimal() == Decimal("1.0000000000000001")
    assert Quantity.from_str("0.0000000000000001").as_decimal() == Decimal("0.0000000000000001")
