"""Authored persistent closed-bar controls; no provider, clock attestation or delivery claim."""

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from fractions import Fraction

import pytest
from pydantic import ValidationError
from test_research_engine import native_plan as native_plan

from stratosphere.paper import runtime as module
from stratosphere.paper.ledger import Account, Instance, PaperError, PaperLedger
from stratosphere.paper.runtime import (
    HOUR_NS,
    ClosedBarMandate,
    ClosedBarRuntime,
    ClosedObservation,
    implementation_digest,
)
from stratosphere.phase0.contracts import fingerprint
from stratosphere.registry.workspace import WorkspaceDatabase


@pytest.fixture
def stream(tmp_path, research_plan, market_bars):
    registry = WorkspaceDatabase(tmp_path / "workspace.sqlite3")
    strategy = registry.register(research_plan.specification, "Authored stream", {"fixture": True})
    account = Account(account_id="local-paper-" + "a" * 32, starting_cash="1000")
    ledger = PaperLedger(tmp_path / "paper" / "account.sqlite3", account)
    instance = Instance(
        instance_id="b" * 32,
        strategy_id=strategy["public_id"],
        strategy_sha256=strategy["spec_sha256"],
        implementation_sha256=implementation_digest(),
        account_id=account.account_id,
        capital="500",
        max_order_notional="100",
        max_base_quantity="1",
        max_quote_age_ns=10**9,
    )
    ledger.add_instance(instance, registry)
    mandate = ClosedBarMandate(
        instance=instance,
        specification=research_plan.specification,
        first_open_ns=market_bars[0].open_ns,
        max_publication_delay_ns=10**9,
        max_receipt_age_ns=10**9,
    )
    runtime = ClosedBarRuntime(ledger, registry)
    return runtime, mandate, account, registry


def observation(bar, *, event=None, **changes):
    return ClosedObservation(
        observation_id=event or fingerprint(bar),
        bar=bar,
        available_ns=bar.open_ns + HOUR_NS,
        **changes,
    )


def approve(stream):
    runtime, mandate, *_ = stream
    runtime.prepare(mandate)
    runtime.approve(mandate.instance.instance_id, mandate.digest)


def push(stream, bar, **changes):
    runtime, mandate, *_ = stream
    value = observation(bar, **changes)
    return runtime.ingest(mandate.instance.instance_id, value, received_ns=value.available_ns)


def test_exact_review_required_no_orders_or_reallocation(stream, market_bars):
    runtime, mandate, *_ = stream
    before = runtime.ledger.snapshot()
    with pytest.raises(PaperError, match="Prepare"):
        push(stream, market_bars[0])
    assert runtime.prepare(mandate) == mandate.digest == runtime.prepare(mandate)
    with pytest.raises(PaperError, match="not been approved"):
        push(stream, market_bars[0])
    with pytest.raises(PaperError, match="exactly"):
        runtime.approve(mandate.instance.instance_id, "f" * 64)
    runtime.approve(mandate.instance.instance_id, mandate.digest)
    with pytest.raises(PaperError, match="exactly"):
        runtime.approve(mandate.instance.instance_id, mandate.digest)
    assert push(stream, market_bars[0]) == {"status": "accepted", "signal": None}
    after = runtime.ledger.snapshot()
    for name in ("account", "cash", "instances", "decisions", "orders", "fills", "reserved_cash"):
        assert before[name] == after[name]


def test_changed_mandate_implementation_definition_and_allocation_are_rejected(
    stream,
    market_bars,
    monkeypatch,
):
    runtime, mandate, *_ = stream
    runtime.prepare(mandate)
    with pytest.raises(PaperError, match="cannot change"):
        runtime.prepare(mandate.model_copy(update={"max_receipt_age_ns": 2 * 10**9}))
    with pytest.raises(PaperError, match="allocation"):
        runtime.prepare(
            mandate.model_copy(
                update={
                    "instance": mandate.instance.model_copy(update={"instance_id": "e" * 32}),
                }
            )
        )
    monkeypatch.setattr(module, "implementation_digest", lambda: "f" * 64)
    with pytest.raises(PaperError, match="implementation changed"):
        runtime.approve(mandate.instance.instance_id, mandate.digest)
    # Historical evidence remains inspectable after a code change.
    assert not runtime.inspect(mandate.instance.instance_id)["approved"]


@pytest.mark.parametrize(
    "change",
    [
        {"feed_profile": "liquid"},
        {"gap_policy": "backfill"},
        {"outputs": "orders"},
        {"first_open_ns": True},
        {"first_open_ns": 1},
        {"max_receipt_age_ns": 0},
        {"max_publication_delay_ns": HOUR_NS},
        {"instance": None},
    ],
)
def test_unsupported_mandates_fail_closed(stream, change):
    _, mandate, *_ = stream
    with pytest.raises(ValidationError):
        ClosedBarMandate.model_validate(mandate.model_dump() | change)


@pytest.mark.parametrize(
    "change",
    [
        {"evidence": "provider_verified"},
        {"feed_profile": "kraken-public"},
        {"available_ns": True},
        {"available_ns": 0},
    ],
)
def test_no_provider_evidence_or_invalid_timestamps(market_bars, change):
    value = observation(market_bars[0]).model_dump()
    with pytest.raises(ValidationError):
        ClosedObservation.model_validate(value | change)


@pytest.mark.parametrize(
    "case,reason",
    [
        ("gap", "gap_or_out_of_order"),
        ("unclosed", "unclosed_or_future_observation"),
        ("future", "unclosed_or_future_observation"),
        ("delayed", "publication_delay_exceeded"),
        ("stale", "stale_observation"),
    ],
)
def test_invalid_observations_are_retained_and_halt_across_restart(
    stream, market_bars, case, reason
):
    runtime, mandate, account, registry = stream
    approve(stream)
    value = observation(market_bars[1] if case == "gap" else market_bars[0])
    received = value.available_ns
    if case == "unclosed":
        value = value.model_copy(update={"available_ns": value.available_ns - 1})
    elif case == "future":
        received -= 1
    elif case == "delayed":
        received += mandate.max_publication_delay_ns + 1
        value = value.model_copy(update={"available_ns": received})
    elif case == "stale":
        received += mandate.max_receipt_age_ns + 1
    result = runtime.ingest(mandate.instance.instance_id, value, received_ns=received)
    assert result == {"status": "halted", "reason": reason, "signal": None}
    other = ClosedBarRuntime(PaperLedger(runtime.ledger.path, account), registry)
    state = other.inspect(mandate.instance.instance_id)
    assert state["accepted_count"] == 0 and not state["signals"]
    assert state["faults"][0]["observation"] == value.model_dump()
    assert (
        other.ingest(
            mandate.instance.instance_id,
            observation(market_bars[0]),
            received_ns=market_bars[0].open_ns + HOUR_NS,
        )["reason"]
        == "runtime_halted"
    )
    assert len(other.inspect(mandate.instance.instance_id)["faults"]) == 2


def test_duplicate_delivery_and_new_ids_do_not_emit_again_and_conflicts_are_retained(
    stream, market_bars
):
    runtime, mandate, *_ = stream
    approve(stream)
    for bar in market_bars[:18]:
        push(stream, bar)
    before = runtime.inspect(mandate.instance.instance_id)
    signaled = next(s for s in before["signals"] if s["side"] == "BUY")
    bar = next(b for b in market_bars if b.close_ns == signaled["signal_ns"])
    assert push(stream, bar) == {"status": "duplicate", "signal": None}
    assert push(stream, bar, event="e" * 64) == {"status": "duplicate", "signal": None}
    after = runtime.inspect(mandate.instance.instance_id)
    assert after["signals"] == before["signals"] and after["accepted_count"] == 18
    # A newly seen delivery alias is retained, so it cannot later identify another candle.
    result = push(stream, market_bars[18], event="e" * 64)
    assert result["reason"] == "conflicting_observation_id"
    assert (
        runtime.inspect(mandate.instance.instance_id)["faults"][-1]["observation"]["bar"]
        == market_bars[18].model_dump()
    )


def test_conflicting_revision_does_not_replace_original_close(stream, market_bars):
    runtime, mandate, *_ = stream
    approve(stream)
    first = market_bars[0]
    push(stream, first)
    revised = first.model_copy(update={"close": "100.5"})
    assert push(stream, revised)["reason"] == "conflicting_closed_candle"
    state = runtime.inspect(mandate.instance.instance_id)
    assert state["window"] == ["100"] and len(state["observations"]) == 1
    assert state["faults"][0]["observation"]["bar"]["close"] == "100.5"


def batch_crossings(bars, fast=3, slow=8):
    closes = [Fraction(b.close) for b in bars]
    differences = [
        sum(closes[i - fast + 1 : i + 1]) / fast - sum(closes[i - slow + 1 : i + 1]) / slow
        for i in range(slow - 1, len(closes))
    ]
    result = []
    for i, (previous, current) in enumerate(
        zip(differences[:-1], differences[1:], strict=True), slow
    ):
        side = "BUY" if previous <= 0 < current else "SELL" if previous >= 0 > current else None
        if side:
            result.append({"side": side, "signal_ns": bars[i].close_ns})
    return result


def test_multiday_reconstruction_and_signal_identity_match_independent_batch_oracle(
    stream, market_bars
):
    runtime, mandate, account, registry = stream
    approve(stream)
    bars = market_bars[:96]  # Four authored days, not a real-time soak.
    for index, bar in enumerate(bars):
        result = push((runtime, mandate, account, registry), bar)
        assert result["status"] == "accepted"
        if index in {6, 7, 8, 24, 48, 71}:
            old = runtime.inspect(mandate.instance.instance_id)
            runtime = ClosedBarRuntime(PaperLedger(runtime.ledger.path, account), registry)
            assert runtime.inspect(mandate.instance.instance_id) == old
    state = runtime.inspect(mandate.instance.instance_id)
    assert [{k: s[k] for k in ("side", "signal_ns")} for s in state["signals"]] == batch_crossings(
        bars
    )
    assert len({s["signal_sha256"] for s in state["signals"]}) == len(state["signals"]) > 10
    for signal in state["signals"]:
        assert signal["received_ns"] >= signal["available_ns"] > signal["signal_ns"]
        assert signal["nominal_next_open_ns"] == signal["signal_ns"] + 1000
        assert signal["kind"] == "raw_crossing_not_order_permission"
    # Fresh interpreter, not merely a new object or a surviving kernel instance.
    code = """import json,sys
from pathlib import Path
from stratosphere.paper.ledger import Account,PaperLedger
from stratosphere.paper.runtime import ClosedBarRuntime,ClosedObservation
from stratosphere.registry.workspace import WorkspaceDatabase
s=json.load(sys.stdin)
r=ClosedBarRuntime(PaperLedger(Path(s['ledger']),Account.model_validate(s['account'])),WorkspaceDatabase(Path(s['registry'])))
before=r.inspect(s['instance'])
o=ClosedObservation.model_validate(s['next'])
result=r.ingest(s['instance'],o,received_ns=o.available_ns)
print(json.dumps({'before':before,'result':result,'retained':r.inspect(s['instance'])}))
"""
    loaded = subprocess.check_output(
        [sys.executable, "-c", code],
        input=json.dumps(
            {
                "ledger": str(runtime.ledger.path),
                "account": account.model_dump(),
                "registry": str(registry.path),
                "instance": mandate.instance.instance_id,
                "next": observation(market_bars[96]).model_dump(),
            }
        ),
        text=True,
        env={"PATH": os.defpath, "PYTHONNOUSERSITE": "1", "PYTHONUTF8": "1"},
    )
    child = json.loads(loaded)
    assert child["before"] == state
    assert child["result"]["status"] == "accepted"
    assert child["retained"] == runtime.inspect(mandate.instance.instance_id)
    assert child["retained"]["accepted_count"] == 97
    assert [
        {k: s[k] for k in ("side", "signal_ns")} for s in child["retained"]["signals"]
    ] == batch_crossings(market_bars[:97])


def test_concurrent_duplicate_receipts_accept_only_one_bar(stream, market_bars):
    runtime, mandate, account, registry = stream
    approve(stream)
    other = ClosedBarRuntime(PaperLedger(runtime.ledger.path, account), registry)
    value = observation(market_bars[0])
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda r: r.ingest(
                    mandate.instance.instance_id, value, received_ns=value.available_ns
                ),
                [runtime, other],
            )
        )
    assert sorted(r["status"] for r in results) == ["accepted", "duplicate"]
    assert runtime.inspect(mandate.instance.instance_id)["accepted_count"] == 1
    assert not runtime.ledger.snapshot()["orders"]


def test_approved_runtime_rechecks_actual_code_before_ingestion(stream, market_bars, monkeypatch):
    runtime, mandate, *_ = stream
    approve(stream)
    monkeypatch.setattr(module, "implementation_digest", lambda: "0" * 64)
    before = runtime.ledger.snapshot()
    with pytest.raises(PaperError, match="implementation changed"):
        push(stream, market_bars[0])
    assert runtime.ledger.snapshot() == before


def test_gap_halt_preserves_existing_order_reservations_and_positions(stream, market_bars):
    from stratosphere.paper.ledger import Intent

    runtime, mandate, *_ = stream
    approve(stream)
    order = Intent(
        intent_id="d" * 32,
        instance_id=mandate.instance.instance_id,
        side="BUY",
        quantity="0.01",
        price_bound="100",
        fee_allowance="0",
        created_ns=100,
        expires_ns=120,
        quote_ns=99,
        signal_sha256="e" * 64,
        proposal_sha256="f" * 64,
    )
    assert runtime.ledger.reserve(order, now_ns=100) is None
    before = runtime.ledger.snapshot()
    assert push(stream, market_bars[1])["reason"] == "gap_or_out_of_order"
    after = runtime.ledger.snapshot()
    for key in ("cash", "instances", "orders", "decisions", "reserved_cash", "fills"):
        assert after[key] == before[key]
    assert after["reserved_cash"] == "1"


def test_clock_regression_and_explicit_out_of_order_are_not_sorted(stream, market_bars):
    runtime, mandate, *_ = stream
    approve(stream)
    push(stream, market_bars[0])
    value = observation(market_bars[1])
    assert (
        runtime.ingest(
            mandate.instance.instance_id, value, received_ns=market_bars[0].open_ns + HOUR_NS - 1
        )["reason"]
        == "receipt_clock_regression"
    )
    assert runtime.inspect(mandate.instance.instance_id)["accepted_count"] == 1


def test_append_failure_rolls_back_candle_and_signal_together(stream, market_bars, monkeypatch):
    runtime, mandate, *_ = stream
    approve(stream)
    for bar in market_bars[:11]:
        push(stream, bar)
    before = runtime.ledger.snapshot()
    append = runtime.ledger._append

    def crash(db, chain, event):
        append(db, chain, event)
        raise RuntimeError("Authored crash before transaction commit")

    with monkeypatch.context() as patch:
        patch.setattr(runtime.ledger, "_append", crash)
        with pytest.raises(RuntimeError, match="before transaction commit"):
            push(stream, market_bars[11])
    assert runtime.ledger.snapshot() == before
    assert push(stream, market_bars[11])["signal"]["side"] == "BUY"
    assert len(runtime.inspect(mandate.instance.instance_id)["signals"]) == 1


def test_registered_behavior_change_cannot_retarget_the_runtime(stream, market_bars, monkeypatch):
    from stratosphere.research.contracts import SmaParameters, compile_sma_spec

    runtime, mandate, *_ = stream
    approve(stream)
    other = compile_sma_spec(SmaParameters(fast_period=4, slow_period=10, quantity="0.01"))
    original = runtime.registry.strategy
    monkeypatch.setattr(
        runtime.registry,
        "strategy",
        lambda identity: (
            original(identity)
            | {
                "specification": other.model_dump(mode="json"),
                "spec_sha256": fingerprint(other),
            }
        ),
    )
    before = runtime.ledger.snapshot()
    with pytest.raises(PaperError, match="registered definition"):
        push(stream, market_bars[0])
    assert runtime.ledger.snapshot() == before


def test_journal_version_and_damaged_stream_are_not_silently_recreated(stream, market_bars):
    import sqlite3

    runtime, mandate, account, registry = stream
    with sqlite3.connect(runtime.ledger.path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
    approve(stream)
    push(stream, market_bars[0])
    with sqlite3.connect(runtime.ledger.path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 2
        db.execute("DROP TRIGGER retain_update")  # Corrupt only this temporary fixture.
        db.execute("UPDATE events SET payload='{}' WHERE seq=(SELECT max(seq) FROM events)")
    damaged = runtime.ledger.path.read_bytes()
    with pytest.raises(PaperError, match="integrity"):
        PaperLedger(runtime.ledger.path, account)
    assert runtime.ledger.path.read_bytes() == damaged


@pytest.mark.engine
def test_restarted_raw_crossings_match_actual_native_signal_timing(
    stream, native_plan, market_bars
):
    from stratosphere.research.engine import run_simulation

    runtime, mandate, account, registry = stream
    approve(stream)
    bars = market_bars[:72]
    for index, bar in enumerate(bars):
        if index in {7, 11, 12, 31, 48}:
            runtime = ClosedBarRuntime(PaperLedger(runtime.ledger.path, account), registry)
        push((runtime, mandate, account, registry), bar)
    raw = runtime.inspect(mandate.instance.instance_id)["signals"]
    held = False
    filtered = []
    for signal in raw:
        if (signal["side"] == "BUY" and not held) or (signal["side"] == "SELL" and held):
            filtered.append({"side": signal["side"], "signal_ns": signal["signal_ns"]})
            held = signal["side"] == "BUY"
    native = run_simulation(native_plan, bars)
    assert native["signals"] == filtered and len(filtered) > 8
    assert all(f["fill_ns"] > f["signal_ns"] for f in native["fills"])
    assert runtime.ledger.snapshot()["orders"] == {}


def test_distinct_strategy_instances_keep_separate_streams_and_faults(stream, market_bars):
    from stratosphere.research.contracts import SmaParameters, compile_sma_spec

    runtime, mandate, account, registry = stream
    approve(stream)
    specification = compile_sma_spec(SmaParameters(fast_period=4, slow_period=10, quantity="0.02"))
    strategy = registry.register(specification, "Other authored agent", {"fixture": "other"})
    instance = mandate.instance.model_copy(
        update={
            "instance_id": "c" * 32,
            "strategy_id": strategy["public_id"],
            "strategy_sha256": strategy["spec_sha256"],
        }
    )
    runtime.ledger.add_instance(instance, registry)
    other = ClosedBarMandate.model_validate(
        mandate.model_dump()
        | {
            "instance": instance.model_dump(),
            "specification": specification.model_dump(),
        }
    )
    runtime.prepare(other)
    runtime.approve(instance.instance_id, other.digest)
    for bar in market_bars[:20]:
        push(stream, bar)
        push((runtime, other, account, registry), bar)
    assert push(stream, market_bars[21])["reason"] == "gap_or_out_of_order"
    assert push((runtime, other, account, registry), market_bars[20])["status"] == "accepted"
    first = runtime.inspect(mandate.instance.instance_id)
    second = runtime.inspect(instance.instance_id)
    assert first["faults"] and not second["faults"]
    assert first["accepted_count"] == 20 and second["accepted_count"] == 21
    assert [{k: s[k] for k in ("side", "signal_ns")} for s in second["signals"]] == batch_crossings(
        market_bars[:21], 4, 10
    )
    assert not (
        {s["signal_sha256"] for s in first["signals"]}
        & {s["signal_sha256"] for s in second["signals"]}
    )
    assert runtime.ledger.snapshot()["cash"] == "1000" and not runtime.ledger.snapshot()["orders"]


@pytest.mark.parametrize("received", [True, 0, -1, "100"])
def test_invalid_receipt_times_do_not_mutate_the_journal(stream, market_bars, received):
    runtime, mandate, *_ = stream
    approve(stream)
    before = runtime.ledger.snapshot()
    with pytest.raises(PaperError, match="receipt time"):
        runtime.ingest(
            mandate.instance.instance_id, observation(market_bars[0]), received_ns=received
        )
    assert runtime.ledger.snapshot() == before
