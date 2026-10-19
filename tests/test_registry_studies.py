"""Registered definitions and variants can own reviewed studies without invented parent plans."""

import asyncio
import json
from decimal import Decimal
from pathlib import Path

import pytest
from test_bounded_studies import monthly
from test_bounded_studies import studies as studies

from stratosphere.catalog.contracts import AssessmentTarget
from stratosphere.catalog.service import AssessmentService
from stratosphere.phase0.contracts import StrategySpec, canonical_bytes, fingerprint
from stratosphere.research.contracts import (
    ResearchConfig,
    ResearchPlan,
    compile_sma_spec,
    sma_parameters,
)
from stratosphere.research.metrics import report_markdown
from stratosphere.research.service import ResearchService
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.studies.contracts import load_study_protocol
from stratosphere.studies.service import StudyService
from stratosphere.studies.worker import CellRequest, cell_bars


async def prepare_registered(
    studies, *, config=None, identity=None, candidates=None, month="2025-01"
):
    identity = identity or studies.test_parent["public_id"]
    return await studies.prepare_registered(
        identity,
        studies.registry_anchor(identity, config),
        "Longer SMA pairs may reduce validation cost sensitivity; reject if costs do not fall.",
        candidates or [{"fast": 3, "slow": 8}, {"fast": 4, "slow": 10}],
        month,
        10,
    )


def test_legacy_plan_and_protocol_bytes_keep_their_canonical_identity():
    data = json.loads((Path(__file__).parent / "fixtures/legacy-study-v1.json").read_text())
    protocol = load_study_protocol(data["protocol"])
    assert protocol.model_dump(mode="json") == data["protocol"]
    assert protocol.digest == data["sha256"]
    plan = ResearchPlan.model_validate(data["research_plan"])
    assert plan.digest == data["research_plan_sha256"]
    assert canonical_bytes(compile_sma_spec(sma_parameters(plan.specification))) == canonical_bytes(
        plan.specification
    )
    with pytest.raises(ValueError, match="Unsupported"):
        load_study_protocol(data["protocol"] | {"schema_version": "bounded-study/unknown"})
    for invalid in (None, [], True, "invalid"):
        with pytest.raises(ValueError, match="must be an object"):
            load_study_protocol(invalid)


@pytest.mark.parametrize(
    "change", ["symbol", "timing", "execution", "missing", "assumption", "indicator"]
)
def test_full_behavior_must_match_not_just_indicator_labels(research_plan, change):
    payload = research_plan.specification.model_dump(mode="json")
    if change == "symbol":
        payload["instrument_constraints"]["fixed_symbols"] = ["ETHUSDT"]
    elif change == "timing":
        payload["signal_timing"]["submit"] = "after_signal"
    elif change == "execution":
        payload["execution_requirements"]["reinforcement"] = True
    elif change == "missing":
        payload["missing_data_policy"]["action"] = "skip"
    elif change == "assumption":
        payload["assumptions"].append("Different arithmetic semantics")
    else:
        payload["indicators"][0]["input"] = "open"
    changed = StrategySpec.model_validate(payload)
    with pytest.raises(ValueError, match="complete definition"):
        sma_parameters(changed)


async def test_registry_protocol_approval_reports_and_exact_variant_attribution_without_parent_job(
    studies, tmp_path
):
    identity = studies.test_parent["public_id"]
    assert not studies.database.jobs(identity)
    config = ResearchConfig(fee_rate="0.002", spread_bps="3", slippage_bps="2")
    job, protocol = await prepare_registered(studies, config=config)
    assert protocol.schema_version == "bounded-study/2" and protocol.config == config
    wire = protocol.model_dump(mode="json")
    assert not {"source_sha256", "original_plan_sha256", "parent_plan", "draft", "evidence"} & set(
        wire["registry_anchor"]
    )
    assert not any(c[0] in {"run", "fetch"} for c in studies.test_calls)
    with pytest.raises(ResearchError):
        await studies.run(job)
    with pytest.raises(ResearchError):
        studies.approve(job, "0" * 64)
    studies.approve(job, protocol.digest)
    report = await studies.run(job)
    assert report["schema_version"] == "study-report/2"
    assert report["selection"]["selected_index"] == 1 and report["native_attempts"] == 10
    assert report["metrics_strategy_id"] == report["attempts"][1]["strategy_id"]
    assert report["attempts"][0]["strategy_id"] == identity
    assert "bounded study" in report_markdown(report) and studies.report(job) == report
    research = ResearchService(
        ResearchStore(tmp_path / "no-research-jobs"), online_check=studies.online_check
    )
    service = AssessmentService(studies.database, research, studies)
    target = AssessmentTarget(asset_class="crypto_spot", symbol="BTCUSDT", timeframe_seconds=3600)
    parent = (await service.assess(identity, target))["assessment"]
    child_id = report["metrics_strategy_id"]
    child = (await service.assess(child_id, target))["assessment"]
    assert parent["evidence"][0]["final_holdout"] == "not_evaluated"
    assert "holdout_1" not in parent["evidence"][0]["metrics"]
    assert child["evidence"][0]["final_holdout"] == "completed_for_exact_variant"
    assert not research.store.jobs()
    # A derived variant has no standalone research job and can anchor another reviewed study.
    studies.test_snapshots["2025-04"] = monthly("2025-04")
    before_agent = studies.database.strategy(child_id)["agent_id"]
    next_job, next_protocol = await prepare_registered(
        studies,
        identity=child_id,
        candidates=[{"fast": 4, "slow": 10}, {"fast": 5, "slow": 12}],
        month="2025-03",
    )
    assert (
        next_protocol.parent_strategy_id == child_id
        and studies.store.state(next_job) == "awaiting_approval"
    )
    assert studies.database.strategy(child_id)["agent_id"] == before_agent
    assert not research.store.jobs()


async def test_mismatched_anchor_missing_baseline_and_changed_cost_digest_cannot_be_approved(
    studies,
):
    identity = studies.test_parent["public_id"]
    anchor = studies.registry_anchor(identity)
    other = studies.database.register(
        compile_sma_spec(anchor.parameters.model_copy(update={"fast_period": 4})),
        "Other",
        {"kind": "authored"},
    )
    with pytest.raises(ResearchError, match="anchor belongs"):
        await studies.prepare_registered(
            other["public_id"], anchor, "A hypothesis", [{"fast": 3, "slow": 8}], "2025-01", 6
        )
    with pytest.raises(ValueError, match="baseline"):
        await prepare_registered(
            studies, candidates=[{"fast": 4, "slow": 10}, {"fast": 5, "slow": 12}]
        )
    job, protocol = await prepare_registered(studies)
    changed = protocol.model_copy(
        update={
            "registry_anchor": anchor.model_copy(
                update={"config": ResearchConfig(fee_rate="0.003")}
            )
        }
    )
    assert changed.digest != protocol.digest
    with pytest.raises(ResearchError):
        studies.approve(job, changed.digest)
    assert all(
        studies.store.state(j) in {"failed", "awaiting_approval"} for j in studies.store.jobs()
    )
    assert not any(c[0] in {"fetch", "run"} for c in studies.test_calls)


def corrupt_definition(studies):
    identity = studies.test_parent["public_id"]
    anchor = studies.registry_anchor(identity)
    changed = compile_sma_spec(anchor.parameters.model_copy(update={"fast_period": 4}))
    # Deliberate corruption of this temporary fixture database, outside the immutable app API.
    with studies.database.connect() as database:
        database.execute("DROP TRIGGER immutable_strategy")
        database.execute(
            "UPDATE strategies SET spec_json=?,spec_sha256=? WHERE public_id=?",
            (canonical_bytes(changed).decode(), fingerprint(changed), identity),
        )


async def test_definition_is_checked_again_after_the_checksum_await(studies):
    identity = studies.test_parent["public_id"]
    original = studies.pinner
    calls = 0

    async def pin(month):
        nonlocal calls
        result = await original(month)
        calls += 1
        if calls == 2:
            corrupt_definition(studies)
        return result

    studies.pinner = pin
    with pytest.raises(ResearchError, match="changed strategy"):
        await prepare_registered(studies)
    assert not studies.database.jobs(identity)
    assert not any(c[0] in {"fetch", "run"} for c in studies.test_calls)


@pytest.mark.parametrize("stage", ["approve", "run"])
async def test_changed_definition_after_review_never_acquires_prices_or_starts_native(
    studies, stage
):
    job, protocol = await prepare_registered(studies)
    if stage == "run":
        studies.approve(job, protocol.digest)
    corrupt_definition(studies)
    with pytest.raises(ResearchError, match="changed strategy"):
        if stage == "approve":
            studies.approve(job, protocol.digest)
        else:
            await studies.run(job)
    assert not any(c[0] in {"fetch", "run"} for c in studies.test_calls)


async def test_registry_study_resumes_after_restart_without_reusing_a_consumed_slot(studies):
    job, protocol = await prepare_registered(studies)
    studies.approve(job, protocol.digest)
    original, entered = studies.runner, asyncio.Event()

    async def waiting(store, cell, multiplier):
        request = store.read(cell, "request.json")
        if request["candidate_index"] == 0 and request["window"] == "validation":
            entered.set()
            await asyncio.Future()
        return await original(store, cell, multiplier)

    studies.runner = waiting
    task = asyncio.create_task(studies.run(job))
    await asyncio.wait_for(entered.wait(), 5)
    studies.stop_state = "paused"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    restarted = None

    async def fetch(pin):
        assert pin.month != "2025-02" or restarted.store.exists(job, "selection.json")
        return studies.test_snapshots[pin.month][1]

    restarted = StudyService(
        studies.store,
        studies.database,
        pinner=studies.pinner,
        fetcher=fetch,
        runner=original,
        online_check=studies.online_check,
    )
    restarted.recover()
    assert restarted.store.protocol(job).digest == protocol.digest
    report = await restarted.run(job)
    assert report["attempts"][0]["cells"]["validation_1"]["status"] == "canceled"
    assert report["selection"]["selected_index"] == 1
    assert studies.test_calls.count(("run", 0, "development", 1)) == 1
    assert len(list(studies.store.directory(job).glob("cell_*_started.json"))) == 10


@pytest.mark.engine
async def test_native_registry_study_matches_independent_oracle_with_reviewed_costs(studies):
    from test_research_engine import oracle

    from stratosphere.research.service import run_worker

    studies.runner = run_worker
    config = ResearchConfig(
        initial_balance="12500", fee_rate="0.002", spread_bps="3", slippage_bps="2"
    )
    job, protocol = await prepare_registered(studies, config=config)
    studies.approve(job, protocol.digest)
    report = await studies.run(job)
    assert report["holdout_status"] == "completed_once" and report["native_attempts"] == 10
    cases = [
        (a["index"], key.split("_")[0], int(key.rsplit("_", 1)[1]), cell)
        for a in report["attempts"]
        for key, cell in a["cells"].items()
    ]
    selected = report["selection"]["selected_index"]
    cases.extend(
        (selected, "holdout", int(multiplier), cell)
        for multiplier, cell in report["holdout"].items()
    )
    for index, window, multiplier, cell in cases:
        request = CellRequest(
            protocol=protocol, candidate_index=index, window=window, selected_index=selected
        )
        plan, bars, start = cell_bars(request, studies.store.directory(job))
        signals, fills, equity, held, pending = oracle(plan, bars, multiplier, trade_start_ns=start)
        result = cell["response"]["result"]
        assert result["engine"] == "nautilus_trader" and result["signals"] == signals
        assert [Decimal(p["equity"]) for p in result["equity_curve"]] == equity
        assert Decimal(result["balances"]["BTC"]) == held and result["pending_signal"] == pending
        assert len(result["fills"]) == len(fills)
        for actual, expected in zip(result["fills"], fills, strict=True):
            for key, value in expected.items():
                if key in {"price", "quantity", "commission"}:
                    assert Decimal(actual[key]) == Decimal(value)
                else:
                    assert actual[key] == value
    assert studies.report(job) == report
