"""Specialist compilation, finite research/variant ownership and actual native execution."""

import asyncio
import json
from decimal import Decimal

import pytest
from test_strategy_intake import Replies
from test_workflow_rebuild import raw_frame

from stratosphere.development.compiler import compile_strategy, load_generated
from stratosphere.development.contracts import Definition, Market
from stratosphere.development.service import StrategyDevelopment
from stratosphere.development.signals import SignalProgram
from stratosphere.development.worker import simulate
from stratosphere.phase0.contracts import StrategySpec
from stratosphere.phase0.signals import SmaCross
from stratosphere.registry.workspace import WorkspaceDatabase
from stratosphere.research.contracts import SmaParameters, compile_sma_spec
from stratosphere.research.store import ResearchError
from stratosphere.sources.contracts import SuppliedRequest
from stratosphere.sources.service import SourceService
from stratosphere.sources.store import SourceStore
from stratosphere.workflow.discovery import StrategyDiscovery


async def online():
    pass


def specification(*, fast=3, algorithm="sma"):
    value = compile_sma_spec(
        SmaParameters(fast_period=fast, slow_period=8, quantity="1")
    ).model_dump(mode="json")
    value["instrument_constraints"] = {
        "selection": "deployment_choice",
        "asset_classes": ["crypto_spot"],
        "fixed_symbols": [],
    }
    value["assumptions"] = ["Application-compiled research; no verified source conformance"]
    if algorithm == "rsi":
        value["indicators"] = [
            {
                "id": "strength",
                "algorithm": "rsi",
                "algorithm_version": 1,
                "input": "close",
                "period": 2,
                "timeframe_seconds": 3600,
            }
        ]
        value["entry_rules"] = [
            {
                "id": "enter",
                "action": "open_long",
                "condition": {
                    "op": "lt",
                    "args": [
                        {"op": "indicator", "value": "strength"},
                        {"op": "decimal", "value": "30"},
                    ],
                },
            }
        ]
        value["exit_rules"] = [
            {
                "id": "exit",
                "action": "close_long",
                "condition": {
                    "op": "gt",
                    "args": [
                        {"op": "indicator", "value": "strength"},
                        {"op": "decimal", "value": "70"},
                    ],
                },
            }
        ]
        value["data_requirements"]["warmup_bars"] = 3
    elif algorithm == "ema":
        for indicator in value["indicators"]:
            indicator["algorithm"] = "ema"
    return StrategySpec.model_validate(value)


def market(symbol="BTC-USD"):
    return Market(
        symbol=symbol,
        asset_class="crypto_spot",
        interval="1h",
        start="2025-01-01",
        end="2025-01-03",
    )


def definition(spec=None, markets=None):
    return Definition(
        status="ready",
        name="Fixture strategy",
        hypothesis="Compare frozen targets",
        specification=spec or specification(),
        markets=markets or (market(),),
        rule_indices={
            key: i for i, key in enumerate(("entry", "exit", "market", "timing", "sizing", "risk"))
        },
        questions=(),
    )


@pytest.fixture
async def intake(tmp_path):
    sources = SourceService(SourceStore(tmp_path / "sources"), online_check=online)
    discovery = StrategyDiscovery(sources)
    source = await sources.supply(
        SuppliedRequest(label="Authored rules"),
        b"Buy and sell under explicit signal rules. Fixed units, one long position, "
        b"hourly closed bars, next open. Fully funded; no leverage, stops or reinforcement.",
    )
    reply = {
        "name": "Fixture strategy",
        "summary": "Source rules",
        "rules": [
            {
                "category": category,
                "interpretation": "Authored " + category,
                "evidence_indices": [1],
            }
            for category in ("entry", "exit", "market", "timing", "sizing", "risk")
        ],
        "questions": [],
    }
    result = await discovery.create(
        "Extract strategy", Replies(reply), "fixture", source_id=source.id
    )
    return discovery, result


@pytest.fixture
def database(tmp_path):
    return WorkspaceDatabase(tmp_path / "library.sqlite3")


def synthetic_result(payload, multiplier):
    from stratosphere.development.contracts import CellPlan, PriceSnapshot

    plan = CellPlan.model_validate(payload["plan"])
    snapshot = PriceSnapshot.model_validate(payload["snapshot"])
    return {
        "plan_sha256": plan.digest,
        "snapshot_sha256": snapshot.digest,
        "cash_precision": 2,
        "cost_multiplier": multiplier,
        "signals": [],
        "fills": [],
        "open_positions": 0,
        "equity_curve": [
            {
                "timestamp_ns": b.close_ns,
                "cash": "10000",
                "base": "0",
                "close": b.close,
                "equity": "10000",
            }
            for b in snapshot.bars
        ],
    }


def runner_factory(calls, *, native=False, fail_at=None):
    async def runner(directory, action, name, *, multiplier=1):
        calls.append((action, multiplier))
        payload = json.loads((directory / name).read_text())
        if fail_at == (action, multiplier):
            raise ResearchError("Authored worker failure")
        if action == "fetch":
            return raw_frame(Market.model_validate(payload))
        if native:
            return simulate(payload, directory, multiplier)
        return synthetic_result(payload, multiplier)

    return runner


def bar(price):
    return {key: str(price) for key in ("open", "high", "low", "close", "volume")}


def test_sma_program_matches_existing_causal_crossings():
    generated = SignalProgram(specification().model_dump(mode="json"))
    old = SmaCross(3, 8)
    held = False
    for value in [100 + i % 16 if i % 32 < 16 else 131 - i % 16 for i in range(100)]:
        expected = old.push(str(value))
        if (expected == "SELL" and not held) or (expected == "BUY" and held):
            expected = None
        actual = generated.push(bar(value), held)
        assert actual == expected
        if actual:
            held = actual == "BUY"


def test_rsi_wilder_seed_and_smoothing_are_explicit():
    program = SignalProgram(specification(algorithm="rsi").model_dump(mode="json"))
    measured = []
    for price in (1, 2, 3, 2, 1, 2):
        program.push(bar(price), False)
        measured.append(program.previous[1]["strength"])
    assert measured[:2] == [None, None]
    assert measured[2:] == [100, 50, 25, Decimal("62.5")]


def test_ema_seed_uses_sma_and_then_exact_alpha():
    spec = specification(algorithm="ema").model_dump(mode="json")
    spec["indicators"][0]["period"] = 2
    program = SignalProgram(spec)
    measured = []
    for price in (1, 3, 5, 7):
        program.push(bar(price), False)
        measured.append(program.previous[1]["fast"])
    assert measured == [None, 2, 4, 6]


def test_compiler_only_imports_exact_application_bytes(tmp_path):
    spec = specification(algorithm="rsi")
    path = tmp_path / "strategy.py"
    path.write_bytes(compile_strategy(spec))
    generated = load_generated(path, spec)()
    assert isinstance(generated, SignalProgram)
    path.write_text("raise RuntimeError('Acquired code must never run')")
    with pytest.raises(ValueError, match="compiler output"):
        load_generated(path, spec)


@pytest.mark.parametrize(
    "field,change",
    [
        ("position_sizing", {"mode": "fraction_equity", "amount": "0.1"}),
        (
            "signal_timing",
            {"evaluate": "intrabar", "submit": "next_bar_open", "precedence": "exit_before_entry"},
        ),
    ],
)
def test_unsupported_definition_cannot_generate_executable_code(field, change):
    data = specification().model_dump(mode="json")
    data[field] = change
    with pytest.raises(ValueError):
        compile_strategy(StrategySpec.model_validate(data))


async def test_external_definition_registers_one_stable_owner_without_reacquisition(
    intake, database, tmp_path
):
    discovery, brief = intake
    calls = []
    program = Definition.model_validate(definition().model_dump(mode="json"))
    service = StrategyDevelopment(
        tmp_path / "development",
        discovery,
        database,
        runner=runner_factory(calls),
        online_check=online,
    )
    job = await service.prepare(brief["id"], program)
    plan = service.plan(job)
    assert database.strategy(plan["owner"])["agent_id"]
    assert calls == [("fetch", 1)]
    assert await service.prepare(brief["id"], program) == job
    assert len(calls) == 1
    with pytest.raises(ResearchError, match="exact frozen strategy"):
        await service.run(job, owner="9" * 24)


async def test_one_program_four_target_budget_and_complete_attempt_history(
    intake, database, tmp_path
):
    discovery, brief = intake
    calls = []
    program = Definition.model_validate(
        definition(
            markets=(market(), market("SOL-USD"), market("ETH-USD"), market("DOGE-USD"))
        ).model_dump(mode="json")
    )
    service = StrategyDevelopment(
        tmp_path / "development",
        discovery,
        database,
        runner=runner_factory(calls),
        online_check=online,
    )
    job = await service.prepare(brief["id"], program)
    result = await service.run(job, owner=service.plan(job)["owner"])
    assert len(result["programs"]) == 1 and len(result["results"]) == 4
    assert result["native_starts"] == 8 and result["selected_cell_id"] is None
    assert result["execution"] == "blocked" and result["holdout"] == "none"
    assert len([a for a, _ in calls if a == "fetch"]) == 4
    assert len({r["strategy_id"] for r in result["results"]}) == 4
    for row in result["results"]:
        assert (service.cells.directory(row["cell_id"]) / "strategy.py").is_file()
        assert row["conditions"]["prior_20_bar_direction"]
    with pytest.raises(ResearchError, match="consumed"):
        await service.run(job, owner=result["owner"])


async def test_native_failure_consumes_slot_and_is_not_retried(intake, database, tmp_path):
    discovery, brief = intake
    calls = []
    program = Definition.model_validate(definition().model_dump(mode="json"))
    service = StrategyDevelopment(
        tmp_path / "development",
        discovery,
        database,
        runner=runner_factory(calls, fail_at=("simulate", 1)),
        online_check=online,
    )
    job = await service.prepare(brief["id"], program)
    report = await service.run(job, owner=service.plan(job)["owner"])
    assert report["native_starts"] == 1 and report["results"][0]["state"] == "failed"
    assert calls == [("fetch", 1), ("simulate", 1)]


@pytest.mark.engine
@pytest.mark.parametrize("algorithm", ["sma", "ema", "rsi"])
async def test_actual_compiled_code_drives_native_backtest(intake, database, tmp_path, algorithm):
    discovery, brief = intake
    calls = []
    program = Definition.model_validate(
        definition(specification(algorithm=algorithm)).model_dump(mode="json")
    )
    service = StrategyDevelopment(
        tmp_path / "development",
        discovery,
        database,
        runner=runner_factory(calls, native=True),
        online_check=online,
    )
    job = await service.prepare(brief["id"], program)
    report = await service.run(job, owner=service.plan(job)["owner"])
    cell = report["results"][0]
    assert cell["state"] == "completed", cell
    assert report["native_starts"] == 2
    assert cell["metrics"]["base"]["filled_orders"] > 0
    assert cell["metrics"]["base"]["initial_equity"] == "10000.00000000"
    assert service.report(job) == report


def test_duplicate_market_hypotheses_do_not_create_more_evidence():
    a = market()
    b = a.model_copy(update={"hypothesis": "Different metadata"})
    with pytest.raises(ValueError, match="Repeated markets"):
        definition(markets=(a, b))


async def test_dataset_tamper_blocks_every_native_start(intake, database, tmp_path):
    discovery, brief = intake
    calls = []
    service = StrategyDevelopment(
        tmp_path / "development",
        discovery,
        database,
        runner=runner_factory(calls),
        online_check=online,
    )
    job = await service.prepare(
        brief["id"], Definition.model_validate(definition().model_dump(mode="json"))
    )
    pin = service.plan(job)["datasets"][0]
    path = service.datasets.directory(pin["id"]) / "returned_frame.json"
    raw = json.loads(path.read_text())
    raw["rows"][0]["close"] = "100.5"
    path.write_text(json.dumps(raw))
    with pytest.raises(ResearchError, match="dataset changed"):
        await service.run(job, owner=service.store.read(job, "plan.json")["owner"])
    assert calls == [("fetch", 1)]


async def test_cancellation_retains_consumed_cell_without_resume(intake, database, tmp_path):
    discovery, brief = intake
    calls = []
    entered = asyncio.Event()
    fallback = runner_factory(calls)

    async def blocked(directory, action, name, *, multiplier=1):
        if action == "simulate":
            entered.set()
            await asyncio.Event().wait()
        return await fallback(directory, action, name, multiplier=multiplier)

    service = StrategyDevelopment(
        tmp_path / "development", discovery, database, runner=blocked, online_check=online
    )
    job = await service.prepare(
        brief["id"], Definition.model_validate(definition().model_dump(mode="json"))
    )
    owner = service.plan(job)["owner"]
    task = asyncio.create_task(service.run(job, owner=owner))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert service.store.state(job) == "canceled" and "canceled" in service.show(job)
    cell_id = service.store.read(job, "candidate_a.json")["cell_id"]
    assert service.cells.read(cell_id, "start_base.json")["consumed"] is True
    with pytest.raises(ResearchError, match="consumed"):
        await service.run(job, owner=owner)


async def test_all_blocked_markets_are_reported_without_native_or_revision_calls(
    intake, database, tmp_path
):
    discovery, brief = intake
    calls = []

    async def bad(directory, action, name, **kwargs):
        calls.append(action)
        result = raw_frame(Market.model_validate(json.loads((directory / name).read_text())))
        result["rows"][0]["dividends"] = "1"
        return result

    program = Definition.model_validate(definition().model_dump(mode="json"))
    service = StrategyDevelopment(
        tmp_path / "development", discovery, database, runner=bad, online_check=online
    )
    job = await service.prepare(brief["id"], program)
    report = await service.run(job, owner=service.plan(job)["owner"])
    assert report["datasets"][0]["state"] == "blocked" and not report["results"]
    assert report["native_starts"] == 0 and calls == ["fetch"]


async def test_generated_code_tamper_blocks_report_reader(intake, database, tmp_path):
    discovery, brief = intake
    calls = []
    program = Definition.model_validate(definition().model_dump(mode="json"))
    service = StrategyDevelopment(
        tmp_path / "development",
        discovery,
        database,
        runner=runner_factory(calls),
        online_check=online,
    )
    job = await service.prepare(brief["id"], program)
    result = await service.run(job, owner=service.plan(job)["owner"])
    path = service.cells.directory(result["results"][0]["cell_id"]) / "strategy.py"
    path.write_text("raise RuntimeError('Never import modified code')")
    with pytest.raises(ResearchError, match="code/input changed"):
        service.report(job)


@pytest.mark.engine
async def test_actual_equity_program_and_model_free_subprocess(intake, database, tmp_path):
    from datetime import UTC, datetime, timedelta

    from stratosphere.development.transport import worker

    discovery, brief = intake
    target = Market(
        symbol="AMZN", asset_class="equity", interval="1d", start="2025-01-01", end="2025-02-18"
    )
    spec = specification(algorithm="rsi").model_dump(mode="json")
    spec["instrument_constraints"]["asset_classes"] = ["equity"]
    spec["timeframes"] = [86400]
    spec["indicators"][0]["timeframe_seconds"] = 86400
    spec = StrategySpec.model_validate(spec)

    async def native(directory, action, name, *, multiplier=1):
        if action == "fetch":
            request = Market.model_validate(json.loads((directory / name).read_text()))
            raw = raw_frame(request)
            start = datetime(2025, 1, 1, tzinfo=UTC)
            for i, row in enumerate(raw["rows"]):
                row["timestamp_ns"] = int((start + timedelta(days=i)).timestamp() * 1e9)
            raw["observed_at_ns"] = int(datetime(2025, 2, 19, tzinfo=UTC).timestamp() * 1e9)
            raw["metadata"]["instrument_type"] = "EQUITY"
            return raw
        return await worker(directory, action, name, multiplier=multiplier)

    program = Definition.model_validate(definition(spec, markets=(target,)).model_dump(mode="json"))
    service = StrategyDevelopment(
        tmp_path / "development", discovery, database, runner=native, online_check=online
    )
    job = await service.prepare(brief["id"], program)
    result = await service.run(job, owner=service.plan(job)["owner"])
    row = result["results"][0]
    assert row["state"] == "completed", row
    assert row["market"]["asset_class"] == "equity"
    assert row["metrics"]["base"]["filled_orders"] > 0
    assert (service.store.directory(job) / "report.md").is_file()
    assert not list(service.store.directory(job).glob("model_*.json"))


async def test_owner_development_evidence_excludes_unrelated_strategy(intake, database, tmp_path):
    discovery, brief = intake
    calls = []
    program = Definition.model_validate(definition().model_dump(mode="json"))
    service = StrategyDevelopment(
        tmp_path / "development",
        discovery,
        database,
        runner=runner_factory(calls),
        online_check=online,
    )
    job = await service.prepare(brief["id"], program)
    report = await service.run(job, owner=service.plan(job)["owner"])
    unrelated = database.register(specification(fast=6), "Private unrelated strategy", {})
    assert service.evidence_for(unrelated["public_id"]) == []
    own = service.evidence_for(report["owner"])
    assert own and own[0]["intake_id"] == brief["id"]
    assert "Private unrelated strategy" not in json.dumps(own)


def test_retained_hourly_scope_does_not_expire_with_provider_download_window():
    old = Market(
        symbol="BTC-USD",
        asset_class="crypto_spot",
        interval="1h",
        start="2010-01-01",
        end="2010-02-01",
    )
    assert (
        old.start.year == 2010
    )  # New acquisitions enforce provider age; historical readers do not.


async def test_symbol_summary_prefers_filtered_candidate_over_high_score_with_one_trade(
    intake, database, tmp_path
):
    discovery, _ = intake
    service = StrategyDevelopment(tmp_path / "development", discovery, database)
    scope = market().model_dump(mode="json")

    def row(identity, gain, trades):
        metrics = {"net_return_pct": str(gain), "completed_trades": trades, "max_drawdown_pct": "1"}
        return {
            "cell_id": identity,
            "strategy_id": "1" * 24,
            "market": scope,
            "state": "completed",
            "metrics": {"base": metrics, "stress": metrics},
        }

    report = service.build_report(
        "a" * 32,
        {
            "schema_version": "strategy-development-plan/3",
            "owner": "1" * 24,
            "datasets": [],
            "definition": {"markets": []},
        },
        [],
        [row("a" * 32, 20, 1), row("b" * 32, 5, 10)],
        4,
    )
    assert report["selected_cell_id"] == "b" * 32
    assert report["market_summary"][0]["best_sampled_cell_id"] == "b" * 32
    assert report["market_summary"][0]["passes_research_filters"] is True


async def test_external_variants_have_explicit_lineage_and_no_internal_model_calls(
    intake, database, tmp_path, monkeypatch
):
    async def forbidden(*args, **kwargs):
        raise AssertionError("The engine attempted model inference")

    monkeypatch.setattr("stratosphere.models.structured.structured_response", forbidden)
    discovery, brief = intake
    calls = []
    service = StrategyDevelopment(
        tmp_path / "development", discovery, database, runner=runner_factory(calls)
    )
    first = await service.prepare(brief["id"], definition())
    owner = service.plan(first)["owner"]
    await service.run(first, owner=owner)
    variant = await service.prepare(brief["id"], definition(specification(fast=4)), parent=owner)
    variant_owner = service.plan(variant)["owner"]
    assert variant_owner != owner
    assert owner in database.strategy(variant_owner)["parent_public_ids"]
    plan = service.plan(variant)
    assert plan["schema_version"] == "strategy-development-plan/3"
    assert plan["limits"]["max_model_calls"] == 0
    assert plan["limits"]["max_native_starts"] == 8
    assert not list(service.store.root.glob("*/model_*.json"))


async def test_external_budget_tampering_cannot_start_native_work(intake, database, tmp_path):
    from stratosphere.workflow.discovery import digest

    discovery, brief = intake
    calls = []
    service = StrategyDevelopment(
        tmp_path / "development", discovery, database, runner=runner_factory(calls)
    )
    identity = await service.prepare(brief["id"], definition())
    plan = service.plan(identity)
    owner = plan["owner"]
    plan["limits"]["max_native_starts"] = 1000
    directory = service.store.directory(identity)
    (directory / "plan.json").write_text(json.dumps(plan))
    (directory / "plan_seal.json").write_text(json.dumps({"sha256": digest(plan)}))
    with pytest.raises(ResearchError, match="budget/selection"):
        await service.run(identity, owner=owner)
    assert calls == [("fetch", 1)]


async def test_wrong_source_rule_cannot_register_or_acquire_data(intake, database, tmp_path):
    discovery, brief = intake
    calls = []
    program = definition().model_dump(mode="json")
    program["rule_indices"]["entry"] = 1
    service = StrategyDevelopment(
        tmp_path / "development", discovery, database, runner=runner_factory(calls)
    )
    with pytest.raises(ResearchError, match="invalid intake rule"):
        await service.prepare(brief["id"], program)
    assert calls == [] and database.catalog() == []
