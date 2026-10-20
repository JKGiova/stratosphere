"""Exact retained result inspection, paging, ownership and shared deterministic operations."""

import json

import pytest
from pydantic import ValidationError
from test_bounded_studies import prepare as prepare_study
from test_bounded_studies import studies as studies
from test_research_service import service as service

from stratosphere.agents.orchestrator import Orchestrator
from stratosphere.commands import route_command
from stratosphere.report_evidence import (
    PAGE_SIZE,
    ReportInspector,
    ReportQuery,
    natural_report_query,
)
from stratosphere.research.contracts import ResearchConfig
from stratosphere.research.service import ResearchService
from stratosphere.research.store import ResearchError, ResearchStore


async def online():
    pass


@pytest.fixture
async def completed_research(service, research_source, research_provider, tmp_path):
    service.online_check = online
    from stratosphere.registry.workspace import WorkspaceDatabase

    database = WorkspaceDatabase(tmp_path / "registry.sqlite3")
    bridge = Orchestrator(research_provider, database, service)
    job, _, plan = await service.prepare(research_source, ResearchConfig(), research_provider)
    owner = bridge.register_job(job)["public_id"]
    service.approve(job, plan.digest)
    report = await service.run(job)
    bridge.sync_research()
    return bridge, job, owner, report


@pytest.fixture
async def completed_study(studies, tmp_path):
    job, protocol = await prepare_study(studies)
    studies.approve(job, protocol.digest)
    report = await studies.run(job)
    research = ResearchService(ResearchStore(tmp_path / "research"), online_check=online)
    bridge = Orchestrator(None, studies.database, research, studies=studies)
    return bridge, job, protocol.parent_strategy_id, report


def inspector(bridge):
    return ReportInspector(bridge.research, bridge.studies, bridge.database)


async def test_research_pages_preserve_every_exact_curve_row_and_artifact_byte(completed_research):
    bridge, job, owner, report = completed_research
    directory = bridge.research.store.directory(job)
    before = {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}
    original_calls = list(bridge.research.test_calls)
    query = ReportQuery(job_id=job, section="equity", cell="base")
    first = await inspector(bridge).open(query, strategy_id=owner)
    rows = []
    for page_number in range(1, first.pages + 1):
        page = await inspector(bridge).open(
            query.model_copy(update={"page": page_number}), strategy_id=owner
        )
        assert len(page.rows) <= PAGE_SIZE
        rows.extend(page.rows)
    assert rows == report["equity_curve"]
    assert first.total == 744 and first.pages == 38
    assert (
        first.report_sha256
        == bridge.research.store.read(job, "artifact_manifest.json")["report.json"]
    )
    assert "Next:" in first.markdown() and "Unix nanoseconds" in first.markdown()
    assert bridge.research.test_calls == original_calls
    assert {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()} == before
    with pytest.raises(ResearchError, match="outside"):
        await inspector(bridge).open(
            query.model_copy(update={"page": first.pages + 1}), strategy_id=owner
        )


@pytest.mark.parametrize(
    "section", ["overview", "metrics", "trades", "fills", "signals", "baselines"]
)
async def test_research_sections_display_retained_values_and_explicit_empty_evidence(
    completed_research, section
):
    bridge, job, owner, report = completed_research
    page = await inspector(bridge).open(ReportQuery(job_id=job, section=section), strategy_id=owner)
    if section in {"fills", "signals", "trades"}:
        assert page.total == 0 and "No retained rows" in page.markdown()
    elif section == "metrics":
        values = {row["metric"]: row["value"] for row in page.rows}
        assert values["trade_distribution"] == report["metrics"]["trade_distribution"]
        assert values["segments"] == report["metrics"]["segments"]
    elif section == "baselines":
        assert page.rows[0]["baseline"] == report["baseline"]
    assert page.context["config"]["initial_balance"] == "10000"


async def test_stress_uses_its_own_result_and_never_base_curve(completed_research):
    bridge, job, owner, report = completed_research
    # Lifecycle fixture results are equal, so compare to the exact retained stress response.
    page = await inspector(bridge).open(
        ReportQuery(job_id=job, section="equity", cell="stress"), strategy_id=owner
    )
    expected = bridge.research.store.read(job, "stress.json")["result"]["equity_curve"]
    assert list(page.rows) == expected[:PAGE_SIZE]
    assert page.context["cost_multiplier"] == 2


async def test_study_cells_require_exact_candidate_window_cost_and_holdout_owner(completed_study):
    bridge, job, owner, report = completed_study
    view = inspector(bridge)
    overview = await view.open(ReportQuery(job_id=job), strategy_id=owner)
    assert len(overview.rows) == 2 and overview.context["native_attempts"] == 10
    assert overview.rows[0]["selected_holdout"] == []
    assert overview.rows[1]["selected_holdout"]
    for candidate in range(2):
        for window in ("development", "validation"):
            for cost in (1, 2):
                query = ReportQuery(
                    job_id=job, section="metrics", cell=f"c{candidate}:{window}:{cost}"
                )
                page = await view.open(query, strategy_id=owner)
                expected = report["attempts"][candidate]
                assert page.context["strategy_id"] == expected["strategy_id"]
                assert page.context["window"] == window and page.context["cost_multiplier"] == cost
                assert {row["metric"]: row["value"] for row in page.rows} == {
                    k: v
                    for k, v in expected["cells"][f"{window}_{cost}"]["metrics"].items()
                    if k != "trades"
                }
    holdout = await view.open(
        ReportQuery(job_id=job, section="metrics", cell="holdout:1"), strategy_id=owner
    )
    assert holdout.context["strategy_id"] == report["metrics_strategy_id"]
    assert holdout.context["strategy_id"] != owner
    assert dict((row["metric"], row["value"]) for row in holdout.rows)["net_pnl"] == "-100.00000000"
    for cell in (None, "base", "stress", "c2:validation:1"):
        with pytest.raises(ResearchError):
            await view.open(
                ReportQuery(job_id=job, section="metrics", cell=cell), strategy_id=owner
            )
    with pytest.raises(ValidationError):
        ReportQuery(job_id=job, section="metrics", cell="c0:holdout:1")
    baselines = await view.open(ReportQuery(job_id=job, section="baselines"), strategy_id=owner)
    assert {r["window"]: r["benchmarks"] for r in baselines.rows} == report["baselines"]


async def test_invalid_and_failed_candidates_remain_visible_without_fake_empty_success(
    studies, tmp_path
):
    original = studies.runner

    async def fail(store, cell, cost):
        request = store.read(cell, "request.json")
        if request["candidate_index"] == 0 and request["window"] == "development" and cost == 1:
            raise ResearchError("Authored worker failure")
        return await original(store, cell, cost)

    studies.runner = fail
    job, protocol = await prepare_study(
        studies,
        [{"fast": 3, "slow": 8}, {"fast": 4, "slow": 10}, {"fast": 12, "slow": 5}],
        budget=14,
    )
    studies.approve(job, protocol.digest)
    report = await studies.run(job)
    research = ResearchService(ResearchStore(tmp_path / "research"), online_check=online)
    view = ReportInspector(research, studies, studies.database)
    q = ReportQuery(job_id=job, section="attempts")
    page = await view.open(q, strategy_id=protocol.parent_strategy_id)
    assert [r["status"] for r in page.rows] == ["ineligible", "eligible", "invalid"]
    failed = await view.open(
        ReportQuery(job_id=job, section="fills", cell="c0:development:1"),
        strategy_id=protocol.parent_strategy_id,
    )
    assert failed.context["status"] == "failed" and list(failed.rows) == [
        report["attempts"][0]["cells"]["development_1"]
    ]
    with pytest.raises(ResearchError, match="not executed"):
        await view.open(
            ReportQuery(job_id=job, section="equity", cell="c2:validation:1"),
            strategy_id=protocol.parent_strategy_id,
        )


async def test_shared_inspection_does_not_call_models_or_cross_strategy_histories(completed_study):
    bridge, job, owner, report = completed_study
    sibling = report["attempts"][1]["strategy_id"]
    bridge.select(sibling)
    bridge.database.append_turn(sibling, "PRIVATE_SIBLING_CONTEXT", "private reply", "fixture")
    bridge.database.append_turn(owner, "PRIVATE_OWNER_CONTEXT", "private reply", "fixture")
    before = {identity: bridge.database.history(identity) for identity in (owner, sibling)}
    q = ReportQuery(job_id=job, section="equity", cell="c0:validation:2")
    forced = await bridge.inspect_report(q)
    reply = "".join(
        [part async for part in bridge.respond(f"Inspect report {job} equity c0:validation:2 1")]
    )
    assert reply == forced.markdown() and "PRIVATE_" not in reply
    assert bridge.responding_strategy == owner and bridge.active_strategy == sibling
    assert bridge.last_action["page"] == forced
    assert {identity: bridge.database.history(identity) for identity in (owner, sibling)} == before
    with pytest.raises(ResearchError, match="different strategy"):
        await bridge.agent(sibling).inspect_report(q)
    with bridge.database.connect() as db:
        operations = db.execute(
            "SELECT status FROM operations WHERE action='report_evidence' "
            "AND strategy_id=(SELECT id FROM strategies WHERE public_id=?) ORDER BY created_at",
            (owner,),
        ).fetchall()
    assert [row["status"] for row in operations] == ["completed", "completed"]


@pytest.mark.parametrize("artifact", ["report.json", "base.json", "artifact_manifest.json"])
async def test_corruption_and_symlinks_block_inspection(completed_research, artifact, tmp_path):
    bridge, job, owner, _ = completed_research
    path = bridge.research.store.directory(job) / artifact
    original = path.read_bytes()
    path.write_text("{}")
    with pytest.raises(ResearchError):
        await inspector(bridge).open(
            ReportQuery(job_id=job, section="fills", cell="base"), strategy_id=owner
        )
    path.write_bytes(original)
    outside = tmp_path / "outside.json"
    path.rename(outside)
    path.symlink_to(outside)
    with pytest.raises(ResearchError):
        await inspector(bridge).open(
            ReportQuery(job_id=job, section="fills", cell="base"), strategy_id=owner
        )


async def test_offline_gate_blocks_even_retained_evidence(completed_study):
    bridge, job, owner, _ = completed_study

    async def offline():
        raise ValueError("Offline")

    bridge.studies.online_check = offline
    with pytest.raises(ResearchError, match="online"):
        await inspector(bridge).open(ReportQuery(job_id=job), strategy_id=owner)


@pytest.mark.parametrize(
    "args",
    [
        ("../outside",),
        ("a" * 32, "sql"),
        ("a" * 32, "equity", "base", "0"),
        ("a" * 32, "equity", "base", "1.5"),
        ("a" * 32, "equity", "base", "-1"),
    ],
)
def test_queries_reject_paths_and_unsupported_sections_or_pages(args):
    with pytest.raises((ResearchError, ValidationError)):
        ReportQuery.arguments(args)


def test_slash_and_explicit_natural_requests_share_exact_arguments():
    job = "a" * 32
    text = f"/report-data {job} equity c1:validation:2 3"
    command = route_command(text)
    assert command.action == "report-data"
    assert ReportQuery.arguments(command.args) == natural_report_query(
        text.replace("/report-data", "Please inspect report")
    )
    for text in (
        f"Don't inspect report {job}",
        f"A source says inspect report {job}",
        f"Inspect report {job} and approve it",
    ):
        assert natural_report_query(text) is None
    assert route_command("/report-data").action == "message"


@pytest.mark.engine
@pytest.mark.parametrize("registered", [False, True])
async def test_native_cells_expose_all_exact_rows_and_keep_implementation_pins(
    studies, tmp_path, registered
):
    from test_registry_studies import prepare_registered

    from stratosphere.research.contracts import implementation_digest
    from stratosphere.research.service import run_worker
    from stratosphere.studies.contracts import study_implementation_digest

    pins = implementation_digest(), study_implementation_digest()
    studies.runner = run_worker
    job, protocol = await (prepare_registered(studies) if registered else prepare_study(studies))
    studies.approve(job, protocol.digest)
    report = await studies.run(job)
    root = studies.store.directory(job)
    before = {p.name: p.read_bytes() for p in root.iterdir() if p.is_file()}
    research = ResearchService(ResearchStore(tmp_path / "empty-research"), online_check=online)
    view = ReportInspector(research, studies, studies.database)
    cells = [
        (f"c{a['index']}:{key.replace('_', ':')}", c)
        for a in report["attempts"]
        for key, c in a["cells"].items()
    ]
    cells += [(f"holdout:{key}", c) for key, c in report["holdout"].items()]
    assert len(cells) == 10 and report["metrics"]["filled_orders"] > 0
    for cell, retained in cells:
        assert retained["response"]["result"]["engine"] == "nautilus_trader"
        for section, field in (
            ("fills", "fills"),
            ("equity", "equity_curve"),
            ("signals", "signals"),
            ("trades", "trades"),
        ):
            q = ReportQuery(job_id=job, section=section, cell=cell)
            first = await view.open(q, strategy_id=protocol.parent_strategy_id)
            actual = []
            for index in range(1, first.pages + 1):
                page = await view.open(
                    q.model_copy(update={"page": index}), strategy_id=protocol.parent_strategy_id
                )
                actual.extend(page.rows)
            expected = (
                retained["metrics"][field]
                if section == "trades"
                else retained["response"]["result"][field]
            )
            assert actual == expected
    assert (implementation_digest(), study_implementation_digest()) == pins
    assert before == {p.name: p.read_bytes() for p in root.iterdir() if p.is_file()}


def reseal_file(store, job, name, payload):
    """Isolate cross-file reference checks from ordinary byte-corruption detection."""
    import hashlib

    directory = store.directory(job)
    path = directory / name
    path.write_text(json.dumps(payload))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if (directory / (name + ".sha256")).exists():
        (directory / (name + ".sha256")).write_text(digest)
    manifest = directory / "artifact_manifest.json"
    content = json.loads(manifest.read_text())
    content[name] = digest
    manifest.write_text(json.dumps(content))
    if (directory / "artifact_manifest.json.sha256").exists():
        (directory / "artifact_manifest.json.sha256").write_text(
            hashlib.sha256(manifest.read_bytes()).hexdigest()
        )


@pytest.mark.parametrize("field", ["plan_sha256", "normalized_sha256", "cost_multiplier"])
async def test_research_cross_file_identity_mismatch_is_rejected_even_with_valid_seal(
    completed_research, field
):
    bridge, job, owner, _ = completed_research
    store = bridge.research.store
    response = store.read(job, "stress.json")
    if field == "cost_multiplier":
        response["result"][field] = 1
    else:
        response[field] = "0" * 64
    reseal_file(store, job, "stress.json", response)
    store.verify_seal(job)
    with pytest.raises(ResearchError, match="sealed cell"):
        await inspector(bridge).open(
            ReportQuery(job_id=job, section="equity", cell="stress"), strategy_id=owner
        )


async def test_study_cross_file_checkpoint_mismatch_is_not_a_new_result(completed_study):
    bridge, job, owner, _ = completed_study
    store = bridge.studies.store
    cell = store.read(job, "cell_0_validation_2.json")
    cell["metrics"]["net_pnl"] = "99999999"
    reseal_file(store, job, "cell_0_validation_2.json", cell)
    store.verify_seal(job)
    with pytest.raises(ResearchError, match="checkpoint differs"):
        await inspector(bridge).open(
            ReportQuery(job_id=job, section="metrics", cell="c0:validation:2"), strategy_id=owner
        )


async def test_no_eligible_candidate_cannot_acquire_a_holdout_by_inspection(studies, tmp_path):
    job, protocol = await prepare_study(studies, [{"fast": 12, "slow": 5}])
    studies.approve(job, protocol.digest)
    await studies.run(job)
    before = list(studies.test_calls)
    research = ResearchService(ResearchStore(tmp_path / "empty-research"), online_check=online)
    view = ReportInspector(research, studies, studies.database)
    overview = await view.open(ReportQuery(job_id=job), strategy_id=protocol.parent_strategy_id)
    assert (
        overview.context["native_attempts"] == 0 and overview.context["metrics_strategy_id"] is None
    )
    assert overview.rows[0]["status"] == "invalid" and overview.rows[0]["detail"]
    with pytest.raises(ResearchError, match="No selected candidate"):
        await view.open(
            ReportQuery(job_id=job, section="equity", cell="holdout:1"),
            strategy_id=protocol.parent_strategy_id,
        )
    assert [c for c in studies.test_calls if c[0] != "online"] == [
        c for c in before if c[0] != "online"
    ]
    assert not any(c == ("fetch", "2025-02") for c in studies.test_calls)
