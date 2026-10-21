"""Private specialist analysis binds all measured facts without test/approval authority."""

import asyncio
import json

import pytest
from test_bounded_studies import prepare as prepare_study
from test_bounded_studies import studies as studies
from test_report_evidence import completed_research as completed_research
from test_report_evidence import completed_study as completed_study
from test_report_evidence import reseal_file
from test_research_service import service as service

from stratosphere.agents.orchestrator import Orchestrator
from stratosphere.commands import route_command
from stratosphere.models.streaming import ModelError, schema_delivery_contract
from stratosphere.registry.workspace import RegistryError, WorkspaceDatabase
from stratosphere.report_analysis import (
    AnalysisRequest,
    ReportAnalysisService,
    analysis_markdown,
    natural_analysis_request,
    report_facts,
)
from stratosphere.research.service import ResearchService
from stratosphere.research.store import ResearchError, ResearchStore


class AnalysisProvider:
    def __init__(self, *, reply=None, wait=False, change=None):
        self.reply = reply
        self.wait, self.change = wait, change
        self.requests = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def stream_json(self, messages, schema):
        self.requests.append((messages, schema))
        evidence = json.loads(messages[1]["content"])
        self.entered.set()
        if self.wait:
            await self.release.wait()
        if self.change:
            self.change()
        reply = self.reply or {
            "findings": [
                {
                    "category": "limitation",
                    "interpretation": "The retained limitations require further verification.",
                    "fact_ids": [
                        next(f["id"] for f in evidence["facts"] if f["path"] == "/limitations")
                    ],
                }
            ],
            "questions": [],
        }
        if isinstance(reply, Exception):
            raise reply
        yield json.dumps(reply)


def analyzer(bridge):
    return bridge.report_analyses


async def analyze(bridge, job, owner, provider=None):
    return await analyzer(bridge).analyze(
        AnalysisRequest(job_id=job),
        strategy_id=owner,
        provider=provider or AnalysisProvider(),
        model_ref="fixture/analysis",
    )


def files(directory):
    return {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}


async def test_complete_facts_preserve_all_scopes_denominators_and_raw_decimal_values(
    completed_study,
):
    bridge, job, owner, report = completed_study
    provider = AnalysisProvider()
    before = files(bridge.studies.store.directory(job))
    calls = list(bridge.studies.test_calls)
    result = await analyze(bridge, job, owner, provider)
    evidence = result.evidence
    assert evidence.agent_id == bridge.database.strategy(owner)["agent_id"]
    assert result.schema_delivery == schema_delivery_contract()
    assert evidence.facts == report_facts(bridge.studies.store.read(job, "report.json"), owner)
    holdout = [f for f in evidence.facts if f.path == "/holdout/1/metrics"][0]
    assert holdout.scope["strategy_id"] == report["metrics_strategy_id"] != owner
    assert holdout.value["net_pnl"] == report["metrics"]["net_pnl"] == "-100.00000000"
    assert holdout.scope["role"] == "selected_variant_only"
    assert "trades" not in holdout.value and holdout.omitted_fields == ("trades",)
    all_metrics = [f for f in evidence.facts if f.path.endswith("/metrics")]
    assert len(all_metrics) == 10
    assert {f.scope["candidate_index"] for f in all_metrics} == {0, 1}
    assert {f.scope["cost_multiplier"] for f in all_metrics} == {1, 2}
    assert [f.value for f in evidence.facts if f.path == "/attempted_candidates"] == [2]
    assert all("trade_distribution" in f.value for f in all_metrics)
    assert len([f for f in evidence.facts if f.path.startswith("/baselines/")]) == 3
    assert evidence.context["config"]["initial_balance"] == "10000"
    assert (
        evidence.context["registered_definition"]
        == bridge.database.strategy(owner)["specification"]
    )
    prompt = json.dumps(provider.requests)
    assert "equity_curve" not in prompt and '"fills"' not in prompt
    assert before == files(bridge.studies.store.directory(job))
    assert [c for c in bridge.studies.test_calls if c[0] != "online"] == [
        c for c in calls if c[0] != "online"
    ]
    assert "unverified" in analysis_markdown(result)
    assert await analyzer(bridge).open(result.id) == result


async def test_research_analysis_preserves_continuous_segments_and_stress_own_metrics(
    completed_research,
):
    bridge, job, owner, report = completed_research
    value = await analyze(bridge, job, owner)
    facts = {f.path: f for f in value.evidence.facts}
    for key in ("metrics", "doubled_cost_stress"):
        assert facts["/" + key].value == {k: v for k, v in report[key].items() if k != "trades"}
        assert all(
            "not a separate holdout" in s["convention"] for s in facts["/" + key].value["segments"]
        )
    assert facts["/baseline"].scope["cost_multiplier"] == 1
    assert facts["/metrics"].scope["strategy_id"] == owner


async def test_model_receives_no_private_histories_or_other_strategy_sources(completed_study):
    bridge, job, owner, report = completed_study
    other = report["attempts"][1]["strategy_id"]
    bridge.database.append_turn(owner, "PRIVATE_OWNER", "private", "fixture")
    bridge.database.append_turn(other, "PRIVATE_OTHER", "private", "fixture")
    provider = AnalysisProvider(wait=True)
    bridge.provider = provider
    bridge.select(other)
    before = {s: bridge.database.history(s) for s in (owner, other)}
    task = asyncio.create_task(bridge.analyze_report(AnalysisRequest(job_id=job)))
    await asyncio.wait_for(provider.entered.wait(), timeout=5)
    bridge.select(owner)
    bridge.select(other)
    provider.wait = False
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not bridge.database.report_analyses()
    assert "PRIVATE_" not in json.dumps(provider.requests)
    assert before == {s: bridge.database.history(s) for s in (owner, other)}
    assert bridge.active_strategy == other
    with bridge.database.connect() as db:
        row = db.execute("SELECT * FROM operations WHERE action='report_analysis'").fetchone()
    assert row["status"] == "canceled"


@pytest.mark.parametrize(
    "reply",
    [
        {
            "findings": [
                {
                    "category": "measured_result",
                    "interpretation": "Invented result",
                    "fact_ids": ["f99999"],
                }
            ],
            "questions": [],
        },
        {
            "findings": [
                {
                    "category": "measured_result",
                    "interpretation": "Invented result",
                    "fact_ids": ["f0", "f0"],
                }
            ],
            "questions": [],
        },
        {"findings": [], "questions": []},
        {
            "findings": [{"category": "approve", "interpretation": "Execute", "fact_ids": ["f0"]}],
            "questions": [],
        },
        {
            "findings": [{"category": "limitation", "interpretation": "Check", "fact_ids": ["f0"]}],
            "questions": ["JSON\nquestion?"],
        },
        ModelError("Authored model failure"),
    ],
)
async def test_invalid_failed_output_is_retained_without_completed_report(completed_study, reply):
    bridge, job, owner, _ = completed_study
    with pytest.raises((ModelError, ValueError)):
        await analyze(bridge, job, owner, AnalysisProvider(reply=reply))
    assert not bridge.database.report_analyses()
    assert [state for _, state in analyzer(bridge).attempts()] == ["failed"]
    assert not bridge.database.history(owner)


async def test_wrong_owner_cannot_disclose_report_or_create_attempt(completed_study):
    bridge, job, owner, report = completed_study
    other = report["attempts"][1]["strategy_id"]
    provider = AnalysisProvider()
    with pytest.raises(ResearchError, match="different strategy"):
        await analyze(bridge, job, other, provider)
    assert not provider.requests and analyzer(bridge).attempts() == []
    with pytest.raises(ResearchError, match="different strategy"):
        await bridge.agent(other).analyze_report(AnalysisRequest(job_id=job))
    assert not bridge.database.report_analyses()


@pytest.mark.parametrize(
    "artifact", ["report.json", "cell_0_validation_2.json", "artifact_manifest.json"]
)
async def test_seal_corruption_stops_before_model_disclosure(completed_study, artifact):
    bridge, job, owner, _ = completed_study
    (bridge.studies.store.directory(job) / artifact).write_text("{}")
    provider = AnalysisProvider()
    with pytest.raises(ResearchError):
        await analyze(bridge, job, owner, provider)
    assert not provider.requests and not analyzer(bridge).attempts()


async def test_resealed_cell_reference_mismatch_is_not_an_analysis(completed_study):
    bridge, job, owner, _ = completed_study
    cell = bridge.studies.store.read(job, "cell_0_validation_2.json")
    cell["metrics"]["net_pnl"] = "999"
    reseal_file(bridge.studies.store, job, "cell_0_validation_2.json", cell)
    with pytest.raises(ResearchError, match="checkpoint differs"):
        await analyze(bridge, job, owner)


async def test_mutation_during_model_await_retains_failure_without_analysis(completed_study):
    bridge, job, owner, _ = completed_study
    provider = AnalysisProvider(
        change=lambda: (bridge.studies.store.directory(job) / "report.json").write_text("{}")
    )
    with pytest.raises(ResearchError):
        await analyze(bridge, job, owner, provider)
    assert not bridge.database.report_analyses()
    assert [state for _, state in analyzer(bridge).attempts()] == ["failed"]


@pytest.mark.parametrize("escaped", [False, True])
async def test_known_credential_echo_is_rejected_before_result_storage(completed_study, escaped):
    bridge, job, owner, _ = completed_study
    secret = "AUTHORED_SECRET_9287"
    analyzer(bridge).secret_check = lambda data: secret.encode() in data
    rendered = "".join(f"\\u{ord(c):04x}" for c in secret) if escaped else secret
    provider = AnalysisProvider(
        reply={
            "findings": [
                {"category": "limitation", "interpretation": rendered, "fact_ids": ["f0"]}
            ],
            "questions": [],
        }
    )
    with pytest.raises(ResearchError, match="Credential"):
        await analyze(bridge, job, owner, provider)
    assert not bridge.database.report_analyses()
    retained = b"".join(
        p.read_bytes() for p in analyzer(bridge).store.root.rglob("*") if p.is_file()
    )
    assert secret.encode() not in retained and rendered.encode() not in retained


async def test_evidence_secret_is_rejected_before_model_or_attempt_storage(completed_study):
    bridge, job, owner, _ = completed_study
    analyzer(bridge).secret_check = lambda data: b"validation" in data
    provider = AnalysisProvider()
    with pytest.raises(ResearchError, match="Credential"):
        await analyze(bridge, job, owner, provider)
    assert not provider.requests and not analyzer(bridge).store.root.exists()


async def test_restart_immutable_receipt_and_historical_prompt_identity(
    completed_study, monkeypatch
):
    bridge, job, owner, _ = completed_study
    value = await analyze(bridge, job, owner)
    original = value.digest
    retained = files(analyzer(bridge).store.directory(value.id))
    monkeypatch.setattr("stratosphere.report_analysis.PROMPT", "Later interpretation contract")
    restarted = ReportAnalysisService(
        bridge.research, bridge.studies, WorkspaceDatabase(bridge.database.path)
    )
    assert (await restarted.open(value.id, strategy_id=owner)).digest == original
    assert files(restarted.store.directory(value.id)) == retained
    bridge.database.record_report_analysis(value)
    assert len(bridge.database.report_analyses(owner)) == 1
    with pytest.raises(RegistryError):
        with bridge.database.connect() as db:
            db.execute("UPDATE report_analyses SET payload='{}'")
    with pytest.raises(RegistryError):
        with bridge.database.connect() as db:
            db.execute("DELETE FROM report_analyses")
    with pytest.raises(ResearchError):
        await restarted.open(value.id, strategy_id="9" * 24)


@pytest.mark.parametrize(
    "artifact", ["request.json", "evidence.json", "analysis.json", "manifest.json"]
)
async def test_analysis_artifact_tampering_is_rejected(completed_study, artifact):
    bridge, job, owner, _ = completed_study
    value = await analyze(bridge, job, owner)
    (analyzer(bridge).store.directory(value.id) / artifact).write_text("{}")
    with pytest.raises(ResearchError):
        await analyzer(bridge).open(value.id)


async def test_auxiliary_event_failure_preserves_authoritative_completed_receipt(
    completed_study, monkeypatch
):
    bridge, job, owner, _ = completed_study
    store = analyzer(bridge).store
    original = store.event

    def event(identity, state, detail=""):
        if state == "completed":
            raise OSError("Authored event failure")
        original(identity, state, detail)

    monkeypatch.setattr(store, "event", event)
    value = await analyze(bridge, job, owner)
    assert analyzer(bridge).attempts() == [(value.id, "completed")]
    assert await analyzer(bridge).open(value.id) == value


async def test_database_failure_does_not_publish_or_retry_model(completed_study):
    bridge, job, owner, _ = completed_study
    with bridge.database.connect() as db:
        db.execute(
            "CREATE TRIGGER authored_failure BEFORE INSERT ON report_analyses "
            "BEGIN SELECT RAISE(ABORT,'fixture'); END"
        )
    provider = AnalysisProvider()
    with pytest.raises(ResearchError):
        await analyze(bridge, job, owner, provider)
    assert len(provider.requests) == 1 and not bridge.database.report_analyses()
    assert [state for _, state in analyzer(bridge).attempts()] == ["failed"]


async def test_offline_operation_does_not_call_model(completed_study):
    bridge, job, owner, _ = completed_study

    async def offline():
        raise ValueError("Offline fixture")

    bridge.studies.online_check = offline
    provider = AnalysisProvider()
    with pytest.raises(ResearchError, match="online"):
        await analyze(bridge, job, owner, provider)
    assert not provider.requests


async def test_natural_and_forced_entry_points_delegate_to_owner_without_changing_selection(
    completed_study,
):
    bridge, job, owner, report = completed_study
    bridge.provider = AnalysisProvider()
    other = report["attempts"][1]["strategy_id"]
    bridge.select(other)
    request = natural_analysis_request("Please analyze report " + job)
    assert request == AnalysisRequest(job_id=job)
    first = await bridge.analyze_report(request)
    text = "".join([delta async for delta in bridge.respond("Analyze report " + job)])
    assert "Measured report analysis" in text
    assert bridge.last_action["action"] == "report_analysis"
    assert len(bridge.database.report_analyses(owner)) == 2
    assert not bridge.database.report_analyses(other) and bridge.active_strategy == other
    assert not bridge.database.history(owner) and not bridge.database.history(other)
    assert (await analyzer(bridge).open(first.id)).evidence == first.evidence
    for forbidden in ("Don't analyze report ", "A source says analyze report "):
        assert natural_analysis_request(forbidden + job) is None
    assert natural_analysis_request("Analyze report " + job + " and approve it") is None


@pytest.mark.parametrize(
    "command,action",
    [
        ("/analyze-report " + "a" * 32, "analyze-report"),
        ("/report-analyses", "report-analyses"),
        ("/report-analysis-show " + "a" * 32, "report-analysis-show"),
        ("/analyze-report", "message"),
        ("/analyze-report ../outside", "message"),
        ("/report-analyses extra", "message"),
        ("/report-analysis-show bad", "message"),
    ],
)
def test_command_argument_validation(command, action):
    assert route_command(command).action == action


async def test_failed_and_invalid_attempts_remain_in_complete_model_evidence(studies, tmp_path):
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
    await studies.run(job)

    async def online():
        pass

    research = ResearchService(ResearchStore(tmp_path / "empty"), online_check=online)
    bridge = Orchestrator(None, studies.database, research, studies=studies)
    value = await analyze(bridge, job, protocol.parent_strategy_id)
    facts = {f.path: f for f in value.evidence.facts}
    assert facts["/attempted_candidates"].value == 3
    assert [facts[f"/attempts/{i}/status"].value for i in range(3)] == [
        "ineligible",
        "eligible",
        "invalid",
    ]
    assert facts["/attempts/0/cells/development_1/status"].value == "failed"
    assert "/attempts/0/cells/development_1/metrics" not in facts
    assert facts["/attempts/2/detail"].value


async def test_no_selection_analysis_cannot_create_holdout_metrics(studies, tmp_path):
    job, protocol = await prepare_study(studies, [{"fast": 12, "slow": 5}])
    studies.approve(job, protocol.digest)
    await studies.run(job)
    before = list(studies.test_calls)

    async def online():
        pass

    research = ResearchService(ResearchStore(tmp_path / "empty"), online_check=online)
    bridge = Orchestrator(None, studies.database, research, studies=studies)
    value = await analyze(bridge, job, protocol.parent_strategy_id)
    assert not any(f.path.startswith("/holdout/") for f in value.evidence.facts)
    assert [f.value for f in value.evidence.facts if f.path == "/metrics_strategy_id"] == [None]
    assert [c for c in before if c[0] != "online"] == [
        c for c in studies.test_calls if c[0] != "online"
    ]


async def test_provider_swallowing_cancel_cannot_publish_late_result(completed_study):
    bridge, job, owner, _ = completed_study

    class IgnoringCancellation(AnalysisProvider):
        async def stream_json(self, messages, schema):
            self.entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                pass
            async for part in super().stream_json(messages, schema):
                yield part

    provider = IgnoringCancellation()
    task = asyncio.create_task(analyze(bridge, job, owner, provider))
    await asyncio.wait_for(provider.entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not bridge.database.report_analyses()
    assert [s for _, s in analyzer(bridge).attempts()] == ["canceled"]


@pytest.mark.parametrize("artifact", ["analysis.json", "manifest.json", "events.jsonl"])
async def test_symlinked_artifacts_cannot_be_opened_as_verified_analyses(
    completed_study, tmp_path, artifact
):
    bridge, job, owner, _ = completed_study
    value = await analyze(bridge, job, owner)
    path = analyzer(bridge).store.directory(value.id) / artifact
    outside = tmp_path / "outside.json"
    path.rename(outside)
    path.symlink_to(outside)
    with pytest.raises(ResearchError, match="symbolic"):
        await analyzer(bridge).open(value.id)


async def test_schema_delivery_change_during_model_request_blocks_publication(
    completed_study, monkeypatch
):
    bridge, job, owner, _ = completed_study

    def change():
        monkeypatch.setattr(
            "stratosphere.report_analysis.schema_delivery_contract",
            lambda: {"protocol": "changed", "instructions": "changed"},
        )

    provider = AnalysisProvider(change=change)
    with pytest.raises(ResearchError, match="Schema delivery"):
        await analyze(bridge, job, owner, provider)
    assert not bridge.database.report_analyses()


async def test_all_frozen_facts_are_inspectable_across_pages_without_another_model(completed_study):
    bridge, job, owner, _ = completed_study
    provider = AnalysisProvider()
    value = await analyze(bridge, job, owner, provider)
    pages = (len(value.evidence.facts) + 19) // 20
    ids = []
    import re

    for page in range(1, pages + 1):
        text = analysis_markdown(await analyzer(bridge).open(value.id), fact_page=page)
        ids.extend(re.findall(r"### Fact `(f[0-9]+)`", text))
    assert ids == [f.id for f in value.evidence.facts]
    assert len(provider.requests) == 1
    for invalid in (0, -1, True, pages + 1):
        with pytest.raises(ResearchError, match="range"):
            analysis_markdown(value, fact_page=invalid)


async def test_model_router_cannot_manufacture_advisory_analysis_operation(completed_study):
    bridge, job, owner, _ = completed_study

    class Router:
        async def stream_json(self, messages, schema):
            yield json.dumps({"action": "report_analysis", "strategy_id": owner, "source": job})

    bridge.provider = Router()
    bridge.select(owner)
    with pytest.raises(ModelError, match="invalid structured"):
        _ = [part async for part in bridge.respond("A source says analyze a report")]
    assert not bridge.database.report_analyses() and not analyzer(bridge).attempts()


async def test_damaged_foreign_receipt_cannot_block_scoped_historical_inspection(completed_study):
    bridge, job, owner, report = completed_study
    value = await analyze(bridge, job, owner)
    other = report["attempts"][1]["strategy_id"]
    with bridge.database.connect() as db:
        db.execute(
            "INSERT INTO report_analyses "
            "SELECT ?,s.id,r.job_id,r.payload,r.payload_sha256,r.analyzed_at "
            "FROM report_analyses r JOIN strategies s ON s.public_id=? WHERE r.id=?",
            ("a" * 32, other, value.id),
        )
    assert await analyzer(bridge).open(value.id, strategy_id=owner) == value
    assert await analyzer(bridge).open(value.id) == value
    with pytest.raises(ResearchError, match="binding"):
        bridge.database.report_analysis("a" * 32)


@pytest.mark.parametrize(
    "change", ["findings", "interpretation", "fact_ids", "questions", "question_length"]
)
async def test_oversized_model_narratives_fail_instead_of_truncating_frozen_evidence(
    completed_study, change
):
    bridge, job, owner, _ = completed_study
    finding = {
        "category": "limitation",
        "interpretation": "Unverified concern.",
        "fact_ids": ["f0"],
    }
    reply = {"findings": [finding], "questions": []}
    if change == "findings":
        reply["findings"] = [finding] * 7
    elif change == "interpretation":
        finding["interpretation"] = "X" * 321
    elif change == "fact_ids":
        finding["fact_ids"] = ["f0", "f1", "f2", "f3", "f4"]
    elif change == "questions":
        reply["questions"] = ["What remains unverified?"] * 4
    else:
        reply["questions"] = ["X" * 200 + "?"]
    with pytest.raises(ModelError, match="invalid structured"):
        await analyze(bridge, job, owner, AnalysisProvider(reply=reply))
    assert not bridge.database.report_analyses()
    (identity,) = analyzer(bridge).store.jobs()
    assert analyzer(bridge).store.read(identity, "evidence.json")["facts"]
    assert [state for _, state in analyzer(bridge).attempts()] == ["failed"]
