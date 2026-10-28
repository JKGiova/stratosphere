"""Chat and forced commands open the same exact-identity study review for registered variants."""

import json

import pytest
from test_bounded_studies import studies as studies
from test_research_tui import connect, send
from textual.widgets import DataTable, Input, TextArea

from stratosphere.agents.orchestrator import Orchestrator
from stratosphere.phase0.contracts import StrategySpec
from stratosphere.research.service import ResearchService
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.studies.widgets import StudyForm
from stratosphere.tui import StratosphereApp


class ProposalProvider:
    def __init__(self, alternatives=None):
        self.calls = []
        self.alternatives = alternatives if alternatives is not None else [{"fast": 4, "slow": 10}]

    async def stream_json(self, messages, schema):
        self.calls.append(messages)
        yield json.dumps(
            {"action": "study"}
            if "action" in schema.get("properties", {})
            else {
                "hypothesis": (
                    "Longer pairs reduce validation fills and costs; reject if costs do not fall."
                ),
                **(
                    {"alternatives": self.alternatives}
                    if "alternatives" in schema.get("properties", {})
                    else {}
                ),
                "month": "2025-01",
            }
        )


async def test_strategy_tests_and_study_proposal_need_no_parent_job_or_price_request(
    studies, tmp_path
):
    research = ResearchService(
        ResearchStore(tmp_path / "empty-research"), online_check=studies.online_check
    )
    provider = ProposalProvider()
    bridge = Orchestrator(provider, studies.database, research, studies=studies)
    identity = studies.test_parent["public_id"]
    bridge.select(identity)
    reply = "".join(
        [p async for p in bridge.agent(identity).respond("Test this definition", action="tests")]
    )
    assert bridge.agent(identity).last_action == {"action": "study", "strategy_id": identity}
    assert "approving any test" in reply and not provider.calls
    _ = "".join([p async for p in bridge.respond("Propose a bounded experiment")])
    assert bridge.last_action["strategy_id"] == identity and "job_id" not in bridge.last_action
    assert bridge.last_action["suggestion"]["max_simulations"] == 10
    assert len(provider.calls) == 2 and not research.store.jobs() and not studies.store.jobs()
    assert not any(c[0] in {"fetch", "run", "pin"} for c in studies.test_calls)


@pytest.mark.parametrize(
    "alternatives",
    [
        [],
        [{"fast": 3, "slow": 8}],
        [{"fast": 4, "slow": 10}, {"fast": 4, "slow": 10}],
        [{"fast": 10, "slow": 4}],
    ],
)
async def test_model_alternatives_cannot_duplicate_or_replace_the_fixed_baseline(
    studies, tmp_path, alternatives
):
    from stratosphere.models.streaming import ModelError

    provider = ProposalProvider(alternatives)
    research = ResearchService(ResearchStore(tmp_path / "jobs"), online_check=studies.online_check)
    bridge = Orchestrator(provider, studies.database, research, studies=studies)
    identity = studies.test_parent["public_id"]
    bridge.select(identity)
    with pytest.raises(ModelError):
        _ = [p async for p in bridge.respond("Propose a bounded experiment")]
    assert not studies.store.jobs() and not research.store.jobs() and not bridge.last_action
    assert not studies.database.history(identity)


async def test_unsupported_registered_behavior_stops_before_a_model_or_test(studies, tmp_path):
    payload = studies.test_plan.specification.model_dump(mode="json")
    payload["execution_requirements"]["session_end"] = "close"
    record = studies.database.register(
        StrategySpec.model_validate(payload), "Different exits", {"kind": "authored"}
    )
    provider = ProposalProvider()
    research = ResearchService(ResearchStore(tmp_path / "jobs"), online_check=studies.online_check)
    bridge = Orchestrator(provider, studies.database, research, studies=studies)
    with pytest.raises(ResearchError, match="differs from"):
        _ = [p async for p in bridge.agent(record["public_id"]).respond("Compare", action="study")]
    assert not provider.calls and not studies.store.jobs()


async def test_inline_registry_form_pins_strategy_and_costs_through_approval_and_report(
    studies, tmp_path
):
    provider = ProposalProvider()
    research = ResearchService(
        ResearchStore(tmp_path / "ui-research"), online_check=studies.online_check
    )
    app = StratosphereApp(
        provider_factory=lambda settings: provider, research_service=research, study_service=studies
    )
    studies.database = app.database
    first = app.database.register(
        studies.test_plan.specification, "Registry baseline", {"kind": "authored"}
    )
    other_spec = StrategySpec.model_validate(
        studies.test_plan.specification.model_dump()
        | {"position_sizing": {"mode": "fixed_base", "amount": "0.02"}}
    )
    other = app.database.register(other_spec, "Other strategy", {"kind": "authored"})
    async with app.run_test(size=(45, 12)) as pilot:
        await connect(app, pilot)
        app.session.select(first["public_id"])
        await send(app, pilot, "/study")
        assert isinstance(app.research_form, StudyForm) and app.research_form.registered
        assert app.research_form.parent_job is None
        app.query_one("#study-balance", Input).focus()
        await pilot.pause()
        assert app.query_one("#conversation").scrollable_content_region.contains_region(
            app.query_one("#study-balance").region
        )
        app.query_one("#study-fee", Input).value = "0.002"
        app.query_one(
            "#study-hypothesis", TextArea
        ).text = "Longer pairs reduce modeled costs; reject if they do not."
        app.query_one("#study-candidates", Input).value = "3/8,4/10"
        app.query_one("#study-budget", Input).value = "10"
        # Selection changes must not silently retarget the form that is already being reviewed.
        app.session.select(other["public_id"])
        app.query_one("#prepare-study").focus()
        await pilot.press("enter")
        await app.research_worker.wait()
        await pilot.pause()
        (job,) = studies.store.jobs()
        protocol = studies.store.protocol(job)
        assert protocol.parent_strategy_id == first["public_id"]
        assert protocol.config.fee_rate == "0.002" and not research.store.jobs()
        assert not provider.calls and not any(c[0] in {"run", "fetch"} for c in studies.test_calls)
        await send(app, pilot, f"/approve {job} {protocol.digest}")
        await app.research_worker.wait()
        await pilot.pause()
        assert studies.store.state(job) == "completed"
        assert app.query_one("#reports", DataTable).row_count == 1
        await send(app, pilot, f"/report {job}")
        assert app.active_view == "reports"
        assert "holdout" in app.query_one("#report-preview").content.markup.lower()
        for size in ((144, 40), (70, 50), (45, 12)):
            await pilot.resize_terminal(*size)
            await pilot.pause()
            assert app.query_one("#reports-pane").display
        assert (
            studies.database.reports(first["public_id"])[0]["report"]["schema_version"]
            == "study-report/2"
        )
        assert not studies.database.reports(other["public_id"])


async def test_explicit_legacy_study_form_preserves_owner_and_cancel_without_model_routing(
    studies, tmp_path
):
    provider = ProposalProvider()
    research = ResearchService(
        ResearchStore(tmp_path / "ui-research"), online_check=studies.online_check
    )
    app = StratosphereApp(
        provider_factory=lambda settings: provider, research_service=research, study_service=studies
    )
    studies.database = app.database
    record = app.database.register(
        studies.test_plan.specification, "Selected baseline", {"kind": "authored"}
    )
    async with app.run_test(size=(144, 40)) as pilot:
        await connect(app, pilot)
        app.session.select(record["public_id"])
        await send(app, pilot, "/study " + record["public_id"])
        await pilot.pause()
        assert isinstance(app.research_form, StudyForm) and app.research_form.registered
        assert app.query_one("#study-candidates", Input).value == "3/8"
        await pilot.press("escape")
        await pilot.pause()
        assert not app.research_form and not studies.store.jobs() and not research.store.jobs()

    assert provider.calls == []


async def test_explicit_parameter_pairs_are_application_owned_and_the_model_cannot_rewrite_them(
    studies, tmp_path
):
    provider = ProposalProvider()
    research = ResearchService(ResearchStore(tmp_path / "jobs"), online_check=studies.online_check)
    bridge = Orchestrator(provider, studies.database, research, studies=studies)
    identity = studies.test_parent["public_id"]
    bridge.select(identity)
    _ = [p async for p in bridge.respond("Compare baseline 3/8 with 5/12 and 6/15 in January 2025")]
    assert bridge.last_action["suggestion"]["candidates"] == [
        {"fast": 3, "slow": 8},
        {"fast": 5, "slow": 12},
        {"fast": 6, "slow": 15},
    ]
    assert bridge.last_action["suggestion"]["max_simulations"] == 14
    assert not studies.store.jobs() and not research.store.jobs()
    recorded = json.loads(studies.database.history(identity)[-1]["content"])
    assert recorded["status"] == "unapproved_proposal"
    assert recorded["proposal"] == bridge.last_action["suggestion"]


def test_parameter_references_exclude_urls_and_do_not_duplicate_textual_baseline_mentions():
    from stratosphere.agents.orchestrator import requested_study_pairs

    pairs = requested_study_pairs(
        "SMA 3/8, then 4/10. Keep 3/8 as baseline; https://example.com/5/12 and 0.5/2"
    )
    assert [(p.fast, p.slow) for p in pairs] == [(3, 8), (4, 10)]
    assert [(p.fast, p.slow) for p in requested_study_pairs("Compare -3/8 and 4/-10")] == [
        (-3, 8),
        (4, -10),
    ]


@pytest.mark.parametrize("kind", ["model", "connector"])
async def test_escaped_credentials_in_registry_study_forms_are_rejected_before_storage(
    studies, tmp_path, kind
):
    import time

    from pydantic import SecretStr

    from stratosphere.legacy.liquid import ReadSession

    provider = ProposalProvider()
    research = ResearchService(
        ResearchStore(tmp_path / "ui-research"), online_check=studies.online_check
    )
    app = StratosphereApp(
        provider_factory=lambda settings: provider, research_service=research, study_service=studies
    )
    studies.database = app.database
    record = app.database.register(
        studies.test_plan.specification, "Baseline", {"kind": "authored"}
    )
    async with app.run_test(size=(144, 40)) as pilot:
        await connect(app, pilot)
        app.session.select(record["public_id"])
        await send(app, pilot, "/study")
        key = 'private-"\\-credential'
        if kind == "model":
            app.settings = app.settings.model_copy(update={"api_key": SecretStr(key)})
        else:
            app.liquid.session = ReadSession(SecretStr(key), time.monotonic() + 60)
        app.query_one("#study-hypothesis", TextArea).text = "A hypothesis containing " + key
        app.query_one("#prepare-study").focus()
        await pilot.press("enter")
        await pilot.pause()
        assert app.research_form is not None and not studies.store.jobs()
        assert "Remove credentials" in str(app.query_one("#study-error").content)
        assert not research.store.jobs() and not provider.calls


@pytest.mark.parametrize("invent_target", [False, True])
async def test_changing_library_selection_during_routing_cannot_retarget_private_work(
    studies, tmp_path, invent_target
):
    import asyncio

    from stratosphere.models.streaming import ModelError

    identity = studies.test_parent["public_id"]
    other_spec = StrategySpec.model_validate(
        studies.test_plan.specification.model_dump()
        | {"position_sizing": {"mode": "fixed_base", "amount": "0.02"}}
    )
    other = studies.database.register(other_spec, "Other", {"kind": "authored"})["public_id"]
    studies.database.append_turn(
        identity, "PRIVATE_FIRST_MARKER", "First private context", "fixture"
    )
    studies.database.append_turn(other, "PRIVATE_OTHER_MARKER", "Other private context", "fixture")
    entered, released = asyncio.Event(), asyncio.Event()

    class WaitingProvider(ProposalProvider):
        async def stream_json(self, messages, schema):
            if "action" in schema.get("properties", {}):
                self.calls.append(messages)
                entered.set()
                await released.wait()
                yield json.dumps(
                    {"action": "study", "strategy_id": other if invent_target else None}
                )
            else:
                async for part in super().stream_json(messages, schema):
                    yield part

    provider = WaitingProvider()
    research = ResearchService(ResearchStore(tmp_path / "jobs"), online_check=studies.online_check)
    bridge = Orchestrator(provider, studies.database, research, studies=studies)
    bridge.select(identity)

    async def request():
        return [p async for p in bridge.respond("Propose another bounded experiment")]

    task = asyncio.create_task(request())
    await asyncio.wait_for(entered.wait(), 5)
    bridge.select(other)
    released.set()
    if invent_target:
        with pytest.raises(ModelError):
            await task
        assert not bridge.last_action and len(studies.database.history(identity)) == 2
    else:
        await task
        assert (
            bridge.responding_strategy == identity and bridge.last_action["strategy_id"] == identity
        )
        assert bridge.active_strategy == other
        assert "PRIVATE_OTHER_MARKER" not in json.dumps(provider.calls[-1])
        assert len(studies.database.history(identity)) == 4
    assert len(studies.database.history(other)) == 2 and not studies.store.jobs()
