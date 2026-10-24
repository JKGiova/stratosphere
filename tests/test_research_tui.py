import asyncio
import json

import pytest
from textual.widgets import DataTable, Input, TextArea

from stratosphere.research.service import ResearchService
from stratosphere.research.store import ResearchStore
from stratosphere.research.widgets import ResearchReview
from stratosphere.tui import StratosphereApp


async def send(app, pilot, text):
    app.action_select_view("chat")
    await pilot.pause()
    field = app.query_one("#prompt", Input)
    field.value = text
    field.focus()
    await pilot.press("enter")
    await pilot.pause()


@pytest.fixture
def ui_service(tmp_path, research_snapshot):
    calls = []

    async def fetch():
        calls.append("fetch")
        return research_snapshot

    async def runner(store, job_id, multiplier):
        calls.append(multiplier)
        plan = store.plan(job_id)
        return {
            "plan_sha256": plan.digest,
            "normalized_sha256": research_snapshot.normalized_sha256,
            "result": {
                "engine_version": "1.231.0",
                "signals": [],
                "fills": [],
                "equity_curve": [
                    {
                        "timestamp_ns": b.close_ns,
                        "cash": "10000",
                        "base": "0",
                        "close": b.close,
                        "equity": "10000",
                    }
                    for b in research_snapshot.bars
                ],
                "pending_signal": None,
                "open_positions": 0,
                "balances": {"USDT": "10000"},
            },
        }

    async def online_check():
        pass

    service = ResearchService(
        ResearchStore(tmp_path / "ui-research"),
        fetcher=fetch,
        runner=runner,
        online_check=online_check,
    )
    service.calls = calls
    return service


async def connect(app, pilot):
    app.query_one("#api-key", Input).value = "tui-private-test-key"
    app.query_one("#api-key", Input).focus()
    await pilot.press("enter")
    await pilot.pause()


@pytest.mark.asyncio
async def test_source_review_approval_real_records_report_and_context(
    ui_service,
    research_provider,
    research_source,
):
    app = StratosphereApp(
        provider_factory=lambda settings: research_provider, research_service=ui_service
    )
    async with app.run_test(size=(144, 40)) as pilot:
        payload = json.loads(research_provider.reply)
        payload["name"] = "\x1b[31m" + payload["name"]
        research_provider.reply = json.dumps(payload)
        await connect(app, pilot)
        await send(app, pilot, "/new")
        assert app.research_form is not None and len(app.screen_stack) == 1
        app.query_one("#research-source", TextArea).text = research_source
        app.query_one("#prepare-research").focus()
        await pilot.press("enter")
        await pilot.pause()
        await app.research_worker.wait()
        await pilot.pause()
        assert len(research_provider.requests) == 1 and ui_service.calls == []
        review = app.query_one(ResearchReview)
        conversation = app.query_one("#conversation")
        assert review.region.y >= conversation.region.y
        assert review.region.bottom <= conversation.region.bottom
        job = review.job_id
        assert ui_service.store.state(job) == "awaiting_approval"
        assert app.query_one("#strategies", DataTable).row_count == 1
        assert app.query_one("#tests", DataTable).row_count == 1
        review.query_one("Button").focus()
        await pilot.pause()
        await pilot.press("enter")
        await pilot.pause()
        await app.research_worker.wait()
        await pilot.pause()
        assert ui_service.calls == ["fetch", 1, 2]
        assert ui_service.store.state(job) == "completed"
        assert app.query_one("#reports", DataTable).row_count == 1
        assert not app.query("#session-pane")
        await send(app, pilot, "/reports")
        assert app.active_view == "reports"
        await app.open_report_panel(job)
        assert "Net P&L" in app.query_one("#report-preview").content.markup
        for width, height in [(45, 12), (110, 36), (144, 40)]:
            await pilot.resize_terminal(width, height)
            await pilot.pause()
            assert app.active_view == "reports" and app.query_one("#reports-pane").display
        assert app.query_one("#composer").border_subtitle == "Enter send · / commands"
        assert "measured report context" in app.session.research_context
        await send(app, pilot, f"/report {job}")
        await send(app, pilot, f"/show {job}")
        assert app.active_view == "focus"
        content = list(app.query_one("#focus-body").children)[0].content
        assert "\x1b" not in content.markup
        assert len(research_provider.requests) == 1
        assert "tui-private-test-key" not in app.export_screenshot()
        await send(app, pilot, f"/approve {job} {review.plan_sha256}")
        assert ui_service.calls == ["fetch", 1, 2]
        app.action_select_view("library")
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        # A horizontal scrollbar must not consume the only data row.
        import xml.etree.ElementTree as ET

        rendered = " ".join(ET.fromstring(app.export_screenshot()).itertext())
        assert "Authored" in rendered


@pytest.mark.asyncio
async def test_research_form_secret_guard_validation_resize_and_cancel(
    ui_service,
    research_provider,
    research_source,
):
    app = StratosphereApp(
        provider_factory=lambda settings: research_provider, research_service=ui_service
    )
    async with app.run_test(size=(120, 36)) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/new")
        form = app.research_form
        field = app.query_one("#research-initial_balance", Input)
        field.value = "-1"
        field.focus()
        await pilot.press("enter")
        assert app.research_form is form
        assert ui_service.store.jobs() == []
        field.value = "10000"
        app.query_one("#research-source", TextArea).text = research_source + " tui-private-test-key"
        await pilot.press("enter")
        await pilot.pause()
        assert ui_service.store.jobs() == [] and research_provider.requests == []
        assert "API key detected" in str(app.query_one("#research-error").content)
        app.query_one("#research-source", TextArea).text = research_source
        for width, height in [(144, 32), (60, 36), (120, 24), (45, 12)]:
            await pilot.resize_terminal(width, height)
            await pilot.pause()
            assert app.research_form is form and field.value == "10000"
            assert app.query_one("#research-source", TextArea).text == research_source
        field.focus()
        await pilot.press("tab")
        assert app.query_one("#research-fee_rate", Input).has_focus
        await pilot.press("escape")
        await pilot.pause()
        assert app.research_form is None and app.query_one("#prompt", Input).has_focus
        assert research_provider.requests == [] and ui_service.calls == []


@pytest.mark.asyncio
async def test_running_research_can_pause_resume_without_focus_theft_or_extra_inference(
    ui_service,
    research_provider,
    research_source,
):
    app = StratosphereApp(
        provider_factory=lambda settings: research_provider, research_service=ui_service
    )
    async with app.run_test(size=(120, 36)) as pilot:
        await connect(app, pilot)
        from stratosphere.research.contracts import ResearchConfig

        job, _, plan = await ui_service.prepare(
            research_source, ResearchConfig(), research_provider
        )
        entered = asyncio.Event()
        original = ui_service.runner

        async def wait_runner(store, current, multiplier):
            entered.set()
            await asyncio.Event().wait()

        ui_service.runner = wait_runner
        await send(app, pilot, f"/approve {job} {plan.digest}")
        await asyncio.wait_for(entered.wait(), 3)
        await pilot.press("f2")
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        assert app.active_view == "focus"
        # Use the same validated command operation; no model chooses the job or interprets the ID.
        await app.handle_research_command("pause", (job,))
        from textual.worker import WorkerCancelled

        with pytest.raises(WorkerCancelled):
            await app.research_worker.wait()
        await pilot.pause()
        assert app.active_view == "focus"
        assert ui_service.store.state(job) == "paused"
        ui_service.runner = original
        await app.handle_research_command("resume", (job,))
        await app.research_worker.wait()
        await pilot.pause()
        assert ui_service.store.state(job) == "completed"
        assert app.query_one("#reports", DataTable).row_count == 1
        assert app.active_view == "focus" and app.focused.id == "focus-body"
        assert len(research_provider.requests) == 1


@pytest.mark.asyncio
async def test_quit_pauses_an_approved_job_for_restart(
    ui_service,
    research_provider,
    research_source,
):
    app = StratosphereApp(
        provider_factory=lambda settings: research_provider, research_service=ui_service
    )
    async with app.run_test(size=(120, 36)) as pilot:
        await connect(app, pilot)
        from stratosphere.research.contracts import ResearchConfig

        job, _, plan = await ui_service.prepare(
            research_source, ResearchConfig(), research_provider
        )
        entered = asyncio.Event()

        async def waiting(store, current, multiplier):
            entered.set()
            await asyncio.Event().wait()

        ui_service.runner = waiting
        await send(app, pilot, f"/approve {job} {plan.digest}")
        await entered.wait()
        await pilot.press("ctrl+q")
    assert not app.is_running
    assert ui_service.store.state(job) == "paused" and ui_service.active_job is None


@pytest.mark.asyncio
async def test_explicit_legacy_commands_preserve_existing_approval_gate(
    ui_service,
    research_source,
    research_draft,
):
    from stratosphere.agents.orchestrator import ROUTER_PROMPT
    from stratosphere.research.contracts import ResearchConfig

    class RoutedProvider:
        def __init__(self):
            self.route = {"action": "tests"}

        async def stream(self, messages):
            if messages[0]["content"] == ROUTER_PROMPT:
                yield json.dumps(self.route)
            else:
                yield research_draft.model_dump_json()

    provider = RoutedProvider()
    app = StratosphereApp(provider_factory=lambda settings: provider, research_service=ui_service)
    async with app.run_test(size=(144, 40)) as pilot:
        await connect(app, pilot)
        job, _, plan = await ui_service.prepare(research_source, ResearchConfig(), provider)
        record = app.session.register_job(job)
        app.session.select(record["public_id"])
        await send(app, pilot, f"/show {job}")
        await pilot.pause()
        assert ui_service.calls == []
        assert app.response_worker is None
        assert ui_service.store.state(job) == "awaiting_approval"
        ui_service.approve(job, plan.digest)
        app.session.sync_research()
        await send(app, pilot, f"/resume {job}")
        await pilot.pause()
        assert app.research_worker is not None
        await app.research_worker.wait()
        await pilot.pause()
        assert ui_service.store.state(job) == "completed"
        assert ui_service.calls == ["fetch", 1, 2]
        assert app.query_one("#reports", DataTable).row_count == 1
        provider.route = {"action": "report"}
        await send(app, pilot, f"/report {job}")
        await pilot.pause()
        assert app.active_view == "reports"
        provider.route = {"action": "new", "source": research_source}
        await send(app, pilot, "/new " + research_source)
        await pilot.pause()
        assert app.research_form is not None
        assert app.query_one("#research-source", TextArea).text == research_source
