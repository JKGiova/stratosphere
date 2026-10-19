"""Explicit private-account reviews share one operation and survive UI restart."""

import asyncio

import pytest
from test_paper_decisions import decision as decision
from test_paper_reviews import Provider
from test_paper_reviews import reviews as reviews
from test_paper_runtime import stream as stream
from test_research_tui import connect, send
from test_research_tui import ui_service as ui_service
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Static
from textual.worker import WorkerCancelled

from stratosphere.commands import route_command
from stratosphere.tui import StratosphereApp


@pytest.mark.parametrize(
    "text,action",
    [
        ("/paper-review", "paper-review"),
        ("/paper-reviews", "paper-reviews"),
        ("/paper-review-show " + "a" * 32, "paper-review-show"),
        ("/paper-review /tmp/account.sqlite3", "message"),
        ("/paper-reviews unexpected", "message"),
        ("/paper-review-show ../account", "message"),
        ("/paper-review-show", "message"),
    ],
)
def test_review_command_arguments(text, action):
    assert route_command(text).action == action


async def test_explicit_chat_and_command_review_reports_resize_restart(ui_service, reviews):
    service, provider = reviews
    original = service.journal.read_bytes()
    kwargs = dict(
        provider_factory=lambda settings: provider,
        research_service=ui_service,
        paper_review_service=service,
    )
    app = StratosphereApp(**kwargs)
    async with app.run_test(size=(180, 48)) as pilot:
        assert "authored account snapshot" in " ".join(str(x.content) for x in app.query(Static))
        await send(app, pilot, "/paper-review")
        assert not provider.calls and not service.store.jobs()
        await connect(app, pilot)
        await send(app, pilot, "/paper-review")
        await app.research_worker.wait()
        await pilot.pause()
        (record,) = service.records()
        assert app.request_state == "READY"
        assert app.query_one("#reports", DataTable).row_count == 1
        await send(app, pilot, "/paper-review-show " + record.id)
        assert app.active_view == "reports"
        assert (
            "interpretation quality unverified" in app.query_one("#report-preview").content.markup
        )
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        await pilot.press("pagedown")
        await pilot.pause()
        assert app.query_one("#report-body", VerticalScroll).scroll_y > 0
        await send(app, pilot, "Please review the authored paper account budget and risk")
        await app.response_worker.wait()
        await app.research_worker.wait()
        await pilot.pause()
        assert len(provider.calls) == 4 and len(service.records()) == 2
        assert not app.database.catalog() and not app.session.general.history
        assert not ui_service.calls and not ui_service.store.jobs()
        assert service.journal.read_bytes() == original
    restarted = StratosphereApp(**kwargs)
    async with restarted.run_test(size=(100, 30)) as pilot:
        assert restarted.query_one("#reports", DataTable).row_count == 2
        await connect(restarted, pilot)
        await send(restarted, pilot, "/paper-review-show " + record.id)
        assert restarted.active_view == "reports"
        assert len(provider.calls) == 4


async def test_missing_journal_does_not_call_model_or_create_account(ui_service, tmp_path):
    provider = Provider()
    app = StratosphereApp(provider_factory=lambda settings: provider, research_service=ui_service)
    async with app.run_test(size=(100, 30)) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/paper-review")
        await app.research_worker.wait()
        assert not app.paper_reviews.journal.exists()
        assert not app.paper_reviews.store.root.exists()
        assert not provider.calls
        assert app.request_state == "MODEL SET · NOT YET VERIFIED"
        assert "no account was created" in " ".join(str(x.content) for x in app.query(Static))


async def test_busy_cancel_keeps_partial_attempt_and_no_account_mutation(ui_service, reviews):
    service, provider = reviews
    entered, hold = asyncio.Event(), asyncio.Event()
    original = provider.stream_json

    async def pause(messages, schema):
        if schema["title"] == "RiskProposal":
            entered.set()
            await hold.wait()
        async for value in original(messages, schema):
            yield value

    provider.stream_json = pause
    app = StratosphereApp(
        provider_factory=lambda settings: provider,
        research_service=ui_service,
        paper_review_service=service,
    )
    before = service.journal.read_bytes()
    async with app.run_test(size=(100, 30)) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/paper-review")
        await entered.wait()
        assert app.request_state == "RESEARCH · Reviewing authored paper"
        assert "Model unavailable" not in str(app.query_one("#status").content)
        worker = app.research_worker
        await send(app, pilot, "/paper-review")
        assert app.research_worker is worker and len(provider.calls) == 1
        await send(app, pilot, "/model")
        assert not app.query("#api-key")
        await send(app, pilot, "/cancel")
        with pytest.raises(WorkerCancelled):
            await worker.wait()
        await pilot.pause()
        (attempt,) = service.attempts()
        assert attempt["state"] == "canceled" and not service.records()
        assert app.request_state == "CANCELED"
        assert app.query_one("#reports", DataTable).row_count == 0
        assert service.journal.read_bytes() == before
