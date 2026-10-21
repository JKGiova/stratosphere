"""Both explicit entry points expose verified private analyses in responsive Reports."""

import asyncio

import pytest
from test_report_analysis import AnalysisProvider
from test_report_evidence import completed_research as completed_research
from test_research_service import service as service
from test_research_tui import connect, send
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Static

from stratosphere.tui import StratosphereApp


@pytest.mark.parametrize("size", [(180, 48), (45, 12)])
async def test_commands_chat_owner_routing_navigation_and_scrolling(completed_research, size):
    bridge, job, _, _ = completed_research
    provider = AnalysisProvider(wait=True)
    app = StratosphereApp(research_service=bridge.research, provider_factory=lambda _: provider)
    async with app.run_test(size=size) as pilot:
        await connect(app, pilot)
        owner = app.database.strategy_for_job(job)["public_id"]
        plan = bridge.research.store.plan(job)
        from stratosphere.research.contracts import build_spec

        other = app.database.register(
            build_spec(plan.draft.model_copy(update={"fast_period": 4})),
            "Other private strategy",
            {"text": "other private source"},
        )["public_id"]
        app.session.select(other)
        app.database.append_turn(other, "PRIVATE_OTHER", "private", "fixture")
        await send(app, pilot, "/analyze-report " + job)
        await asyncio.wait_for(provider.entered.wait(), 5)
        app.session.select(owner)
        app.session.select(other)
        provider.release.set()
        await app.research_worker.wait()
        await pilot.pause()
        (record,) = app.database.report_analyses()
        identity = record["id"]
        assert record["strategy_id"] == owner and app.session.active_strategy == other
        assert "PRIVATE_OTHER" not in str(provider.requests)
        assert app.query_one("#reports", DataTable).row_count == 2
        await send(app, pilot, "/reports")
        await pilot.pause()
        assert app.active_view == "reports"
        assert (
            "Model interpretations are unverified"
            in app.query_one("#report-preview", Static).content.markup
        )
        body = app.query_one("#report-body", VerticalScroll)
        body.focus()
        await pilot.press("end")
        await pilot.pause()
        assert body.scroll_y > 0
        await send(app, pilot, "/report-analysis-show " + identity)
        assert app.active_view == "focus" and app.focused.id == "focus-body"
        await pilot.press("pagedown")
        await pilot.pause()
        assert app.query_one("#focus-body", VerticalScroll).scroll_y > 0
        await send(app, pilot, "Analyze report " + job)
        await app.response_worker.wait()
        await pilot.pause()
        assert app.session.responding_strategy == owner
        assert app.query_one("#reports", DataTable).row_count == 3
        assert len(provider.requests) == 2 and not app.database.history(owner)
        assert len(app.database.history(other)) == 2
        await send(app, pilot, "/report-analyses")
        assert "completed" in " ".join(str(x.content) for x in app.query(Static))
        await pilot.press("tab", "shift+tab")
        assert app.selected_report_key.startswith("report-analysis:")
