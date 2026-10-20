"""Forced and chat inspection share Reports, responsive scrolling and private ownership."""

import pytest
from test_report_evidence import completed_research as completed_research
from test_report_evidence import service as service
from test_research_tui import connect, send
from textual.containers import VerticalScroll
from textual.widgets import Static

from stratosphere.tui import StratosphereApp


class NoModel:
    async def stream_json(self, *args):
        raise AssertionError("Report inspection must not request model output")
        yield ""


@pytest.mark.parametrize("size", [(180, 48), (45, 12)])
async def test_reports_show_paged_rows_and_natural_requests_without_model_calls(
    completed_research, size
):
    bridge, job, owner, _ = completed_research
    app = StratosphereApp(research_service=bridge.research, provider_factory=lambda _: NoModel())
    async with app.run_test(size=size) as pilot:
        await connect(app, pilot)
        owner = app.database.strategy_for_job(job)["public_id"]
        await send(app, pilot, f"/report-data {job} equity stress 2")
        await pilot.pause()
        assert app.selected_report_key == job
        preview = app.query_one("#report-preview", Static).content.markup
        assert "page 2/38" in preview and "cost_multiplier" in preview and "## Row 20" in preview
        body = app.query_one("#report-body", VerticalScroll)
        body.focus()
        await pilot.press("end")
        await pilot.pause()
        assert body.scroll_y > 0 and body.size.height >= 2
        await send(app, pilot, f"Inspect report {job} equity stress 3")
        await app.response_worker.wait()
        await pilot.pause()
        assert "page 3/38" in app.query_one("#report-preview", Static).content.markup
        assert body.scroll_y == 0
        assert app.session.responding_strategy == owner
        assert not app.database.history(owner)
        assert app.session.general.history == []
        await pilot.press("tab", "shift+tab")
        await pilot.pause()
        assert app.selected_report_key == job
