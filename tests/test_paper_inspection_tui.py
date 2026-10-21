"""Online explicit account views populate Positions without disclosing data to a model."""

import pytest
from test_paper_decisions import activate, evaluate
from test_paper_decisions import decision as decision
from test_paper_inspection import inspector as inspector
from test_paper_reviews import Provider
from test_paper_runtime import stream as stream
from test_research_tui import connect, send
from test_research_tui import ui_service as ui_service
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Static

from stratosphere.commands import route_command
from stratosphere.paper.ledger import PaperError
from stratosphere.tui import StratosphereApp


@pytest.mark.parametrize(
    "text,action",
    [
        ("/paper", "paper"),
        ("/paper orders 2", "paper"),
        ("/paper health", "paper"),
        ("/paper fills 1", "paper"),
        ("/portfolio", "portfolio"),
        ("/paper /tmp/account", "message"),
        ("/paper positions 0", "message"),
        ("/paper positions -1", "message"),
        ("/paper orders 1 extra", "message"),
        ("/paper health 1000000000", "message"),
    ],
)
def test_explicit_inspection_arguments(text, action):
    assert route_command(text).action == action


@pytest.mark.parametrize("size", [(180, 48), (45, 12)])
async def test_positions_orders_fills_health_resize_and_read_failure(
    ui_service, inspector, decision, size
):
    activate(decision)
    evaluate(decision)
    before = inspector.journal.read_bytes()
    provider = Provider()
    app = StratosphereApp(
        research_service=ui_service,
        paper_inspector=inspector,
        provider_factory=lambda settings: provider,
    )
    async with app.run_test(size=size) as pilot:
        await connect(app, pilot)
        original_history = list(app.session.general.history)
        await send(app, pilot, "/paper")
        assert app.active_view == "portfolio" and app.focused.id == "portfolio-body"
        table = app.query_one("#positions", DataTable)
        assert table.row_count == 1 and "P/L" not in [
            column.label.plain for column in table.columns.values()
        ]
        body = app.query_one("#portfolio-body", VerticalScroll)
        await pilot.pause()
        assert body.region.height >= 3
        assert table.columns[next(iter(table.columns))].label.plain == "BTC"
        if size == (45, 12):
            assert table.region.height == 0
        else:
            assert table.region.height > 0
        await pilot.press("end")
        await pilot.pause()
        assert body.scroll_y > 0
        assert "Authored account" in str(app.query_one("#portfolio-status", Static).content)
        assert "1.01101" in app.query_one("#portfolio-preview", Static).content.markup
        await send(app, pilot, "/paper orders")
        assert app.active_view == "focus"
        assert "reserved" in app.query_one("#focus-body").query_one(Static).content.markup
        await pilot.press("end")
        await pilot.pause()
        assert app.query_one("#focus-body", VerticalScroll).scroll_y > 0
        await send(app, pilot, "/paper fills")
        assert "No retained rows" in app.query_one("#focus-body").query_one(Static).content.markup
        await send(app, pilot, "/paper health")
        assert "closed_bar_stream" in app.query_one("#focus-body").query_one(Static).content.markup
        await send(app, pilot, "/portfolio")
        assert app.active_view == "portfolio"
        await send(app, pilot, "Inspect the authored paper account health")
        await app.response_worker.wait()
        await pilot.pause()
        assert (
            app.active_view == "focus" and app.session.last_action["action"] == "paper_inspection"
        )
        await pilot.press("tab", "shift+tab")

        async def offline():
            raise PaperError("Offline inspection is unavailable")

        inspector.online_check = offline
        await send(app, pilot, "/paper")
        assert table.row_count == 0
        assert "refresh unavailable" in str(app.query_one("#portfolio-status", Static).content)
        assert not provider.calls and not ui_service.calls
        assert app.session.general.history == original_history
        assert inspector.journal.read_bytes() == before


async def test_no_journal_does_not_create_account_or_invoke_model(ui_service):
    provider = Provider()
    app = StratosphereApp(research_service=ui_service, provider_factory=lambda settings: provider)
    async with app.run_test(size=(100, 30)) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/paper")
        assert not app.paper_inspector.journal.exists()
        assert not provider.calls
        assert "no account was created" in " ".join(
            str(widget.content) for widget in app.query(Static)
        )
