"""The same specialist source review is reachable by chat and forced commands."""

import pytest
from test_research_tui import connect, send
from test_research_tui import ui_service as ui_service
from test_source_reviews import ReviewProvider
from test_source_reviews import reviews as reviews
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Static

from stratosphere.commands import route_command
from stratosphere.tui import StratosphereApp


@pytest.mark.parametrize(
    "text,action",
    [
        ("/source-review " + "a" * 32, "source-review"),
        ("/source-review " + "a" * 32 + " " + "1" * 24, "source-review"),
        ("/source-reviews", "source-reviews"),
        ("/source-review-show " + "a" * 32, "source-review-show"),
        ("/source-review", "message"),
        ("/source-reviews unexpected", "message"),
        ("/source-review-show", "message"),
    ],
)
def test_source_review_command_arguments(text, action):
    assert route_command(text).action == action


async def test_review_report_focus_keyboard_resize_and_natural_language_share_operation(
    ui_service, reviews, research_plan
):
    _, _, _, sources, snapshot, _, _ = reviews
    provider = ReviewProvider()
    app = StratosphereApp(
        provider_factory=lambda settings: provider,
        research_service=ui_service,
        source_service=sources,
    )
    identity = app.database.register(
        research_plan.specification, "UI source fixture", {"text": "authored"}
    )
    async with app.run_test(size=(144, 40)) as pilot:
        await connect(app, pilot)
        app.session.select(identity["public_id"])
        await send(app, pilot, "/source-review " + snapshot.id)
        await app.research_worker.wait()
        await pilot.pause()
        (record,) = app.database.source_reviews()
        review_id = record["id"]
        assert app.query_one("#reports", DataTable).row_count == 1
        await send(app, pilot, "/reports")
        await pilot.pause()
        assert app.active_view == "reports" and app.focused.id == "reports-pane"
        await pilot.resize_terminal(45, 12)
        await pilot.pause()
        await pilot.press("pagedown")
        await pilot.pause()
        body = app.query_one("#report-body", VerticalScroll)
        assert app.focused is body and body.scroll_y > 0
        await send(app, pilot, "/source-review-show " + review_id)
        assert app.active_view == "focus" and app.focused.id == "focus-body"
        text = app.query_one("#focus-body").children[0].content.markup
        assert "source lines 3–3" in text and "Execution remains blocked" in text
        await pilot.press("pagedown")
        await pilot.pause()
        assert app.query_one("#focus-body", VerticalScroll).scroll_y > 0
        provider.route = {
            "action": "source_review",
            "source": snapshot.id,
            "strategy_id": None,
            "target": None,
        }
        await send(app, pilot, "Review source " + snapshot.id)
        await app.response_worker.wait()
        await pilot.pause()
        assert (
            len(app.database.source_reviews()) == 2
            and app.query_one("#reports", DataTable).row_count == 2
        )
        assert (
            len(app.database.catalog()) == 1
            and not app.research.store.jobs()
            and not ui_service.calls
        )
        assert all(r["strategy_id"] == identity["public_id"] for r in app.database.source_reviews())
        await send(app, pilot, "/source-reviews")
        assert "completed" in " ".join(str(x.content) for x in app.query(Static))


async def test_source_review_requires_selected_agent_and_invalid_evidence_leaves_no_report(
    ui_service, reviews, research_plan
):
    _, _, _, sources, snapshot, _, _ = reviews
    provider = ReviewProvider(
        {
            "findings": [
                {
                    "category": "rule_candidate",
                    "interpretation": "wrong",
                    "evidence_index": 99,
                }
            ],
            "questions": [],
        }
    )
    app = StratosphereApp(
        provider_factory=lambda settings: provider,
        research_service=ui_service,
        source_service=sources,
    )
    identity = app.database.register(research_plan.specification, "Fixture", {"text": "authored"})
    async with app.run_test(size=(100, 30)) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/source-review " + snapshot.id)
        assert not provider.requests and not app.database.source_reviews()
        await send(app, pilot, "/source-review " + snapshot.id + " " + identity["public_id"])
        await app.research_worker.wait()
        await pilot.pause()
        assert (
            not app.database.source_reviews()
            and app.query_one("#reports", DataTable).row_count == 0
        )
        assert not app.research.store.jobs() and not app.database.history(identity["public_id"])
        assert "unavailable evidence passage" in " ".join(str(x.content) for x in app.query(Static))
