"""Explicit import creates one stable agent; its separate study still requires approval."""

import pytest
from test_research_tui import connect, send
from test_research_tui import ui_service as ui_service
from test_source_registration import imports as imports
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Static

from stratosphere.commands import route_command
from stratosphere.sources.registration import RegistrationRequest
from stratosphere.tui import StratosphereApp


class NoModel:
    async def stream_json(self, *args):
        raise AssertionError("Registration cannot require a model")
        yield ""


@pytest.mark.parametrize(
    "text,action",
    [
        ("/source-register " + "a" * 32, "source-register"),
        ("/source-register " + "a" * 32 + ' "Name with spaces" ' + "1" * 24, "source-register"),
        ("/source-registrations", "source-registrations"),
        ("/source-registration-show " + "a" * 32, "source-registration-show"),
        ("/source-register", "message"),
        ("/source-registration-show", "message"),
    ],
)
def test_source_registration_arguments_are_explicit(text, action):
    command = route_command(text)
    assert command.action == action
    if action == "source-register":
        RegistrationRequest.arguments(command.args)


@pytest.mark.parametrize("size", [(180, 48), (45, 12)])
async def test_registration_forced_chat_receipt_resize_and_stable_identity(
    imports, ui_service, size
):
    service, _, source = imports
    app = StratosphereApp(
        research_service=ui_service,
        source_service=service.sources,
        provider_factory=lambda _: NoModel(),
    )
    async with app.run_test(size=size) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/source-register " + source.id + ' "Explicit native import"')
        await app.research_worker.wait()
        assert app.query_one("#strategies", DataTable).row_count == 1
        (receipt_id,) = app.database.source_registration_ids()
        receipt = await app.source_registrations.open(receipt_id)
        assert app.session.active_strategy is None
        await send(app, pilot, "/source-registration-show " + receipt_id)
        body = app.query_one("#focus-body", VerticalScroll)
        assert receipt.strategy_id in body.query_one(Static).content.markup
        body.focus()
        await pilot.press("end")
        await pilot.pause()
        assert body.scroll_y > 0
        app.session.select(receipt.strategy_id)
        await send(app, pilot, "Register source " + source.id)
        await app.response_worker.wait()
        assert len(app.database.source_registration_ids()) == 2
        assert app.query_one("#strategies", DataTable).row_count == 1
        assert app.session.active_strategy == receipt.strategy_id
        assert not app.database.history(receipt.strategy_id)
        assert not app.research.store.jobs() and not app.studies.store.jobs()
        assert not app.database.reports() and app.query_one("#reports", DataTable).row_count == 0
