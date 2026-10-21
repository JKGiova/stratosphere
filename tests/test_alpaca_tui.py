"""Paper setup remains inline, masked, read-only and separate from model histories."""

import pytest
from test_alpaca import KEY, SECRET
from test_alpaca import paper as paper
from test_research_tui import connect, send
from test_research_tui import ui_service as ui_service
from textual.widgets import Input, Static

from stratosphere.commands import route_command
from stratosphere.connectors.alpaca_widgets import AlpacaSetup
from stratosphere.tui import StratosphereApp


@pytest.mark.parametrize(
    "command", ["/alpaca", "/alpaca-connect", "/alpaca-disconnect", "/alpaca-save"]
)
def test_only_fixed_alpaca_commands(command):
    assert route_command(command).action == command[1:]
    assert route_command(command + " extra").action == "message"


def test_environment_paper_credentials_are_guarded_before_connection(ui_service, monkeypatch):
    monkeypatch.setenv("APCA_API_KEY_ID", KEY)
    monkeypatch.setenv("APCA_API_SECRET_KEY", SECRET)
    app = StratosphereApp(research_service=ui_service)
    assert app.alpaca.credentials is None
    assert app.contains_session_credential(KEY.encode())
    assert app.contains_session_credential(SECRET.encode())
    assert app.sources.store.has_secret(("prefix " + SECRET).encode())


@pytest.mark.parametrize("size", [(180, 48), (45, 12)])
async def test_inline_masking_check_and_export_without_a_model(ui_service, paper, size):
    client, provider = paper
    client.disconnect()

    def forbidden(settings):
        raise AssertionError("A model must not be constructed for the paper diagnostic")

    app = StratosphereApp(
        research_service=ui_service, alpaca_connection=client, provider_factory=forbidden
    )
    async with app.run_test(size=size) as pilot:
        await pilot.press("escape")
        await send(app, pilot, "/alpaca-connect")
        assert isinstance(app.setup, AlpacaSetup) and len(app.screen_stack) == 1
        first = app.query_one("#alpaca-key-id", Input)
        second = app.query_one("#alpaca-secret-key", Input)
        first.value = KEY
        second.value = SECRET
        first.focus()
        await pilot.press("enter")
        assert second.has_focus and first.password and second.password
        assert KEY not in app.export_screenshot() and SECRET not in app.export_screenshot()
        await pilot.press("enter")
        await pilot.pause()
        await app.alpaca_worker.wait()
        await pilot.pause()
        assert app.setup is None and app.session is None and app.active_view == "focus"
        assert first.value == "" and second.value == ""
        content = app.query_one("#focus-body").query_one(Static).content.markup
        assert "BTC/USD" in content and "0.000000001" in content
        await send(app, pilot, "/alpaca-save")
        check = client.last_check
        path = app.research.store.root / "alpaca-checks" / check.id / "check.json"
        assert path.exists() and KEY not in path.read_text() and SECRET not in path.read_text()
        await send(app, pilot, "/alpaca-disconnect")
        assert client.credentials is None and client.last_check is None
        assert len(provider.requests) == 3 and not ui_service.calls
        await send(app, pilot, "/alpaca")
        await app.alpaca_worker.wait()
        assert len(provider.requests) == 3


async def test_cancel_setup_and_model_history_exclusion(ui_service, paper):
    client, provider = paper
    app = StratosphereApp(
        research_service=ui_service,
        alpaca_connection=client,
        provider_factory=lambda settings: None,
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await connect(app, pilot)
        history = list(app.session.general.history)
        await send(app, pilot, "/alpaca-connect")
        app.query_one("#alpaca-key-id", Input).value = KEY
        app.query_one("#alpaca-secret-key", Input).value = SECRET
        await pilot.press("escape")
        await pilot.pause()
        assert app.setup is None and app.session.general.history == history
        assert not provider.requests
