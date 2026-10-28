"""Retired provider commands cannot reach models, credentials, browsers or transports."""

import pytest
from test_liquid import Provider
from test_research_tui import connect, send
from test_research_tui import ui_service as ui_service
from textual.widgets import Static

from stratosphere.commands import HELP, route_command
from stratosphere.tui import StratosphereApp


@pytest.mark.parametrize(
    "command",
    [
        "/liquid",
        "/liquid-connect",
        "/liquid-disconnect",
        "/liquid-tools",
        "/liquid-catalog-save",
        "/liquid-account",
        "/liquid-account-reads",
        "/liquid-account-show " + "a" * 32,
        "/alpaca-paper-connect",
        "/alpaca-paper-forget",
        "/portfolio-run alpaca",
        "/portfolio-run liquid",
        "/strategy-approve " + "a" * 32 + " " + "b" * 64,
    ],
)
def test_retired_commands_cannot_dispatch(command):
    result = route_command(command)
    assert result.action == "message" and "retired" in result.message
    assert "/liquid-connect" not in HELP and "/alpaca-paper-connect" not in HELP


async def test_retired_ui_never_restores_or_dispatches(ui_service, research_provider):
    remote = Provider()
    app = StratosphereApp(
        research_service=ui_service,
        liquid_connection=remote.connection,
        provider_factory=lambda settings: research_provider,
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await connect(app, pilot)
        assert app.liquid_worker is None
        for command in ("/liquid-connect", "/alpaca-paper-connect", "/portfolio-run alpaca"):
            await send(app, pilot, command)
        assert app.liquid_worker is None and app.paper_worker is None
        assert not remote.browser_urls and not research_provider.requests
        assert any("retired" in str(item.content) for item in app.query(Static))


async def test_mt5_read_is_private_and_never_calls_model(
    ui_service, research_provider, monkeypatch
):
    class ReadOnly:
        async def read(self, operation, **args):
            assert operation == "account" and not args
            return {"connected": "1", "currency": "USD", "login": "private-fixture-account"}

    monkeypatch.setattr("stratosphere.mt5.bridge.MT5Bridge", ReadOnly)
    app = StratosphereApp(
        research_service=ui_service, provider_factory=lambda settings: research_provider
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await connect(app, pilot)
        before = list(app.session.history)
        await app.read_mt5(("status",)).wait()
        assert app.session.history == before and not research_provider.requests
