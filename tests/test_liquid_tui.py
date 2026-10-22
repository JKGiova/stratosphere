"""Connector inspection is independent of models, research and trade activation."""

import asyncio

import pytest
from test_liquid import CODE, TOKEN, Provider
from test_research_tui import connect, send
from test_research_tui import ui_service as ui_service
from textual.containers import VerticalScroll
from textual.widgets import Static

from stratosphere.commands import route_command
from stratosphere.tui import StratosphereApp


async def test_removed_broker_commands_cannot_reach_models_or_start_connector_work(
    ui_service, research_provider
):
    provider = Provider()
    app = StratosphereApp(
        provider_factory=lambda settings: research_provider,
        research_service=ui_service,
        liquid_connection=provider.connection,
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await connect(app, pilot)
        for command in ("/alpaca", "/alpaca-connect", "/alpaca-save", "/alpaca-disconnect"):
            assert route_command(command).message.startswith("Unknown command.")
            await send(app, pilot, command)
        assert not research_provider.requests and not provider.browser_urls
        assert app.liquid_worker is None and provider.connection.session is None
        assert app.setup is None and not app.research.store.jobs()
        await send(app, pilot, "/liquid")
        await app.liquid_worker.wait()
        assert app.active_view == "focus" and not research_provider.requests


@pytest.mark.parametrize("command", ["/liquid", "/liquid-connect", "/liquid-disconnect"])
def test_liquid_commands_cannot_accept_pasted_tokens_or_account_arguments(command):
    assert route_command(command).action == command[1:]
    assert route_command(command + " " + TOKEN).action == "message"


async def test_small_tui_discovery_connection_scroll_and_disconnect_leave_research_untouched(
    ui_service, research_provider
):
    provider = Provider()
    app = StratosphereApp(
        provider_factory=lambda settings: research_provider,
        research_service=ui_service,
        liquid_connection=provider.connection,
    )
    async with app.run_test(size=(45, 12)) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/liquid")
        await app.liquid_worker.wait()
        await pilot.pause()
        assert not provider.browser_urls and app.active_view == "focus"
        await send(app, pilot, "/liquid-connect")
        await app.liquid_worker.wait()
        await pilot.pause()
        assert app.active_view == "focus" and provider.connection.session is not None
        body = app.query_one("#focus-body", VerticalScroll)
        assert app.focused is body
        await pilot.press("pagedown")
        await pilot.pause()
        assert body.scroll_y > 0
        content = " ".join(str(s.content) for s in app.query(Static))
        assert TOKEN not in content and CODE not in content
        assert not research_provider.requests and not app.database.catalog()
        assert not app.research.store.jobs() and not ui_service.calls
        # A connected provider credential pasted in chat must not reach model history.
        await send(app, pilot, TOKEN)
        assert not research_provider.requests
        await send(app, pilot, "/liquid-disconnect")
        assert provider.connection.session is None


async def test_cancel_pending_connector_closes_callback_and_quit_forgets_completed_session(
    ui_service, research_provider
):
    provider = Provider()
    opened = asyncio.Event()

    async def browser(url):
        opened.set()
        return True

    provider.connection.open_browser = browser
    app = StratosphereApp(
        provider_factory=lambda settings: research_provider,
        research_service=ui_service,
        liquid_connection=provider.connection,
    )
    async with app.run_test(size=(144, 40)) as pilot:
        await connect(app, pilot)
        await send(app, pilot, "/liquid-connect")
        await asyncio.wait_for(opened.wait(), 2)
        await send(app, pilot, "/liquid-disconnect")
        await pilot.pause()
        assert provider.callbacks[-1].closed and provider.connection.session is None
        assert not provider.connection._busy
        provider.connection.open_browser = provider.browser
        await send(app, pilot, "/liquid-connect")
        await app.liquid_worker.wait()
        assert provider.connection.session is not None
        await app.action_quit()
        assert provider.connection.session is None


@pytest.mark.parametrize("command", ["/liquid-tools", "/liquid-catalog-save"])
def test_catalog_commands_reject_account_or_credential_arguments(command):
    assert route_command(command).action == command[1:]
    assert route_command(command + " " + TOKEN).action == "message"
    assert route_command("/liquid-tool fixture_portfolio").args == ("fixture_portfolio",)
    assert route_command("/liquid-tool").action == "message"
    assert route_command("/liquid-tool https://example.com").action == "message"


async def test_catalog_schema_inspection_and_private_export_need_no_model_and_call_no_tool(
    ui_service,
):
    from stratosphere.connectors.catalog import LiquidCatalog

    provider = Provider()
    app = StratosphereApp(research_service=ui_service, liquid_connection=provider.connection)
    async with app.run_test(size=(45, 12)) as pilot:
        await pilot.press("escape")
        await pilot.pause()
        await send(app, pilot, "/liquid-tools")
        await app.liquid_worker.wait()
        assert provider.connection.session is None and not provider.browser_urls
        await send(app, pilot, "/liquid-connect")
        await app.liquid_worker.wait()
        await send(app, pilot, "/liquid-tools")
        await app.liquid_worker.wait()
        await pilot.pause()
        assert "fixture_portfolio" in app.query_one("#focus-body").children[0].content.markup
        await send(app, pilot, "/liquid-tool fixture_portfolio")
        await app.liquid_worker.wait()
        await pilot.pause()
        text = app.query_one("#focus-body").children[0].content.markup
        assert "inputSchema" in text and "untrusted" in text
        await pilot.press("pagedown")
        await pilot.pause()
        assert app.query_one("#focus-body", VerticalScroll).scroll_y > 0
        await send(app, pilot, "/liquid-catalog-save")
        await app.liquid_worker.wait()
        exported = tuple((app.research.store.root / "liquid-catalogs").iterdir())
        assert len(exported) == 1 and LiquidCatalog.load(exported[0]).names == (
            "fixture_portfolio",
        )
        assert not app.session and not app.settings and not app.database.catalog()
        assert not app.research.store.jobs() and not ui_service.calls
        content = " ".join(str(w.content) for w in app.query(Static))
        assert TOKEN not in content and CODE not in content
        await send(app, pilot, "/liquid-disconnect")
        assert provider.connection.session is None
