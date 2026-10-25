"""Current roles must not promote retained paper evidence or activate future transports."""

import pytest
from test_paper_portfolios import portfolio as portfolio
from test_research_tui import ui_service as ui_service

from stratosphere.commands import HELP, route_command
from stratosphere.research.store import ResearchError
from stratosphere.tui import AssistantMessage, StratosphereApp
from stratosphere.workflow.portfolio_setup import configured_portfolios, require_paper_provider
from stratosphere.workflow.portfolios import PaperPortfolios


def test_roles_preserve_paper_identity_bindings_and_evidence_across_reopen(portfolio):
    service, package = portfolio
    service.approve(package.package_id, package.digest)
    historical = service.portfolios()
    before = service.path.read_bytes()
    rows = configured_portfolios(service)
    assert [(r["provider"], r["mode"]) for r in rows] == [
        ("alpaca", "paper"),
        ("liquid", "live"),
        ("ftmo", "evaluation"),
    ]
    assert rows[0]["id"] == package.portfolio_id
    assert rows[0]["account_id"] == package.account_id
    assert len({r["id"] for r in rows}) == len({r["agent_id"] for r in rows}) == 3
    legacy_liquid = next(p for p in historical if p["provider"] == "liquid")
    assert rows[1]["id"] != legacy_liquid["id"]
    assert rows[1]["agent_id"] != legacy_liquid["agent_id"]
    assert all(not r["execution_available"] and r["account_id"] is None for r in rows[1:])
    reopened = PaperPortfolios(service.path, campaigns=service.campaigns, database=service.database)
    assert configured_portfolios(reopened) == rows
    assert reopened.portfolios() == historical
    assert reopened.packages(approved=True) == [package]
    assert service.path.read_bytes() == before


@pytest.mark.parametrize("provider", ["liquid", "ftmo", "unknown"])
def test_future_or_unknown_role_cannot_enter_paper_workflow(provider):
    with pytest.raises(ResearchError):
        require_paper_provider(provider)


async def test_portfolio_ui_exposes_roles_and_blocks_future_routes_before_credentials(
    portfolio, ui_service, monkeypatch
):
    service, _ = portfolio
    before = service.path.read_bytes()
    app = StratosphereApp(research_service=ui_service, portfolio_service=service)

    async def forbidden_restore():
        pytest.fail("A planned portfolio must never load paper credentials or submit orders")

    monkeypatch.setattr(app, "restore_paper_adapter", forbidden_restore)
    async with app.run_test(size=(140, 40)) as pilot:
        await app.handle_portfolio_command("portfolios", ())
        for provider in ("liquid", "ftmo"):
            for text in (
                f"/portfolio-run {provider}",
                f"/strategy-finish {'a' * 32} {'1' * 24} {provider}",
            ):
                command = route_command(text)
                assert command.action != "message"
                await app.handle_portfolio_command(command.action, command.args)
        await pilot.pause()
        messages = "\n".join(m.source for m in app.query(AssistantMessage))
        assert "Alpaca paper" in messages and "Liquid live perpetuals" in messages
        assert "FTMO two-step Swing evaluation" in messages and "MT5" in messages
        assert "Liquid live perpetual execution is not implemented or activated" in messages
        assert "FTMO bridge is not implemented or activated" in messages
        assert app.paper_worker is None
    assert service.packages(approved=True) == []
    assert service.path.read_bytes() == before
    assert "/portfolio-run alpaca" in HELP and "/portfolio-run <alpaca|liquid>" not in HELP
