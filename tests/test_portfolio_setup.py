"""Only MT5 is active; historical paper identities/evidence cannot become MT5 authority."""

import pytest
from test_paper_portfolios import portfolio as portfolio
from test_research_tui import ui_service as ui_service
from textual.widgets import Static

from stratosphere.research.store import ResearchError
from stratosphere.tui import AssistantMessage, StratosphereApp
from stratosphere.workflow.portfolio_setup import configured_portfolios, require_paper_provider


def test_active_mt5_role_preserves_legacy_journal_bytes(portfolio):
    service, package = portfolio
    service.approve(package.package_id, package.digest)
    before = service.path.read_bytes()
    rows = configured_portfolios(service)
    assert [(row["provider"], row["mode"]) for row in rows] == [("mt5", "unbound")]
    assert rows[0]["account_id"] is None and rows[0]["execution_available"] is False
    assert rows[0]["id"] != package.portfolio_id
    assert service.packages(approved=True) == [package]
    assert service.path.read_bytes() == before


@pytest.mark.parametrize("provider", ["alpaca", "liquid", "ftmo", "mt5"])
def test_legacy_approval_route_cannot_activate_any_provider(provider):
    with pytest.raises(ResearchError, match="retired"):
        require_paper_provider(provider)


async def test_active_setup_and_direct_legacy_execution_gate(portfolio, ui_service):
    service, _ = portfolio
    app = StratosphereApp(research_service=ui_service, portfolio_service=service)
    async with app.run_test(size=(140, 40)):
        await app.handle_portfolio_command("portfolios", ())
        await app.handle_portfolio_command("portfolio-run", ("alpaca",))
        messages = "\n".join(item.source for item in app.query(AssistantMessage))
        notices = "\n".join(str(item.content) for item in app.query(Static))
        assert "MT5 / FTMO" in messages and "Retired provider execution" in notices
        assert app.paper_worker is None
