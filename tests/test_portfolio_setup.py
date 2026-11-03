"""Only MT5 is active; historical paper identities/evidence cannot become MT5 authority."""

import pytest
from test_paper_portfolios import portfolio as portfolio

from stratosphere.research.store import ResearchError
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
