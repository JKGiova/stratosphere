"""Active MT5 setup, separate from retained legacy paper journals and approvals."""

import uuid

from stratosphere.research.store import ResearchError
from stratosphere.workflow.risk import portfolio_agent_id


def require_paper_provider(provider):
    raise ResearchError(
        "The Alpaca/Liquid paper workflow is retired. MT5 needs its own exact deployment approval."
    )


def configured_portfolios(paper_service=None):
    """An unbound role never inherits account IDs, evidence or order authority from old journals."""
    identity = uuid.uuid5(uuid.NAMESPACE_URL, "stratosphere:mt5:unbound").hex
    return [
        {
            "id": identity,
            "provider": "mt5",
            "mode": "unbound",
            "label": "MT5 / FTMO",
            "bridge": "Local StratosphereBridge Expert Advisor",
            "agent_id": portfolio_agent_id(identity),
            "account_id": None,
            "halted": None,
            "execution_available": False,
            "blocker": "Read /mt5-status to inspect the exact account. "
            "Connection grants no trading "
            "authority; approve an exact MT5 deployment through the headless connector.",
        }
    ]
