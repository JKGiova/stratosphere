"""Current portfolio roles, separate from retained paper journals and order authority."""

import uuid
from dataclasses import dataclass
from typing import Literal

from stratosphere.research.store import ResearchError
from stratosphere.workflow.risk import portfolio_agent_id


@dataclass(frozen=True)
class PortfolioTarget:
    provider: Literal["alpaca", "liquid", "ftmo"]
    mode: Literal["paper", "live", "evaluation"]
    label: str
    bridge: str
    blocker: str | None = None


TARGETS = (
    PortfolioTarget("alpaca", "paper", "Alpaca paper", "Alpaca paper API"),
    PortfolioTarget(
        "liquid",
        "live",
        "Liquid live perpetuals",
        "Liquid (planned)",
        "Liquid live perpetual execution is not implemented or activated. The saved read-only "
        "grant is diagnostic access only; historical paper state grants no live authority.",
    ),
    PortfolioTarget(
        "ftmo",
        "evaluation",
        "FTMO two-step Swing evaluation",
        "MT5 or a verified equivalent (planned)",
        "The FTMO bridge is not implemented or activated. Verify the exact platform, account, "
        "evaluation phase and Swing rules before deployment.",
    ),
)


def require_paper_provider(provider):
    """Only the current paper target can enter final approval or runner setup."""
    if provider == "alpaca":
        return
    target = next((t for t in TARGETS if t.provider == provider), None)
    raise ResearchError(
        target.blocker if target else "Only Alpaca paper supports final approval and execution"
    )


def configured_portfolios(paper_service):
    """Project roles are not account bindings; never relabel an old paper identity as live."""
    alpaca = next(p for p in paper_service.portfolios() if p["provider"] == "alpaca")
    result = []
    for target in TARGETS:
        if target.provider == "alpaca":
            row = alpaca.copy()
        else:
            identity = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"stratosphere:personal:{target.provider}:{target.mode}",
            ).hex
            row = {
                "id": identity,
                "provider": target.provider,
                "mode": target.mode,
                "agent_id": portfolio_agent_id(identity),
                "account_id": None,
                "halted": None,
                "execution_available": False,
                "blocker": target.blocker,
            }
        row.update(label=target.label, bridge=target.bridge)
        result.append(row)
    return result
