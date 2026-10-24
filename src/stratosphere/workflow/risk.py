"""Isolated advisory roles and deterministic capital/position validation; no order transport."""

import asyncio
import json
import time
import uuid
from decimal import ROUND_FLOOR, Decimal

from stratosphere.models.structured import structured_response
from stratosphere.research.store import ResearchError
from stratosphere.workflow.contracts import CapitalProposal, PositionSizingProposal, SignalVerdict


def portfolio_agent_id(portfolio_id):
    return uuid.uuid5(uuid.NAMESPACE_URL, "stratosphere:portfolio-risk:" + portfolio_id).hex


def current_snapshot(definition, snapshot, at_ns):
    if definition.currency != snapshot.currency:
        raise ResearchError("Portfolio currency mismatch")
    if not snapshot.reconciled or not snapshot.as_of_ns <= at_ns < snapshot.expires_at_ns:
        raise ResearchError("A current reconciled portfolio snapshot is required")
    if Decimal(snapshot.daily_loss) >= (
        Decimal(snapshot.equity) * Decimal(definition.max_daily_loss_fraction)
    ):
        raise ResearchError("The approved daily loss limit is reached")


def validate_capital(definition, snapshot, proposal, *, at_ns):
    current_snapshot(definition, snapshot, at_ns)
    if (
        proposal.portfolio_id != snapshot.portfolio_id
        or proposal.snapshot_sha256 != snapshot.digest
    ):
        raise ResearchError("Capital proposal belongs to another portfolio snapshot")
    if set(proposal.allocations) != set(snapshot.strategy_capital):
        raise ResearchError("Capital proposals must cover exactly the assigned strategies")
    ceiling = Decimal(snapshot.equity) * Decimal(definition.max_strategy_fraction)
    values = {identity: Decimal(value) for identity, value in proposal.allocations.items()}
    if any(value > ceiling for value in values.values()):
        raise ResearchError("Proposed strategy capital exceeds approved portfolio limits")
    if sum(values.values()) > Decimal(snapshot.equity):
        raise ResearchError("The portfolio cannot allocate more than its equity")
    for identity, value in values.items():
        committed = Decimal(snapshot.committed_capital[identity])
        if committed and value != Decimal(snapshot.strategy_capital[identity]):
            raise ResearchError("Open or pending positions lock that strategy's capital")
    return proposal


def position_preview(
    definition,
    snapshot,
    verdict,
    proposal,
    *,
    context_sha256,
    entry_price,
    stop_price,
    quantity_increment,
    at_ns,
):
    current_snapshot(definition, snapshot, at_ns)
    if verdict.verdict != "reliable":
        raise ResearchError("The strategy agent rejected this signal")
    if (
        verdict.context_sha256 != context_sha256
        or verdict.strategy_id != proposal.strategy_id
        or verdict.signal_sha256 != proposal.signal_sha256
        or proposal.portfolio_id != snapshot.portfolio_id
        or proposal.snapshot_sha256 != snapshot.digest
    ):
        raise ResearchError("Signal review, strategy and portfolio pins do not match")
    if proposal.strategy_id not in snapshot.strategy_capital:
        raise ResearchError("The strategy is not assigned to this portfolio")
    entry, stop, increment = map(Decimal, (entry_price, stop_price, quantity_increment))
    if (
        not all(value.is_finite() and value > 0 for value in (entry, stop, increment))
        or stop >= entry
    ):
        raise ResearchError(
            "A positive long entry, protective stop and quantity increment are required"
        )
    equity = Decimal(snapshot.equity)
    requested = Decimal(proposal.requested_notional)
    available = Decimal(snapshot.strategy_capital[proposal.strategy_id]) - Decimal(
        snapshot.committed_capital[proposal.strategy_id]
    )
    cap = min(
        available, Decimal(snapshot.free_cash), equity * Decimal(definition.max_position_fraction)
    )
    risk_cap = equity * Decimal(definition.max_risk_fraction)
    if requested > cap or requested / entry * (entry - stop) > risk_cap:
        raise ResearchError("The requested position exceeds available capital or risk limits")
    quantity = (requested / entry / increment).to_integral_value(rounding=ROUND_FLOOR) * increment
    if quantity <= 0:
        raise ResearchError("Position is below the allowed quantity increment")
    return {
        "schema_version": "position-preview/1",
        "status": "preview_only",
        "portfolio_id": snapshot.portfolio_id,
        "strategy_id": proposal.strategy_id,
        "signal_sha256": proposal.signal_sha256,
        "snapshot_sha256": snapshot.digest,
        "quantity": str(quantity),
        "notional": str(quantity * entry),
        "nominal_stop_loss": str(quantity * (entry - stop)),
        "execution_authorized": False,
        "limitations": [
            "Stop loss is nominal; gaps and execution costs can exceed it.",
            "No reservation, broker submission or actual execution is implemented.",
        ],
    }


class AdvisoryAgents:
    """Role calls receive only explicit application-owned facts, never account diagnostics."""

    def __init__(self, provider, *, secret_check=lambda data: False, clock_ns=time.time_ns):
        self.provider, self.secret_check = provider, secret_check
        self.clock_ns = clock_ns

    async def call(self, prompt, context, contract):
        content = json.dumps(context, ensure_ascii=False)
        if self.secret_check(content.encode()):
            raise ResearchError("Credential-bearing advisory context was rejected")
        async with asyncio.timeout(180):
            result = await structured_response(
                self.provider,
                [{"role": "system", "content": prompt}, {"role": "user", "content": content}],
                contract,
            )
        if self.secret_check(result.model_dump_json().encode()):
            raise ResearchError("Credential-bearing advisory result was rejected")
        return result

    async def review_signal(self, strategy_id, signal_sha256, context_sha256, facts):
        verdict = await self.call(
            "Review this exact strategy trigger using only supplied facts. Return reliable or "
            "not_reliable, with explanation and literal fact IDs in evidence. Uncertainty or "
            "insufficient evidence means not_reliable. This is advisory, not a profit guarantee "
            "or permission to allocate capital or trade. Copy owner/signal/context pins exactly.",
            {
                "strategy_id": strategy_id,
                "signal_sha256": signal_sha256,
                "context_sha256": context_sha256,
                "facts": facts,
            },
            SignalVerdict,
        )
        if (
            verdict.strategy_id != strategy_id
            or verdict.signal_sha256 != signal_sha256
            or verdict.context_sha256 != context_sha256
            or not set(verdict.evidence).issubset(facts)
        ):
            raise ResearchError("Strategy signal review references invalid evidence or ownership")
        return verdict

    async def capital(self, definition, snapshot, *, at_ns):
        current_snapshot(definition, snapshot, at_ns)
        proposal = await self.call(
            "You are the risk-management agent for this exact portfolio. Propose strategy capital "
            "for every assigned exact strategy ID, using supplied equity/limits. Include zero "
            "allocations when appropriate. Positions and reservations lock the current allocation. "
            "Return an advisory proposal only, never approval, quantities or orders.",
            {
                "agent_id": portfolio_agent_id(snapshot.portfolio_id),
                "definition": definition.model_dump(mode="json"),
                "snapshot": snapshot.model_dump(mode="json"),
                "snapshot_sha256": snapshot.digest,
            },
            CapitalProposal,
        )
        return validate_capital(definition, snapshot, proposal, at_ns=max(at_ns, self.clock_ns()))

    async def size(
        self,
        definition,
        snapshot,
        verdict,
        *,
        context_sha256,
        entry_price,
        stop_price,
        quantity_increment,
        at_ns,
    ):
        current_snapshot(definition, snapshot, at_ns)
        if verdict.verdict != "reliable":
            raise ResearchError("The strategy agent rejected this signal")
        if (
            verdict.context_sha256 != context_sha256
            or verdict.strategy_id not in snapshot.strategy_capital
        ):
            raise ResearchError("The signal does not belong to this portfolio context")
        proposal = await self.call(
            "You are this portfolio's risk-management agent. Propose requested_notional in the "
            "portfolio quote currency for the exact reviewed signal. Respect all supplied capital, "
            "cash, position and nominal stop-risk limits. Copy pins exactly. This is advisory; "
            "application code validates and rounds quantity. Never issue orders.",
            {
                "agent_id": portfolio_agent_id(snapshot.portfolio_id),
                "definition": definition.model_dump(mode="json"),
                "snapshot": snapshot.model_dump(mode="json"),
                "snapshot_sha256": snapshot.digest,
                "verdict": verdict.model_dump(mode="json"),
                "entry_price": entry_price,
                "stop_price": stop_price,
                "quantity_increment": quantity_increment,
            },
            PositionSizingProposal,
        )
        return position_preview(
            definition,
            snapshot,
            verdict,
            proposal,
            context_sha256=context_sha256,
            entry_price=entry_price,
            stop_price=stop_price,
            quantity_increment=quantity_increment,
            at_ns=max(at_ns, self.clock_ns()),
        )
