"""Reviewed authored signal decisions and account-wide gates; no provider transport."""

import hashlib
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from stratosphere.paper.ledger import (
    QUOTE_QUANTUM,
    TERMINAL,
    Intent,
    PaperError,
    PaperLedger,
    amount,
    reservation_reason,
    text,
)
from stratosphere.paper.runtime import HOUR_NS, ClosedBarMandate, ClosedBarRuntime
from stratosphere.paper.runtime import implementation_digest as stream_implementation_digest
from stratosphere.phase0.contracts import Contract, Digest, fingerprint


def implementation_digest():
    return hashlib.sha256(
        stream_implementation_digest().encode() + b"\0" + Path(__file__).read_bytes()
    ).hexdigest()


class StreamPin(Contract):
    instance_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    stream_sha256: Digest


class DecisionPlan(Contract):
    schema_version: Literal["authored-signal-decisions/1"] = "authored-signal-decisions/1"
    account_id: str = Field(pattern=r"^local-paper-[a-f0-9]{32}$")
    implementation_sha256: Digest
    streams: tuple[StreamPin, ...] = Field(min_length=1)
    max_gross_notional: str
    max_concentration_pct: str
    max_loss_quote: str
    max_drawdown_quote: str
    max_spread_bps: str
    slippage_bps: str
    fee_rate_cap: str
    max_quote_age_ns: int = Field(strict=True, gt=0, lt=HOUR_NS)
    signal_ttl_ns: int = Field(strict=True, gt=0, lt=HOUR_NS)
    intent_ttl_ns: int = Field(strict=True, gt=0, lt=HOUR_NS)
    purpose: Literal["authored_reservations_only_no_transport"] = (
        "authored_reservations_only_no_transport"
    )

    @model_validator(mode="after")
    def limits(self):
        for key in (
            "max_gross_notional",
            "max_concentration_pct",
            "max_loss_quote",
            "max_drawdown_quote",
        ):
            object.__setattr__(self, key, amount(getattr(self, key), positive=True))
        for key in ("max_spread_bps", "slippage_bps", "fee_rate_cap"):
            object.__setattr__(self, key, amount(getattr(self, key)))
        if not (
            Decimal(self.max_concentration_pct) <= 100
            and Decimal(self.slippage_bps) < 10000
            and Decimal(self.fee_rate_cap) < 1
        ):
            raise ValueError("Unsupported authored risk/cost limits")
        if len({pin.instance_id for pin in self.streams}) != len(self.streams):
            raise ValueError("A decision plan must pin distinct personal streams")
        return self

    @property
    def digest(self):
        return fingerprint(self)


class AuthoredQuote(Contract):
    quote_id: Digest
    market: Literal["BTCUSDT.SIM"] = "BTCUSDT.SIM"
    evidence: Literal["authored_fixture_only"] = "authored_fixture_only"
    bid: str
    ask: str
    bid_size: str
    ask_size: str
    event_ns: int = Field(strict=True, gt=0)
    available_ns: int = Field(strict=True, gt=0)

    @model_validator(mode="after")
    def valid(self):
        for key in ("bid", "ask", "bid_size", "ask_size"):
            object.__setattr__(self, key, amount(getattr(self, key), positive=True))
        if Decimal(self.bid) > Decimal(self.ask) or self.available_ns < self.event_ns:
            raise ValueError("Crossed prices or availability before the authored quote event")
        return self


def _active(state):
    value = state["signal_plan"]
    if value is None:
        raise PaperError("Prepare an authored signal decision plan")
    return value


def _verify_code(value):
    expected = value["plan"]["implementation_sha256"]
    if expected != implementation_digest() or expected != _IMPORTED_SOURCE_DIGEST:
        raise PaperError("Signal decision implementation changed; review a new plan")


def _quote_reason(value, quote, received_ns):
    old = value["quotes"].get(quote.quote_id)
    if old:
        return "duplicate" if old == quote.model_dump() else "conflicting_quote_id"
    same = value["quote_events"].get(str(quote.event_ns))
    if same:
        prior = value["quotes"][same]
        return (
            "duplicate"
            if {k: v for k, v in prior.items() if k != "quote_id"}
            == quote.model_dump(exclude={"quote_id"})
            else "conflicting_quote_event"
        )
    if received_ns < quote.available_ns:
        return "future_quote"
    if received_ns - quote.event_ns > value["plan"]["max_quote_age_ns"]:
        return "stale_quote"
    if value["last_receipt_ns"] is not None and received_ns < value["last_receipt_ns"]:
        return "quote_receipt_regression"
    if (
        value["latest_quote"]
        and quote.event_ns < value["quotes"][value["latest_quote"]]["event_ns"]
    ):
        return "out_of_order_quote"
    return None


def _mark(state, quote, extra=None):
    value = _active(state)
    plan = value["plan"]
    bid, ask = Decimal(quote["bid"]), Decimal(quote["ask"])
    base = sum((i["base"] for i in state["instances"].values()), Decimal(0))
    equity = state["cash"] + base * bid
    peak = max(value["peak_equity"], equity)
    orders = [o for o in state["orders"].values() if o["status"] not in TERMINAL]
    if extra is not None:
        orders.append(extra)
    buys = [o for o in orders if o["intent"]["side"] == "BUY"]
    reserved = sum((PaperLedger._reserved(o) for o in buys), Decimal(0))
    bought = sum((PaperLedger._remaining(o) for o in buys), Decimal(0))
    sell_fees = sum(
        (
            max(Decimal(0), Decimal(o["intent"]["fee_allowance"]) - o["fees"])
            for o in orders
            if o["intent"]["side"] == "SELL"
        ),
        Decimal(0),
    )
    sell_slippage = sum(
        (
            PaperLedger._remaining(o) * max(Decimal(0), bid - Decimal(o["intent"]["price_bound"]))
            for o in orders
            if o["intent"]["side"] == "SELL"
        ),
        Decimal(0),
    )
    floor = equity - reserved + bought * bid - sell_fees - sell_slippage
    gross = base * ask + sum(
        (PaperLedger._remaining(o) * max(ask, Decimal(o["intent"]["price_bound"])) for o in buys),
        Decimal(0),
    )
    loss = max(Decimal(0), Decimal(state["account"]["starting_cash"]) - equity)
    drawdown = max(Decimal(0), peak - equity)
    breaches = []
    if loss >= Decimal(plan["max_loss_quote"]):
        breaches.append("account_loss_limit")
    if drawdown >= Decimal(plan["max_drawdown_quote"]):
        breaches.append("account_drawdown_limit")
    return {
        "equity": text(equity),
        "peak_equity": text(peak),
        "loss_quote": text(loss),
        "drawdown_quote": text(drawdown),
        "projected_equity_floor": text(floor),
        "projected_gross_notional": text(gross),
        "reserved_cash": text(reserved),
        "pending_exit_cost_bound": text(sell_fees + sell_slippage),
        "gross_limit_exceeded": gross > Decimal(plan["max_gross_notional"]),
        "concentration_exceeded": floor <= 0
        or gross * 100 > Decimal(plan["max_concentration_pct"]) * floor,
        "breaches": breaches,
        "projected_breaches": [
            reason
            for reason, loss, limit in (
                (
                    "account_loss_limit",
                    Decimal(state["account"]["starting_cash"]) - floor,
                    plan["max_loss_quote"],
                ),
                ("account_drawdown_limit", peak - floor, plan["max_drawdown_quote"]),
            )
            if loss >= Decimal(limit)
        ],
    }


def _retain_mark(value, metrics, origin):
    value["peak_equity"] = Decimal(metrics["peak_equity"])
    for reason in metrics["breaches"]:
        if reason not in value["risk_latches"]:
            value["risk_latches"].append(reason)
            value["risk_findings"].append(
                {
                    "reason": reason,
                    "origin": origin,
                    "quote_id": value["latest_quote"],
                    "metrics": metrics,
                }
            )


def mark_after_fill(state, event_id):
    value = _active(state)
    if value["approved"] and value["latest_quote"]:
        _retain_mark(
            value, _mark(state, value["quotes"][value["latest_quote"]]), "fill:" + event_id
        )


def _fresh_quote(state, now_ns):
    value = _active(state)
    if value["quote_faults"]:
        return "quote_reconciliation_required"
    if not value["latest_quote"] or value["last_quote_status"] != "accepted":
        return "no_current_quote"
    quote = value["quotes"][value["latest_quote"]]
    if (
        now_ns < value["last_receipt_ns"]
        or now_ns - quote["event_ns"] > value["plan"]["max_quote_age_ns"]
    ):
        return "stale_or_future_quote"
    if (Decimal(quote["ask"]) - Decimal(quote["bid"])) * 10000 > Decimal(
        value["plan"]["max_spread_bps"]
    ) * Decimal(quote["bid"]):
        return "spread_limit"
    return None


def _liquidity_reason(state, quote, side, extra=None):
    pending = [
        o
        for o in state["orders"].values()
        if o["status"] not in TERMINAL and o["intent"]["side"] == side
    ]
    if extra is not None:
        pending.append(extra)
    needed = sum((PaperLedger._remaining(o) for o in pending), Decimal(0))
    value = _active(state)
    for decision in value["consumed"].values():
        intent = decision.get("intent")
        if intent and intent["side"] == side and decision["quote_id"] == quote["quote_id"]:
            order = state["orders"].get(intent["intent_id"])
            if order:
                needed += order["filled"]
    if needed > Decimal(quote["ask_size" if side == "BUY" else "bid_size"]):
        return "displayed_liquidity_limit"
    return None


def _signal(state, identity, digest):
    runtime = state["runtimes"].get(identity)
    if runtime is None:
        raise PaperError("That instance has no retained stream")
    signal = next((s for s in runtime["signals"] if s["signal_sha256"] == digest), None)
    if signal is None:
        raise PaperError("Select an exact retained signal owned by this instance")
    return runtime, signal


def _evaluate(state, identity, digest, now_ns):
    value = _active(state)
    plan = value["plan"]
    runtime, signal = _signal(state, identity, digest)
    pin = next((p for p in plan["streams"] if p["instance_id"] == identity), None)
    reason = None
    metrics = None
    intent = None
    if pin is None or pin["stream_sha256"] != runtime["digest"]:
        reason = "stream_not_reviewed"
    elif not runtime["approved"] or runtime["faults"]:
        reason = "stream_halted_or_unapproved"
    elif not signal["received_ns"] <= now_ns < signal["received_ns"] + plan["signal_ttl_ns"]:
        reason = "signal_expired_or_future"
    elif (
        signal["nominal_next_open_ns"]
        != runtime["mandate"]["first_open_ns"] + runtime["accepted_count"] * HOUR_NS
    ):
        reason = "superseded_signal"
    else:
        reason = _fresh_quote(state, now_ns)
    quote = value["quotes"].get(value["latest_quote"])
    if reason is None and quote["event_ns"] < max(
        signal["available_ns"], signal["received_ns"], signal["nominal_next_open_ns"]
    ):
        reason = "quote_precedes_signal_availability"
    instance = state["instances"][identity]
    if reason is None and now_ns - quote["event_ns"] > instance["mandate"]["max_quote_age_ns"]:
        reason = "instance_quote_age"
    if reason is None:
        metrics = _mark(state, quote)
        pending = [
            o
            for o in state["orders"].values()
            if o["status"] not in TERMINAL and o["intent"]["instance_id"] == identity
        ]
        if pending:
            reason = "instance_has_pending_order"
        elif signal["side"] == "BUY" and instance["base"] != 0:
            reason = "reinforcement_not_supported"
        elif signal["side"] == "SELL" and instance["base"] == 0:
            reason = "already_flat"
    if reason is None:
        side = signal["side"]
        quantity = runtime["parameters"]["quantity"] if side == "BUY" else text(instance["base"])
        impact = Decimal(plan["slippage_bps"]) / 10000
        price = (
            Decimal(quote["ask"]) * (1 + impact)
            if side == "BUY"
            else Decimal(quote["bid"]) * (1 - impact)
        ).quantize(QUOTE_QUANTUM, rounding=ROUND_CEILING if side == "BUY" else ROUND_FLOOR)
        if price <= 0:
            reason = "nonpositive_execution_bound"
        elif Decimal(quantity) > Decimal(instance["mandate"]["max_base_quantity"]) or Decimal(
            quantity
        ) * price > Decimal(instance["mandate"]["max_order_notional"]):
            reason = "instance_size_limit"
        else:
            fee = (Decimal(quantity) * price * Decimal(plan["fee_rate_cap"])).quantize(
                QUOTE_QUANTUM, rounding=ROUND_CEILING
            )
            intent = Intent(
                intent_id=hashlib.sha256(
                    (value["digest"] + identity + digest).encode()
                ).hexdigest()[:32],
                instance_id=identity,
                side=side,
                quantity=quantity,
                price_bound=text(price),
                fee_allowance=text(fee),
                created_ns=now_ns,
                expires_ns=min(
                    now_ns + plan["intent_ttl_ns"], signal["received_ns"] + plan["signal_ttl_ns"]
                ),
                quote_ns=quote["event_ns"],
                signal_sha256=digest,
                proposal_sha256=value["digest"],
            )
            reason = (
                "intent_id_already_retained"
                if intent.intent_id in state["decisions"]
                else reservation_reason(state, intent, now_ns)
            )
            extra = {
                "intent": intent.model_dump(),
                "status": "reserved",
                "filled": Decimal(0),
                "fees": Decimal(0),
            }
            metrics = _mark(state, quote, extra)
            if reason is None and side == "BUY":
                if value["risk_latches"] or metrics["breaches"] or metrics["projected_breaches"]:
                    reason = "account_risk_limit"
                elif metrics["gross_limit_exceeded"]:
                    reason = "gross_exposure_limit"
                elif metrics["concentration_exceeded"]:
                    reason = "concentration_limit"
            if reason is None:
                reason = _liquidity_reason(state, quote, side, extra)
    return {
        "instance_id": identity,
        "signal_sha256": digest,
        "observed_ns": now_ns,
        "plan_sha256": value["digest"],
        "quote_id": value["latest_quote"],
        "status": "reserved" if reason is None else "blocked",
        "reason": reason,
        "intent": intent.model_dump() if intent is not None and reason is None else None,
        "metrics": metrics,
    }


def reduce_event(state, event):
    kind = event["kind"]
    if kind == "signal_plan":
        if state["signal_plan"] is not None:
            raise PaperError("A signal decision plan cannot be replaced")
        plan = DecisionPlan.model_validate(event["plan"])
        if plan.account_id != state["account"]["account_id"]:
            raise PaperError("Signal plan account mismatch")
        state["signal_plan"] = {
            "plan": plan.model_dump(mode="json"),
            "digest": plan.digest,
            "approved": False,
            "quotes": {},
            "quote_events": {},
            "latest_quote": None,
            "last_quote_status": None,
            "last_receipt_ns": None,
            "quote_faults": [],
            "quote_attempts": [],
            "consumed": {},
            "peak_equity": Decimal(state["account"]["starting_cash"]),
            "risk_latches": [],
            "risk_findings": [],
        }
        return
    value = _active(state)
    if kind == "signal_approval":
        if (
            value["approved"]
            or value["digest"] != event["digest"]
            or state["orders"]
            or state["fills"]
        ):
            raise PaperError("Invalid retained signal plan approval")
        value["approved"] = True
    elif kind == "signal_quote":
        quote = AuthoredQuote.model_validate(event["quote"])
        reason = _quote_reason(value, quote, event["received_ns"])
        if reason != event["reason"] or not value["approved"]:
            raise PaperError("Invalid retained quote decision")
        value["quote_attempts"].append(event)
        if reason and reason != "duplicate":
            value["last_quote_status"] = "blocked"
            if reason in {"conflicting_quote_id", "conflicting_quote_event"}:
                value["quote_faults"].append(event)
        else:
            same = value["quote_events"].get(str(quote.event_ns))
            value["quotes"][quote.quote_id] = quote.model_dump()
            value["quote_events"][str(quote.event_ns)] = same or quote.quote_id
            # A duplicate never rewinds the current mark or clears a rejected observation.
            if reason is None:
                value["latest_quote"] = quote.quote_id
                value["last_receipt_ns"] = event["received_ns"]
                value["last_quote_status"] = "accepted"
                _retain_mark(value, _mark(state, quote.model_dump()), "quote:" + quote.quote_id)
    elif kind == "signal_decision":
        if not value["approved"]:
            raise PaperError("A retained signal decision bypassed approval")
        record = event["decision"]
        key = record["instance_id"] + ":" + record["signal_sha256"]
        if key in value["consumed"] or record != _evaluate(
            state, record["instance_id"], record["signal_sha256"], record["observed_ns"]
        ):
            raise PaperError("Invalid retained signal decision")
        value["consumed"][key] = record
        if record["metrics"]:
            _retain_mark(value, record["metrics"], "signal:" + record["signal_sha256"])
        if record["intent"]:
            PaperLedger._reduce(
                state,
                {
                    "kind": "decision",
                    "intent": record["intent"],
                    "observed_ns": record["observed_ns"],
                    "reason": None,
                },
            )
    else:
        raise PaperError("Unknown reviewed signal event")


def delivery_reason(state, intent, now_ns):
    value = _active(state)
    if not value["approved"]:
        return "plan_unapproved"
    _verify_code(value)
    record = value["consumed"].get(intent["instance_id"] + ":" + intent["signal_sha256"])
    if not record or record["intent"] != intent:
        return "reviewed_signal_required"
    runtime, signal = _signal(state, intent["instance_id"], intent["signal_sha256"])
    if runtime["faults"]:
        return "stream_halted"
    if (
        signal["nominal_next_open_ns"]
        != runtime["mandate"]["first_open_ns"] + runtime["accepted_count"] * HOUR_NS
        or now_ns < runtime["last_received_ns"]
    ):
        return "superseded_or_future_stream"
    if not signal["received_ns"] <= now_ns < signal["received_ns"] + value["plan"]["signal_ttl_ns"]:
        return "signal_expired"
    reason = _fresh_quote(state, now_ns)
    if reason:
        return reason
    quote = value["quotes"][value["latest_quote"]]
    if quote["event_ns"] < intent["quote_ns"]:
        return "quote_regressed"
    if (intent["side"] == "BUY" and Decimal(quote["ask"]) > Decimal(intent["price_bound"])) or (
        intent["side"] == "SELL" and Decimal(quote["bid"]) < Decimal(intent["price_bound"])
    ):
        return "price_bound_breached"
    metrics = _mark(state, quote)
    if intent["side"] == "BUY" and (
        value["risk_latches"]
        or metrics["breaches"]
        or metrics["projected_breaches"]
        or metrics["gross_limit_exceeded"]
        or metrics["concentration_exceeded"]
    ):
        return "account_risk_limit"
    return _liquidity_reason(state, quote, intent["side"])


class SignalDecisions:
    """Internal authored reservation gateway; it cannot submit/cancel provider orders."""

    def __init__(self, ledger, registry):
        self.ledger, self.registry = ledger, registry

    def _pins(self, state, plan, *, require_healthy=False):
        _verify_code({"plan": plan.model_dump()})
        if plan.account_id != state["account"]["account_id"]:
            raise PaperError("Review the exact authored account")
        for pin in plan.streams:
            runtime = state["runtimes"].get(pin.instance_id)
            if (
                not runtime
                or runtime["digest"] != pin.stream_sha256
                or not runtime["approved"]
                or (require_healthy and runtime["faults"])
            ):
                raise PaperError("Review exact approved personal streams")
            ClosedBarRuntime(self.ledger, self.registry)._verify(
                ClosedBarMandate.model_validate(runtime["mandate"])
            )

    def prepare(self, plan: DecisionPlan):
        plan = DecisionPlan.model_validate(plan.model_dump())
        with self.ledger._transaction() as db:
            state, chain = self.ledger._load(db)
            self._pins(state, plan, require_healthy=True)
            if state["orders"] or state["fills"]:
                raise PaperError("Decision risk baseline requires an untouched authored account")
            if state["signal_plan"] is not None:
                raise PaperError("A signal decision plan is already retained")
            self.ledger._append(
                db, chain, {"kind": "signal_plan", "plan": plan.model_dump(mode="json")}
            )
            db.execute("PRAGMA user_version=3")
        return plan.digest

    def approve(self, digest):
        with self.ledger._transaction() as db:
            state, chain = self.ledger._load(db)
            value = _active(state)
            self._pins(state, DecisionPlan.model_validate(value["plan"]), require_healthy=True)
            if digest != value["digest"] or value["approved"] or state["orders"] or state["fills"]:
                raise PaperError("Approve exactly the pending untouched-account decision plan")
            self.ledger._append(db, chain, {"kind": "signal_approval", "digest": digest})

    def begin_delivery(self, intent_id, *, now_ns: int):
        """Claim once after fresh registry/risk checks; no transport is invoked."""
        self._time(now_ns)
        with self.ledger._transaction() as db:
            state, chain = self.ledger._load(db)
            value = _active(state)
            self._pins(state, DecisionPlan.model_validate(value["plan"]))
            self.ledger._claim_delivery(db, state, chain, intent_id, now_ns)

    def observe_quote(self, quote: AuthoredQuote, *, received_ns: int):
        quote = AuthoredQuote.model_validate(quote.model_dump())
        self._time(received_ns)
        with self.ledger._transaction() as db:
            state, chain = self.ledger._load(db)
            value = _active(state)
            _verify_code(value)
            if not value["approved"]:
                raise PaperError("The signal decision plan is not approved")
            reason = _quote_reason(value, quote, received_ns)
            self.ledger._append(
                db,
                chain,
                {
                    "kind": "signal_quote",
                    "quote": quote.model_dump(),
                    "received_ns": received_ns,
                    "reason": reason,
                },
            )
            return reason

    @staticmethod
    def _time(now_ns):
        if type(now_ns) is not int or now_ns <= 0:
            raise PaperError("Use a positive authored decision time")

    def evaluate(self, identity, signal_sha256, *, now_ns: int):
        self._time(now_ns)
        with self.ledger._transaction() as db:
            state, chain = self.ledger._load(db)
            value = _active(state)
            _verify_code(value)
            if not value["approved"]:
                raise PaperError("The signal decision plan is not approved")
            key = identity + ":" + signal_sha256
            if old := value["consumed"].get(key):
                return {"status": "duplicate", "retained": old}
            pin = next((p for p in value["plan"]["streams"] if p["instance_id"] == identity), None)
            if pin:
                runtime = state["runtimes"].get(identity)
                if runtime:
                    ClosedBarRuntime(self.ledger, self.registry)._verify(
                        ClosedBarMandate.model_validate(runtime["mandate"])
                    )
            record = _evaluate(state, identity, signal_sha256, now_ns)
            self.ledger._append(db, chain, {"kind": "signal_decision", "decision": record})
            return record


_IMPORTED_SOURCE_DIGEST = implementation_digest()
