"""Durable authored-paper reservations and fill attribution, without a submit path."""

import hashlib
import json
import os
import sqlite3
from contextlib import contextmanager
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, localcontext
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator

from stratosphere.phase0.contracts import Contract, Digest, decimal_text
from stratosphere.registry.workspace import encoded
from stratosphere.research.store import ResearchError

TERMINAL = {"filled", "canceled", "rejected", "expired"}
QUOTE_QUANTUM = Decimal("0.00000001")


class PaperError(ResearchError):
    """A safe paper-ledger error; never a provider response."""


def amount(value, *, positive=False):
    value = decimal_text(value)
    integer, _, fraction = value.lstrip("-").partition(".")
    if len(integer) > 24 or len(fraction) > 8 or Decimal(value) < 0:
        raise ValueError("Paper inputs require nonnegative amounts with at most eight decimals")
    if positive and Decimal(value) == 0:
        raise ValueError("A positive paper amount is required")
    return value


class Account(Contract):
    schema_version: Literal["authored-paper-account/1"] = "authored-paper-account/1"
    account_id: str = Field(pattern=r"^local-paper-[a-f0-9]{32}$")
    starting_cash: str
    market: Literal["BTCUSDT.SIM"] = "BTCUSDT.SIM"
    evidence: Literal["authored_fixture_only"] = "authored_fixture_only"

    @model_validator(mode="after")
    def validate_cash(self) -> Self:
        object.__setattr__(self, "starting_cash", amount(self.starting_cash, positive=True))
        return self


class Instance(Contract):
    """Personal allocation pins; hashes do not prove an executable artifact or validation."""

    instance_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    strategy_sha256: Digest
    implementation_sha256: Digest
    account_id: str = Field(pattern=r"^local-paper-[a-f0-9]{32}$")
    capital: str
    max_order_notional: str
    max_base_quantity: str
    max_quote_age_ns: int = Field(strict=True, gt=0)

    @model_validator(mode="after")
    def validate_limits(self) -> Self:
        for name in ("capital", "max_order_notional", "max_base_quantity"):
            object.__setattr__(self, name, amount(getattr(self, name), positive=True))
        return self


class Intent(Contract):
    intent_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    instance_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    side: Literal["BUY", "SELL"]
    quantity: str
    price_bound: str
    fee_allowance: str
    created_ns: int = Field(strict=True, gt=0)
    expires_ns: int = Field(strict=True, gt=0)
    quote_ns: int = Field(strict=True, gt=0)
    signal_sha256: Digest
    proposal_sha256: Digest

    @model_validator(mode="after")
    def validate_intent(self) -> Self:
        for name in ("quantity", "price_bound", "fee_allowance"):
            object.__setattr__(
                self, name, amount(getattr(self, name), positive=name != "fee_allowance")
            )
        if not self.quote_ns <= self.created_ns < self.expires_ns:
            raise ValueError("Paper intent timestamps are inconsistent")
        return self


class Fill(Contract):
    event_id: Digest
    intent_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    quantity: str
    price: str
    notional: str
    fee: str

    @model_validator(mode="after")
    def validate_fill(self) -> Self:
        for name in ("quantity", "price", "notional", "fee"):
            object.__setattr__(
                self, name, amount(getattr(self, name), positive=name in {"quantity", "price"})
            )
        return self


def text(value):
    result = format(value, "f")
    return result.rstrip("0").rstrip(".") if "." in result else result


def reservation_reason(state, intent, now_ns):
    """Shared cash/position gates; the reviewed decision pipeline adds its risk policy."""
    instance = state["instances"].get(intent.instance_id)
    if instance is None:
        raise PaperError("Select an existing personal paper instance")
    pending = [o for o in state["orders"].values() if o["status"] not in TERMINAL]
    own = [o for o in pending if o["intent"]["instance_id"] == intent.instance_id]
    quantity, price = Decimal(intent.quantity), Decimal(intent.price_bound)
    cost = (quantity * price).quantize(QUOTE_QUANTUM, rounding=ROUND_CEILING) + Decimal(
        intent.fee_allowance
    )
    reason = None
    if state["blocks"] or any(o["status"] in {"dispatching", "ambiguous"} for o in pending):
        reason = "reconciliation_required"
    elif (
        not intent.created_ns <= now_ns < intent.expires_ns
        or now_ns - intent.quote_ns > instance["mandate"]["max_quote_age_ns"]
    ):
        reason = "stale_or_expired"
    elif own:
        reason = "instance_has_pending_order"
    elif quantity > Decimal(instance["mandate"]["max_base_quantity"]) or quantity * price > Decimal(
        instance["mandate"]["max_order_notional"]
    ):
        reason = "instance_size_limit"
    elif intent.side == "BUY":
        if state["entries_blocked"]:
            reason = "entries_blocked"
        elif instance["base"] != 0:
            reason = "reinforcement_not_supported"
        elif (
            cost + sum(PaperLedger._reserved(o) for o in pending) > state["cash"]
            or cost > instance["cash"]
        ):
            reason = "insufficient_unreserved_cash"
    elif quantity > instance["base"]:
        reason = "insufficient_attributed_position"
    elif Decimal(intent.fee_allowance) > (quantity * price).quantize(
        QUOTE_QUANTUM, rounding=ROUND_FLOOR
    ):
        reason = "sell_fee_exceeds_minimum_proceeds"
    return reason


class PaperLedger:
    """One journal per authored cash account; transactions serialize all reservations.

    Application callbacks supply trusted fixture/native observations. This class does
    not authenticate accounts, approve mandates, verify quotes, or send/cancel orders.
    Full portfolio loss/drawdown controls and runtime reconciliation remain separate.
    """

    def __init__(self, path: Path, account: Account):
        account = Account.model_validate(account.model_dump())
        self.path = path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if path.is_symlink() or path.parent.is_symlink():
            raise PaperError("Paper state cannot use a symbolic link")
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        os.close(descriptor)
        path.chmod(0o600)
        with self._transaction() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2, 3):
                raise PaperError("Unsupported paper journal version")
            db.execute(
                "CREATE TABLE IF NOT EXISTS events "
                "(seq INTEGER PRIMARY KEY, payload TEXT NOT NULL, digest TEXT NOT NULL)"
            )
            for operation in ("UPDATE", "DELETE"):
                db.execute(
                    f"CREATE TRIGGER IF NOT EXISTS retain_{operation.lower()} "
                    f"BEFORE {operation} ON events BEGIN "
                    "SELECT RAISE(ABORT,'Paper events are immutable'); END"
                )
            db.execute(f"PRAGMA user_version={max(1, version)}")
            state, chain = self._load(db)
            if state["account"] is None:
                self._append(db, chain, {"kind": "account", "value": account.model_dump()})
            elif state["account"] != account.model_dump():
                raise PaperError("An existing paper account cannot be replaced or re-funded")

    @contextmanager
    def _transaction(self):
        db = sqlite3.connect(self.path, timeout=10)
        try:
            with localcontext() as context, db:
                context.prec = 100
                db.execute("BEGIN IMMEDIATE")
                yield db
        except sqlite3.Error:
            raise PaperError(
                "Paper transaction failed; no partial mutation was committed"
            ) from None
        finally:
            db.close()

    @staticmethod
    def _append(db, chain, event):
        payload = encoded(event)
        digest = hashlib.sha256(chain.encode() + b"\0" + payload.encode()).hexdigest()
        db.execute("INSERT INTO events(payload,digest) VALUES (?,?)", (payload, digest))

    @staticmethod
    def _load(db):
        state = {
            "account": None,
            "cash": Decimal(0),
            "instances": {},
            "orders": {},
            "decisions": {},
            "fills": {},
            "blocks": [],
            "entries_blocked": False,
            "runtimes": {},
            "signal_plan": None,
        }
        chain = ""
        try:
            for expected, (seq, payload, digest) in enumerate(
                db.execute("SELECT seq,payload,digest FROM events ORDER BY seq"), 1
            ):
                if (
                    seq != expected
                    or hashlib.sha256(chain.encode() + b"\0" + payload.encode()).hexdigest()
                    != digest
                ):
                    raise PaperError("Paper journal integrity check failed; state was retained")
                PaperLedger._reduce(state, json.loads(payload))
                chain = digest
        except ValueError, KeyError, TypeError:
            raise PaperError("Paper journal payload is damaged; state was retained") from None
        return state, chain

    @staticmethod
    def _reduce(state, event):
        kind = event["kind"]
        if kind == "account":
            account = Account.model_validate(event["value"])
            if state["account"] is not None:
                raise PaperError("Paper journal contains multiple account origins")
            state["account"] = account.model_dump()
            state["cash"] = Decimal(account.starting_cash)
        elif kind == "instance":
            instance = Instance.model_validate(event["value"])
            state["instances"][instance.instance_id] = {
                "mandate": instance.model_dump(),
                "cash": Decimal(instance.capital),
                "base": Decimal(0),
            }
        elif kind == "decision":
            intent = Intent.model_validate(event["intent"])
            state["decisions"][intent.intent_id] = event
            if event["reason"] is None:
                state["orders"][intent.intent_id] = {
                    "intent": intent.model_dump(),
                    "status": "reserved",
                    "filled": Decimal(0),
                    "fees": Decimal(0),
                    "filled_notional": Decimal(0),
                    "cancellation_requested": False,
                }
        elif kind == "cancel_request":
            order = state["orders"][event["intent_id"]]
            order["cancellation_requested"] = True
        elif kind == "status":
            state["orders"][event["intent_id"]]["status"] = event["status"]
        elif kind == "block":
            state["blocks"].append(event["reason"])
        elif kind == "entries":
            state["entries_blocked"] = event["blocked"]
        elif kind == "fill":
            fill = Fill.model_validate(event["value"])
            order = state["orders"][fill.intent_id]
            instance = state["instances"][order["intent"]["instance_id"]]
            quantity, fee = Decimal(fill.quantity), Decimal(fill.fee)
            change = Decimal(fill.notional) * (-1 if order["intent"]["side"] == "BUY" else 1) - fee
            state["cash"] += change
            instance["cash"] += change
            instance["base"] += quantity * (1 if order["intent"]["side"] == "BUY" else -1)
            order["filled"] += quantity
            order["fees"] += fee
            order["filled_notional"] += Decimal(fill.notional)
            if order["status"] != "ambiguous":
                order["status"] = (
                    "filled"
                    if order["filled"] >= Decimal(order["intent"]["quantity"])
                    else "partially_filled"
                )
            state["fills"][fill.event_id] = fill.model_dump()
            if state["signal_plan"] is not None:
                from stratosphere.paper.decisions import mark_after_fill

                mark_after_fill(state, fill.event_id)
        elif isinstance(kind, str) and kind.startswith("signal_"):
            from stratosphere.paper.decisions import reduce_event

            reduce_event(state, event)
        elif isinstance(kind, str) and kind.startswith("runtime_"):
            from stratosphere.paper.runtime import reduce_event

            reduce_event(state, event)
        else:
            raise PaperError("Unknown paper journal event")

    def add_instance(self, instance: Instance, registry):
        instance = Instance.model_validate(instance.model_dump())
        strategy = registry.strategy(instance.strategy_id)
        if strategy["spec_sha256"] != instance.strategy_sha256:
            raise PaperError("The personal instance must pin its registered strategy identity")
        with self._transaction() as db:
            state, chain = self._load(db)
            existing = state["instances"].get(instance.instance_id)
            if existing:
                if existing["mandate"] != instance.model_dump():
                    raise PaperError("A personal instance's pinned mandate cannot change")
                return
            allocated = sum(Decimal(i["mandate"]["capital"]) for i in state["instances"].values())
            if instance.account_id != state["account"]["account_id"] or allocated + Decimal(
                instance.capital
            ) > Decimal(state["account"]["starting_cash"]):
                raise PaperError("Instance account/allocation exceeds the authored account")
            self._append(db, chain, {"kind": "instance", "value": instance.model_dump()})

    @staticmethod
    def _remaining(order):
        return max(Decimal(0), Decimal(order["intent"]["quantity"]) - order["filled"])

    @staticmethod
    def _reserved(order):
        if order["status"] in TERMINAL or order["intent"]["side"] != "BUY":
            return Decimal(0)
        notional = (
            PaperLedger._remaining(order) * Decimal(order["intent"]["price_bound"])
        ).quantize(QUOTE_QUANTUM, rounding=ROUND_CEILING)
        return notional + max(Decimal(0), Decimal(order["intent"]["fee_allowance"]) - order["fees"])

    def reserve(self, intent: Intent, *, now_ns: int):
        intent = Intent.model_validate(intent.model_dump())
        if type(now_ns) is not int or now_ns <= 0:
            raise PaperError("Use an explicit positive paper observation time")
        with self._transaction() as db:
            state, chain = self._load(db)
            if old := state["decisions"].get(intent.intent_id):
                if old["intent"] != intent.model_dump():
                    raise PaperError("An idempotency ID cannot be reused with changed parameters")
                return old[
                    "reason"
                ]  # None means retained reservation, never permission to resubmit.
            reason = (
                "reviewed_signal_required"
                if state["signal_plan"] is not None
                else reservation_reason(state, intent, now_ns)
            )
            self._append(
                db,
                chain,
                {
                    "kind": "decision",
                    "intent": intent.model_dump(),
                    "observed_ns": now_ns,
                    "reason": reason,
                },
            )
            return reason

    def begin_delivery(self, intent_id, *, now_ns: int):
        """Claim legacy authored delivery once. Reviewed accounts use SignalDecisions."""
        with self._transaction() as db:
            state, chain = self._load(db)
            if state["signal_plan"] is not None:
                raise PaperError("Use the reviewed signal gateway to claim delivery")
            self._claim_delivery(db, state, chain, intent_id, now_ns)

    def _claim_delivery(self, db, state, chain, intent_id, now_ns):
        order = self._order(state, intent_id)
        intent = order["intent"]
        instance = state["instances"][intent["instance_id"]]
        if (
            type(now_ns) is not int
            or not intent["created_ns"] <= now_ns < intent["expires_ns"]
            or now_ns - intent["quote_ns"] > instance["mandate"]["max_quote_age_ns"]
            or order["status"] != "reserved"
            or state["blocks"]
            or order.get("cancellation_requested")
            or (state["entries_blocked"] and intent["side"] == "BUY")
            or any(o["status"] in {"ambiguous", "dispatching"} for o in state["orders"].values())
        ):
            raise PaperError("Delivery is blocked or already claimed; reconcile before retrying")
        if state["signal_plan"] is not None:
            from stratosphere.paper.decisions import delivery_reason

            if delivery_reason(state, intent, now_ns) is not None:
                raise PaperError("Reviewed signal delivery is blocked; no claim was recorded")
        self._append(
            db,
            chain,
            {
                "kind": "status",
                "intent_id": intent_id,
                "status": "dispatching",
                "observed_ns": now_ns,
            },
        )

    @staticmethod
    def _order(state, intent_id):
        if intent_id not in state["orders"]:
            raise PaperError("That intent has no retained paper reservation")
        return state["orders"][intent_id]

    def observe_fill(self, fill: Fill):
        fill = Fill.model_validate(fill.model_dump())
        with self._transaction() as db:
            state, chain = self._load(db)
            if old := state["fills"].get(fill.event_id):
                if old != fill.model_dump():
                    self._append(
                        db,
                        chain,
                        {
                            "kind": "block",
                            "reason": "conflicting_fill_event",
                            "observation": fill.model_dump(),
                        },
                    )
                    return False
                return True
            order = self._order(state, fill.intent_id)
            event = {"kind": "fill", "value": fill.model_dump()}
            intent = order["intent"]
            late = order["status"] in TERMINAL or order["status"] == "reserved"
            self._reduce(state, event)  # Actual observed economics are retained even on a breach.
            self._append(db, chain, event)
            _, chain = self._load(db)
            instance = state["instances"][intent["instance_id"]]
            notional_bound = (order["filled"] * Decimal(intent["price_bound"])).quantize(
                QUOTE_QUANTUM, rounding=ROUND_CEILING if intent["side"] == "BUY" else ROUND_FLOOR
            )
            original_budget = (
                Decimal(intent["quantity"]) * Decimal(intent["price_bound"])
            ).quantize(QUOTE_QUANTUM, rounding=ROUND_CEILING) + Decimal(intent["fee_allowance"])
            breached = (
                late
                or order["filled"] > Decimal(intent["quantity"])
                or order["fees"] > Decimal(intent["fee_allowance"])
                or (intent["side"] == "BUY" and order["filled_notional"] > notional_bound)
                or (
                    intent["side"] == "BUY"
                    and order["filled_notional"] + order["fees"] + self._reserved(order)
                    > original_budget
                )
                or (intent["side"] == "SELL" and order["filled_notional"] < notional_bound)
                or abs(Decimal(fill.notional) - Decimal(fill.quantity) * Decimal(fill.price))
                > QUOTE_QUANTUM
                or (
                    intent["side"] == "BUY" and Decimal(fill.price) > Decimal(intent["price_bound"])
                )
                or (
                    intent["side"] == "SELL"
                    and Decimal(fill.price) < Decimal(intent["price_bound"])
                )
                or min(state["cash"], instance["cash"], instance["base"]) < 0
            )
            if breached:
                self._append(db, chain, {"kind": "block", "reason": "observed_execution_breach"})
            return not breached

    def cancel_request(self, intent_id):
        """Retain cancellation intent without claiming a provider canceled anything."""
        with self._transaction() as db:
            state, chain = self._load(db)
            order = self._order(state, intent_id)
            if order["status"] in TERMINAL:
                raise PaperError("That paper order is already terminal")
            self._append(db, chain, {"kind": "cancel_request", "intent_id": intent_id})

    def mark_ambiguous(self, intent_id):
        self._set_status(intent_id, "ambiguous")

    def _set_status(self, intent_id, status):
        with self._transaction() as db:
            state, chain = self._load(db)
            order = self._order(state, intent_id)
            if order["status"] in TERMINAL:
                raise PaperError("A terminal paper order cannot be reopened by a control request")
            self._append(db, chain, {"kind": "status", "intent_id": intent_id, "status": status})

    def settle(self, intent_id, status, *, confirmed_filled: str):
        """Accept a trusted fixture terminal observation only after fills reconcile."""
        if not isinstance(status, str) or status not in TERMINAL:
            raise PaperError("A terminal paper status is required")
        confirmed_filled = amount(confirmed_filled)
        with self._transaction() as db:
            state, chain = self._load(db)
            order = self._order(state, intent_id)
            if (
                order["filled"] != Decimal(confirmed_filled)
                or (status == "filled" and order["filled"] != Decimal(order["intent"]["quantity"]))
                or (status == "rejected" and order["filled"] != 0)
            ):
                self._append(
                    db, chain, {"kind": "status", "intent_id": intent_id, "status": "ambiguous"}
                )
                return False
            if order["status"] in TERMINAL and order["status"] != status:
                self._append(
                    db,
                    chain,
                    {
                        "kind": "status",
                        "intent_id": intent_id,
                        "status": "ambiguous",
                        "reason": "conflicting_terminal_status",
                        "observed_status": status,
                    },
                )
                return False
            self._append(db, chain, {"kind": "status", "intent_id": intent_id, "status": status})
            return True

    def block_entries(self):
        with self._transaction() as db:
            _, chain = self._load(db)
            self._append(db, chain, {"kind": "entries", "blocked": True})

    def snapshot(self):
        with self._transaction() as db:
            state, chain = self._load(db)
            state["journal_sha256"] = chain
            state["reserved_cash"] = sum(
                (self._reserved(o) for o in state["orders"].values()), Decimal(0)
            )

            def normalize(value):
                if isinstance(value, Decimal):
                    return text(value)
                if isinstance(value, dict):
                    return {key: normalize(item) for key, item in value.items()}
                if isinstance(value, list):
                    return [normalize(item) for item in value]
                return value

            return normalize(state)
