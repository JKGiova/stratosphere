"""Read-only operational views of an existing authored account journal."""

import json
import re
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from pydantic import AwareDatetime, JsonValue

from stratosphere.paper.ledger import TERMINAL, Account, PaperError, PaperLedger, text
from stratosphere.phase0.contracts import Contract, Digest

Section = Literal["positions", "orders", "fills", "health"]
PAGE_SIZE = 20


def natural_account_inspection(value):
    match = re.fullmatch(
        r"\s*(?:please\s+)?inspect\s+(?:the\s+)?authored\s+(?:paper\s+)?account\s+"
        r"(positions|orders|fills|health)(?:\s+page\s+([1-9][0-9]{0,8}))?\s*",
        value,
        re.IGNORECASE,
    )
    return (match[1].lower(), match[2] or "1") if match else None


def _json(value):
    if isinstance(value, Decimal):
        return text(value)
    if isinstance(value, dict):
        return {key: _json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json(item) for item in value]
    return value


class JournalView(Contract):
    schema_version: Literal["authored-account-view/1"] = "authored-account-view/1"
    account: Account
    journal_sha256: Digest
    captured_at: AwareDatetime
    summary: dict[str, JsonValue]
    positions: tuple[dict[str, JsonValue], ...]
    orders: tuple[dict[str, JsonValue], ...]
    fills: tuple[dict[str, JsonValue], ...]
    health: tuple[dict[str, JsonValue], ...]
    execution_eligible: Literal[False] = False

    def page(self, section: Section, number=1):
        if section not in {"positions", "orders", "fills", "health"}:
            raise PaperError("Select positions, orders, fills or health")
        rows = getattr(self, section)
        pages = max(1, (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE)
        if type(number) is not int or not 1 <= number <= pages:
            raise PaperError(f"Select a page from 1 through {pages}")
        return rows[(number - 1) * PAGE_SIZE : number * PAGE_SIZE], pages

    def markdown(self, section: Section, number=1):
        rows, pages = self.page(section, number)
        summary = json.dumps(self.summary, ensure_ascii=False, indent=2)
        lines = [
            "# Authored account · " + section,
            "Retained fixture observations; provider account, feed and execution are unverified.",
            "Captured " + self.captured_at.isoformat() + ". Refresh explicitly with /paper.",
            f"Journal `{self.journal_sha256}` · account `{self.account.account_id}`",
            f"Page {number}/{pages} · {len(getattr(self, section))} total rows · "
            f"use `/paper {section} <page>` for another capture.",
            "## Account summary\n\n```json\n" + summary + "\n```",
        ]
        if not rows:
            lines.append("No retained rows in this section.")
        for index, row in enumerate(rows, (number - 1) * PAGE_SIZE + 1):
            lines.append(
                f"## Row {index}\n\n```json\n"
                + json.dumps(row, ensure_ascii=False, indent=2)
                + "\n```"
            )
        return "\n\n".join(lines)


def _projection(state, chain, at):
    account = Account.model_validate(state["account"])
    orders = []
    positions = []
    health = []
    pending = [order for order in state["orders"].values() if order["status"] not in TERMINAL]
    reserved = sum((PaperLedger._reserved(order) for order in pending), Decimal(0))
    for identity, record in state["instances"].items():
        own = [order for order in pending if order["intent"]["instance_id"] == identity]
        positions.append(
            {
                **record,
                "reserved_cash": sum((PaperLedger._reserved(order) for order in own), Decimal(0)),
                "pending_orders": len(own),
            }
        )
    for order in state["orders"].values():
        instance = state["instances"][order["intent"]["instance_id"]]
        orders.append(
            {
                **order,
                "strategy_id": instance["mandate"]["strategy_id"],
                "remaining_quantity": PaperLedger._remaining(order),
                "reserved_cash": PaperLedger._reserved(order),
            }
        )
    health.append({"kind": "account", "entries_blocked": state["entries_blocked"]})
    health.extend({"kind": "account_block", "reason": reason} for reason in state["blocks"])
    for identity, stream in state["runtimes"].items():
        health.append(
            {
                "kind": "closed_bar_stream",
                "instance_id": identity,
                "mandate": stream["mandate"],
                "digest": stream["digest"],
                "approved": stream["approved"],
                "accepted_count": stream["accepted_count"],
                "last_received_ns": stream["last_received_ns"],
                "signals": len(stream["signals"]),
                "fault_count": len(stream["faults"]),
            }
        )
        health.extend(
            {"kind": "stream_fault", "instance_id": identity, "observation": fault}
            for fault in stream["faults"]
        )
    plan = state["signal_plan"]
    quote_status = "unavailable"
    if plan is not None:
        health.append(
            {
                "kind": "decision_plan",
                **{
                    key: plan[key]
                    for key in (
                        "plan",
                        "digest",
                        "approved",
                        "risk_latches",
                        "latest_quote",
                        "last_receipt_ns",
                        "last_quote_status",
                        "peak_equity",
                    )
                },
            }
        )
        if plan["latest_quote"]:
            latest = plan["quotes"][plan["latest_quote"]]
            delta = at - datetime(1970, 1, 1, tzinfo=UTC)
            now_ns = (delta.days * 86400 + delta.seconds) * 10**9 + delta.microseconds * 1000
            event_age = now_ns - latest["event_ns"]
            receipt_age = now_ns - plan["last_receipt_ns"]
            recent = (
                0 <= event_age <= plan["plan"]["max_quote_age_ns"]
                and receipt_age >= 0
                and plan["last_quote_status"] == "accepted"
                and not plan["quote_faults"]
            )
            quote_status = "authored_declared_recent" if recent else "authored_stale_or_invalid"
            health.append(
                {
                    "kind": "last_authored_quote",
                    **latest,
                    "event_age_ns_at_capture": event_age,
                    "receipt_age_ns_at_capture": receipt_age,
                    "status_at_capture": quote_status,
                }
            )
        for key in ("quote_faults", "quote_attempts", "risk_findings"):
            health.extend({"kind": key, "observation": finding} for finding in plan[key])
        health.extend(
            {"kind": "signal_decision", "observation": decision}
            for decision in plan["consumed"].values()
        )
    health.extend(
        {"kind": "denied_decision", "observation": decision}
        for decision in state["decisions"].values()
        if decision["reason"] is not None
    )
    ambiguous = any(order["status"] in {"ambiguous", "dispatching"} for order in pending)
    return JournalView(
        account=account,
        journal_sha256=chain,
        captured_at=at,
        summary=_json(
            {
                "settled_cash": state["cash"],
                "reserved_cash": reserved,
                "unreserved_cash": state["cash"] - reserved,
                "held_base": sum((row["base"] for row in positions), Decimal(0)),
                "instance_count": len(positions),
                "pending_order_count": len(pending),
                "fill_count": len(state["fills"]),
                "entries_blocked": state["entries_blocked"],
                "reconciliation_required": bool(state["blocks"]) or ambiguous,
                "risk_stops": plan["risk_latches"] if plan else [],
                "quote_status_at_capture": quote_status,
                "valuation": "unavailable; no current authenticated mark or P/L",
                "feed_service": "not connected; retained streams do not prove a running service",
            }
        ),
        positions=_json(positions),
        orders=_json(orders),
        fills=list(state["fills"].values()),
        health=_json(health),
    )


class JournalInspector:
    def __init__(
        self,
        journal: Path,
        *,
        online_check,
        secret_check=lambda data: False,
        clock=lambda: datetime.now(UTC),
    ):
        self.journal = journal
        self.online_check, self.secret_check, self.clock = online_check, secret_check, clock

    async def capture(self):
        path = self.journal.expanduser().absolute()
        if any(parent.is_symlink() for parent in (path, *path.parents)) or not path.is_file():
            raise PaperError(
                "No readable authored paper journal is configured; no account was created"
            )
        await self.online_check()
        at = self.clock()
        if at.tzinfo is None or at.utcoffset() is None:
            raise PaperError("Use a timezone-aware account capture clock")
        database = None
        try:
            database = sqlite3.connect("file:" + quote(str(path), safe="/") + "?mode=ro", uri=True)
            database.execute("PRAGMA query_only=ON")
            database.execute("BEGIN")
            if database.execute("PRAGMA user_version").fetchone()[0] not in (1, 2, 3):
                raise PaperError("Unsupported authored journal version")
            with localcontext() as context:
                context.prec = 100
                state, chain = PaperLedger._load(database)
                view = _projection(state, chain, at.astimezone(UTC))
            serialized = view.model_dump_json()
            values = [serialized]

            def strings(value):
                if isinstance(value, str):
                    values.append(value)
                elif isinstance(value, dict):
                    for key, item in value.items():
                        strings(key)
                        strings(item)
                elif isinstance(value, list):
                    for item in value:
                        strings(item)

            strings(view.model_dump(mode="json"))
            for value in values:
                decoded = re.sub(
                    r"\\u([a-fA-F0-9]{4})", lambda match: chr(int(match[1], 16)), value
                )
                if self.secret_check(value.encode()) or self.secret_check(
                    decoded.encode(errors="replace")
                ):
                    raise PaperError("Credential-bearing account evidence cannot be displayed")
            return view
        except sqlite3.Error, ValueError, TypeError, KeyError:
            raise PaperError(
                "The authored account journal could not be verified; no account was changed"
            ) from None
        finally:
            if database is not None:
                database.close()
