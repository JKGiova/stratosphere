"""Exact MT5 deployment approvals and account-serialized, non-replaying order intents."""

import fcntl
import hashlib
import os
import time
import uuid
from contextlib import contextmanager
from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from stratosphere.mt5.bridge import (
    SYMBOL,
    BridgeError,
    UncertainOrder,
    atomic_write,
    decode,
    encode,
    private_path,
)
from stratosphere.phase0.contracts import Contract, Digest, decimal_text, fingerprint


class Deployment(Contract):
    schema_version: Literal["mt5-deployment/1"] = "mt5-deployment/1"
    strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    strategy_sha256: Digest
    report_sha256: Digest
    implementation_sha256: Digest
    ea_sha256: Digest
    login: str = Field(pattern=r"^[1-9][0-9]{0,18}$")
    server: str = Field(min_length=1, max_length=100, pattern=r'^[^\t\r\n\x00"]+$')
    mode: Literal["demo", "real"]
    symbol: str = Field(pattern=SYMBOL.pattern)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    max_volume: str
    max_order_risk: str
    max_order_margin: str
    max_spread: str
    min_equity: str
    cost_buffer: str
    max_orders: int = Field(strict=True, ge=1, le=1000)
    expires: int = Field(strict=True, gt=0)
    limitations: tuple[str, ...] = (
        "Not an FTMO rules certification or a validated strategy runner",
        "One account position at a time, no reinforcement; mandatory broker stop-loss",
        "Native stop risk excludes gap/slippage beyond the explicit cost buffer",
    )

    @model_validator(mode="after")
    def bounded(self):
        for name in (
            "max_volume",
            "max_order_risk",
            "max_order_margin",
            "max_spread",
            "min_equity",
            "cost_buffer",
        ):
            value = decimal_text(getattr(self, name))
            if Decimal(value) <= 0:
                raise ValueError("Positive exact MT5 risk limits are required")
            object.__setattr__(self, name, value)
        return self

    @property
    def digest(self):
        return fingerprint(self)


class Order(Contract):
    signal_sha256: Digest
    side: Literal["buy", "sell"]
    volume: str
    stop_loss: str
    take_profit: str = "0"

    @model_validator(mode="after")
    def valid(self):
        for name in ("volume", "stop_loss", "take_profit"):
            value = decimal_text(getattr(self, name))
            if Decimal(value) < 0 or (name != "take_profit" and Decimal(value) == 0):
                raise ValueError("Positive volume and mandatory stop loss required")
            object.__setattr__(self, name, value)
        return self


class MT5Trading:
    def __init__(self, bridge):
        self.bridge = bridge

    @contextmanager
    def lease(self):
        session = self.bridge.session()
        key = tuple(session.get(name) for name in ("login", "server", "mode"))
        if self.bridge.account_key and self.bridge.account_key != key:
            raise BridgeError("MT5 account changed before acquiring the trading lease")
        path = self.bridge.state_directory(session) / "trading.lock"
        private_path(path)
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise BridgeError("Another MT5 account operation owns the trading lease") from exc
            if self.bridge.session() != session:
                raise BridgeError("MT5 session changed while acquiring the trading lease")
            self.bridge.account_key = key
            yield
        finally:
            os.close(fd)

    async def approve(self, deployment: Deployment, exact_digest: str, *, allow_real=False):
        if deployment.digest != exact_digest:
            raise BridgeError("Review and approve the exact finished MT5 deployment digest")
        if deployment.mode == "real" and not allow_real:
            raise BridgeError("Real-account activation requires explicit --allow-real")
        with self.lease():
            account = await self.bridge.account()
            self.verify(deployment, account)
            # Native policy is not enough: the client journal must also record exact user approval.
            with self.bridge.journal() as db:
                db.execute(
                    "INSERT INTO approvals VALUES (?,?,'approved')",
                    (exact_digest, deployment.model_dump_json().encode()),
                )
            fields = deployment.model_dump(exclude={"limitations"})
            fields["digest"] = deployment.digest
            atomic_write(self.bridge.root / "approvals" / (exact_digest + ".tsv"), encode(fields))
        return exact_digest

    def verify(self, deployment, account, *, entry=True):
        if entry and deployment.expires <= int(time.time()):
            raise BridgeError("MT5 deployment expired")
        if deployment.implementation_sha256 != self.bridge.implementation_digest:
            raise BridgeError("MT5 implementation changed; old approval cannot authorize it")
        for key in ("login", "server", "mode", "currency", "ea_sha256"):
            if account.get(key) != getattr(deployment, key):
                raise BridgeError("MT5 approval/account/EA identity mismatch")
        if account.get("connected") != "1":
            raise BridgeError("MT5 account is disconnected")
        for digest in (deployment.strategy_sha256, deployment.report_sha256):
            path = self.bridge.state_directory() / "evidence" / (digest + ".bin")
            private_path(path)
            if (
                not path.is_file()
                or path.stat().st_size > 8_000_000
                or (hashlib.sha256(path.read_bytes()).hexdigest() != digest)
            ):
                raise BridgeError("Exact MT5 strategy/report evidence missing or changed")

    def deployment(self, digest, *, exit_only=False):
        with self.bridge.journal() as db:
            row = db.execute(
                "SELECT payload,state FROM approvals WHERE digest=?", (digest,)
            ).fetchone()
            if not row or row[1] not in ({"approved", "revoked"} if exit_only else {"approved"}):
                raise BridgeError("No current user-approved MT5 deployment")
            value = Deployment.model_validate_json(row[0])
            if value.digest != digest:
                raise BridgeError("Retained MT5 approval changed")
            return value

    async def revoke(self, digest):
        with self.lease(), self.bridge.journal() as db:
            value = self.deployment(digest, exit_only=True)
            db.execute("UPDATE approvals SET state='revoked' WHERE digest=?", (digest,))
            fields = value.model_dump(exclude={"limitations"})
            fields.update(digest=digest, revoked="1")
            atomic_write(self.bridge.root / "approvals" / (digest + ".tsv"), encode(fields))

    async def order(self, digest, order: Order, *, check_only=False):
        with self.lease():
            deployment = self.deployment(digest)
            account = await self.bridge.account()
            self.verify(deployment, account)
            if Decimal(order.volume) > Decimal(deployment.max_volume):
                raise BridgeError("MT5 volume exceeds approved bound")
            identity = hashlib.sha256((digest + order.signal_sha256).encode()).hexdigest()[:32]
            with self.bridge.journal() as db:
                pending = db.execute(
                    "SELECT 1 FROM requests WHERE op IN ('submit','close') "
                    "AND state IN ('reserved','uncertain')"
                ).fetchone()
                if pending:
                    raise BridgeError("MT5 outcome unresolved; account entries remain halted")
                count = db.execute(
                    "SELECT COUNT(*) FROM requests WHERE op='submit' "
                    "AND instr(CAST(payload AS TEXT),?)>0",
                    (digest,),
                ).fetchone()[0]
                if count >= deployment.max_orders:
                    raise BridgeError("MT5 deployment order budget exhausted")
            args = {
                "approval": digest,
                "symbol": deployment.symbol,
                "side": order.side,
                "volume": order.volume,
                "sl": order.stop_loss,
                "tp": order.take_profit,
                "magic": str(int(identity[:15], 16)),
            }
            result = await self.bridge._request(
                "check" if check_only else "submit",
                args,
                identity=None if check_only else identity,
                trade=not check_only,
            )
            if not check_only:
                await self.settle(identity, result, deployment.symbol, args["magic"])
            return result

    async def settle(self, identity, result, symbol, magic, *, closing_ticket=None):
        """Do not release ambiguity merely because OrderSend returned true or a broker ID."""
        try:
            if result.get("retcode") not in {"10009", "10010"}:
                raise BridgeError("Nonterminal or uncertain broker submission result")
            positions = await self.bridge.read("positions")
            orders = await self.bridge.read("orders")
            if int(orders.get("count", "0")):
                raise BridgeError("Provider orders remain open")
            rows = [
                {
                    key: positions.get(f"p.{i}.{key}")
                    for key in ("symbol", "magic", "volume", "ticket")
                }
                for i in range(1, int(positions.get("count", "0")) + 1)
            ]
            if closing_ticket:
                if any(row["ticket"] == closing_ticket for row in rows):
                    raise BridgeError("Close is partial or position remains")
            elif (
                len(rows) != 1
                or rows[0]["symbol"] != symbol
                or rows[0]["magic"] != magic
                or (Decimal(rows[0]["volume"]) != Decimal(result["filled_volume"]))
            ):
                raise BridgeError("Actual position differs from the reported fill")
        except BaseException as exc:
            with self.bridge.journal() as db:
                db.execute("UPDATE requests SET state='uncertain' WHERE id=?", (identity,))
            raise UncertainOrder(
                "MT5 result needs reconciliation; new entries remain halted"
            ) from exc

    async def close(self, digest, ticket: str):
        """Protective full exit only for an exactly journal-owned position; no LLM dependency."""
        if not ticket.isdigit() or int(ticket) <= 0:
            raise BridgeError("Exact positive MT5 position ticket required")
        with self.lease():
            deployment = self.deployment(digest, exit_only=True)
            account = await self.bridge.account()
            self.verify(deployment, account, entry=False)
            positions = await self.bridge.read("positions")
            match = next(
                (
                    positions
                    for i in range(1, int(positions.get("count", "0")) + 1)
                    if positions.get(f"p.{i}.ticket") == ticket
                ),
                None,
            )
            if match is None:
                raise BridgeError("MT5 position no longer exists")
            index = next(
                i
                for i in range(1, int(positions["count"]) + 1)
                if positions[f"p.{i}.ticket"] == ticket
            )
            magic = positions[f"p.{index}.magic"]
            if positions[f"p.{index}.symbol"] != deployment.symbol:
                raise BridgeError("MT5 position belongs to another instrument")
            with self.bridge.journal() as db:
                rows = db.execute("SELECT payload FROM requests WHERE op='submit'").fetchall()
            if not any(
                decode(row[0]).get("magic") == magic and decode(row[0]).get("approval") == digest
                for row in rows
            ):
                raise BridgeError("Cannot close an externally owned MT5 position")
            with self.bridge.journal() as db:
                previous = db.execute(
                    "SELECT payload,state FROM requests WHERE op='close'"
                ).fetchall()
            if any(
                decode(row[0]).get("ticket") == ticket and row[1] in {"reserved", "uncertain"}
                for row in previous
            ):
                raise BridgeError("Close outcome unresolved; do not send another close")
            # A new explicit operator close may follow a confirmed native pre-submit rejection.
            # An ambiguous prior close is blocked above; the transport itself never retries.
            identity = uuid.uuid4().hex
            result = await self.bridge._request(
                "close",
                {"approval": digest, "ticket": ticket, "symbol": deployment.symbol, "magic": magic},
                identity=identity,
                trade=True,
            )
            await self.settle(identity, result, deployment.symbol, magic, closing_ticket=ticket)
            return result
