"""Headless gold strategy connection to the existing EA; no portfolio-management loop."""

import argparse
import asyncio
import fcntl
import json
import os
import time
import uuid
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from stratosphere.credentials import CredentialStore, credential_io
from stratosphere.models import create_provider
from stratosphere.models.streaming import ModelError
from stratosphere.mt5.bridge import BridgeError, MT5Bridge, decode, private_path
from stratosphere.mt5.gold_strategy import (
    BAR_COUNT,
    FRAME,
    PROFILE,
    SYMBOL,
    compiled_script,
    digest,
    document,
    entry_bounds,
    evaluate,
    validate_volume,
)
from stratosphere.mt5.trading import Deployment, MT5Trading, Order
from stratosphere.phase0.contracts import Contract, Digest, decimal_text, fingerprint
from stratosphere.registry.workspace import WorkspaceDatabase
from stratosphere.research.store import default_research_root
from stratosphere.workflow.risk import AdvisoryAgents


class SizingProposal(Contract):
    schema_version: Literal["mt5-gold-sizing-proposal/1"] = "mt5-gold-sizing-proposal/1"
    context_sha256: Digest
    decision: Literal["enter", "skip"]
    volume: str
    explanation: str = Field(min_length=1, max_length=1500)

    @model_validator(mode="after")
    def valid(self):
        from decimal import Decimal

        value = Decimal(decimal_text(self.volume))
        if (
            value < 0
            or (self.decision == "enter" and value == 0)
            or (self.decision == "skip" and value != 0)
        ):
            raise ValueError("Use positive lots to enter or exactly zero to skip")
        return self


def save_new(path, value):
    private_path(path)
    data = (
        value if isinstance(value, bytes) else json.dumps(value, sort_keys=True, indent=2).encode()
    )
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def retain(path, value):
    if path.exists():
        private_path(path)
        if path.read_bytes() != value:
            raise BridgeError("Retained gold connection evidence changed")
    else:
        save_new(path, value)


async def saved_provider():
    settings = await credential_io(CredentialStore().load_model)
    if settings is None:
        raise BridgeError("Save model settings in the TUI before an approved gold sizing call")
    return create_provider(settings)


class GoldConnection:
    def __init__(self, bridge, database, *, provider_factory=saved_provider):
        self.bridge, self.database = bridge, database
        self.trading = MT5Trading(bridge)
        self.provider_factory = provider_factory
        self.document = document()
        self.record = database.register(
            self.document.formal_spec,
            self.document.name,
            {"kind": "operator-gold-mt5/1", "profile": PROFILE},
        )
        self.folder = (
            bridge.state_directory()
            / "gold"
            / fingerprint(self.document.formal_spec)
            / bridge.implementation_digest
        )
        private_path(self.folder)
        self.folder.mkdir(parents=True, mode=0o700, exist_ok=True)
        retain(self.folder / "strategy.json", self.document.model_dump_json().encode())
        retain(self.folder / "gold_golden.py", compiled_script())
        binding = {
            "schema_version": "mt5-gold-binding/1",
            "strategy_id": self.record["public_id"],
            "agent_id": self.record["agent_id"],
            "account": {k: bridge.session()[k] for k in ("login", "server", "mode", "ea_sha256")},
            "implementation_sha256": bridge.implementation_digest,
            "strategy_sha256": bridge.retain_evidence(self.document.model_dump_json().encode()),
            "script_sha256": bridge.retain_evidence(compiled_script()),
            "activation": "Exact deployment approval required; attachment grants no authority",
        }
        retain(self.folder / "binding.json", json.dumps(binding, sort_keys=True).encode())
        self.binding = binding

    def record_event(self, value):
        path = self.folder / (uuid.uuid4().hex + ".json")
        save_new(path, value)
        return path

    async def capture(self):
        session = self.bridge.session()
        account = await self.bridge.account()
        if account["connected"] != "1":
            raise BridgeError("Gold strategy requires a connected MT5 account")
        if any(account[k] != self.binding["account"][k] for k in self.binding["account"]):
            raise BridgeError("Gold connection/account/EA binding changed")
        symbol = await self.bridge.read("symbol", symbol=SYMBOL)
        rates = await self.bridge.read("rates", symbol=SYMBOL, frame=FRAME, count=BAR_COUNT)
        positions = await self.bridge.read("positions")
        orders = await self.bridge.read("orders")
        quote = await self.bridge.read("quote", symbol=SYMBOL)
        if self.bridge.session() != session:
            raise BridgeError("Gold EA session changed during observation")
        signal = evaluate(rates, quote)
        observation = {
            "schema_version": "mt5-gold-observation/1",
            "captured_utc": int(time.time()),
            "session": session,
            "account": account,
            "symbol": symbol,
            "quote": quote,
            "positions": positions,
            "orders": orders,
            "signal": signal,
        }
        # Full rates already have a durable bridge receipt; its exact response is also pinned here.
        observation["rates_response_sha256"] = self.bridge.retain_evidence(
            json.dumps(rates, sort_keys=True).encode()
        )
        self.record_event(observation)
        return observation

    async def status(self):
        observation = await self.capture()
        return {
            "strategy_id": self.record["public_id"],
            "agent_id": self.record["agent_id"],
            "signal": observation["signal"],
            "positions": int(observation["positions"]["count"]),
            "orders": int(observation["orders"]["count"]),
            "script": str(self.folder / "gold_golden.py"),
            "connection": "Existing StratosphereBridge EA; read-only status, not activation",
        }

    async def draft(self, limits):
        observation = await self.capture()
        expected = {
            "max_volume",
            "max_order_risk",
            "max_order_margin",
            "max_spread",
            "min_equity",
            "cost_buffer",
            "max_orders",
            "expires",
        }
        if (
            not isinstance(limits, dict)
            or set(limits) != expected
            or type(limits["expires"]) is not int
            or type(limits["max_orders"]) is not int
        ):
            raise BridgeError("Supply exactly the eight explicit gold deployment limits")
        if (
            limits.get("max_orders") != 1
            or not int(time.time()) < limits.get("expires", 0) <= int(time.time()) + 86400
        ):
            raise BridgeError("Gold pilot deployments require one entry and expiry within 24 hours")
        account = observation["account"]
        if account["mode"] != "demo":
            raise BridgeError("This gold connector pilot supports demo accounts only")
        report = {
            "schema_version": "mt5-gold-connection-review/1",
            "strategy_id": self.record["public_id"],
            "specification_sha256": fingerprint(self.document.formal_spec),
            "implementation_sha256": self.bridge.implementation_digest,
            "signal": observation["signal"],
            "profile": PROFILE,
            "validation": (
                "Operator-defined signal connection; no CFD backtest, profitability "
                "or FTMO certification"
            ),
        }
        deployment = Deployment.model_validate(
            limits
            | {
                "strategy_id": self.record["public_id"],
                "symbol": SYMBOL,
                "strategy_sha256": self.binding["strategy_sha256"],
                "report_sha256": self.bridge.retain_evidence(
                    json.dumps(report, sort_keys=True).encode()
                ),
                "implementation_sha256": self.bridge.implementation_digest,
                **{k: account[k] for k in ("ea_sha256", "login", "server", "mode", "currency")},
            }
        )
        path = self.folder / (deployment.digest + ".deployment.json")
        retain(path, deployment.model_dump_json().encode())
        return {
            "deployment": deployment.model_dump(mode="json"),
            "digest": deployment.digest,
            "path": str(path),
            "approved": False,
        }

    def verify_deployment(self, deployment, observation, *, entry=True):
        self.trading.verify(deployment, observation["account"], entry=entry)
        if (
            deployment.strategy_id != self.record["public_id"]
            or deployment.symbol != SYMBOL
            or deployment.strategy_sha256 != self.binding["strategy_sha256"]
            or deployment.max_orders != 1
            or deployment.mode != "demo"
            or (entry and deployment.expires > int(time.time()) + 86400)
        ):
            raise BridgeError(
                "Deployment does not bind this exact gold strategy and one-entry profile"
            )
        path = self.bridge.state_directory() / "evidence" / (deployment.report_sha256 + ".bin")
        report = json.loads(path.read_bytes())
        if (
            report.get("schema_version") != "mt5-gold-connection-review/1"
            or report.get("strategy_id") != self.record["public_id"]
            or report.get("specification_sha256") != fingerprint(self.document.formal_spec)
            or report.get("profile") != PROFILE
            or report.get("implementation_sha256") != self.bridge.implementation_digest
        ):
            raise BridgeError("Gold deployment has no matching connection review")

    def owned_position(self, positions, deployment_digest):
        if int(positions["count"]) != 1:
            raise BridgeError("Gold connection supports one exact account position only")
        if positions["p.1.symbol"] != SYMBOL or positions["p.1.type"] != "0":
            raise BridgeError("Gold connection cannot manage external or short exposure")
        with self.bridge.journal() as db:
            rows = db.execute("SELECT payload,state FROM requests WHERE op='submit'").fetchall()
        if not any(
            decode(payload).get("approval") == deployment_digest
            and decode(payload).get("magic") == positions["p.1.magic"]
            and state == "observed"
            for payload, state in rows
        ):
            raise BridgeError("Gold position has no exact owned entry receipt")
        return positions["p.1.ticket"]

    async def advisory(self, observation, deployment):
        bounds = entry_bounds(
            observation["signal"],
            observation["quote"],
            observation["symbol"],
            observation["account"],
            deployment,
        )
        context = {
            "schema_version": "mt5-gold-sizing-context/1",
            "strategy": PROFILE,
            "signal": observation["signal"],
            "bounds": bounds,
        }
        context_sha = digest(context)
        slot = self.folder / (
            digest(
                {"deployment": deployment.digest, "signal": observation["signal"]["signal_sha256"]}
            )
            + ".advisory-slot.json"
        )
        # Freeze the one advisory operation before inference. Lost/failed calls never replay.
        reservation = {
            "context_sha256": context_sha,
            "created_utc": int(time.time()),
            "context": context,
            "state": "reserved",
        }
        try:
            save_new(slot, reservation)
        except FileExistsError:
            raise BridgeError("Gold advisory slot already consumed; no automatic retry") from None
        try:
            provider = await self.provider_factory()
            settings = getattr(provider, "settings", None)
            key = settings.api_key.get_secret_value() if settings else ""
            advisor = AdvisoryAgents(
                provider, secret_check=lambda data: bool(key and key.encode() in data)
            )
            proposal = await advisor.call(
                "You are the isolated position-risk agent for one bounded gold demo/pilot entry. "
                "Choose enter with a broker-step lot quantity within ALL supplied bounds, or skip "
                "with volume zero if uncertain. The signal is application-calculated. Use only "
                "the anonymous numeric facts. No account access, leverage changes, orders "
                "or approval are available. Copy context_sha256 exactly. "
                "This proposal does not authorize trading.",
                {"context_sha256": context_sha, "facts": context},
                SizingProposal,
            )
            if proposal.context_sha256 != context_sha:
                raise BridgeError("Gold sizing proposal context mismatch")
            if proposal.decision == "enter":
                validate_volume(proposal, bounds)
            self.record_event(
                {
                    "kind": "gold-sizing-reply/1",
                    "slot": slot.name,
                    "context_sha256": context_sha,
                    "proposal": proposal.model_dump(mode="json"),
                }
            )
            return proposal, bounds
        except BaseException:
            self.record_event(
                {
                    "kind": "gold-sizing-failed/1",
                    "slot": slot.name,
                    "meaning": "Consumed advisory slot; no entry/retry",
                }
            )
            raise

    async def tick(self, deployment_digest):
        # Separate strategy owner lock prevents concurrent advisory calls. Native order gateway
        # still acquires the canonical account lease and performs fresh account/native checks.
        path = self.folder / "runner.lock"
        private_path(path)
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise BridgeError("Another gold runner owns this strategy connection") from None
            observation = await self.capture()
            if int(observation["orders"]["count"]):
                raise BridgeError("Pending account orders block the gold strategy")
            if int(observation["positions"]["count"]):
                deployment = self.trading.deployment(deployment_digest, exit_only=True)
                self.verify_deployment(deployment, observation, entry=False)
                ticket = self.owned_position(observation["positions"], deployment_digest)
                if observation["signal"]["desired_position"] == "flat":
                    return {
                        "action": "closed_owned_position",
                        "result": await self.trading.close(deployment_digest, ticket),
                    }
                return {"action": "hold_owned_position", "ticket": ticket}
            if observation["signal"]["desired_position"] == "flat":
                return {"action": "stay_flat", "signal": observation["signal"]}
            deployment = self.trading.deployment(deployment_digest)
            self.verify_deployment(deployment, observation)
            if (
                observation["account"].get("trading_input") != "1"
                or observation["account"].get("terminal_trade_allowed") != "1"
            ):
                raise BridgeError(
                    "Approved gold execution also needs EA and terminal trading enabled"
                )
            with self.bridge.journal() as db:
                pending = db.execute(
                    "SELECT 1 FROM requests WHERE op IN ('submit','close') "
                    "AND state IN ('reserved','uncertain')"
                ).fetchone()
                attempts = db.execute("SELECT payload FROM requests WHERE op='submit'").fetchall()
            if pending or any(
                decode(row[0]).get("approval") == deployment_digest for row in attempts
            ):
                raise BridgeError("Gold entry budget consumed or account outcome unresolved")
            before = time.monotonic()
            proposal, _ = await self.advisory(observation, deployment)
            if proposal.decision == "skip":
                return {"action": "agent_skipped", "proposal": proposal.model_dump(mode="json")}
            refreshed = await self.capture()
            if (
                time.monotonic() - before > 120
                or refreshed["session"] != observation["session"]
                or refreshed["signal"]["signal_sha256"] != observation["signal"]["signal_sha256"]
                or int(refreshed["positions"]["count"])
                or int(refreshed["orders"]["count"])
            ):
                raise BridgeError(
                    "Gold signal/session/exposure changed or advisory expired; no order"
                )
            self.verify_deployment(deployment, refreshed)
            current = entry_bounds(
                refreshed["signal"],
                refreshed["quote"],
                refreshed["symbol"],
                refreshed["account"],
                deployment,
            )
            validate_volume(proposal, current)
            # Refresh the initial stop, then let native risk/margin checks gate execution.
            order = Order(
                signal_sha256=refreshed["signal"]["signal_sha256"],
                side="buy",
                volume=proposal.volume,
                stop_loss=current["stop_loss"],
            )
            self.record_event(
                {
                    "kind": "gold-entry-intent/1",
                    "deployment": deployment_digest,
                    "order": order.model_dump(mode="json"),
                    "proposal": proposal.model_dump(mode="json"),
                }
            )
            await self.trading.order(deployment_digest, order, check_only=True)
            result = await self.trading.order(deployment_digest, order)
            return {"action": "submitted_and_reconciled", "result": result}
        finally:
            os.close(fd)


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--root", type=Path)
    sub = root.add_subparsers(dest="command", required=True)
    sub.add_parser("status", help="Register/export the exact strategy and observe the attached EA")
    draft = sub.add_parser("draft", help="Bind explicit finite limits; grants no order authority")
    draft.add_argument("limits", type=Path)
    tick = sub.add_parser(
        "tick", help="One approved strategy evaluation, optional sizing and order"
    )
    tick.add_argument("deployment_digest")
    return root


async def execute(args):
    bridge = MT5Bridge(args.root)
    connection = GoldConnection(
        bridge, WorkspaceDatabase(default_research_root() / "workspace.sqlite3")
    )
    if args.command == "status":
        return await connection.status()
    if args.command == "draft":
        from stratosphere.mt5.__main__ import read_file

        return await connection.draft(json.loads(read_file(args.limits)))
    return await connection.tick(args.deployment_digest)


def main(*, expected_specification=None):
    if expected_specification and expected_specification != fingerprint(document().formal_spec):
        raise SystemExit("Gold generated script no longer matches the application strategy")
    args = parser().parse_args()
    try:
        print(json.dumps(asyncio.run(execute(args)), indent=2))
    except (BridgeError, ModelError, ValueError, OSError, ArithmeticError) as error:
        raise SystemExit(str(error)) from None


if __name__ == "__main__":
    main()
