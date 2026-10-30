"""Bounded operational overnight agent, using the existing MT5 EA execution gateway."""

import argparse
import asyncio
import fcntl
import json
import os
import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field

from stratosphere.models.streaming import ModelError
from stratosphere.mt5.bridge import BridgeError, MT5Bridge, decode, private_path
from stratosphere.mt5.gold import SizingProposal, retain, save_new, saved_provider
from stratosphere.mt5.gold_strategy import digest
from stratosphere.mt5.overnight_markets import FX_TO_USD, intersection
from stratosphere.mt5.overnight_strategy import (
    PROFILE,
    SYMBOLS,
    Window,
    compiled_script,
    document,
    entry_bounds,
    profile,
    signal,
    upcoming,
    validate_volume,
    windows,
)
from stratosphere.mt5.trading import Deployment, MT5Trading, Order
from stratosphere.phase0.contracts import Contract, Digest, fingerprint
from stratosphere.registry.workspace import WorkspaceDatabase
from stratosphere.research.store import default_research_root
from stratosphere.workflow.risk import AdvisoryAgents


class OvernightSizing(SizingProposal):
    schema_version: Literal["mt5-overnight-sizing-proposal/1"] = "mt5-overnight-sizing-proposal/1"


class Review(Contract):
    schema_version: Literal["mt5-overnight-review/1"] = "mt5-overnight-review/1"
    strategy_id: str
    agent_id: str
    specification_sha256: Digest
    implementation_sha256: Digest
    profile: dict
    created_utc: int = Field(strict=True, gt=0)
    expires: int = Field(strict=True, gt=0)
    windows: tuple[Window, ...] = Field(min_length=1, max_length=7)
    model_identity: dict[str, str]
    max_model_calls: int = Field(strict=True, ge=2, le=10)
    evidence: Literal[
        "Source-grounded adaptation; no net-of-cost CFD backtest or FTMO certification"
    ] = "Source-grounded adaptation; no net-of-cost CFD backtest or FTMO certification"


def model_identity(provider):
    settings = getattr(provider, "settings", None)
    if settings is None:
        return {"provider": "authored-fixture"}
    # Whitelist only public model identifiers. Never serialize settings or credentials.
    return {
        "provider": settings.provider,
        "model": settings.model,
        "base_url": settings.resolved_base_url,
    }


class OvernightAgent:
    def __init__(
        self, bridge, database, symbol, *, provider_factory=saved_provider, clock=time.time
    ):
        self.bridge, self.database, self.symbol = bridge, database, symbol
        self.trading, self.clock, self.provider_factory = (
            MT5Trading(bridge),
            clock,
            provider_factory,
        )
        self.stop_event = None
        self.document = document(symbol)
        self.record = database.register(
            self.document.formal_spec,
            self.document.name,
            {"kind": "paper-overnight-mt5/1", "source": PROFILE["source"]},
        )
        self.folder = (
            bridge.state_directory() / "overnight" / fingerprint(self.document.formal_spec)
        )
        private_path(self.folder)
        self.folder.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.runtime = self.folder / bridge.implementation_digest
        private_path(self.runtime)
        self.runtime.mkdir(mode=0o700, exist_ok=True)
        retain(self.runtime / "strategy.json", self.document.model_dump_json().encode())
        retain(self.runtime / "overnight.py", compiled_script(symbol))
        self.strategy_sha = bridge.retain_evidence(self.document.model_dump_json().encode())
        retain(
            self.runtime / "binding.json",
            json.dumps(
                {
                    "strategy_id": self.record["public_id"],
                    "agent_id": self.record["agent_id"],
                    "account": {
                        k: bridge.session()[k] for k in ("login", "server", "mode", "ea_sha256")
                    },
                    "implementation_sha256": bridge.implementation_digest,
                    "strategy_sha256": self.strategy_sha,
                    "script_sha256": bridge.retain_evidence(compiled_script(symbol)),
                },
                sort_keys=True,
            ).encode(),
        )
        self.binding = json.loads((self.runtime / "binding.json").read_bytes())

    def event(self, value):
        path = self.runtime / (uuid.uuid4().hex + ".json")
        save_new(path, value | {"observed_utc": int(self.clock())})
        return path

    def halt(self, message):
        path = self.folder / "halt.json"
        try:
            save_new(path, {"state": "halted", "reason": message, "at": int(self.clock())})
        except FileExistsError:
            pass

    async def capture(self, *, quote=False, planning=False):
        session = self.bridge.session()
        account = await self.bridge.account()
        if (
            account.get("connected") != "1"
            or account.get("mode") != "demo"
            or any(account.get(k) != v for k, v in self.binding["account"].items())
            or abs(int(account["utc_epoch"]) - self.clock()) > 10
        ):
            raise BridgeError("Overnight account/EA/connection/UTC clock gate failed")
        snapshot = {
            "session": session,
            "account": account,
            "positions": await self.bridge.read("positions"),
            "orders": await self.bridge.read("orders"),
        }
        if quote:
            snapshot["symbol"] = await self.bridge.read("symbol", symbol=self.symbol)
            snapshot["quote"] = await self.bridge.read("quote", symbol=self.symbol)
            currency = snapshot["symbol"]["profit_currency"]
            if currency != "USD":
                if currency not in FX_TO_USD:
                    raise BridgeError("No reviewed broker conversion to USD for this CFD")
                pair, reciprocal = FX_TO_USD[currency]
                fx = await self.bridge.read("quote", symbol=pair)
                if not -2000 <= int(fx["age_ms"]) <= 5000:
                    if not planning:
                        raise BridgeError("Stale broker USD-conversion quote")
                    snapshot["stale_conversion"] = True
                rate = Decimal("1") / Decimal(fx["bid"]) if reciprocal else Decimal(fx["ask"])
                snapshot["quote"]["conversion_to_usd"] = format(rate, "f")
                snapshot["currency_quote"] = fx
        if self.bridge.session() != session:
            raise BridgeError("Overnight EA session changed during capture")
        return snapshot

    async def status(self):
        snapshot = await self.capture()
        return {
            "strategy_id": self.record["public_id"],
            "agent_id": self.record["agent_id"],
            "symbol": self.symbol,
            "profile": profile(self.symbol),
            "next_windows": [
                w.model_dump(mode="json") for w in upcoming(int(self.clock()), self.symbol)
            ],
            "positions": int(snapshot["positions"]["count"]),
            "orders": int(snapshot["orders"]["count"]),
            "halted": (self.folder / "halt.json").exists(),
            "script": str(self.runtime / "overnight.py"),
            "activation": "Status does not activate a service or authorize orders",
        }

    async def draft(self, limits):
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
        if not isinstance(limits, dict) or set(limits) != expected:
            raise BridgeError("Supply exactly the eight finite overnight deployment limits")
        now = int(self.clock())
        if (
            type(limits["expires"]) is not int
            or not now < limits["expires"] <= now + 7 * 86400
            or type(limits["max_orders"]) is not int
            or not 1 <= limits["max_orders"] <= 5
        ):
            raise BridgeError(
                "Overnight demo deployments allow at most five entries and seven days"
            )
        if self.database.is_retired(self.record["public_id"]):
            raise BridgeError("This overnight strategy is retired")
        snapshot = await self.capture(quote=True, planning=True)
        if int(snapshot["positions"]["count"]) or int(snapshot["orders"]["count"]):
            raise BridgeError("Prepare the overnight deployment only with a flat account")
        review = Review(
            strategy_id=self.record["public_id"],
            agent_id=self.record["agent_id"],
            specification_sha256=fingerprint(self.document.formal_spec),
            implementation_sha256=self.bridge.implementation_digest,
            profile=profile(self.symbol),
            created_utc=now,
            expires=limits["expires"],
            windows=windows(now, limits["expires"], self.symbol),
            model_identity=model_identity(await self.provider_factory()),
            max_model_calls=2 * limits["max_orders"],
        )
        account = snapshot["account"]
        deployment = Deployment.model_validate(
            limits
            | {
                "strategy_id": self.record["public_id"],
                "symbol": self.symbol,
                "strategy_sha256": self.strategy_sha,
                "report_sha256": self.bridge.retain_evidence(review.model_dump_json().encode()),
                "implementation_sha256": self.bridge.implementation_digest,
                **{k: account[k] for k in ("ea_sha256", "login", "server", "mode", "currency")},
            }
        )
        # Check feasibility without a native order request or model call.
        if not -2000 <= int(snapshot["quote"]["age_ms"]) <= 5000 or snapshot.get(
            "stale_conversion"
        ):
            bounds = {
                "status": "deferred_until_fresh_trigger",
                "reason": "Closed/thin market quote or currency conversion is stale",
                "meaning": "No feasibility or order acceptance claim; runtime gates remain strict",
            }
        else:
            bounds = entry_bounds(
                self.symbol, snapshot["quote"], snapshot["symbol"], account, deployment
            )
        path = self.runtime / (deployment.digest + ".deployment.json")
        retain(path, deployment.model_dump_json().encode())
        retain(
            self.runtime / (deployment.digest + ".review.json"), review.model_dump_json().encode()
        )
        self.event(
            {
                "kind": "overnight-preparation/1",
                "deployment": deployment.digest,
                "snapshot": snapshot,
                "bounds": bounds,
            }
        )
        return {
            "deployment": deployment.model_dump(mode="json"),
            "digest": deployment.digest,
            "path": str(path),
            "review": review.model_dump(mode="json"),
            "bounds": bounds,
            "approved": False,
        }

    def review(self, deployment):
        path = self.bridge.state_directory() / "evidence" / (deployment.report_sha256 + ".bin")
        private_path(path)
        report = Review.model_validate_json(path.read_bytes())
        if (
            deployment.strategy_id != self.record["public_id"]
            or deployment.symbol != self.symbol
            or deployment.strategy_sha256 != self.strategy_sha
            or deployment.mode != "demo"
            or not 1 <= deployment.max_orders <= 5
            or report.strategy_id != self.record["public_id"]
            or report.agent_id != self.record["agent_id"]
            or report.specification_sha256 != fingerprint(self.document.formal_spec)
            or report.implementation_sha256 != self.bridge.implementation_digest
            or report.profile != profile(self.symbol)
            or report.expires != deployment.expires
            or not 0 < report.expires - report.created_utc <= 7 * 86400
            or report.max_model_calls != 2 * deployment.max_orders
            or report.windows != windows(report.created_utc, report.expires, self.symbol)
        ):
            raise BridgeError("Deployment does not bind this exact frozen overnight profile")
        return report

    def owned_position(self, positions, deployment):
        if (
            int(positions["count"]) != 1
            or positions["p.1.symbol"] != self.symbol
            or positions.get("p.1.type") != "0"
        ):
            raise BridgeError("External/short/multiple exposure blocks the overnight agent")
        with self.bridge.journal() as db:
            rows = db.execute("SELECT payload,state FROM requests WHERE op='submit'").fetchall()
        magic = positions["p.1.magic"]
        for payload, state in rows:
            value = decode(payload)
            if value.get("approval") == deployment.digest and value.get("magic") == magic:
                if state != "observed":
                    raise BridgeError("Unresolved overnight entry; reconcile, never replay")
                return positions["p.1.ticket"], value
        raise BridgeError("Position is not owned by this exact overnight deployment")

    def entry_intent(self, deployment, magic):
        path = self.folder / ("intent-" + deployment.digest + "-" + magic + ".json")
        private_path(path)
        if not path.is_file():
            raise BridgeError("Owned position has no retained overnight exit deadline")
        intent = json.loads(path.read_bytes())
        sig = signal(self.symbol, Window.model_validate(intent["window"]))
        expected_magic = str(int(digest_native(deployment.digest, sig["signal_sha256"])[:15], 16))
        if (
            intent.get("deployment") != deployment.digest
            or expected_magic != magic
            or intent.get("signal_sha256") != sig["signal_sha256"]
        ):
            raise BridgeError("Overnight position deadline/ownership changed")
        return intent

    def slots(self, report, deployment):
        # Slots survive runtime changes, process death and new deployment approvals.
        folder = self.folder / "triggers"
        private_path(folder)
        folder.mkdir(mode=0o700, exist_ok=True)
        paths = list(folder.glob("*.json"))
        for path in paths:
            private_path(path)
        consumed = sum(
            json.loads(p.read_bytes()).get("deployment") == deployment.digest for p in paths
        )
        if consumed * 2 >= report.max_model_calls:
            raise BridgeError("Frozen overnight advisory budget exhausted")
        return folder

    async def advise(self, snapshot, deployment, report, sig):
        bounds = entry_bounds(
            self.symbol, snapshot["quote"], snapshot["symbol"], snapshot["account"], deployment
        )
        facts = {
            "profile": profile(self.symbol),
            "signal": sig,
            "bounds": bounds,
            "evidence": report.evidence,
        }
        context_sha = digest(facts)
        slot = self.slots(report, deployment) / (sig["signal_sha256"] + ".json")
        try:
            save_new(
                slot,
                {
                    "deployment": deployment.digest,
                    "signal": sig,
                    "context_sha256": context_sha,
                    "reserved_utc": int(self.clock()),
                },
            )
        except FileExistsError:
            return None, "trigger_already_consumed"
        try:
            provider = await self.provider_factory()
            if model_identity(provider) != report.model_identity:
                raise BridgeError("Model/provider identity changed after deployment review")
            settings = getattr(provider, "settings", None)
            key = settings.api_key.get_secret_value() if settings else ""
            advisors = AdvisoryAgents(
                provider, secret_check=lambda data: bool(key and key.encode() in data)
            )
            async with asyncio.timeout(120):
                verdict = await advisors.review_signal(
                    self.record["public_id"], sig["signal_sha256"], context_sha, facts
                )
                self.event(
                    {
                        "kind": "overnight-signal-review/1",
                        "verdict": verdict.model_dump(mode="json"),
                    }
                )
                if verdict.verdict != "reliable":
                    return None, "strategy_agent_rejected"
                proposal = await advisors.call(
                    "You are the isolated risk agent for this bounded overnight DEMO trade. "
                    "Propose enter and broker-step volume within ALL bounds, or skip and zero. "
                    "The paper does not prove profitability. Financing, spread and gaps matter. "
                    "No account access, leverage changes, tools or order authority. "
                    "Copy context_sha256 exactly.",
                    {"context_sha256": context_sha, "facts": facts},
                    OvernightSizing,
                )
                if proposal.context_sha256 != context_sha:
                    raise BridgeError("Overnight sizing context mismatch")
                if proposal.decision == "enter":
                    validate_volume(proposal, bounds)
                self.event(
                    {
                        "kind": "overnight-sizing/1",
                        "context_sha256": context_sha,
                        "proposal": proposal.model_dump(mode="json"),
                    }
                )
                return (
                    (proposal, "entry_proposed")
                    if proposal.decision == "enter"
                    else (None, "risk_agent_skipped")
                )
        except BaseException:
            self.event(
                {
                    "kind": "overnight-advisory-failed/1",
                    "signal_sha256": sig["signal_sha256"],
                    "state": "Consumed; no retry",
                }
            )
            raise

    async def cycle(self, deployment_digest):
        # Exit path precedes entry expiry, retirement, halt and all model dependencies.
        deployment = self.trading.deployment(deployment_digest, exit_only=True)
        snapshot = await self.capture()
        self.trading.verify(deployment, snapshot["account"], entry=False)
        report = self.review(deployment)
        if int(snapshot["positions"]["count"]):
            ticket, entry = self.owned_position(snapshot["positions"], deployment)
            intent = self.entry_intent(deployment, entry["magic"])
            if self.clock() >= intent["window"]["exit_at"]:
                result = await self.trading.close(deployment_digest, ticket)
                self.event({"kind": "overnight-exit/1", "ticket": ticket, "result": result})
                return {"action": "closed_owned_position", "result": result}
            if int(snapshot["orders"]["count"]):
                raise BridgeError("Unexpected pending orders; protective exit deadline retained")
            return {"action": "hold_owned_position", "exit_at": intent["window"]["exit_at"]}
        if int(snapshot["orders"]["count"]):
            raise BridgeError("Pending/external account orders block overnight entry")
        if (
            self.database.is_retired(self.record["public_id"])
            or (self.folder / "halt.json").exists()
        ):
            return {"action": "halted_flat"}
        if self.stop_event is not None and self.stop_event.is_set():
            return {"action": "stopping_flat"}
        if self.clock() >= deployment.expires:
            return {"action": "expired_flat"}
        self.trading.deployment(deployment_digest)  # Approved, not revoked, for entries.
        self.trading.verify(deployment, snapshot["account"])
        now = self.clock()
        window = next((w for w in report.windows if w.entry_start <= now < w.entry_end), None)
        if window is None:
            return {"action": "waiting_for_entry_window"}
        with self.bridge.journal() as db:
            rows = db.execute(
                "SELECT payload,state FROM requests WHERE op IN ('submit','close')"
            ).fetchall()
        if any(state in {"reserved", "uncertain"} for _, state in rows):
            raise BridgeError("Unresolved native outcome; new entries remain halted")
        submits = [decode(payload) for payload, _ in rows if decode(payload).get("op") == "submit"]
        if sum(p.get("approval") == deployment_digest for p in submits) >= deployment.max_orders:
            return {"action": "entry_budget_exhausted"}
        # Fetch current quote only when an entry is due; closed markets do not break the scheduler.
        snapshot = await self.capture(quote=True)
        if (
            snapshot["account"].get("trading_input") != "1"
            or snapshot["account"].get("terminal_trade_allowed") != "1"
        ):
            raise BridgeError("Approved execution needs EA and terminal trading enabled")
        if not window.entry_start <= self.clock() < min(window.entry_end, deployment.expires):
            return {"action": "entry_window_missed"}
        if int(snapshot["positions"]["count"]) or int(snapshot["orders"]["count"]):
            raise BridgeError("Account exposure changed before strategy review")
        started = time.monotonic()
        sig = signal(self.symbol, window)
        proposal, reason = await self.advise(snapshot, deployment, report, sig)
        if proposal is None:
            return {"action": reason}
        refreshed = await self.capture(quote=True)
        if (
            time.monotonic() - started > 120
            or refreshed["session"] != snapshot["session"]
            or not window.entry_start <= self.clock() < min(window.entry_end, deployment.expires)
            or int(refreshed["positions"]["count"])
            or int(refreshed["orders"]["count"])
            or self.database.is_retired(self.record["public_id"])
            or (self.folder / "halt.json").exists()
            or (self.stop_event is not None and self.stop_event.is_set())
        ):
            raise BridgeError("Signal window/session/exposure/review changed; no order")
        self.trading.verify(deployment, refreshed["account"])
        bounds = entry_bounds(
            self.symbol, refreshed["quote"], refreshed["symbol"], refreshed["account"], deployment
        )
        validate_volume(proposal, bounds)
        order = Order(
            signal_sha256=sig["signal_sha256"],
            side="buy",
            volume=proposal.volume,
            stop_loss=bounds["stop_loss"],
        )
        magic = str(int(digest_native(deployment_digest, sig["signal_sha256"])[:15], 16))
        save_new(
            self.folder / ("intent-" + deployment_digest + "-" + magic + ".json"),
            {
                "deployment": deployment_digest,
                "signal_sha256": sig["signal_sha256"],
                "window": window.model_dump(mode="json"),
                "order": order.model_dump(mode="json"),
            },
        )
        native_deadline = min(window.entry_end, deployment.expires) - 1
        await self.trading.order(
            deployment_digest, order, check_only=True, expires_at=native_deadline
        )
        # Check again after native preflight: its I/O can cross the allowed entry deadline.
        if not window.entry_start <= self.clock() < min(window.entry_end, deployment.expires) or (
            self.stop_event is not None and self.stop_event.is_set()
        ):
            raise BridgeError(
                "Entry stopped or window ended in preflight; trigger remains consumed"
            )
        result = await self.trading.order(deployment_digest, order, expires_at=native_deadline)
        self.event({"kind": "overnight-entry/1", "signal": sig, "result": result})
        return {"action": "submitted_and_reconciled", "result": result}

    async def tick(self, deployment_digest):
        with self.owner():
            try:
                return await self.cycle(deployment_digest)
            except BaseException:
                self.halt("Cycle interrupted or failed; no automatic new-entry recovery")
                raise

    def owner(self):
        from contextlib import contextmanager

        @contextmanager
        def acquire():
            path = self.folder / "runner.lock"
            private_path(path)
            fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            try:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise BridgeError("Another overnight agent owns this exact strategy") from None
                yield
            finally:
                os.close(fd)

        return acquire()

    async def serve(self, deployment_digest, *, interval=15, stop=None):
        if not 5 <= interval <= 30:
            raise BridgeError("Overnight polling interval must be 5..30 seconds")
        # Approval is verified before holding the service lock; startup grants no authority.
        self.review(self.trading.deployment(deployment_digest, exit_only=True))
        stop = stop or asyncio.Event()
        self.stop_event = stop
        wall, monotonic = self.clock(), time.monotonic()
        with self.owner():
            last = None
            try:
                while not stop.is_set():
                    if abs((self.clock() - wall) - (time.monotonic() - monotonic)) > 5:
                        self.halt("UTC clock jumped; protective exits only")
                        wall, monotonic = self.clock(), time.monotonic()
                    result = await self.cycle(deployment_digest)
                    from stratosphere.mt5.bridge import atomic_write

                    atomic_write(
                        self.runtime / "health.json",
                        json.dumps(
                            {
                                "state": "running",
                                "pid": os.getpid(),
                                "at": int(self.clock()),
                                "deployment": deployment_digest,
                                "last_result": result,
                            },
                            sort_keys=True,
                        ).encode(),
                    )
                    if result["action"] != last:
                        self.event({"kind": "overnight-service-transition/1", "result": result})
                        print(json.dumps(result), flush=True)
                        last = result["action"]
                    if result["action"] in {
                        "expired_flat",
                        "halted_flat",
                        "entry_budget_exhausted",
                    }:
                        return result
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=interval)
                    except TimeoutError:
                        pass
            except BaseException:
                self.halt("Service interrupted or failed; inspect receipts before any new entries")
                raise
            finally:
                from stratosphere.mt5.bridge import atomic_write

                atomic_write(
                    self.runtime / "health.json",
                    json.dumps(
                        {
                            "state": "stopped",
                            "at": int(self.clock()),
                            "deployment": deployment_digest,
                            "last_action": last,
                            "halted": (self.folder / "halt.json").exists(),
                            "meaning": "Python exit does not close positions; native SL remains",
                        },
                        sort_keys=True,
                    ).encode(),
                )


def digest_native(deployment, signal_sha):
    import hashlib

    return hashlib.sha256((deployment + signal_sha).encode()).hexdigest()[:32]


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--root", type=Path)
    root.add_argument("--symbol", choices=SYMBOLS, default="US100.cash")
    sub = root.add_subparsers(dest="command", required=True)
    sub.add_parser(
        "universe", help="Compare the connected broker catalog with the paper benchmarks"
    )
    sub.add_parser("status")
    sub.add_parser("draft").add_argument("limits", type=Path)
    for command in ("tick", "serve"):
        sub.add_parser(command).add_argument("deployment_digest")
    return root


async def execute(args):
    bridge = MT5Bridge(args.root)
    if args.command == "universe":
        return intersection(await bridge.read("symbols"))
    agent = OvernightAgent(
        bridge, WorkspaceDatabase(default_research_root() / "workspace.sqlite3"), args.symbol
    )
    if args.command == "status":
        return await agent.status()
    if args.command == "draft":
        from stratosphere.mt5.__main__ import read_file

        return await agent.draft(json.loads(read_file(args.limits)))
    if args.command == "serve":
        import signal as signals

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for event in (signals.SIGINT, signals.SIGTERM):
            loop.add_signal_handler(event, stop.set)
        return await agent.serve(args.deployment_digest, stop=stop)
    return await agent.tick(args.deployment_digest)


def main(*, expected_symbol=None, expected_specification=None):
    command_parser = parser()
    if expected_symbol:
        command_parser.set_defaults(symbol=expected_symbol)
    args = command_parser.parse_args()
    if expected_symbol and (
        args.symbol != expected_symbol
        or expected_specification != fingerprint(document(args.symbol).formal_spec)
    ):
        raise SystemExit("Generated overnight script no longer matches its exact strategy")
    try:
        print(json.dumps(asyncio.run(execute(args)), indent=2))
    except (BridgeError, ModelError, ValueError, OSError, ArithmeticError) as error:
        raise SystemExit(str(error)) from None


if __name__ == "__main__":
    main()
