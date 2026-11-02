"""One-account coordinator for exact overnight agents; no portfolio reallocation loop."""

import argparse
import asyncio
import fcntl
import hashlib
import json
import os
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, TypeAdapter, model_validator

from stratosphere.models.streaming import ModelError
from stratosphere.mt5.bridge import BridgeError, MT5Bridge, atomic_write, private_path
from stratosphere.mt5.exposure import ownership
from stratosphere.mt5.gold import retain, save_new
from stratosphere.mt5.overnight import OvernightAgent
from stratosphere.mt5.overnight_markets import intersection
from stratosphere.mt5.overnight_strategy import signal
from stratosphere.mt5.trading import CoexistingDeployment, Deployment, MT5Trading, deployment_reader
from stratosphere.phase0.contracts import Contract, Digest, fingerprint
from stratosphere.registry.workspace import WorkspaceDatabase
from stratosphere.research.store import default_research_root


class Basket(Contract):
    schema_version: Literal["mt5-overnight-basket/1"] = "mt5-overnight-basket/1"
    implementation_sha256: Digest
    catalog_sha256: Digest
    created_utc: int = Field(strict=True, gt=0)
    deployments: tuple[Deployment, ...] = Field(min_length=1, max_length=10)
    concurrency: Literal["one_account_position"] = "one_account_position"
    selection: Literal["earliest_window_then_least_attempted_then_symbol"] = (
        "earliest_window_then_least_attempted_then_symbol"
    )

    @model_validator(mode="after")
    def consistent(self):
        identities = {
            (d.login, d.server, d.mode, d.currency, d.ea_sha256, d.implementation_sha256, d.expires)
            for d in self.deployments
        }
        if (
            len(identities) != 1
            or len({d.symbol for d in self.deployments}) != len(self.deployments)
            or any(
                d.mode != "demo" or d.implementation_sha256 != self.implementation_sha256
                for d in self.deployments
            )
            or (
                self.schema_version == "mt5-overnight-basket/1"
                and any(d.schema_version != "mt5-deployment/1" for d in self.deployments)
            )
        ):
            raise ValueError(
                "Basket requires unique symbols and one exact demo account/runtime/expiry"
            )
        return self

    @property
    def digest(self):
        return fingerprint(self)


class CoexistingBasket(Basket):
    schema_version: Literal["mt5-overnight-basket/2"] = "mt5-overnight-basket/2"
    deployments: tuple[CoexistingDeployment, ...] = Field(min_length=1, max_length=10)
    concurrency: Literal["one_agent_position_manual_coexistence"] = (
        "one_agent_position_manual_coexistence"
    )
    replaces: Digest | None = None
    recovery_halts: dict[str, Digest] = Field(default_factory=dict)
    previous_usage: dict[str, int] = Field(default_factory=dict)


basket_reader = TypeAdapter(
    Annotated[Basket | CoexistingBasket, Field(discriminator="schema_version")]
)


class OvernightBasket:
    def __init__(self, bridge, database, basket, *, provider_factory=None, clock=time.time):
        self.bridge, self.database, self.basket = bridge, database, basket
        self.clock = clock
        if basket.implementation_sha256 != bridge.implementation_digest:
            raise BridgeError("Overnight basket implementation changed")
        catalog_path = bridge.state_directory() / "evidence" / (basket.catalog_sha256 + ".bin")
        private_path(catalog_path)
        if (
            not catalog_path.is_file()
            or catalog_path.stat().st_size > 8_000_000
            or hashlib.sha256(catalog_path.read_bytes()).hexdigest() != basket.catalog_sha256
        ):
            raise BridgeError("Exact connected basket catalog missing or changed")
        universe = intersection(json.loads(catalog_path.read_bytes()))
        if {m["symbol"] for m in universe["matched"]} != {d.symbol for d in basket.deployments}:
            raise BridgeError(
                "Basket must contain all and only the exact mapped catalog intersection"
            )
        kwargs = {"provider_factory": provider_factory} if provider_factory else {}
        kwargs["clock"] = clock
        self.agents = {
            d.symbol: OvernightAgent(bridge, database, d.symbol, **kwargs)
            for d in basket.deployments
        }
        self.deployments = {d.symbol: d for d in basket.deployments}
        self.reports = {s: self.agents[s].review(d) for s, d in self.deployments.items()}
        self.folder = bridge.state_directory() / "overnight" / "baskets" / basket.digest
        private_path(self.folder)
        self.folder.mkdir(parents=True, mode=0o700, exist_ok=True)
        retain(self.folder / "basket.json", basket.model_dump_json().encode())

    def halt(self, reason):
        for agent in self.agents.values():
            agent.halt(reason)
        try:
            save_new(self.folder / "halt.json", {"at": int(time.time()), "reason": reason})
        except FileExistsError:
            pass

    def previous(self):
        if not isinstance(self.basket, CoexistingBasket) or self.basket.replaces is None:
            return None
        path = self.folder.parent / self.basket.replaces / "basket.json"
        private_path(path)
        previous = Basket.model_validate_json(path.read_bytes())
        if previous.digest != self.basket.replaces:
            raise BridgeError("Replacement source basket changed")
        receipt = path.parent / "approved.json"
        private_path(receipt)
        if (
            not receipt.is_file()
            or json.loads(receipt.read_bytes()).get("basket") != previous.digest
        ):
            raise BridgeError("Replacement source basket has no exact historical approval")
        if {d.symbol for d in previous.deployments} != set(self.deployments):
            raise BridgeError("Replacement cannot change the approved universe")
        expected_halts, usage = {}, {}
        for old in previous.deployments:
            new = self.deployments[old.symbol]
            agent = self.agents[old.symbol]
            halt = agent.folder / "halt.json"
            private_path(halt)
            if not halt.is_file():
                raise BridgeError("Recovery requires retained historical halt evidence")
            expected_halts[old.symbol] = hashlib.sha256(halt.read_bytes()).hexdigest()
            with self.bridge.journal() as db:
                prior = db.execute(
                    "SELECT payload,state FROM approvals WHERE digest=?", (old.digest,)
                ).fetchone()
                if (
                    not prior
                    or prior[1] not in {"approved", "revoked"}
                    or Deployment.model_validate_json(prior[0]) != old
                ):
                    raise BridgeError("Replacement source has no exact historical approval")
                submits = db.execute(
                    "SELECT COUNT(*) FROM requests WHERE op='submit' "
                    "AND instr(CAST(payload AS TEXT),?)>0",
                    (old.digest,),
                ).fetchone()[0]
            slots = agent.folder / "triggers"
            private_path(slots)
            consumed = 0
            for slot in slots.glob("*.json"):
                private_path(slot)
                consumed += json.loads(slot.read_bytes()).get("deployment") == old.digest
            usage[old.symbol] = max(submits, consumed)
            old_report = self.bridge.state_directory() / "evidence" / (old.report_sha256 + ".bin")
            private_path(old_report)
            if hashlib.sha256(old_report.read_bytes()).hexdigest() != old.report_sha256:
                raise BridgeError("Historical report evidence changed")
            if (
                json.loads(old_report.read_bytes())["model_identity"]
                != self.reports[old.symbol].model_identity
            ):
                raise BridgeError("Replacement must preserve the reviewed model identity")
            unchanged = (
                "strategy_id",
                "strategy_sha256",
                "login",
                "server",
                "mode",
                "currency",
                "symbol",
                "max_volume",
                "max_order_risk",
                "max_order_margin",
                "max_spread",
                "min_equity",
                "cost_buffer",
                "expires",
            )
            if (
                any(getattr(new, k) != getattr(old, k) for k in unchanged)
                or new.max_orders != old.max_orders - usage[old.symbol]
            ):
                raise BridgeError("Replacement cannot renew budget, extend expiry or change limits")
        if self.basket.recovery_halts != expected_halts or self.basket.previous_usage != usage:
            raise BridgeError("Recovery halt evidence or consumed budget changed")
        return previous

    async def recover(self, exact_digest):
        """Explicit approved replacement, preserving old latches and every consumed operation."""
        if exact_digest != self.basket.digest:
            raise BridgeError("Recover only the exact reviewed replacement basket")
        previous = self.previous()
        if previous is None:
            raise BridgeError("Only a reviewed v1-to-v2 replacement can recover old halts")
        receipt = self.folder / "recovered.json"
        if receipt.exists():
            raise BridgeError("Recovery receipt already exists; never replay recovery")
        approval = self.folder / "approved.json"
        if (
            not approval.is_file()
            or json.loads(approval.read_bytes()).get("basket") != exact_digest
        ):
            raise BridgeError("Exact replacement approval required before recovery")
        # Exclude a running account coordinator and every member worker throughout recovery.
        lock_path = self.bridge.state_directory() / "overnight-basket.lock"
        private_path(lock_path)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with ExitStack() as stack:
                for agent in self.agents.values():
                    stack.enter_context(agent.owner())
                first = next(iter(self.agents.values()))
                observation = await first.capture()
                owned, _, _ = ownership(
                    self.bridge, observation["positions"], observation["orders"]
                )
                with self.bridge.journal() as db:
                    uncertain = db.execute(
                        "SELECT 1 FROM requests WHERE op IN ('submit','close') "
                        "AND state IN ('reserved','uncertain')"
                    ).fetchone()
                if owned or uncertain:
                    raise BridgeError("Reconcile application exposure/uncertainty before recovery")
                trading = MT5Trading(self.bridge)
                for d in self.basket.deployments:
                    if trading.deployment(d.digest) != d:
                        raise BridgeError("Replacement approval changed")
                    trading.verify(d, observation["account"])
                    if (self.agents[d.symbol].runtime / "halt.json").exists():
                        raise BridgeError("New runtime fault requires separate investigation")
                self.previous()  # Recheck consumed slots after acquiring all owner locks.
                for old in previous.deployments:
                    await trading.revoke(old.digest)
                for d in self.basket.deployments:
                    save_new(
                        self.agents[d.symbol].runtime / ("recovery-" + d.digest + ".json"),
                        {
                            "deployment": d.digest,
                            "basket": exact_digest,
                            "replaces": previous.digest,
                            "previous_halt_sha256": self.basket.recovery_halts[d.symbol],
                            "at": int(self.clock()),
                        },
                    )
                save_new(
                    receipt,
                    {"basket": exact_digest, "replaces": previous.digest, "at": int(self.clock())},
                )
        finally:
            os.close(fd)
        return {"recovered": exact_digest, "previous_halts": "retained", "budgets": "not renewed"}

    async def approve(self, exact_digest):
        if exact_digest != self.basket.digest:
            raise BridgeError("Approve the exact reviewed overnight basket digest")
        if (self.folder / "approved.json").exists():
            raise BridgeError("Basket approval receipt already exists; never recreate authority")
        self.previous()
        # Validate all definitions/account/evidence before changing any native policy.
        account = await self.bridge.account()
        trading = MT5Trading(self.bridge)
        for d in self.basket.deployments:
            trading.verify(d, account)
        with self.bridge.journal() as db:
            if any(
                db.execute("SELECT 1 FROM approvals WHERE digest=?", (d.digest,)).fetchone()
                for d in self.basket.deployments
            ):
                raise BridgeError(
                    "A basket member already has an approval history; never recreate it"
                )
        positions, orders = await self.bridge.read("positions"), await self.bridge.read("orders")
        if isinstance(self.basket, CoexistingBasket):
            owned, _, _ = ownership(self.bridge, positions, orders)
            if owned:
                raise BridgeError("Reconcile existing application positions before basket approval")
            with self.bridge.journal() as db:
                if db.execute(
                    "SELECT 1 FROM requests WHERE op IN ('submit','close') "
                    "AND state IN ('reserved','uncertain')"
                ).fetchone():
                    raise BridgeError("Unresolved native outcome blocks basket approval")
        elif int(positions["count"]) or int(orders["count"]):
            raise BridgeError("Approve an overnight basket only while the account is flat")
        newly_approved = []
        try:
            for d in self.basket.deployments:
                newly_approved.append(d.digest)
                await trading.approve(d, d.digest)
            save_new(
                self.folder / "approved.json",
                {"basket": exact_digest, "at": int(time.time()), "deployments": newly_approved},
            )
        except BaseException:
            # Retain partial receipts, revoke exactly this invocation's created policies.
            for identity in newly_approved:
                with self.bridge.journal() as db:
                    row = db.execute(
                        "SELECT state FROM approvals WHERE digest=?", (identity,)
                    ).fetchone()
                if row and row[0] == "approved":
                    await trading.revoke(identity)
            raise
        return {"approved_basket": exact_digest, "deployments": newly_approved}

    async def cycle(self):
        # Shared account observation avoids treating another member's position as external.
        first = next(iter(self.agents.values()))
        snapshot = await first.capture()
        positions = snapshot["positions"]
        coexistence = isinstance(self.basket, CoexistingBasket)
        external_symbols = set()
        if coexistence:
            owned, external, pending = ownership(self.bridge, positions, snapshot["orders"])
            if len(owned) > 1:
                raise BridgeError("Multiple application positions violate this basket's scope")
            external_symbols = {r["symbol"] for r in external + pending}
            if owned:
                row = owned[0]
                symbol = row["symbol"]
                if (
                    symbol not in self.deployments
                    or row["approval"] != self.deployments[symbol].digest
                ):
                    raise BridgeError("Another application deployment occupies this account")
                result = await self.agents[symbol].cycle(self.deployments[symbol].digest)
                return result | {
                    "symbol": symbol,
                    "blocked_entry_symbols": [
                        s
                        for s, r in self.reports.items()
                        if s != symbol
                        and any(w.entry_start <= self.clock() < w.entry_end for w in r.windows)
                    ],
                }
            with self.bridge.journal() as db:
                if db.execute(
                    "SELECT 1 FROM requests WHERE op IN ('submit','close') "
                    "AND state IN ('reserved','uncertain')"
                ).fetchone():
                    raise BridgeError("Unresolved native outcome; reconcile, never replay")
        elif int(positions["count"]):
            if int(positions["count"]) != 1:
                raise BridgeError("Multiple/external positions violate this basket's exact scope")
            symbol = positions["p.1.symbol"]
            if symbol not in self.agents:
                raise BridgeError("External account exposure blocks the overnight basket")
            agent, deployment = self.agents[symbol], self.deployments[symbol]
            agent.owned_position(positions, deployment)
            now = int(self.clock())
            blocked = [
                s
                for s, r in self.reports.items()
                if s != symbol and any(w.entry_start <= now < w.entry_end for w in r.windows)
            ]
            return (await agent.cycle(deployment.digest)) | {
                "symbol": symbol,
                "blocked_entry_symbols": blocked,
            }
        if int(snapshot["orders"]["count"]) and not coexistence:
            raise BridgeError("External/pending orders block the overnight basket")
        if (self.folder / "halt.json").exists():
            return {"action": "halted_flat"}
        now = int(self.clock())
        if all(now >= d.expires for d in self.basket.deployments):
            return {"action": "expired_flat"}
        choices = []
        deferred = []
        for symbol, agent in self.agents.items():
            deployment = self.deployments[symbol]
            agent.trading.deployment(deployment.digest)
            agent.trading.verify(deployment, snapshot["account"])
            if agent.advisory_exhausted(self.reports[symbol], deployment):
                continue
            if agent.halted(deployment) or self.database.is_retired(agent.record["public_id"]):
                continue
            for window in self.reports[symbol].windows:
                if not window.entry_start <= now < window.entry_end:
                    continue
                if coexistence and symbol in external_symbols:
                    deferred.append(symbol)
                    continue
                sig = signal(symbol, window)
                slot = agent.folder / "triggers" / (sig["signal_sha256"] + ".json")
                private_path(slot)
                if slot.exists():
                    continue
                with self.bridge.journal() as db:
                    attempts = db.execute(
                        "SELECT COUNT(*) FROM requests WHERE op='submit' "
                        "AND instr(CAST(payload AS TEXT),?)>0",
                        (deployment.digest,),
                    ).fetchone()[0]
                if attempts < deployment.max_orders:
                    choices.append((window.entry_start, attempts, symbol))
        if not choices:
            if deferred:
                return {"action": "external_symbol_occupied", "symbols": sorted(deferred)}
            return {"action": "waiting_for_entry_window"}
        _, _, symbol = min(choices)
        return (await self.agents[symbol].cycle(self.deployments[symbol].digest)) | {
            "symbol": symbol
        }

    async def serve(self, *, stop=None, interval=15):
        if not 5 <= interval <= 30:
            raise BridgeError("Basket polling interval must be 5..30 seconds")
        approval = self.folder / "approved.json"
        private_path(approval)
        if (
            not approval.is_file()
            or json.loads(approval.read_bytes()).get("basket") != self.basket.digest
        ):
            raise BridgeError("No exact user-approved overnight basket")
        for d in self.basket.deployments:
            if MT5Trading(self.bridge).deployment(d.digest, exit_only=True) != d:
                raise BridgeError("Basket deployment approval changed")
        if isinstance(self.basket, CoexistingBasket) and self.basket.replaces is not None:
            recovered = self.folder / "recovered.json"
            private_path(recovered)
            if (
                not recovered.is_file()
                or json.loads(recovered.read_bytes()).get("basket") != self.basket.digest
            ):
                raise BridgeError("Reviewed recovery has not completed")
        stop = stop or asyncio.Event()
        for agent in self.agents.values():
            agent.stop_event = stop
        path = self.bridge.state_directory() / "overnight-basket.lock"
        private_path(path)
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise BridgeError("Another overnight basket owns this account") from None
            with ExitStack() as stack:
                for agent in self.agents.values():
                    stack.enter_context(agent.owner())
                wall, mono = time.time(), time.monotonic()
                last = None
                try:
                    while not stop.is_set():
                        if abs((time.time() - wall) - (time.monotonic() - mono)) > 5:
                            self.halt("UTC clock jumped; protective exits only")
                            wall, mono = time.time(), time.monotonic()
                        result = await self.cycle()
                        atomic_write(
                            self.folder / "health.json",
                            json.dumps(
                                {
                                    "state": "running",
                                    "pid": os.getpid(),
                                    "at": int(time.time()),
                                    "basket": self.basket.digest,
                                    "result": result,
                                },
                                sort_keys=True,
                            ).encode(),
                        )
                        if result != last:
                            print(json.dumps(result), flush=True)
                            save_new(self.folder / f"event-{time.time_ns()}.json", result)
                            last = result
                        if result["action"] in {"expired_flat", "halted_flat"}:
                            return result
                        try:
                            await asyncio.wait_for(stop.wait(), timeout=interval)
                        except TimeoutError:
                            pass
                except BaseException:
                    self.halt("Basket interrupted/failed; no automatic entry recovery")
                    raise
                finally:
                    atomic_write(
                        self.folder / "health.json",
                        json.dumps(
                            {
                                "state": "stopped",
                                "at": int(time.time()),
                                "basket": self.basket.digest,
                                "meaning": "Python exit leaves positions and their native SL",
                            }
                        ).encode(),
                    )
        finally:
            os.close(fd)


async def draft(
    bridge, database, limits, *, provider_factory=None, manual_coexistence=False, previous=None
):
    catalog = await bridge.read("symbols")
    universe = intersection(catalog)
    if not universe["matched"]:
        raise BridgeError("No exact reviewed paper benchmarks in the connected FTMO catalog")
    kwargs = {"provider_factory": provider_factory} if provider_factory else {}
    drafts = []
    if isinstance(limits, dict) and set(limits) == {"common", "spreads"}:
        if (
            not isinstance(limits["common"], dict)
            or not isinstance(limits["spreads"], dict)
            or set(limits["spreads"]) != {m["symbol"] for m in universe["matched"]}
        ):
            raise BridgeError("Supply one explicit spread bound for every exact matched CFD")
        resolved = {
            m["symbol"]: limits["common"] | {"max_spread": limits["spreads"][m["symbol"]]}
            for m in universe["matched"]
        }
    else:
        resolved = {m["symbol"]: limits for m in universe["matched"]}
    recovery_halts, previous_usage = {}, {}
    if previous is not None:
        manual_coexistence = True
        if type(previous) is not Basket or {d.symbol for d in previous.deployments} != set(
            resolved
        ):
            raise BridgeError("Replacement requires the exact historical v1 basket universe")
        resolved = {}
        for old in previous.deployments:
            agent = OvernightAgent(bridge, database, old.symbol, **kwargs)
            halt = agent.folder / "halt.json"
            private_path(halt)
            if not halt.is_file():
                raise BridgeError("Replacement requires retained historical agent halts")
            recovery_halts[old.symbol] = hashlib.sha256(halt.read_bytes()).hexdigest()
            with bridge.journal() as db:
                submits = db.execute(
                    "SELECT COUNT(*) FROM requests WHERE op='submit' "
                    "AND instr(CAST(payload AS TEXT),?)>0",
                    (old.digest,),
                ).fetchone()[0]
            slots = agent.folder / "triggers"
            private_path(slots)
            consumed = 0
            for slot in slots.glob("*.json"):
                private_path(slot)
                consumed += json.loads(slot.read_bytes()).get("deployment") == old.digest
            used = max(submits, consumed)
            if used >= old.max_orders:
                raise BridgeError("Historical member budget exhausted; cannot renew it")
            previous_usage[old.symbol] = used
            resolved[old.symbol] = {
                k: getattr(old, k)
                for k in (
                    "max_volume",
                    "max_order_risk",
                    "max_order_margin",
                    "max_spread",
                    "min_equity",
                    "cost_buffer",
                    "expires",
                )
            } | {"max_orders": old.max_orders - used}
    for mapping in universe["matched"]:
        agent = OvernightAgent(bridge, database, mapping["symbol"], **kwargs)
        drafts.append(
            await agent.draft(resolved[mapping["symbol"]], manual_coexistence=manual_coexistence)
        )
    basket_class = CoexistingBasket if manual_coexistence else Basket
    extra = (
        {
            "replaces": previous.digest,
            "recovery_halts": recovery_halts,
            "previous_usage": previous_usage,
        }
        if previous
        else {}
    )
    basket = basket_class(
        implementation_sha256=bridge.implementation_digest,
        catalog_sha256=bridge.retain_evidence(json.dumps(catalog, sort_keys=True).encode()),
        created_utc=int(time.time()),
        deployments=tuple(deployment_reader.validate_python(d["deployment"]) for d in drafts),
        **extra,
    )
    runtime = OvernightBasket(bridge, database, basket, **kwargs)
    runtime.previous()
    return {
        "basket": basket.model_dump(mode="json"),
        "digest": basket.digest,
        "path": str(runtime.folder / "basket.json"),
        "universe": universe,
        "max_total_entry_attempts": sum(d.max_orders for d in basket.deployments),
        "max_total_model_calls": sum(d["review"]["max_model_calls"] for d in drafts),
        "agents": [
            {
                "symbol": d["deployment"]["symbol"],
                "agent_id": d["review"]["agent_id"],
                "bounds": d["bounds"],
                "windows": d["review"]["windows"],
            }
            for d in drafts
        ],
        "approved": False,
    }


async def execute(args):
    from stratosphere.mt5.__main__ import read_file

    bridge = MT5Bridge(args.root)
    database = WorkspaceDatabase(default_research_root() / "workspace.sqlite3")
    if args.command in {"draft", "replace"}:
        previous = (
            Basket.model_validate_json(read_file(args.input)) if args.command == "replace" else None
        )
        return await draft(
            bridge,
            database,
            {} if previous else json.loads(read_file(args.input)),
            manual_coexistence=getattr(args, "manual_coexistence", False),
            previous=previous,
        )
    basket = basket_reader.validate_json(read_file(args.input))
    if args.command == "review":
        return {"basket": basket.model_dump(mode="json"), "digest": basket.digest}
    runtime = OvernightBasket(bridge, database, basket)
    if args.command == "approve":
        return await runtime.approve(args.digest)
    if args.command == "recover":
        return await runtime.recover(args.digest)
    import signal as signals

    stop = asyncio.Event()
    for event in (signals.SIGINT, signals.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(event, stop.set)
    return await runtime.serve(stop=stop)


def main():
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--root", type=Path)
    sub = root.add_subparsers(dest="command", required=True)
    for command in ("draft", "replace", "review", "approve", "recover", "serve"):
        child = sub.add_parser(command)
        child.add_argument("input", type=Path)
        if command == "draft":
            child.add_argument("--manual-coexistence", action="store_true")
        if command in {"approve", "recover"}:
            child.add_argument("digest")
    try:
        print(json.dumps(asyncio.run(execute(root.parse_args())), indent=2))
    except (BridgeError, ModelError, ValueError, OSError, ArithmeticError) as error:
        raise SystemExit(str(error)) from None


if __name__ == "__main__":
    main()
