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
from typing import Literal

from pydantic import Field, model_validator

from stratosphere.models.streaming import ModelError
from stratosphere.mt5.bridge import BridgeError, MT5Bridge, atomic_write, private_path
from stratosphere.mt5.gold import retain, save_new
from stratosphere.mt5.overnight import OvernightAgent
from stratosphere.mt5.overnight_markets import intersection
from stratosphere.mt5.overnight_strategy import signal
from stratosphere.mt5.trading import Deployment, MT5Trading
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
        ):
            raise ValueError(
                "Basket requires unique symbols and one exact demo account/runtime/expiry"
            )
        return self

    @property
    def digest(self):
        return fingerprint(self)


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

    async def approve(self, exact_digest):
        if exact_digest != self.basket.digest:
            raise BridgeError("Approve the exact reviewed overnight basket digest")
        if (self.folder / "approved.json").exists():
            raise BridgeError("Basket approval receipt already exists; never recreate authority")
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
        if int((await self.bridge.read("positions"))["count"]) or int(
            (await self.bridge.read("orders"))["count"]
        ):
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
        if int(positions["count"]):
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
        if int(snapshot["orders"]["count"]):
            raise BridgeError("External/pending orders block the overnight basket")
        if (self.folder / "halt.json").exists():
            return {"action": "halted_flat"}
        now = int(self.clock())
        if all(now >= d.expires for d in self.basket.deployments):
            return {"action": "expired_flat"}
        choices = []
        for symbol, agent in self.agents.items():
            deployment = self.deployments[symbol]
            agent.trading.deployment(deployment.digest)
            agent.trading.verify(deployment, snapshot["account"])
            if (agent.folder / "halt.json").exists() or self.database.is_retired(
                agent.record["public_id"]
            ):
                continue
            for window in self.reports[symbol].windows:
                if not window.entry_start <= now < window.entry_end:
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


async def draft(bridge, database, limits, *, provider_factory=None):
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
    for mapping in universe["matched"]:
        agent = OvernightAgent(bridge, database, mapping["symbol"], **kwargs)
        drafts.append(await agent.draft(resolved[mapping["symbol"]]))
    basket = Basket(
        implementation_sha256=bridge.implementation_digest,
        catalog_sha256=bridge.retain_evidence(json.dumps(catalog, sort_keys=True).encode()),
        created_utc=int(time.time()),
        deployments=tuple(Deployment.model_validate(d["deployment"]) for d in drafts),
    )
    runtime = OvernightBasket(bridge, database, basket, **kwargs)
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
    if args.command == "draft":
        return await draft(bridge, database, json.loads(read_file(args.input)))
    basket = Basket.model_validate_json(read_file(args.input))
    if args.command == "review":
        return {"basket": basket.model_dump(mode="json"), "digest": basket.digest}
    runtime = OvernightBasket(bridge, database, basket)
    if args.command == "approve":
        return await runtime.approve(args.digest)
    import signal as signals

    stop = asyncio.Event()
    for event in (signals.SIGINT, signals.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(event, stop.set)
    return await runtime.serve(stop=stop)


def main():
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--root", type=Path)
    sub = root.add_subparsers(dest="command", required=True)
    for command in ("draft", "review", "approve", "serve"):
        child = sub.add_parser(command)
        child.add_argument("input", type=Path)
        if command == "approve":
            child.add_argument("digest")
    try:
        print(json.dumps(asyncio.run(execute(root.parse_args())), indent=2))
    except (BridgeError, ModelError, ValueError, OSError, ArithmeticError) as error:
        raise SystemExit(str(error)) from None


if __name__ == "__main__":
    main()
