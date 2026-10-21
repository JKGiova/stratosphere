"""Single-owner authored observation service; no provider feed or order transport."""

import argparse
import asyncio
import fcntl
import hashlib
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from decimal import localcontext
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from pydantic import Field, model_validator

from stratosphere.connectors.catalog import strict_json
from stratosphere.paper.ledger import Account, PaperError, PaperLedger
from stratosphere.paper.runtime import (
    ClosedBarMandate,
    ClosedBarRuntime,
    ClosedObservation,
    implementation_digest,
)
from stratosphere.phase0.contracts import Contract, Digest, StrategySpec, fingerprint
from stratosphere.registry.workspace import encoded
from stratosphere.research.data import require_online


def service_digest():
    return hashlib.sha256(
        Path(__file__).read_bytes() + b"\0" + implementation_digest().encode()
    ).hexdigest()


class StreamPin(Contract):
    instance_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    mandate_sha256: Digest


class ServicePlan(Contract):
    schema_version: Literal["authored-observation-service/1"] = "authored-observation-service/1"
    account: Account
    account_journal: str
    registry_database: str
    streams: tuple[StreamPin, ...] = Field(min_length=1)
    implementation_sha256: Digest
    poll_interval_ns: int = Field(strict=True, ge=50_000_000, le=60_000_000_000)
    online_timeout_ns: int = Field(strict=True, ge=50_000_000, le=60_000_000_000)
    heartbeat_interval_ns: int = Field(default=10**9, strict=True, ge=10**9)
    max_silence_ns: int = Field(strict=True, gt=0)
    recovery: Literal["halt_without_backfill_or_deadline_reset"] = (
        "halt_without_backfill_or_deadline_reset"
    )
    authority: Literal["authored_observations_only_no_orders"] = (
        "authored_observations_only_no_orders"
    )

    @model_validator(mode="after")
    def valid_plan(self):
        for name in ("account_journal", "registry_database"):
            value = getattr(self, name)
            if not Path(value).is_absolute() or "\0" in value or str(Path(value)) != value:
                raise ValueError("Use canonical absolute local database paths")
            if ".." in Path(value).parts:
                raise ValueError("Parent traversal is not a database identity")
        identities = [row.instance_id for row in self.streams]
        if identities != sorted(set(identities)):
            raise ValueError("Pin distinct streams in instance-ID order")
        if (
            max(self.poll_interval_ns, self.online_timeout_ns, self.heartbeat_interval_ns)
            >= self.max_silence_ns
            or self.heartbeat_interval_ns < self.poll_interval_ns
        ):
            raise ValueError("Polling and online timeout must fit the explicit silence budget")
        return self

    @property
    def digest(self):
        return fingerprint(self)

    @property
    def directory(self):
        path = Path(self.account_journal)
        return path.with_name(path.name + ".service")


class QueuedObservation(Contract):
    delivery_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    instance_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    observation: ClosedObservation
    received_ns: int = Field(strict=True, gt=0)


def _private(path, *, directory=False):
    if any(value.is_symlink() for value in (path, *path.parents)):
        raise PaperError("Observation service paths cannot use symbolic links")
    if not (path.is_dir() if directory else path.is_file()):
        raise PaperError("Observation service requires existing local state")
    details = path.stat()
    if details.st_mode & 0o077 or (not directory and details.st_nlink != 1):
        raise PaperError("Observation service state must be private and have one file identity")


def _read_database(path):
    _private(path)
    db = sqlite3.connect("file:" + quote(str(path), safe="/") + "?mode=ro", uri=True)
    db.execute("PRAGMA query_only=ON")
    return db


class _ExistingLedger(PaperLedger):
    def __init__(self, path):
        _private(path)
        self.path = path

    @contextmanager
    def _transaction(self):
        _private(self.path)
        db = sqlite3.connect(
            "file:" + quote(str(self.path), safe="/") + "?mode=rw", uri=True, timeout=10
        )
        try:
            with localcontext() as context, db:
                context.prec = 100
                db.execute("BEGIN IMMEDIATE")
                if db.execute("PRAGMA user_version").fetchone()[0] not in (2, 3):
                    raise PaperError("The service requires an existing reviewed stream journal")
                yield db
        except sqlite3.Error:
            raise PaperError("Existing authored account transaction failed") from None
        finally:
            db.close()


class _ExistingRegistry:
    def __init__(self, path):
        self.path = path

    def strategy(self, identity):
        db = _read_database(self.path)
        try:
            row = db.execute(
                "SELECT spec_sha256,spec_json FROM strategies WHERE public_id=?", (identity,)
            ).fetchone()
            if row is None:
                raise PaperError("The service strategy is absent from the existing registry")
            specification = StrategySpec.model_validate(strict_json(row[1].encode()))
            if fingerprint(specification) != row[0]:
                raise PaperError("The service registry definition failed its digest check")
            return {"spec_sha256": row[0], "specification": specification.model_dump(mode="json")}
        except ValueError, KeyError, TypeError, sqlite3.Error:
            raise PaperError("The service registry definition could not be verified") from None
        finally:
            db.close()


def _reduce(state, event):
    kind, at = event["kind"], event["at_ns"]
    fields = {
        "plan": {"plan"},
        "approval": {"digest"},
        "queued": {"delivery"},
        "collision": {"delivery"},
        "started": {"run_id"},
        "heartbeat": {"run_id"},
        "stopped": {"run_id"},
        "halt": {"reason"},
        "ack": {"run_id", "delivery_id", "status", "reason", "signal_sha256"},
    }
    expected = fields.get(kind)
    extra_clock = {"observed_clock_ns"} if "observed_clock_ns" in event else set()
    if expected is None or set(event) != expected | {"kind", "at_ns"} | extra_clock:
        raise PaperError("Invalid retained service event shape")
    if extra_clock and (
        type(event["observed_clock_ns"]) is not int
        or not 0 < event["observed_clock_ns"] < at
        or not (kind == "stopped" or (kind == "halt" and event["reason"] == "clock_regression"))
    ):
        raise PaperError("Invalid retained service clock regression")
    if "run_id" in event and not re.fullmatch(r"[a-f0-9]{32}", event["run_id"]):
        raise PaperError("Invalid retained worker session identity")
    if type(at) is not int or at <= 0 or at < state["last_clock_ns"]:
        raise PaperError("Invalid retained service clock")
    state["last_clock_ns"] = at
    if kind == "plan":
        if state["plan"] is not None:
            raise PaperError("Invalid retained service plan")
        state["plan"] = ServicePlan.model_validate(event["plan"])
        return
    plan = state["plan"]
    if plan is None:
        raise PaperError("Missing service plan")
    identities = {pin.instance_id for pin in plan.streams}
    if kind == "approval":
        if state["approved"] or event["digest"] != plan.digest:
            raise PaperError("Invalid retained service approval")
        state["approved"] = True
    elif kind in {"queued", "collision"}:
        value = QueuedObservation.model_validate(event["delivery"])
        if value.instance_id not in identities or not state["approved"]:
            raise PaperError("Unreviewed retained service delivery")
        old = state["queue"].get(value.delivery_id)
        if kind == "collision":
            if old is None or old == value:
                raise PaperError("Invalid retained delivery conflict")
            state["faults"].append(event)
        elif old is not None:
            raise PaperError("Repeated retained service delivery")
        else:
            state["queue"][value.delivery_id] = value
    elif kind == "started":
        if not state["approved"] or state["faults"]:
            raise PaperError("Service startup bypassed approval or a durable halt")
        if state["run_id"] is not None:
            state["interrupted_runs"].append(state["run_id"])
        state["run_id"] = event["run_id"]
        if not state["progress"]:
            state["progress"] = dict.fromkeys(sorted(identities), at)
        state["last_heartbeat_ns"] = at
    elif kind in {"ack", "heartbeat", "stopped"}:
        if state["run_id"] is None or event["run_id"] != state["run_id"]:
            raise PaperError("Service event has no matching owner session")
        if kind == "ack":
            pending = [key for key in state["queue"] if key not in state["results"]]
            if not pending or pending[0] != event["delivery_id"]:
                raise PaperError("Service acknowledgement bypassed retained delivery order")
            if event["status"] not in {"accepted", "duplicate", "halted"}:
                raise PaperError("Invalid retained service acknowledgement")
            if event["signal_sha256"] is not None and (
                event["status"] != "accepted"
                or not re.fullmatch(r"[a-f0-9]{64}", event["signal_sha256"])
            ):
                raise PaperError("Invalid retained service signal reference")
            if (event["reason"] is not None) != (event["status"] == "halted"):
                raise PaperError("Invalid retained service outcome reason")
            state["results"][event["delivery_id"]] = event
            state["last_heartbeat_ns"] = at
            if event["status"] == "accepted":
                state["progress"][state["queue"][event["delivery_id"]].instance_id] = at
            elif event["status"] == "halted":
                state["faults"].append(event)
        elif kind == "heartbeat":
            state["last_heartbeat_ns"] = at
        else:
            state["run_id"] = None
    elif kind == "halt":
        if event["reason"] not in {
            "clock_regression",
            "silence_exceeded",
            "online_unavailable",
            "processing_failed",
        }:
            raise PaperError("Invalid retained service halt")
        state["faults"].append(event)
    else:
        raise PaperError("Unknown observation service event")


class ServiceJournal:
    """Immutable queue and owner history at the one canonical path for this account."""

    def __init__(self, directory, *, clock=time.time_ns):
        self.directory, self.clock = Path(directory), clock
        self.path = self.directory / "service.sqlite3"
        _private(self.directory, directory=True)
        _private(self.path)

    @classmethod
    def create(cls, plan, *, clock=time.time_ns):
        plan = ServicePlan.model_validate(plan.model_dump())
        _private(Path(plan.account_journal))
        _private(Path(plan.registry_database))
        if plan.implementation_sha256 != service_digest():
            raise PaperError("Review the current observation service implementation")
        plan.directory.mkdir(mode=0o700)
        _private(plan.directory, directory=True)
        descriptor = os.open(
            plan.directory / "service.sqlite3", os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600
        )
        os.close(descriptor)
        value = cls(plan.directory, clock=clock)
        with value._transaction(initial=True) as db:
            db.execute(
                "CREATE TABLE events(seq INTEGER PRIMARY KEY,payload TEXT NOT NULL,"
                "digest TEXT NOT NULL)"
            )
            for action in ("UPDATE", "DELETE"):
                db.execute(
                    f"CREATE TRIGGER retain_{action.lower()} BEFORE {action} ON events "
                    "BEGIN SELECT RAISE(ABORT,'Service evidence is immutable'); END"
                )
            db.execute("PRAGMA user_version=1")
            state, chain = value._load(db, initial=True)
            value._append(db, state, chain, {"kind": "plan", "plan": plan.model_dump(mode="json")})
        return value

    @contextmanager
    def _transaction(self, *, initial=False, readonly=False):
        _private(self.directory, directory=True)
        _private(self.path)
        db = (
            _read_database(self.path)
            if readonly
            else sqlite3.connect(
                "file:" + quote(str(self.path), safe="/") + "?mode=rw", uri=True, timeout=10
            )
        )
        try:
            with db:
                db.execute("BEGIN" if readonly else "BEGIN IMMEDIATE")
                if not initial and db.execute("PRAGMA user_version").fetchone()[0] != 1:
                    raise PaperError("Unsupported observation service journal")
                yield db
        except sqlite3.Error:
            raise PaperError(
                "Observation service journal failed; retained state was preserved"
            ) from None
        finally:
            db.close()

    def _load(self, db, *, initial=False):
        state = {
            "plan": None,
            "approved": False,
            "queue": {},
            "results": {},
            "faults": [],
            "run_id": None,
            "interrupted_runs": [],
            "progress": {},
            "last_clock_ns": 0,
            "last_heartbeat_ns": None,
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
                    raise PaperError("Observation service journal integrity failed")
                _reduce(state, strict_json(payload.encode()))
                chain = digest
            if state["plan"] is None and not initial:
                raise PaperError("The existing observation service has no retained plan")
            if state["plan"] is not None and state["plan"].directory != self.directory:
                raise PaperError("Observation service must use the canonical account directory")
        except ValueError, KeyError, TypeError:
            raise PaperError("Observation service evidence could not be verified") from None
        return state, chain

    def _append(self, db, state, chain, event):
        observed = self.clock()
        if type(observed) is not int or observed <= 0:
            raise PaperError("Use a positive service wall clock")
        if observed < state["last_clock_ns"]:
            if event.get("reason") != "clock_regression" and event["kind"] != "stopped":
                raise PaperError("The service clock regressed")
            event = {**event, "observed_clock_ns": observed}
        event = {**event, "at_ns": max(observed, state["last_clock_ns"])}
        _reduce(state, event)
        payload = encoded(event)
        digest = hashlib.sha256(chain.encode() + b"\0" + payload.encode()).hexdigest()
        db.execute("INSERT INTO events(payload,digest) VALUES (?,?)", (payload, digest))
        return digest

    def snapshot(self):
        with self._transaction(readonly=True) as db:
            state, chain = self._load(db)
            return {**state, "journal_sha256": chain}

    def approve(self, digest):
        ObservationWorker(self)._verify()
        with self._transaction() as db:
            state, chain = self._load(db)
            if digest != state["plan"].digest or state["approved"]:
                raise PaperError("Approve exactly the pending observation service plan")
            self._append(db, state, chain, {"kind": "approval", "digest": digest})

    def enqueue(self, delivery):
        delivery = QueuedObservation.model_validate(delivery.model_dump())
        with self._transaction() as db:
            state, chain = self._load(db)
            old = state["queue"].get(delivery.delivery_id)
            if old == delivery:
                return "duplicate"
            self._append(
                db,
                state,
                chain,
                {
                    "kind": "collision" if old else "queued",
                    "delivery": delivery.model_dump(mode="json"),
                },
            )
            return "conflict" if old else "queued"


class ObservationWorker:
    def __init__(self, journal, *, online_check=require_online):
        self.journal, self.online_check = journal, online_check
        self.descriptor, self.run_id = None, None
        self.plan = journal.snapshot()["plan"]
        self.runtime = ClosedBarRuntime(
            _ExistingLedger(Path(self.plan.account_journal)),
            _ExistingRegistry(Path(self.plan.registry_database)),
        )

    def _verify(self):
        _private(Path(self.plan.account_journal))
        _private(Path(self.plan.registry_database))
        if (
            self.plan.implementation_sha256 != service_digest()
            or service_digest() != _LOADED_DIGEST
        ):
            raise PaperError("Observation service implementation changed; review is required")
        with self.runtime.ledger._transaction() as db:
            if db.execute("PRAGMA user_version").fetchone()[0] not in (2, 3):
                raise PaperError("The service requires an existing reviewed stream journal")
            state, _ = self.runtime.ledger._load(db)
        if state["account"] != self.plan.account.model_dump(mode="json"):
            raise PaperError("The observation service account changed")
        for pin in self.plan.streams:
            stream = state["runtimes"].get(pin.instance_id)
            if (
                not stream
                or not stream["approved"]
                or stream["faults"]
                or stream["digest"] != pin.mandate_sha256
            ):
                raise PaperError("The service requires the exact approved unhalted stream")
            self.runtime._verify(ClosedBarMandate.model_validate(stream["mandate"]))

    @contextmanager
    def own(self):
        if self.descriptor is not None:
            raise PaperError("This worker already owns an observation session")
        lock = Path(self.plan.account_journal + ".runtime.lock")
        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            _private(lock)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise PaperError("Another observation worker owns this account") from None
            self._verify()
            clock_failed = False
            with self.journal._transaction() as db:
                state, chain = self.journal._load(db)
                if self.journal.clock() < state["last_clock_ns"]:
                    self.journal._append(
                        db, state, chain, {"kind": "halt", "reason": "clock_regression"}
                    )
                    clock_failed = True
                else:
                    self.run_id = uuid.uuid7().hex
                    self.journal._append(
                        db, state, chain, {"kind": "started", "run_id": self.run_id}
                    )
            if clock_failed:
                raise PaperError("The service clock regressed; a durable halt was retained")
            self.descriptor = descriptor
            yield self
        finally:
            try:
                if self.descriptor is not None:
                    with self.journal._transaction() as db:
                        state, chain = self.journal._load(db)
                        self.journal._append(
                            db, state, chain, {"kind": "stopped", "run_id": self.run_id}
                        )
            finally:
                self.descriptor, self.run_id = None, None
                os.close(descriptor)

    def _owner(self):
        if self.descriptor is None:
            raise PaperError("Acquire the account observation-worker lock first")
        path = Path(self.plan.account_journal + ".runtime.lock")
        _private(path)
        opened, current = os.fstat(self.descriptor), path.stat()
        if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
            raise PaperError("The account worker lock identity changed")

    def _halt(self, reason):
        with self.journal._transaction() as db:
            state, chain = self.journal._load(db)
            if self.journal.clock() < state["last_clock_ns"]:
                reason = "clock_regression"
            self.journal._append(db, state, chain, {"kind": "halt", "reason": reason})

    async def tick(self, stop=None):
        self._owner()
        state = self.journal.snapshot()
        if state["faults"]:
            return "halted"
        if self.journal.clock() < state["last_clock_ns"]:
            self._halt("clock_regression")
            return "halted"
        if any(
            self.journal.clock() - at > self.plan.max_silence_ns
            for at in state["progress"].values()
        ):
            self._halt("silence_exceeded")
            return "halted"
        try:
            async with asyncio.timeout(self.plan.online_timeout_ns / 10**9) as deadline:
                await self.online_check()
        except TimeoutError, ValueError, OSError:
            self._halt("online_unavailable")
            return "halted"
        if deadline.expired():
            self._halt("online_unavailable")
            return "halted"
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise asyncio.CancelledError
        if stop is not None and stop.is_set():
            return "stopped"
        self._owner()
        try:
            self._verify()
            with self.journal._transaction() as db:
                state, chain = self.journal._load(db)
                if state["faults"]:
                    return "halted"
                now = self.journal.clock()
                if now < state["last_clock_ns"]:
                    self.journal._append(
                        db, state, chain, {"kind": "halt", "reason": "clock_regression"}
                    )
                    return "halted"
                if any(now - at > self.plan.max_silence_ns for at in state["progress"].values()):
                    self.journal._append(
                        db, state, chain, {"kind": "halt", "reason": "silence_exceeded"}
                    )
                    return "halted"
                delivery = next(
                    (row for key, row in state["queue"].items() if key not in state["results"]),
                    None,
                )
                if delivery is None:
                    if now - state["last_heartbeat_ns"] >= self.plan.heartbeat_interval_ns:
                        self.journal._append(
                            db, state, chain, {"kind": "heartbeat", "run_id": self.run_id}
                        )
                    return "idle"
                result = self.runtime.ingest(
                    delivery.instance_id, delivery.observation, received_ns=delivery.received_ns
                )
                self._after_ingest()
                self.journal._append(
                    db,
                    state,
                    chain,
                    {
                        "kind": "ack",
                        "run_id": self.run_id,
                        "delivery_id": delivery.delivery_id,
                        "status": result["status"],
                        "reason": result.get("reason"),
                        "signal_sha256": result["signal"]["signal_sha256"]
                        if result["signal"]
                        else None,
                    },
                )
                return result["status"]
        except PaperError:
            self._halt("processing_failed")
            return "halted"

    def _after_ingest(self):
        """Crash-test boundary: account commit precedes durable service acknowledgement."""

    async def run(self, stop):
        with self.own():
            while not stop.is_set():
                if await self.tick(stop) in {"halted", "stopped"}:
                    return
                try:
                    await asyncio.wait_for(stop.wait(), self.plan.poll_interval_ns / 10**9)
                except TimeoutError:
                    pass


def probe_owner(plan):
    """Probe the OS lock now; a stored PID/heartbeat alone is never a liveness claim."""
    path = Path(plan.account_journal + ".runtime.lock")
    if not path.exists():
        return False
    _private(path)
    descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False
    finally:
        os.close(descriptor)


def launch(journal):
    """Explicit detached local worker with an allowlisted, credential-free environment."""
    state = journal.snapshot()
    if not state["approved"] or state["faults"]:
        raise PaperError("Approve an unhalted observation service before launch")
    ObservationWorker(journal)._verify()
    path = journal.directory / "worker.log"
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        _private(path)
        return subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-m",
                "stratosphere.paper.service",
                "run",
                str(journal.directory),
            ],
            stdin=subprocess.DEVNULL,
            stdout=descriptor,
            stderr=descriptor,
            env={"PATH": os.defpath, "LANG": "C.UTF-8", "PYTHONUNBUFFERED": "1"},
            close_fds=True,
            start_new_session=True,
            cwd=journal.directory,
        )
    finally:
        os.close(descriptor)


async def _run(directory):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(name, stop.set)
    await ObservationWorker(ServiceJournal(directory)).run(stop)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("run", "status"))
    parser.add_argument("directory", type=Path)
    arguments = parser.parse_args()
    try:
        if arguments.operation == "run":
            if any(
                word in name.upper()
                for name in os.environ
                for word in ("KEY", "TOKEN", "SECRET", "PASSWORD")
            ):
                raise PaperError("Start observation workers with a credential-free environment")
            asyncio.run(_run(arguments.directory))
        else:
            asyncio.run(require_online())
            state = ServiceJournal(arguments.directory).snapshot()
            print(
                encoded(
                    {
                        "plan_sha256": state["plan"].digest,
                        "journal_sha256": state["journal_sha256"],
                        "owner_lock_held_now": probe_owner(state["plan"]),
                        "approved": state["approved"],
                        "halted": bool(state["faults"]),
                        "queued": len(state["queue"]),
                        "acknowledged": len(state["results"]),
                        "last_heartbeat_ns": state["last_heartbeat_ns"],
                        "authority": state["plan"].authority,
                    }
                )
            )
    except PaperError, OSError, ValueError, sqlite3.Error:
        print(
            "Observation service unavailable; inspect the retained private state.", file=sys.stderr
        )
        raise SystemExit(1) from None


_LOADED_DIGEST = service_digest()

if __name__ == "__main__":
    main()
