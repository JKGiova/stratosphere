"""Local, bounded MT5 EA transport. No passwords, sockets, DLLs or automatic trade retries."""

import asyncio
import csv
import hashlib
import io
import os
import re
import sqlite3
import time
import uuid
from decimal import Decimal
from pathlib import Path

from stratosphere.connectors.mt5_probe import HISTORY_FRAMES
from stratosphere.phase0.contracts import decimal_text
from stratosphere.research.store import ResearchError

SCHEMA = "stratosphere.mt5.bridge/1"
READS = {"account", "symbol", "quote", "rates", "positions", "orders", "deals"}
SYMBOL = re.compile(r"[A-Za-z0-9_.#-]{1,64}")


class BridgeError(ResearchError):
    pass


class UncertainOrder(BridgeError):
    """An order request may have reached MT5; it must never be replayed."""


class JournalConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


def default_root() -> Path:
    configured = os.getenv("STRATOSPHERE_MT5_ROOT")
    if configured:
        return Path(configured)
    import sys

    if sys.platform == "darwin":
        return Path.home() / (
            "Library/Application Support/net.metaquotes.wine.metatrader5/drive_c/"
            "Program Files/MetaTrader 5/MQL5/Files/stratosphere-bridge"
        )
    raise BridgeError("Set STRATOSPHERE_MT5_ROOT to the active MQL5/Files/stratosphere-bridge path")


def encode(fields: dict) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_NONE)
    for key, value in fields.items():
        value = str(value)
        if not re.fullmatch(r"[a-z0-9_.]+", key) or any(c in value for c in '\t\r\n\0"'):
            raise BridgeError("Unsafe MT5 wire field")
        writer.writerow((key, value))
    return output.getvalue().encode("utf-8")


def decode(raw: bytes) -> dict[str, str]:
    if len(raw) > 32_000_000:
        raise BridgeError("Oversized MT5 response")
    fields = {}
    try:
        for row in csv.reader(io.StringIO(raw.decode("utf-8-sig")), delimiter="\t"):
            if len(row) != 2 or row[0] in fields or "\0" in row[1]:
                raise BridgeError("Malformed/duplicate MT5 response field")
            fields[row[0]] = row[1]
    except UnicodeError as exc:
        raise BridgeError("Invalid MT5 response encoding") from exc
    return fields


def private_path(path: Path) -> None:
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise BridgeError("MT5 paths cannot contain symbolic links")


def atomic_write(path: Path, data: bytes) -> None:
    private_path(path)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


class MT5Bridge:
    provider = "mt5"

    def __init__(self, root: Path | None = None, *, timeout=60.0, state_root=None):
        self.root = Path(root) if root is not None else default_root()
        private_path(self.root)
        if not self.root.is_dir():
            raise BridgeError("MT5 bridge is not installed/running; attach StratosphereBridge EA")
        self.root.chmod(0o700)
        for name in ("requests", "responses", "approvals", "processing", "sent"):
            folder = self.root / name
            private_path(folder)
            folder.mkdir(mode=0o700, exist_ok=True)
            folder.chmod(0o700)
        self.timeout = timeout
        self.account_key = None
        self.state_root = Path(state_root) if state_root is not None else None

    def state_directory(self, session=None):
        session = session or self.session()
        key = tuple(session.get(name) for name in ("server", "login", "mode"))
        identity = hashlib.sha256(repr(key).encode()).hexdigest()
        base = self.state_root
        if base is None:
            import pwd
            import sys

            home = Path(pwd.getpwuid(os.getuid()).pw_dir)
            base = home / (
                "Library/Application Support/stratosphere/mt5"
                if sys.platform == "darwin"
                else ".local/state/stratosphere/mt5"
            )
        path = base / identity
        private_path(path)
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.chmod(0o700)
        return path

    def session(self):
        path = self.root / "session.tsv"
        private_path(path)
        if not path.is_file() or path.stat().st_size > 8192:
            raise BridgeError("MT5 EA session unavailable")
        value = decode(path.read_bytes())
        if value.get("schema") != SCHEMA or not re.fullmatch(
            r"[0-9-]{1,80}", value.get("session", "")
        ):
            raise BridgeError("Unsupported MT5 EA session")
        return value

    def retain_evidence(self, data: bytes) -> str:
        if len(data) > 8_000_000:
            raise BridgeError("Oversized MT5 deployment evidence")
        digest = hashlib.sha256(data).hexdigest()
        folder = self.state_directory() / "evidence"
        private_path(folder)
        folder.mkdir(mode=0o700, exist_ok=True)
        path = folder / (digest + ".bin")
        private_path(path)
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            if path.read_bytes() != data:
                raise BridgeError("Retained MT5 evidence changed") from None
        else:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
        return digest

    def journal(self):
        import fcntl

        directory = self.state_directory()
        path = directory / "client.sqlite3"
        private_path(path)
        schema_lock = directory / "schema.lock"
        private_path(schema_lock)
        lock = os.open(schema_lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        db = None
        try:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                os.close(fd)
                new = True
            except FileExistsError:
                new = False
                if path.stat().st_size == 0:
                    raise BridgeError(
                        "Empty/damaged MT5 journal; do not recreate execution authority"
                    ) from None
            db = sqlite3.connect(path, isolation_level=None, factory=JournalConnection)
            db.execute("PRAGMA synchronous=FULL")
            if new:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "CREATE TABLE requests (id TEXT PRIMARY KEY, op TEXT, account TEXT, "
                    "payload BLOB, state TEXT, response BLOB)"
                )
                db.execute(
                    "CREATE TABLE approvals (digest TEXT PRIMARY KEY, payload BLOB, "
                    "state TEXT NOT NULL)"
                )
                db.execute("PRAGMA user_version=1")
                db.execute("COMMIT")
            expected = {
                "requests": ["id", "op", "account", "payload", "state", "response"],
                "approvals": ["digest", "payload", "state"],
            }
            if db.execute("PRAGMA user_version").fetchone()[0] != 1 or (
                db.execute("PRAGMA quick_check").fetchone()[0] != "ok"
            ):
                raise BridgeError("Unsupported or damaged MT5 journal")
            for name, columns in expected.items():
                if [row[1] for row in db.execute(f"PRAGMA table_info({name})")] != columns:
                    raise BridgeError(
                        "Incomplete MT5 journal; retained authority cannot be recreated"
                    )
            return db
        except sqlite3.DatabaseError as exc:
            if db is not None:
                db.close()
            raise BridgeError("Unreadable/damaged MT5 journal; retain it for inspection") from exc
        except BaseException:
            if db is not None:
                db.close()
            raise
        finally:
            os.close(lock)

    async def _request(self, op: str, args=None, *, identity=None, trade=False):
        if op not in READS | {"check", "submit", "close"}:
            raise BridgeError("Unsupported MT5 operation")
        if (op in {"submit", "close"}) != trade:
            raise BridgeError("MT5 submission requires the approval gateway")
        session = self.session()
        key = (session.get("login"), session.get("server"), session.get("mode"))
        if any(not item for item in key) or (self.account_key and self.account_key != key):
            raise BridgeError("MT5 account changed; create a new explicitly bound connection")
        self.account_key = key
        identity = identity or uuid.uuid4().hex
        if not re.fullmatch(r"[a-f0-9]{32}", identity):
            raise BridgeError("Invalid MT5 request identity")
        fields = {
            "schema": SCHEMA,
            "id": identity,
            "session": session["session"],
            "login": key[0],
            "server": key[1],
            "mode": key[2],
            "op": op,
            "expires": str(int(time.time()) + int(self.timeout)),
        }
        if args:
            if set(args) & fields.keys():
                raise BridgeError("MT5 arguments cannot override account/session/protocol pins")
            fields.update(args)
        payload = encode(fields)
        db = self.journal()
        try:
            db.execute(
                "INSERT INTO requests VALUES (?,?,?,?,?,NULL)",
                (identity, op, repr(key), payload, "reserved"),
            )
        except sqlite3.IntegrityError as exc:
            db.close()
            raise BridgeError("Consumed MT5 request cannot be replayed") from exc
        try:
            atomic_write(self.root / "requests" / (identity + ".tsv"), payload)
            end = time.monotonic() + self.timeout
            response = self.root / "responses" / (identity + ".tsv")
            while time.monotonic() < end:
                private_path(response)
                if response.is_file():
                    if response.stat().st_size > 32_000_000:
                        raise BridgeError("Oversized MT5 response")
                    raw = response.read_bytes()
                    value = decode(raw)
                    if any(
                        value.get(k) != fields[k]
                        for k in ("schema", "id", "session", "login", "server", "mode", "op")
                    ):
                        raise BridgeError("MT5 response account/session/request mismatch")
                    if self.session() != session:
                        raise BridgeError("MT5 bridge restarted or changed account during request")
                    db.execute(
                        "UPDATE requests SET state='observed',response=? WHERE id=?",
                        (raw, identity),
                    )
                    if value.get("status") != "ok":
                        raise BridgeError(value.get("error", "MT5 rejected request"))
                    return value
                await asyncio.sleep(0.05)
            raise BridgeError("MT5 request timed out; no automatic retry")
        except BaseException as exc:
            observed = db.execute("SELECT state FROM requests WHERE id=?", (identity,)).fetchone()
            if observed and observed[0] == "observed":
                raise
            db.execute(
                "UPDATE requests SET state=? WHERE id=? AND state!='observed'",
                ("uncertain" if trade else "failed", identity),
            )
            if trade:
                raise UncertainOrder(
                    f"MT5 order {identity} has an uncertain outcome; "
                    "inspect/reconcile, never replay"
                ) from exc
            raise
        finally:
            db.close()

    async def read(self, op: str, **args):
        if op not in READS:
            raise BridgeError("Read interface cannot check or place orders")
        if "symbol" in args and not SYMBOL.fullmatch(args["symbol"]):
            raise BridgeError("Invalid MT5 symbol")
        if op == "rates":
            if args.get("frame") not in HISTORY_FRAMES:
                raise BridgeError("Unsupported MT5 candle interval")
            count = args.get("count", 512)
            if type(count) is not int or not 1 <= count <= 100_000:
                raise BridgeError("MT5 rate count must be 1..100000")
            args["count"] = str(count)
        result = await self._request(op, args)
        try:
            self.validate_data(op, result, args)
        except (ValueError, KeyError, ArithmeticError) as exc:
            raise BridgeError(
                "MT5 data failed structural validation; raw receipt retained"
            ) from exc
        return result

    @staticmethod
    def validate_data(op, result, args):
        def number(key, *, positive=False):
            value = Decimal(decimal_text(result[key]))
            if positive and value <= 0:
                raise ValueError("Nonpositive executable price")
            return value

        if op == "account":
            if result["connected"] not in {"0", "1"}:
                raise ValueError("Invalid connection state")
            for key in ("balance", "equity", "free_margin", "margin"):
                number(key)
        if op == "quote":
            if number("ask", positive=True) < number("bid", positive=True):
                raise ValueError("Crossed quote")
            int(result["server_time_ms"])
            int(result["age_ms"])
        if op in {"rates", "positions", "orders", "deals"}:
            count = int(result["count"])
            limit = int(args.get("count", 10000 if op == "deals" else 1000))
            if not 0 <= count <= limit:
                raise ValueError("Invalid MT5 row count")
            if op == "rates":
                previous = 0
                for i in range(1, count + 1):
                    prefix = f"b.{i}."
                    now = int(result[prefix + "time"])
                    opening, high, low, closing = (
                        number(prefix + key, positive=True)
                        for key in ("open", "high", "low", "close")
                    )
                    if (
                        now <= previous
                        or low > min(opening, closing)
                        or high < max(opening, closing)
                    ):
                        raise ValueError("Unordered/inconsistent MT5 bars")
                    if (
                        int(result[prefix + "tick_volume"]) < 0
                        or int(result[prefix + "spread"]) < 0
                    ):
                        raise ValueError("Negative volume/spread")
                    previous = now

    async def account(self):
        return await self.read("account")

    @property
    def implementation_digest(self):
        source = Path(__file__).parent
        return hashlib.sha256(
            b"".join(
                (source / name).read_bytes()
                for name in ("bridge.py", "trading.py", "gold.py", "gold_strategy.py")
            )
            + (source.parent / "phase0/contracts.py").read_bytes()
        ).hexdigest()
