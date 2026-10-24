"""Local paper portfolios and a single exact final-strategy approval; no live authority."""

import fcntl
import hashlib
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from stratosphere.connectors.alpaca_paper import LiquidPaper
from stratosphere.phase0.contracts import Contract, Digest, decimal_text, fingerprint
from stratosphere.research.store import ResearchError
from stratosphere.workflow.contracts import PortfolioDefinition
from stratosphere.workflow.research import payload_digest
from stratosphere.workflow.risk import portfolio_agent_id
from stratosphere.yahoo.contracts import YahooRequest


def execution_digest():
    root = Path(__file__).parent
    files = [root / n for n in ("portfolios.py", "execution.py", "risk.py", "contracts.py")]
    files += [
        root.parent / "connectors/alpaca_paper.py",
        root.parent / "phase0/signals.py",
        root.parent / "models/structured.py",
        root.parent / "phase0/contracts.py",
    ]
    return hashlib.sha256(
        b"".join(p.name.encode() + b"\0" + p.read_bytes() for p in files)
    ).hexdigest()


def default_portfolio_path():
    """One OS-user account journal across project/research directories."""
    import pwd
    import sys

    home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    return home / (
        "Library/Application Support/stratosphere/paper-portfolios/portfolios.sqlite3"
        if sys.platform == "darwin"
        else ".local/state/stratosphere/paper-portfolios/portfolios.sqlite3"
    )


def portfolio_id(provider):
    if provider not in {"alpaca", "liquid"}:
        raise ResearchError("Choose alpaca or liquid paper; live is not implemented")
    return uuid.uuid5(uuid.NAMESPACE_URL, "stratosphere:personal:" + provider + ":paper").hex


class PaperLimits(Contract):
    max_strategy_fraction: str = "0.25"
    max_position_fraction: str = "0.05"
    max_risk_fraction: str = "0.01"
    max_daily_loss_fraction: str = "0.02"
    fee_buffer_fraction: str = "0.01"
    max_quote_age_ns: int = Field(default=30_000_000_000, strict=True, gt=0, le=60_000_000_000)
    max_signal_age_ns: int = Field(default=300_000_000_000, strict=True, gt=0, le=600_000_000_000)
    max_entries_per_day: int = Field(default=20, strict=True, ge=1, le=100)
    max_model_calls_per_signal: Literal[3] = 3
    max_slippage_fraction: str = "0.002"

    @model_validator(mode="after")
    def valid(self):
        self.definition("alpaca")
        for name in ("fee_buffer_fraction", "max_slippage_fraction"):
            value = decimal_text(getattr(self, name))
            if not Decimal("0") < Decimal(value) <= Decimal("0.05"):
                raise ValueError("Use a positive buffer of at most five percent")
        return self

    def definition(self, provider):
        return PortfolioDefinition(
            name=provider + " paper",
            provider=provider,
            mode="paper",
            currency="USD",
            **{
                k: getattr(self, k)
                for k in (
                    "max_strategy_fraction",
                    "max_position_fraction",
                    "max_risk_fraction",
                    "max_daily_loss_fraction",
                )
            },
        )


class FinishedStrategy(Contract):
    schema_version: Literal["finished-paper-strategy/1"] = "finished-paper-strategy/1"
    package_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    strategy_sha256: Digest
    campaign_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    report_sha256: Digest
    candidate_job_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    candidate_report_sha256: Digest
    research_metrics: dict
    attempted_candidates: int = Field(strict=True, ge=1, le=12)
    request: YahooRequest
    provider: Literal["alpaca", "liquid"]
    mode: Literal["paper"] = "paper"
    portfolio_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    account_id: str = Field(min_length=1, max_length=100)
    symbol: str = Field(pattern=r"^[A-Z0-9]+/[A-Z]{3}$")
    implementation_sha256: Digest
    limits: PaperLimits
    policy: Literal["cash_long_bounded_quantity_ioc/1"] = "cash_long_bounded_quantity_ioc/1"
    limitations: tuple[str, ...]

    @model_validator(mode="after")
    def identity(self):
        if self.portfolio_id != portfolio_id(self.provider):
            raise ValueError("Portfolio/provider identity mismatch")
        if self.request.asset_class != "crypto_spot" or self.symbol != self.request.symbol.replace(
            "-", "/"
        ):
            raise ValueError("Only identical base/USD crypto spot mappings are implemented")
        if not self.symbol.endswith("/USD") or self.provider != "alpaca":
            raise ValueError("Liquid perpetuals need separate evidence and an order capability")
        return self

    @property
    def digest(self):
        return fingerprint(self)


SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_portfolios (
 id TEXT PRIMARY KEY, provider TEXT UNIQUE NOT NULL, mode TEXT CHECK(mode='paper') NOT NULL,
 account_id TEXT UNIQUE, halted TEXT, baseline_day TEXT, baseline_equity TEXT);
CREATE TABLE IF NOT EXISTS paper_packages (
 id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL, portfolio_id TEXT NOT NULL,
 digest TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL,
 approved_ns INTEGER, capital TEXT NOT NULL DEFAULT '0', cursor TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS paper_active_strategy ON paper_packages(strategy_id)
 WHERE state='approved';
CREATE TABLE IF NOT EXISTS paper_signals (
 id TEXT PRIMARY KEY, package_id TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL,
 created_ns INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS paper_receipts (
 id INTEGER PRIMARY KEY, signal_id TEXT NOT NULL, stage TEXT NOT NULL, payload TEXT NOT NULL,
 created_ns INTEGER NOT NULL, UNIQUE(signal_id,stage));
CREATE TABLE IF NOT EXISTS paper_orders (
 id TEXT PRIMARY KEY, signal_id TEXT UNIQUE NOT NULL, package_id TEXT NOT NULL,
 payload TEXT NOT NULL, state TEXT NOT NULL, observation TEXT, reserved TEXT NOT NULL,
 created_ns INTEGER NOT NULL, intent_sha256 TEXT NOT NULL, observation_sha256 TEXT);
"""


class PaperPortfolios:
    def __init__(self, path, *, campaigns=None, database=None, secret_check=lambda data: False):
        self.path = Path(path)
        if any(p.is_symlink() for p in (self.path, *self.path.parents)):
            raise ResearchError("Paper state cannot use symbolic links")
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        self.path.chmod(0o600)
        self.campaigns, self.database, self.secret_check = campaigns, database, secret_check
        with self.transaction() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in {0, 1}:
                raise ResearchError("Unsupported paper portfolio database version")
            import re

            expected = set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", SCHEMA))
            present = {
                r[0]
                for r in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if version == 1:
                if present != expected:
                    raise ResearchError("Paper journal schema is damaged; no state was recreated")
                rows = {
                    (r["id"], r["provider"], r["mode"])
                    for r in db.execute("SELECT * FROM paper_portfolios")
                }
                wanted = {(portfolio_id(p), p, "paper") for p in ("alpaca", "liquid")}
                if rows != wanted:
                    raise ResearchError(
                        "Paper portfolio identities are damaged; no state was recreated"
                    )
            else:
                if present:
                    raise ResearchError(
                        "Unversioned paper state exists; no implicit migration was applied"
                    )
                db.executescript(SCHEMA)
                for provider in ("alpaca", "liquid"):
                    db.execute(
                        "INSERT INTO paper_portfolios(id,provider,mode) VALUES (?,?,'paper')",
                        (portfolio_id(provider), provider),
                    )
                db.execute("PRAGMA user_version=1")

    def clean(self, value):
        data = json.dumps(value, sort_keys=True, allow_nan=False)
        if self.secret_check(data.encode()):
            raise ResearchError("Credential-bearing paper state was rejected")
        return data

    @contextmanager
    def transaction(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except sqlite3.Error:
            db.rollback()
            raise ResearchError(
                "Paper transaction failed; no partial change was committed"
            ) from None
        finally:
            db.close()

    @contextmanager
    def lease(self, provider):
        identity = portfolio_id(provider)
        fd = os.open(
            self.path.parent / (identity + ".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ResearchError("This paper account is active in another client") from None
            yield
        finally:
            os.close(fd)

    def portfolios(self):
        with self.transaction() as db:
            result = [
                dict(r) for r in db.execute("SELECT * FROM paper_portfolios ORDER BY provider")
            ]
        for row in result:
            row["agent_id"] = portfolio_agent_id(row["id"])
            row["execution_available"] = row["provider"] == "alpaca"
            row["blocker"] = LiquidPaper.blocker if row["provider"] == "liquid" else None
        return result

    def bind(self, provider, account_id):
        if provider != "alpaca":
            raise ResearchError(LiquidPaper.blocker)
        with self.transaction() as db:
            row = db.execute(
                "SELECT * FROM paper_portfolios WHERE id=?", (portfolio_id(provider),)
            ).fetchone()
            if row["account_id"] and row["account_id"] != account_id:
                raise ResearchError(
                    "Paper portfolio is bound to another account; rebinding is not implicit"
                )
            db.execute(
                "UPDATE paper_portfolios SET account_id=? WHERE id=?", (account_id, row["id"])
            )

    def prepare(self, campaign_id, strategy_id, provider, *, limits=None):
        if provider != "alpaca":
            raise ResearchError(LiquidPaper.blocker)
        account = next(p for p in self.portfolios() if p["provider"] == provider)
        if not account["account_id"]:
            raise ResearchError(
                "Connect the paper account before preparing its single final approval"
            )
        report = self.campaigns.report(campaign_id)
        selected = [
            r
            for r in report["results"]
            if (r["strategy_id"] == strategy_id or r["job_id"] == strategy_id)
            and r["state"] == "completed"
        ]
        if len(selected) != 1:
            raise ResearchError("Select one exact successfully tested candidate from this report")
        candidate = selected[0]
        strategy_id = candidate["strategy_id"]
        record = self.database.strategy(strategy_id)
        package = FinishedStrategy(
            package_id=uuid.uuid7().hex,
            strategy_id=strategy_id,
            strategy_sha256=record["spec_sha256"],
            campaign_id=campaign_id,
            report_sha256=payload_digest(report),
            candidate_job_id=candidate["job_id"],
            candidate_report_sha256=candidate["report_sha256"],
            research_metrics=candidate["report"].get("metrics", {}),
            attempted_candidates=len(report["results"]) + len(report.get("blocked", [])),
            request=YahooRequest.model_validate(candidate["report"]["request"]),
            provider=provider,
            portfolio_id=account["id"],
            account_id=account["account_id"],
            symbol=candidate["report"]["request"]["symbol"].replace("-", "/"),
            implementation_sha256=execution_digest(),
            limits=limits or PaperLimits(),
            limitations=(
                "Historical native results belong to the exact Yahoo fixed-quantity "
                "candidate, not Alpaca.",
                "The paper policy can reduce quantity and deny entries; its returns are "
                "not backtested returns.",
                "Long-only cash spot; no leverage, reinforcement or protective stop. "
                "Entire entry capital is risk.",
                "IOC limit orders can fill partially or not at all; exits never wait for an LLM.",
                "No independent holdout, profitability guarantee, live authority or "
                "Liquid perpetual equivalence.",
            ),
        )
        self.verify(package)
        with self.transaction() as db:
            db.execute(
                "INSERT INTO "
                "paper_packages(id,strategy_id,portfolio_id,digest,payload,state) "
                "VALUES (?,?,?,?,?,'pending')",
                (
                    package.package_id,
                    strategy_id,
                    package.portfolio_id,
                    package.digest,
                    self.clean(package.model_dump(mode="json")),
                ),
            )
        return package

    def package(self, identity):
        with self.transaction() as db:
            row = db.execute("SELECT * FROM paper_packages WHERE id=?", (identity,)).fetchone()
        if row is None:
            raise ResearchError("Unknown finished strategy package")
        package = FinishedStrategy.model_validate_json(row["payload"])
        if package.package_id != identity or package.digest != row["digest"]:
            raise ResearchError("Finished strategy package changed")
        return package

    def verify(self, package):
        if package.implementation_sha256 != execution_digest():
            raise ResearchError("Paper implementation changed; review a new finished package")
        if self.database.strategy(package.strategy_id)["spec_sha256"] != package.strategy_sha256:
            raise ResearchError("Strategy definition changed")
        report = self.campaigns.report(package.campaign_id)
        if payload_digest(report) != package.report_sha256:
            raise ResearchError("Finished report changed")
        found = next(
            (r for r in report["results"] if r["job_id"] == package.candidate_job_id), None
        )
        if (
            not found
            or found["state"] != "completed"
            or found["strategy_id"] != package.strategy_id
            or found["report_sha256"] != package.candidate_report_sha256
            or found["report"]["strategy_sha256"] != package.strategy_sha256
            or found["report"]["request"] != package.request.model_dump(mode="json")
            or found["report"].get("metrics", {}) != package.research_metrics
            or package.attempted_candidates
            != len(report["results"]) + len(report.get("blocked", []))
        ):
            raise ResearchError("Finished candidate evidence does not match the exact definition")

    def approve(self, identity, digest):
        package = self.package(identity)
        self.verify(package)
        with self.transaction() as db:
            row = db.execute("SELECT * FROM paper_packages WHERE id=?", (identity,)).fetchone()
            portfolio = db.execute(
                "SELECT * FROM paper_portfolios WHERE id=?", (package.portfolio_id,)
            ).fetchone()
            if row["state"] != "pending" or digest != package.digest:
                raise ResearchError("Approve the complete unchanged finished strategy once")
            if portfolio["account_id"] != package.account_id:
                raise ResearchError("The approved paper account binding changed")
            # One instrument owner avoids ambiguous attribution of netted provider positions.
            for other in db.execute(
                "SELECT payload FROM paper_packages WHERE portfolio_id=? AND state='approved'",
                (package.portfolio_id,),
            ):
                value = FinishedStrategy.model_validate_json(other["payload"])
                if value.symbol == package.symbol:
                    raise ResearchError(
                        "This paper instrument already has an approved strategy owner"
                    )
                if value.limits != package.limits:
                    raise ResearchError(
                        "All strategies in one wallet must use the same approved wallet limits"
                    )
            db.execute(
                "UPDATE paper_packages SET state='approved', approved_ns=? WHERE id=?",
                (time.time_ns(), identity),
            )
        return package

    def packages(self, provider=None, *, approved=False):
        with self.transaction() as db:
            rows = db.execute(
                "SELECT id,portfolio_id,state FROM paper_packages ORDER BY rowid"
            ).fetchall()
        return [
            self.package(r["id"])
            for r in rows
            if (not provider or r["portfolio_id"] == portfolio_id(provider))
            and (not approved or r["state"] == "approved")
        ]

    def receipt(self, signal_id, stage, payload):
        with self.transaction() as db:
            db.execute(
                "INSERT INTO paper_receipts(signal_id,stage,payload,created_ns) VALUES (?,?,?,?)",
                (signal_id, stage, self.clean(payload), time.time_ns()),
            )

    def halt(self, provider, reason):
        with self.transaction() as db:
            db.execute(
                "UPDATE paper_portfolios SET halted=? WHERE id=?", (reason, portfolio_id(provider))
            )


def package_markdown(package):
    return (
        "# Finished strategy: paper approval\n\n"
        f"Strategy `{package.strategy_id}` · package `{package.package_id}`\n\n"
        f"Portfolio **{package.provider} paper** · account `{package.account_id}` · "
        f"{package.symbol}\n\n"
        f"SMA {package.request.fast}/{package.request.slow}, {package.request.interval}; "
        f"maximum {package.request.quantity} units, binary entry review, portfolio "
        f"capital/size review. "
        "Opposite crossing exits the reconciled owned quantity without model approval.\n\n"
        f"Historical fixed-quantity evidence: `{package.candidate_job_id}`, digest "
        f"`{package.candidate_report_sha256}`; {package.attempted_candidates} total attempts. "
        f"Inspect all attempts with `/flow-show {package.campaign_id}`.\n\n"
        "```json\n" + json.dumps(package.research_metrics, indent=2) + "\n```\n\n"
        f"Policy `{package.policy}` · implementation `{package.implementation_sha256}`\n\n"
        "```json\n" + package.limits.model_dump_json(indent=2) + "\n```\n\n"
        "The single approval allows automatic paper orders within these limits while the "
        "runner is active. "
        "No approval is requested for each signal or research run.\n\n"
        + "\n".join("- " + s for s in package.limitations)
        + f"\n\nApprove: `/strategy-approve {package.package_id} {package.digest}`"
    )
