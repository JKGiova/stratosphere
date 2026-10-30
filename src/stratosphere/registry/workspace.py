"""Transactional local development registry; never an unauthenticated shared service."""

import hashlib
import json
import os
import secrets
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from stratosphere.phase0.contracts import StrategySpec, canonical_bytes, fingerprint
from stratosphere.research.store import ResearchError


class RegistryError(ResearchError):
    """A safe, deterministic registry failure."""


def now() -> str:
    return datetime.now(UTC).isoformat()


def encoded(value) -> str:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )


SCHEMA = """
CREATE TABLE IF NOT EXISTS retired_strategies (
 public_id TEXT PRIMARY KEY REFERENCES strategies(public_id), reason TEXT NOT NULL,
 retired_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS immutable_retirement BEFORE UPDATE ON retired_strategies
BEGIN SELECT RAISE(ABORT, 'Retirement receipts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS retain_retirement BEFORE DELETE ON retired_strategies
BEGIN SELECT RAISE(ABORT, 'Retirement receipts are retained evidence'); END;
CREATE TABLE IF NOT EXISTS strategies (
 id TEXT PRIMARY KEY, public_id TEXT NOT NULL UNIQUE CHECK(length(public_id)=24),
 spec_sha256 TEXT NOT NULL UNIQUE, spec_json TEXT NOT NULL, name TEXT NOT NULL,
 parent_id TEXT REFERENCES strategies(id), created_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS immutable_strategy BEFORE UPDATE ON strategies
BEGIN SELECT RAISE(ABORT, 'Strategy identities are immutable'); END;
CREATE TABLE IF NOT EXISTS strategy_lineage (
 parent_id TEXT NOT NULL REFERENCES strategies(id),
 child_id TEXT NOT NULL REFERENCES strategies(id),
 created_at TEXT NOT NULL, PRIMARY KEY(parent_id,child_id), CHECK(parent_id<>child_id)
);
CREATE INDEX IF NOT EXISTS lineage_child ON strategy_lineage(child_id);
CREATE TABLE IF NOT EXISTS sources (
 id TEXT PRIMARY KEY, source_sha256 TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS strategy_sources (
 strategy_id TEXT NOT NULL REFERENCES strategies(id),
 source_id TEXT NOT NULL REFERENCES sources(id),
 evidence_json TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(strategy_id,source_id)
);
CREATE TABLE IF NOT EXISTS submissions (
 id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL REFERENCES strategies(id),
 source_id TEXT NOT NULL REFERENCES sources(id), resolution TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS agents (
 strategy_id TEXT PRIMARY KEY REFERENCES strategies(id), agent_id TEXT NOT NULL UNIQUE,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS agent_turns (
 id INTEGER PRIMARY KEY, strategy_id TEXT NOT NULL REFERENCES agents(strategy_id),
 user_text TEXT NOT NULL, assistant_text TEXT NOT NULL, model_ref TEXT NOT NULL,
 created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS agent_turns_strategy ON agent_turns(strategy_id,id);
CREATE TABLE IF NOT EXISTS operations (
 id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL REFERENCES strategies(id),
 action TEXT NOT NULL, job_id TEXT, status TEXT NOT NULL, detail TEXT NOT NULL,
 created_at TEXT NOT NULL,
 finished_at TEXT
);
CREATE INDEX IF NOT EXISTS operations_strategy ON operations(strategy_id,created_at);
CREATE TABLE IF NOT EXISTS research_jobs (
 job_id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL REFERENCES strategies(id),
 plan_sha256 TEXT NOT NULL, state TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_strategy ON research_jobs(strategy_id);
CREATE TABLE IF NOT EXISTS reports (
 id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL REFERENCES strategies(id), kind TEXT NOT NULL,
 job_id TEXT NOT NULL UNIQUE, report_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reports_strategy ON reports(strategy_id,created_at);
CREATE TABLE IF NOT EXISTS source_reviews (
 id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL REFERENCES strategies(id),
 payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL, reviewed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS source_reviews_strategy ON source_reviews(strategy_id,reviewed_at);
CREATE TRIGGER IF NOT EXISTS immutable_source_review BEFORE UPDATE ON source_reviews
BEGIN SELECT RAISE(ABORT, 'Source reviews are immutable'); END;
CREATE TRIGGER IF NOT EXISTS retain_source_review BEFORE DELETE ON source_reviews
BEGIN SELECT RAISE(ABORT, 'Source reviews are retained evidence'); END;
CREATE TABLE IF NOT EXISTS source_registrations (
 id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL REFERENCES strategies(id),
 payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS source_registrations_strategy ON source_registrations(strategy_id);
CREATE TRIGGER IF NOT EXISTS immutable_source_registration BEFORE UPDATE ON source_registrations
BEGIN SELECT RAISE(ABORT, 'Source registrations are immutable receipts'); END;
CREATE TRIGGER IF NOT EXISTS retain_source_registration BEFORE DELETE ON source_registrations
BEGIN SELECT RAISE(ABORT, 'Source registrations are retained receipts'); END;
CREATE TABLE IF NOT EXISTS report_analyses (
 id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL REFERENCES strategies(id),
 job_id TEXT NOT NULL REFERENCES research_jobs(job_id), payload TEXT NOT NULL,
 payload_sha256 TEXT NOT NULL, analyzed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS report_analyses_owner ON report_analyses(strategy_id,analyzed_at);
CREATE TRIGGER IF NOT EXISTS immutable_report_analysis BEFORE UPDATE ON report_analyses
BEGIN SELECT RAISE(ABORT, 'Report analyses are immutable'); END;
CREATE TRIGGER IF NOT EXISTS retain_report_analysis BEFORE DELETE ON report_analyses
BEGIN SELECT RAISE(ABORT, 'Report analyses are retained evidence'); END;
CREATE TABLE IF NOT EXISTS assessments (
 id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL REFERENCES strategies(id),
 payload TEXT NOT NULL, assessed_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS immutable_assessment BEFORE UPDATE ON assessments
BEGIN SELECT RAISE(ABORT, 'Assessments are immutable snapshots'); END;
CREATE TRIGGER IF NOT EXISTS retain_assessment BEFORE DELETE ON assessments
BEGIN SELECT RAISE(ABORT, 'Assessments are retained evidence'); END;
CREATE TABLE IF NOT EXISTS dataset_exposures (
 url TEXT NOT NULL, study_id TEXT NOT NULL, role TEXT NOT NULL,
 archive_sha256 TEXT NOT NULL, claimed_at TEXT NOT NULL,
 PRIMARY KEY(url,study_id,role), CHECK(role IN ('development','holdout'))
);
"""


class WorkspaceDatabase:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if path.is_symlink():
            raise RegistryError("The workspace database cannot be a symbolic link.")
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        os.close(fd)
        path.chmod(0o600)
        with self.connect() as db:
            db.executescript(SCHEMA)
            db.execute("PRAGMA user_version=1")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        except sqlite3.Error:
            raise RegistryError(
                "Workspace transaction failed; no partial record was committed."
            ) from None
        finally:
            db.close()

    def register(
        self,
        spec: StrategySpec,
        name: str,
        source: dict,
        evidence=(),
        parent=None,
        *,
        comparison=False,
    ) -> dict:
        """Exact identity reuse, atomic public-ID retry, source accumulation and one agent."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            return self._register(db, spec, name, source, evidence, parent, comparison=comparison)

    def register_created_job(self, spec, name, source, job_id, plan_sha256):
        """Atomically bind an explicit idea, stable agent and awaiting-approval job."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM research_jobs WHERE job_id=?", (job_id,)).fetchone()
            if old:
                row = db.execute(
                    "SELECT * FROM strategies WHERE id=?", (old["strategy_id"],)
                ).fetchone()
                if old["plan_sha256"] != plan_sha256 or row["spec_sha256"] != fingerprint(spec):
                    raise RegistryError("The created job is already bound to a different plan.")
                return self._public(db, row) | {"resolution": "retained_creation"}
            record = self._register(db, spec, name, source)
            row = self._strategy(db, record["public_id"])
            db.execute(
                "INSERT INTO research_jobs VALUES (?,?,?,?,?)",
                (job_id, row["id"], plan_sha256, "awaiting_approval", now()),
            )
            return record

    def _register(self, db, spec, name, source, evidence=(), parent=None, *, comparison=False):
        """The transaction owner commits identity, provenance, lineage and any receipt together."""
        digest, canonical = fingerprint(spec), canonical_bytes(spec).decode()
        if not name.strip():
            raise RegistryError("A descriptive strategy name is required.")
        at = now()
        row = db.execute("SELECT * FROM strategies WHERE spec_sha256=?", (digest,)).fetchone()
        if row and row["spec_json"] != canonical:
            raise RegistryError(
                "Behavior digest collision; automatic identity merging was blocked."
            )
        parent_row = self._strategy(db, parent) if parent else None
        if comparison and row and parent_row:
            # Reusing known behavior is a comparison, not a new derivation of an ancestor.
            source = {**source, "comparison_parent_strategy_id": parent}
            parent_row = None
        source_json = encoded(source)
        source_hash = hashlib.sha256(source_json.encode()).hexdigest()
        if row and parent_row and row["id"] == parent_row["id"]:
            raise RegistryError("A variant must change behavior, not only its name or source.")
        if (
            row
            and parent_row
            and db.execute(
                "WITH RECURSIVE descendants(id) AS ("
                "SELECT child_id FROM strategy_lineage WHERE parent_id=? UNION "
                "SELECT l.child_id FROM strategy_lineage l JOIN descendants d ON l.parent_id=d.id) "
                "SELECT 1 FROM descendants WHERE id=?",
                (row["id"], parent_row["id"]),
            ).fetchone()
        ):
            raise RegistryError("A variant lineage cannot create a cycle.")
        resolution = "exact_duplicate" if row else "variant" if parent else "new_strategy"
        if not row:
            internal = uuid.uuid7().hex
            for _ in range(100):
                public = "".join(str(secrets.randbelow(10)) for _ in range(24))
                try:
                    db.execute(
                        "INSERT INTO strategies VALUES (?,?,?,?,?,?,?)",
                        (
                            internal,
                            public,
                            digest,
                            canonical,
                            name,
                            parent_row["id"] if parent_row else None,
                            at,
                        ),
                    )
                    break
                except sqlite3.IntegrityError:
                    if not db.execute(
                        "SELECT 1 FROM strategies WHERE public_id=?", (public,)
                    ).fetchone():
                        raise
            else:
                raise RegistryError("Public identity allocation could not complete; retry later.")
            row = self._strategy(db, public)
            db.execute("INSERT INTO agents VALUES (?,?,?)", (internal, uuid.uuid7().hex, at))
        if parent_row:
            db.execute(
                "INSERT OR IGNORE INTO strategy_lineage VALUES (?,?,?)",
                (parent_row["id"], row["id"], at),
            )
        source_row = db.execute(
            "SELECT * FROM sources WHERE source_sha256=?", (source_hash,)
        ).fetchone()
        if source_row and source_row["payload"] != source_json:
            raise RegistryError("Source digest collision; the snapshot was not overwritten.")
        source_id = source_row["id"] if source_row else uuid.uuid7().hex
        if not source_row:
            db.execute(
                "INSERT INTO sources VALUES (?,?,?,?)",
                (source_id, source_hash, source_json, at),
            )
        db.execute(
            "INSERT OR IGNORE INTO strategy_sources VALUES (?,?,?,?)",
            (row["id"], source_id, encoded(list(evidence)), at),
        )
        db.execute(
            "INSERT INTO submissions VALUES (?,?,?,?,?)",
            (uuid.uuid7().hex, row["id"], source_id, resolution, at),
        )
        return self._public(db, row) | {"resolution": resolution}

    def register_source(self, identity, request, proof, requested_at):
        """One atomic, idempotent binding of native source, strategy, agent and receipt."""
        from stratosphere.sources.registration import (
            SourceRegistration,
            native_definition,
            source_payload,
        )

        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute(
                "SELECT * FROM source_registrations WHERE id=?", (identity,)
            ).fetchone()
            if old:
                value = self._source_registration(db, old)
                if (
                    value.request != request
                    or value.proof != proof
                    or value.requested_at != requested_at
                ):
                    raise RegistryError("A registration identity cannot bind different inputs.")
                return value
            spec = native_definition(proof.snapshot)
            origin = source_payload(identity, request, proof)
            record = self._register(
                db,
                spec,
                request.name or proof.snapshot.title or "Imported SMA definition",
                origin,
                [{"kind": "whole_native_definition", "piece_sha256": proof.piece_sha256}],
                request.parent_strategy_id,
            )
            value = SourceRegistration(
                id=identity,
                request=request,
                requested_at=requested_at,
                recorded_at=now(),
                proof=proof,
                strategy_id=record["public_id"],
                agent_id=record["agent_id"],
                registered_name=record["name"],
                resolution=record["resolution"],
                source_payload_sha256=hashlib.sha256(encoded(origin).encode()).hexdigest(),
            )
            strategy = self._strategy(db, record["public_id"])
            db.execute(
                "INSERT INTO source_registrations VALUES (?,?,?,?,?)",
                (
                    identity,
                    strategy["id"],
                    value.model_dump_json(),
                    value.digest,
                    value.recorded_at.isoformat(),
                ),
            )
            return value

    def _source_registration(self, db, row):
        from stratosphere.sources.registration import SourceRegistration, source_payload

        value = SourceRegistration.model_validate_json(row["payload"])
        strategy = db.execute(
            "SELECT * FROM strategies WHERE id=?", (row["strategy_id"],)
        ).fetchone()
        record = self._public(db, strategy)
        source = db.execute(
            "SELECT s.* FROM sources s JOIN strategy_sources l ON l.source_id=s.id "
            "WHERE l.strategy_id=? AND s.source_sha256=?",
            (row["strategy_id"], value.source_payload_sha256),
        ).fetchone()
        origin = source_payload(value.id, value.request, value.proof)
        if (
            value.id != row["id"]
            or value.digest != row["payload_sha256"]
            or value.recorded_at.isoformat() != row["created_at"]
            or value.strategy_id != record["public_id"]
            or value.agent_id != record["agent_id"]
            or value.proof.strategy_sha256 != record["spec_sha256"]
            or source is None
            or source["payload"] != encoded(origin)
            or hashlib.sha256(source["payload"].encode()).hexdigest() != value.source_payload_sha256
        ):
            raise RegistryError("Source registration receipt failed its integrity/owner check.")
        count = db.execute(
            "SELECT COUNT(*) FROM submissions WHERE strategy_id=? AND source_id=?",
            (row["strategy_id"], source["id"]),
        ).fetchone()[0]
        if count != 1:
            raise RegistryError("Source registration has ambiguous submission delivery.")
        if (
            value.request.parent_strategy_id
            and value.request.parent_strategy_id not in record["parent_public_ids"]
        ):
            raise RegistryError("Source registration parent binding is missing.")
        return value

    def source_registration(self, identity):
        try:
            with self.connect() as db:
                row = db.execute(
                    "SELECT * FROM source_registrations WHERE id=?", (identity,)
                ).fetchone()
                if row is None:
                    raise RegistryError("No committed source registration receipt for this ID.")
                return self._source_registration(db, row)
        except ValueError, KeyError, TypeError:
            raise RegistryError("Source registration receipt failed its integrity check.") from None

    def source_registration_ids(self):
        with self.connect() as db:
            return [
                r["id"]
                for r in db.execute("SELECT id FROM source_registrations ORDER BY created_at,id")
            ]

    @staticmethod
    def _strategy(db, public_id):
        row = db.execute("SELECT * FROM strategies WHERE public_id=?", (public_id,)).fetchone()
        if not row:
            raise RegistryError("Select an existing full strategy public ID from Library.")
        return row

    @staticmethod
    def _public(db, row):
        specification = StrategySpec.model_validate_json(row["spec_json"])
        if fingerprint(specification) != row["spec_sha256"]:
            raise RegistryError("Stored strategy identity failed its integrity check.")
        agent = db.execute(
            "SELECT agent_id FROM agents WHERE strategy_id=?", (row["id"],)
        ).fetchone()
        parent = db.execute(
            "SELECT public_id FROM strategies WHERE id=?", (row["parent_id"],)
        ).fetchone()
        return {
            "public_id": row["public_id"],
            "name": row["name"],
            "spec_sha256": row["spec_sha256"],
            "specification": specification.model_dump(mode="json"),
            "parent_public_ids": [
                p["public_id"]
                for p in db.execute(
                    "SELECT s.public_id FROM strategies s JOIN strategy_lineage l "
                    "ON l.parent_id=s.id "
                    "WHERE l.child_id=? ORDER BY s.public_id",
                    (row["id"],),
                )
            ],
            "parent_public_id": parent["public_id"] if parent else None,
            "agent_id": agent["agent_id"],
            "created_at": row["created_at"],
            "validation": "registration_does_not_imply_validation",
        }

    def strategy(self, public_id: str) -> dict:
        with self.connect() as db:
            return self._public(db, self._strategy(db, public_id))

    def retire(self, public_id: str, reason: str) -> None:
        """Remove active routing while retaining the exact identity, agent and all evidence."""
        if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 1000:
            raise RegistryError("An explicit retirement reason is required")
        with self.connect() as db:
            self._strategy(db, public_id)
            db.execute(
                "INSERT OR IGNORE INTO retired_strategies VALUES (?,?,?)",
                (public_id, reason, now()),
            )

    def is_retired(self, public_id: str) -> bool:
        with self.connect() as db:
            return bool(
                db.execute(
                    "SELECT 1 FROM retired_strategies WHERE public_id=?", (public_id,)
                ).fetchone()
            )

    def catalog(self, *, include_retired=False) -> list[dict]:
        with self.connect() as db:
            return [
                self._public(db, row)
                for row in db.execute(
                    "SELECT * FROM strategies WHERE ? OR public_id NOT IN "
                    "(SELECT public_id FROM retired_strategies) ORDER BY created_at,public_id",
                    (include_retired,),
                ).fetchall()
            ]

    def attach_job(self, public_id: str, job_id: str, plan_sha256: str, state: str) -> None:
        with self.connect() as db:
            row = self._strategy(db, public_id)
            old = db.execute("SELECT * FROM research_jobs WHERE job_id=?", (job_id,)).fetchone()
            if old and (old["strategy_id"] != row["id"] or old["plan_sha256"] != plan_sha256):
                raise RegistryError(
                    "A research job cannot be reassigned to another strategy or plan."
                )
            db.execute(
                "INSERT INTO research_jobs VALUES (?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET "
                "state=excluded.state,updated_at=excluded.updated_at",
                (job_id, row["id"], plan_sha256, state, now()),
            )

    def strategy_for_job(self, job_id: str) -> dict:
        with self.connect() as db:
            row = db.execute(
                "SELECT s.* FROM strategies s JOIN research_jobs j "
                "ON j.strategy_id=s.id WHERE job_id=?",
                (job_id,),
            ).fetchone()
            if not row:
                raise RegistryError("That job has no registered strategy agent.")
            return self._public(db, row)

    def jobs(self, public_id: str) -> list[dict]:
        with self.connect() as db:
            row = self._strategy(db, public_id)
            return [
                dict(j)
                for j in db.execute(
                    "SELECT job_id,plan_sha256,state,updated_at FROM research_jobs "
                    "WHERE strategy_id=? ORDER BY updated_at",
                    (row["id"],),
                )
            ]

    def record_report(self, job_id: str, report: dict, *, kind="research") -> str:
        """Call only after the research service verifies its immutable evidence seal."""
        with self.connect() as db:
            job = db.execute("SELECT * FROM research_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not job or report["plan_sha256"] != job["plan_sha256"]:
                raise RegistryError(
                    "Report plan identity does not match the attached strategy job."
                )
            strategy = db.execute(
                "SELECT * FROM strategies WHERE id=?", (job["strategy_id"],)
            ).fetchone()
            if report["strategy_sha256"] != strategy["spec_sha256"]:
                raise RegistryError("Report behavior differs from its strategy agent.")
            payload = encoded(report)
            digest = hashlib.sha256(payload.encode()).hexdigest()
            old = db.execute("SELECT * FROM reports WHERE job_id=?", (job_id,)).fetchone()
            if old and (old["id"] != digest or old["report_json"] != payload):
                raise RegistryError("A retained report cannot be replaced.")
            db.execute(
                "INSERT OR IGNORE INTO reports VALUES (?,?,?,?,?,?)",
                (digest, job["strategy_id"], kind, job_id, payload, now()),
            )
            return digest

    def reports(self, public_id: str | None = None) -> list[dict]:
        with self.connect() as db:
            params = ()
            clause = ""
            if public_id is not None:
                clause, params = " WHERE s.id=?", (self._strategy(db, public_id)["id"],)
            rows = db.execute(
                "SELECT r.id,r.job_id,r.kind,r.report_json,r.created_at,s.public_id,s.name "
                "FROM reports r JOIN strategies s ON s.id=r.strategy_id"
                + clause
                + " ORDER BY r.created_at",
                params,
            ).fetchall()
            result = []
            for row in rows:
                if hashlib.sha256(row["report_json"].encode()).hexdigest() != row["id"]:
                    raise RegistryError("Retained report metadata failed its integrity check.")
                result.append({**dict(row), "report": json.loads(row["report_json"])})
            return result

    def sources(self, public_id: str) -> list[dict]:
        with self.connect() as db:
            strategy = self._strategy(db, public_id)
            rows = db.execute(
                "SELECT s.payload,s.source_sha256,l.evidence_json FROM sources s "
                "JOIN strategy_sources l ON l.source_id=s.id WHERE l.strategy_id=?",
                (strategy["id"],),
            ).fetchall()
            result = []
            for row in rows:
                if hashlib.sha256(row["payload"].encode()).hexdigest() != row["source_sha256"]:
                    raise RegistryError("Retained source failed its integrity check.")
                result.append(
                    {
                        "source": json.loads(row["payload"]),
                        "evidence": json.loads(row["evidence_json"]),
                        "source_sha256": row["source_sha256"],
                    }
                )
            return result

    def history(self, public_id: str) -> list[dict[str, str]]:
        with self.connect() as db:
            row = self._strategy(db, public_id)
            result = []
            for turn in db.execute(
                "SELECT user_text,assistant_text FROM agent_turns WHERE strategy_id=? ORDER BY id",
                (row["id"],),
            ):
                result += [
                    {"role": "user", "content": turn["user_text"]},
                    {"role": "assistant", "content": turn["assistant_text"]},
                ]
            return result

    def append_turn(self, public_id: str, user: str, reply: str, model: str) -> None:
        with self.connect() as db:
            row = self._strategy(db, public_id)
            db.execute(
                "INSERT INTO agent_turns "
                "(strategy_id,user_text,assistant_text,model_ref,created_at) VALUES (?,?,?,?,?)",
                (row["id"], user, reply, model, now()),
            )

    def clear_history(self, public_id: str) -> None:
        with self.connect() as db:
            row = self._strategy(db, public_id)
            db.execute("DELETE FROM agent_turns WHERE strategy_id=?", (row["id"],))

    def record_source_review(self, public_id: str, review: dict):
        from stratosphere.sources.review import SourceReview

        value = SourceReview.model_validate(review)
        payload = encoded(value.model_dump(mode="json"))
        with self.connect() as db:
            strategy = self._strategy(db, public_id)
            if value.strategy_id != public_id or value.strategy_sha256 != strategy["spec_sha256"]:
                raise RegistryError("A source review must belong to its exact strategy identity.")
            old = db.execute("SELECT * FROM source_reviews WHERE id=?", (value.id,)).fetchone()
            if old:
                if old["strategy_id"] != strategy["id"] or old["payload"] != payload:
                    raise RegistryError("An immutable source review cannot be replaced.")
                return
            db.execute(
                "INSERT INTO source_reviews VALUES (?,?,?,?,?)",
                (
                    value.id,
                    strategy["id"],
                    payload,
                    hashlib.sha256(payload.encode()).hexdigest(),
                    value.reviewed_at.isoformat(),
                ),
            )

    @staticmethod
    def _source_review(row):
        from stratosphere.sources.review import SourceReview

        if hashlib.sha256(row["payload"].encode()).hexdigest() != row["payload_sha256"]:
            raise RegistryError("Retained source review failed its integrity check.")
        value = SourceReview.model_validate_json(row["payload"])
        if (
            value.id != row["id"]
            or value.reviewed_at.isoformat() != row["reviewed_at"]
            or value.strategy_id != row["public_id"]
            or value.strategy_sha256 != row["spec_sha256"]
        ):
            raise RegistryError("Retained source review has an invalid strategy owner.")
        return {
            "id": value.id,
            "name": row["name"],
            "strategy_id": value.strategy_id,
            "reviewed_at": value.reviewed_at.isoformat(),
            "review": value.model_dump(mode="json"),
        }

    def source_reviews(self, public_id=None):
        with self.connect() as db:
            if public_id:
                self._strategy(db, public_id)
            rows = db.execute(
                "SELECT r.*,s.public_id,s.spec_sha256,s.name FROM source_reviews r "
                "JOIN strategies s ON s.id=r.strategy_id "
                + ("WHERE s.public_id=? " if public_id else "")
                + "ORDER BY r.reviewed_at,r.id",
                (public_id,) if public_id else (),
            )
            return [self._source_review(row) for row in rows]

    def source_review(self, identity, *, strategy_id=None):
        with self.connect() as db:
            row = db.execute(
                "SELECT r.*,s.public_id,s.spec_sha256,s.name FROM source_reviews r "
                "JOIN strategies s ON s.id=r.strategy_id WHERE r.id=?",
                (identity,),
            ).fetchone()
            if row is None or (strategy_id and row["public_id"] != strategy_id):
                raise RegistryError("That source review is unavailable for this strategy.")
            return self._source_review(row)

    def record_report_analysis(self, analysis):
        from stratosphere.report_analysis import ReportAnalysis

        value = ReportAnalysis.model_validate(analysis)
        evidence = value.evidence
        payload = encoded(value.model_dump(mode="json"))
        with self.connect() as db:
            strategy = self._strategy(db, evidence.strategy_id)
            agent = db.execute(
                "SELECT agent_id FROM agents WHERE strategy_id=?", (strategy["id"],)
            ).fetchone()
            job = db.execute(
                "SELECT * FROM research_jobs WHERE job_id=?", (evidence.job_id,)
            ).fetchone()
            if (
                evidence.strategy_sha256 != strategy["spec_sha256"]
                or agent["agent_id"] != evidence.agent_id
                or job is None
                or job["strategy_id"] != strategy["id"]
                or job["plan_sha256"] != evidence.plan_sha256
            ):
                raise RegistryError("A report analysis must bind its exact strategy, agent and job")
            old = db.execute(
                "SELECT payload FROM report_analyses WHERE id=?", (value.id,)
            ).fetchone()
            if old:
                if old["payload"] != payload:
                    raise RegistryError("An immutable report analysis cannot be replaced")
                return
            db.execute(
                "INSERT INTO report_analyses VALUES (?,?,?,?,?,?)",
                (
                    value.id,
                    strategy["id"],
                    evidence.job_id,
                    payload,
                    hashlib.sha256(payload.encode()).hexdigest(),
                    value.analyzed_at.isoformat(),
                ),
            )

    @staticmethod
    def _report_analysis(row):
        from stratosphere.report_analysis import ReportAnalysis

        if hashlib.sha256(row["payload"].encode()).hexdigest() != row["payload_sha256"]:
            raise RegistryError("Report analysis integrity check failed")
        value = ReportAnalysis.model_validate_json(row["payload"])
        evidence = value.evidence
        if (
            value.id != row["id"]
            or evidence.strategy_id != row["public_id"]
            or evidence.strategy_sha256 != row["spec_sha256"]
            or evidence.agent_id != row["agent_id"]
            or evidence.job_id != row["job_id"]
            or evidence.plan_sha256 != row["plan_sha256"]
            or value.analyzed_at.isoformat() != row["analyzed_at"]
        ):
            raise RegistryError("Report analysis owner/agent/job binding failed")
        return {
            "id": value.id,
            "strategy_id": evidence.strategy_id,
            "name": row["name"],
            "analyzed_at": value.analyzed_at.isoformat(),
            "analysis": value.model_dump(mode="json"),
        }

    def report_analyses(self, public_id=None):
        with self.connect() as db:
            if public_id:
                self._strategy(db, public_id)
            rows = db.execute(
                "SELECT r.*,s.public_id,s.spec_sha256,s.name,a.agent_id,j.plan_sha256 "
                "FROM report_analyses r JOIN strategies s ON s.id=r.strategy_id "
                "JOIN agents a ON a.strategy_id=s.id "
                "LEFT JOIN research_jobs j ON j.job_id=r.job_id AND j.strategy_id=s.id "
                + ("WHERE s.public_id=? " if public_id else "")
                + "ORDER BY r.analyzed_at,r.id",
                (public_id,) if public_id else (),
            ).fetchall()
            return [self._report_analysis(row) for row in rows]

    def report_analysis(self, identity, *, strategy_id=None):
        with self.connect() as db:
            row = db.execute(
                "SELECT r.*,s.public_id,s.spec_sha256,s.name,a.agent_id,j.plan_sha256 "
                "FROM report_analyses r JOIN strategies s ON s.id=r.strategy_id "
                "JOIN agents a ON a.strategy_id=s.id "
                "LEFT JOIN research_jobs j ON j.job_id=r.job_id AND j.strategy_id=s.id "
                "WHERE r.id=? " + ("AND s.public_id=?" if strategy_id else ""),
                (identity, strategy_id) if strategy_id else (identity,),
            ).fetchone()
            if row is None:
                raise RegistryError("That report analysis is unavailable for this strategy")
            return self._report_analysis(row)

    def operation(self, public_id: str, action: str, *, job_id=None) -> str:
        with self.connect() as db:
            row = self._strategy(db, public_id)
            operation_id = uuid.uuid7().hex
            db.execute(
                "INSERT INTO operations VALUES (?,?,?,?,?,?,?,?)",
                (operation_id, row["id"], action, job_id, "running", "", now(), None),
            )
            return operation_id

    def finish_operation(
        self, operation_id: str, state: str, detail="", *, allow_recovery=False
    ) -> None:
        if state not in {"completed", "failed", "canceled", "awaiting_approval"}:
            raise RegistryError("Unsupported operation outcome.")
        with self.connect() as db:
            cursor = db.execute(
                "UPDATE operations SET status=?,detail=?,finished_at=? "
                "WHERE id=? AND status='running'",
                (state, detail, now(), operation_id),
            )
            if cursor.rowcount != 1:
                if allow_recovery:
                    row = db.execute(
                        "SELECT status FROM operations WHERE id=?", (operation_id,)
                    ).fetchone()
                    if row and row["status"] == state:
                        return
                raise RegistryError("Operation already finished or unavailable.")

    def claim_dataset(self, url: str, digest: str, study_id: str, role: str) -> None:
        """Claim before download; failed/canceled acquisition conservatively consumes exposure."""
        if role not in {"development", "holdout"}:
            raise RegistryError("Unsupported dataset exposure role.")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute("SELECT * FROM dataset_exposures WHERE url=?", (url,)).fetchall()
            for row in rows:
                if (row["study_id"], row["role"], row["archive_sha256"]) == (
                    study_id,
                    role,
                    digest,
                ):
                    continue
                if role == "holdout" or row["role"] == "holdout":
                    raise RegistryError(
                        "This monthly archive has prior local exposure. It cannot be reused "
                        "as a final holdout or optimized after holdout consumption. "
                        "Review a protocol with fresh chronological windows."
                    )
            db.execute(
                "INSERT OR IGNORE INTO dataset_exposures VALUES (?,?,?,?,?)",
                (url, study_id, role, digest, now()),
            )

    def record_assessment(self, public_id: str, assessment: dict) -> str:
        payload = encoded(assessment)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        with self.connect() as db:
            strategy = self._strategy(db, public_id)
            if (
                assessment.get("strategy_id") != public_id
                or assessment.get("strategy_sha256") != strategy["spec_sha256"]
            ):
                raise RegistryError("An assessment cannot be assigned to another strategy")
            old = db.execute("SELECT payload FROM assessments WHERE id=?", (digest,)).fetchone()
            if old and old["payload"] != payload:
                raise RegistryError("Assessment digest collision; no evidence was replaced")
            db.execute(
                "INSERT OR IGNORE INTO assessments VALUES (?,?,?,?)",
                (digest, strategy["id"], payload, assessment["assessed_at"]),
            )
            return digest

    def assessment(self, digest: str) -> dict:
        with self.connect() as db:
            row = db.execute("SELECT * FROM assessments WHERE id=?", (digest,)).fetchone()
            if not row or hashlib.sha256(row["payload"].encode()).hexdigest() != digest:
                raise RegistryError("The retained assessment is unavailable or damaged")
            payload = json.loads(row["payload"])
            strategy = self._strategy(db, payload["strategy_id"])
            if (
                strategy["id"] != row["strategy_id"]
                or payload["strategy_sha256"] != strategy["spec_sha256"]
                or payload["assessed_at"] != row["assessed_at"]
            ):
                raise RegistryError("The retained assessment identity or timestamp is damaged")
            return {
                "id": digest,
                "assessment": payload,
                "assessed_at": row["assessed_at"],
            }

    def assessments(self, public_id: str | None = None) -> list[dict]:
        with self.connect() as db:
            params = ()
            clause = ""
            if public_id:
                clause, params = " WHERE strategy_id=?", (self._strategy(db, public_id)["id"],)
            rows = db.execute(
                "SELECT id FROM assessments" + clause + " ORDER BY assessed_at,id", params
            ).fetchall()
        return [self.assessment(row["id"]) for row in rows]
