"""Private immutable local research artifacts; not the shared strategy registry."""

import fcntl
import hashlib
import json
import os
import re
import sys
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from stratosphere.research.contracts import ResearchPlan

SEALED_ARTIFACTS = (
    "source.json",
    "draft.json",
    "plan.json",
    "implementation.json",
    "approval.json",
    "archive.zip",
    "request.json",
    "base.json",
    "stress.json",
    "report.json",
    "report.md",
)


class ResearchError(Exception):
    """Safe operation error without credentials, provider bodies or arbitrary code output."""


def default_research_root() -> Path:
    if value := os.getenv("STRATOSPHERE_RESEARCH_DIR"):
        return Path(value).expanduser()
    if sys.platform == "darwin":
        return Path.home() / "Library/Application Support/stratosphere/research"
    return (
        Path(os.getenv("XDG_STATE_HOME", str(Path.home() / ".local/state")))
        / "stratosphere/research"
    )


class ResearchStore:
    def __init__(self, root: Path):
        self.root = root

    def directory(self, job_id: str) -> Path:
        if not re.fullmatch(r"[a-f0-9]{32}", job_id):
            raise ResearchError("Use the complete research job ID shown in Tests.")
        return self.root / job_id

    def create(self, source: str) -> str:
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        job_id = uuid.uuid7().hex
        self.directory(job_id).mkdir(mode=0o700)
        self.write(job_id, "source.json", {"kind": "pasted_text", "text": source})
        self.event(job_id, "extracting")
        return job_id

    def write(self, job_id: str, name: str, payload) -> None:
        if not re.fullmatch(r"[a-z_]+\.(json|md|zip)", name):
            raise ResearchError("Invalid artifact name")
        data = (
            payload
            if isinstance(payload, bytes)
            else (
                payload.encode()
                if isinstance(payload, str)
                else (
                    json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
                ).encode()
            )
        )
        path = self.directory(job_id) / name
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())

    def read(self, job_id: str, name: str):
        path = self.directory(job_id) / name
        if not path.is_file():
            raise ResearchError("The requested research artifact is unavailable.")
        return json.loads(path.read_text())

    def event(self, job_id: str, state: str, detail: str = "") -> None:
        payload = {"at": datetime.now(UTC).isoformat(), "state": state, "detail": detail}
        descriptor = os.open(
            self.directory(job_id) / "events.jsonl", os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600
        )
        with os.fdopen(descriptor, "a") as file:
            file.write(json.dumps(payload) + "\n")
            file.flush()
            os.fsync(file.fileno())

    def state(self, job_id: str) -> str:
        lines = (self.directory(job_id) / "events.jsonl").read_text().splitlines()
        for line in reversed(lines):
            try:
                return json.loads(line)["state"]
            except ValueError, KeyError:
                continue
        raise ResearchError("Research state is damaged; retained artifacts were not changed.")

    def jobs(self) -> list[str]:
        if not self.root.exists():
            return []
        return sorted(
            path.name
            for path in self.root.iterdir()
            if path.is_dir() and re.fullmatch(r"[a-f0-9]{32}", path.name)
        )

    @contextmanager
    def lock(self, job_id: str):
        descriptor = os.open(self.directory(job_id) / "active.lock", os.O_RDWR | os.O_CREAT, 0o600)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ResearchError("This job is already running in another client.") from None
            yield
        finally:
            os.close(descriptor)

    def seal(self, job_id: str) -> None:
        directory = self.directory(job_id)
        manifest = {
            name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
            for name in SEALED_ARTIFACTS
        }
        if (directory / "artifact_manifest.json").exists():
            self.verify_seal(job_id)
        else:
            self.write(job_id, "artifact_manifest.json", manifest)

    def verify_seal(self, job_id: str) -> None:
        manifest = self.read(job_id, "artifact_manifest.json")
        if set(manifest) != set(SEALED_ARTIFACTS):
            raise ResearchError("Research artifact manifest is incomplete.")
        directory = self.directory(job_id)
        for name in SEALED_ARTIFACTS:
            path = directory / name
            if (
                not path.is_file()
                or hashlib.sha256(path.read_bytes()).hexdigest() != manifest[name]
            ):
                raise ResearchError("Research artifact integrity check failed.")

    def plan(self, job_id: str) -> ResearchPlan:

        plan = ResearchPlan.model_validate(self.read(job_id, "plan.json"))
        source = self.read(job_id, "source.json")["text"]
        if hashlib.sha256(source.encode()).hexdigest() != plan.source_sha256:
            raise ResearchError("Source integrity check failed; no operation was executed.")
        plan.draft.validate_source(source)
        return plan
