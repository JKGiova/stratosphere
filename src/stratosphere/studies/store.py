"""Exclusive private study artifacts and per-attempt checkpoints."""

import hashlib
import json
import os
import re
import uuid

from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.studies.contracts import RegistryStudyProtocol, StudyProtocol, load_study_protocol

MANDATORY = {
    "source.json",
    "protocol.json",
    "implementation.json",
    "approval.json",
    "development.zip",
    "development.json",
    "selection.json",
    "report.json",
    "report.md",
}


class StudyStore(ResearchStore):
    def create(self, source: str) -> str:
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        study_id = uuid.uuid7().hex
        self.directory(study_id).mkdir(mode=0o700)
        self.write(study_id, "source.json", {"kind": "study_proposal", "text": source})
        self.event(study_id, "preparing")
        return study_id

    def write(self, job_id: str, name: str, payload) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]*\.(json|md|zip)", name):
            raise ResearchError("Invalid study artifact name")
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
        descriptor = os.open(
            self.directory(job_id) / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        with os.fdopen(descriptor, "wb") as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        checksum = self.directory(job_id) / (name + ".sha256")
        fd = os.open(checksum, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as file:
            file.write(hashlib.sha256(data).hexdigest())
            file.flush()
            os.fsync(file.fileno())

    def read(self, job_id: str, name: str):
        if not re.fullmatch(r"[a-z][a-z0-9_]*\.json", name):
            raise ResearchError("Invalid study JSON artifact name")
        path = self.directory(job_id) / name
        checksum = self.directory(job_id) / (name + ".sha256")
        if (
            path.is_symlink()
            or not path.is_file()
            or not checksum.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != checksum.read_text()
        ):
            raise ResearchError("Study checkpoint integrity check failed")
        return json.loads(path.read_text())

    def exists(self, job_id: str, name: str) -> bool:
        return (self.directory(job_id) / name).is_file()

    def protocol(self, job_id: str) -> StudyProtocol | RegistryStudyProtocol:
        return load_study_protocol(self.read(job_id, "protocol.json"))

    def seal(self, job_id: str) -> None:
        directory = self.directory(job_id)
        manifest = {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(directory.iterdir())
            if p.is_file()
            and p.suffix in {".json", ".md", ".zip"}
            and p.name != "artifact_manifest.json"
        }
        if not MANDATORY <= manifest.keys():
            raise ResearchError("Study evidence is incomplete")
        self.write(job_id, "artifact_manifest.json", manifest)

    def verify_seal(self, job_id: str) -> None:
        manifest = self.read(job_id, "artifact_manifest.json")
        if not isinstance(manifest, dict) or not MANDATORY <= manifest.keys():
            raise ResearchError("Study artifact manifest is incomplete")
        actual = {
            p.name
            for p in self.directory(job_id).iterdir()
            if p.suffix in {".json", ".zip", ".md"} and p.name != "artifact_manifest.json"
        }
        if actual != set(manifest):
            raise ResearchError("Study artifact manifest does not cover the retained evidence")
        for name, digest in manifest.items():
            if not re.fullmatch(r"[a-z][a-z0-9_]*\.(json|md|zip)", name):
                raise ResearchError("Invalid study manifest entry")
            path = self.directory(job_id) / name
            if (
                path.is_symlink()
                or not path.is_file()
                or hashlib.sha256(path.read_bytes()).hexdigest() != digest
            ):
                raise ResearchError("Study artifact integrity check failed")
