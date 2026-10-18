"""Private, immutable material and retrieval attempts; never imports acquired repository code."""

import hashlib
import os
import re
import uuid
from pathlib import Path

from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.sources.contracts import (
    RawMaterial,
    SourceRequest,
    SourceSnapshot,
    SuppliedRequest,
)


def load_request(payload):
    return (SourceRequest if "url" in payload else SuppliedRequest).model_validate(payload)


def parser_digest():
    digest = hashlib.sha256()
    for path in sorted(Path(__file__).parent.glob("*.py")):
        digest.update(path.name.encode() + b"\0" + path.read_bytes())
    return digest.hexdigest()


class SourceStore(ResearchStore):
    def __init__(self, root: Path, *, known_secrets=()):
        super().__init__(root)
        self.known_secrets = tuple(s for s in known_secrets if s)
        self.session_secret_check = None

    def directory(self, source_id):
        if not re.fullmatch(r"[a-f0-9]{32}", source_id):
            raise ResearchError("Use a complete source ID shown by /sources.")
        path = super().directory(source_id)
        if path.is_symlink():
            raise ResearchError("Source attempt directories cannot be symbolic links")
        return path

    def read(self, source_id, name):
        if name not in {"request.json", "snapshot.json", "manifest.json"}:
            raise ResearchError("Invalid source artifact name")
        if (self.directory(source_id) / name).is_symlink():
            raise ResearchError("Source artifact integrity check failed")
        return super().read(source_id, name)

    def has_secret(self, content: bytes) -> bool:
        return any(secret.encode() in content for secret in self.known_secrets) or bool(
            self.session_secret_check and self.session_secret_check(content)
        )

    def create(self, request: SourceRequest | SuppliedRequest):
        if self.has_secret(request.model_dump_json().encode()):
            raise ResearchError("Remove credentials from the source request; they were not saved")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        source_id = uuid.uuid7().hex
        self.directory(source_id).mkdir(mode=0o700)
        self.write(source_id, "request.json", request.model_dump(mode="json"))
        self.event(source_id, "acquiring")
        return source_id

    def raw(self, source_id, content: bytes, media_type: str, scope: str) -> RawMaterial:
        if self.has_secret(content):
            raise ResearchError("Credential-bearing source material was rejected before storage")
        digest = hashlib.sha256(content).hexdigest()
        path = self.directory(source_id) / ("blob_" + digest + ".bin")
        if path.exists():
            if path.is_symlink() or path.read_bytes() != content:
                raise ResearchError("A retained source blob cannot be replaced")
        else:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as file:
                file.write(content)
                file.flush()
                os.fsync(file.fileno())
        return RawMaterial(
            sha256=digest, media_type=media_type, scope=scope, byte_count=len(content)
        )

    def finish(self, snapshot: SourceSnapshot):
        if self.has_secret(snapshot.model_dump_json().encode()):
            raise ResearchError("Credential-bearing source metadata was rejected before storage")
        request = load_request(self.read(snapshot.id, "request.json"))
        if snapshot.provider != request.provider or snapshot.requested_url != request.url:
            raise ResearchError("Acquired source differs from the requested provider/URL")
        for material in snapshot.raw_materials:
            self._verify_blob(snapshot.id, material)
        self.write(snapshot.id, "snapshot.json", snapshot.model_dump(mode="json"))
        self.write(
            snapshot.id,
            "manifest.json",
            {
                "snapshot_sha256": snapshot.digest,
                "request_sha256": hashlib.sha256(
                    (self.directory(snapshot.id) / "request.json").read_bytes()
                ).hexdigest(),
            },
        )
        self.event(snapshot.id, snapshot.access_state)

    def _verify_blob(self, source_id, material: RawMaterial):
        path = self.directory(source_id) / ("blob_" + material.sha256 + ".bin")
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_size != material.byte_count
            or hashlib.sha256(path.read_bytes()).hexdigest() != material.sha256
        ):
            raise ResearchError("Source material integrity check failed")

    def snapshot(self, source_id):
        snapshot = SourceSnapshot.model_validate(self.read(source_id, "snapshot.json"))
        manifest = self.read(source_id, "manifest.json")
        request = load_request(self.read(source_id, "request.json"))
        if (
            snapshot.id != source_id
            or snapshot.digest != manifest.get("snapshot_sha256")
            or snapshot.requested_url != request.url
            or snapshot.provider != request.provider
            or hashlib.sha256((self.directory(source_id) / "request.json").read_bytes()).hexdigest()
            != manifest.get("request_sha256")
        ):
            raise ResearchError("Source snapshot integrity check failed")
        for material in snapshot.raw_materials:
            self._verify_blob(source_id, material)
        return snapshot
