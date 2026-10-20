"""Explicit native-definition source binding, separate from conversion, approval and tests."""

import asyncio
import hashlib
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from stratosphere.phase0.contracts import (
    Contract,
    Digest,
    StrategySpec,
    canonical_bytes,
    fingerprint,
)
from stratosphere.research.contracts import implementation_digest, sma_parameters
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.sources.contracts import SourceRequest, SourceSnapshot, SuppliedRequest
from stratosphere.sources.store import load_request


class RegistrationRequest(Contract):
    source_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    name: str | None = Field(default=None, min_length=1, pattern=r"\S")
    parent_strategy_id: str | None = Field(default=None, pattern=r"^[0-9]{24}$")

    @classmethod
    def arguments(cls, args):
        if not 1 <= len(args) <= 3:
            raise ResearchError("Usage: /source-register <source-id> [name|-] [parent-id]")
        return cls(
            source_id=args[0],
            name=args[1] if len(args) > 1 and args[1] != "-" else None,
            parent_strategy_id=args[2] if len(args) > 2 else None,
        )


def native_definition(snapshot, *, supported=True):
    """Only whole literal JSON, without duplicate keys, coercions or unmentioned defaults."""
    if snapshot.access_state != "acquired" or len(snapshot.pieces) != 1:
        raise ResearchError(
            "Supply one complete native JSON definition; partial/metadata material "
            "cannot register it."
        )
    if snapshot.provider not in {"supplied", "github"} or snapshot.pieces[0].kind not in {
        "text",
        "code",
    }:
        raise ResearchError(
            "Registration accepts supplied native JSON or one pinned GitHub JSON file, "
            "not media/code conversion."
        )
    if snapshot.provider == "github" and not (
        snapshot.repository_commit and snapshot.repository_blob
    ):
        raise ResearchError(
            "A GitHub definition requires retained immutable commit/blob references."
        )

    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def invalid_constant(_):
        raise ValueError("nonfinite JSON")

    try:
        data = json.loads(
            snapshot.pieces[0].text.removeprefix("\ufeff"),
            object_pairs_hook=object_pairs,
            parse_constant=invalid_constant,
        )
        spec = StrategySpec.model_validate(data)
        encoded = json.dumps(
            data, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode()
        if encoded != canonical_bytes(spec):
            raise ValueError("incomplete/coerced definition")
        if supported:
            sma_parameters(spec)
        return spec
    except ValueError, TypeError, KeyError, RecursionError:
        raise ResearchError(
            "The complete JSON must match the supported strategy/1 SMA definition without "
            "missing fields, duplicate keys or changed behavior. Executable source conversion "
            "requires its separate conformance path."
        ) from None


class NativeSourceProof(Contract):
    schema_version: Literal["native-source-binding/1"] = "native-source-binding/1"
    snapshot: SourceSnapshot
    source_request: SourceRequest | SuppliedRequest
    source_request_sha256: Digest
    piece_sha256: Digest
    strategy_sha256: Digest
    reader_sha256: Digest
    reviewed_implementation_sha256: Digest
    match: Literal["complete_supported_native_definition"] = "complete_supported_native_definition"
    executable_attestation: Literal[False] = False

    @model_validator(mode="after")
    def bound(self):
        spec = native_definition(self.snapshot, supported=False)
        if (
            self.source_request.provider != self.snapshot.provider
            or self.source_request.url != self.snapshot.requested_url
        ):
            raise ValueError("Acquisition request/snapshot identity mismatch")
        if (
            self.piece_sha256 != self.snapshot.pieces[0].digest
            or fingerprint(spec) != self.strategy_sha256
        ):
            raise ValueError("Native definition/source identity mismatch")
        return self


class SourceRegistration(Contract):
    schema_version: Literal["source-registration/1"] = "source-registration/1"
    id: str = Field(pattern=r"^[a-f0-9]{32}$")
    request: RegistrationRequest
    requested_at: AwareDatetime
    recorded_at: AwareDatetime
    proof: NativeSourceProof
    strategy_id: str = Field(pattern=r"^[0-9]{24}$")
    agent_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    registered_name: str = Field(min_length=1, pattern=r"\S")
    resolution: Literal["new_strategy", "variant", "exact_duplicate"]
    source_payload_sha256: Digest
    execution: Literal["definition_registered_no_approval_or_test"] = (
        "definition_registered_no_approval_or_test"
    )

    @model_validator(mode="after")
    def coherent(self):
        if self.request.source_id != self.proof.snapshot.id:
            raise ValueError("Source registration request/snapshot mismatch")
        for field in ("requested_at", "recorded_at"):
            object.__setattr__(self, field, getattr(self, field).astimezone(UTC))
        if self.recorded_at < self.requested_at:
            raise ValueError("Receipt predates the request")
        return self

    @property
    def digest(self):
        return fingerprint(self)


def source_payload(identity, request, proof):
    return {
        "kind": "registered_native_source/1",
        "registration_id": identity,
        "request": request.model_dump(mode="json"),
        "proof": proof.model_dump(mode="json"),
    }


class SourceRegistrationService:
    def __init__(self, sources, database, *, clock=lambda: datetime.now(UTC)):
        self.sources, self.database, self.clock = sources, database, clock
        self.store = ResearchStore(sources.store.root.parent / "source-registrations")

    async def register(self, request: RegistrationRequest):
        await self.sources.require_online()
        if self.sources.store.has_secret(request.model_dump_json().encode()):
            raise ResearchError(
                "Credential-bearing registration request was rejected before storage"
            )
        identity = uuid.uuid7().hex
        requested_at = self.clock()
        if requested_at.tzinfo is None or requested_at.utcoffset() is None:
            raise ResearchError("Registration requires an aware request clock")
        requested_at = requested_at.astimezone(UTC)
        if self.store.root.is_symlink():
            raise ResearchError("Registration attempt paths cannot be symbolic links")
        self.store.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.store.directory(identity).mkdir(mode=0o700)
        self.store.write(
            identity,
            "request.json",
            {"request": request.model_dump(mode="json"), "requested_at": requested_at.isoformat()},
        )
        self.store.event(identity, "validating")
        try:
            snapshot = await self.sources.open(request.source_id)
            spec = native_definition(snapshot)
            proof = NativeSourceProof(
                snapshot=snapshot,
                source_request=load_request(self.sources.store.read(snapshot.id, "request.json")),
                source_request_sha256=self.sources.store.read(snapshot.id, "manifest.json")[
                    "request_sha256"
                ],
                piece_sha256=snapshot.pieces[0].digest,
                strategy_sha256=fingerprint(spec),
                reader_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                reviewed_implementation_sha256=implementation_digest(),
            )
            if self.sources.store.has_secret(proof.model_dump_json().encode()):
                raise ResearchError(
                    "Credential-bearing definition was rejected before registration"
                )
            # No await between the final source check and the single registry transaction.
            if (
                self.sources.store.snapshot(snapshot.id).digest != snapshot.digest
                or self.sources.store.read(snapshot.id, "manifest.json")["request_sha256"]
                != proof.source_request_sha256
            ):
                raise ResearchError("Source changed before registration")
            receipt = self.database.register_source(identity, request, proof, requested_at)
        except asyncio.CancelledError:
            self.store.event(identity, "canceled", "No registry transaction was started")
            raise
        except Exception:
            self.store.event(identity, "failed", "Definition registration could not be verified")
            raise
        # SQLite is authoritative if a crash/disk failure prevents this auxiliary event.
        try:
            self.store.event(identity, "completed", receipt.strategy_id)
        except OSError:
            pass
        return receipt

    async def open(self, identity):
        await self.sources.require_online()
        value = self.database.source_registration(identity)
        if (
            self.sources.store.snapshot(value.request.source_id).digest
            != value.proof.snapshot.digest
            or self.sources.store.read(value.request.source_id, "manifest.json")["request_sha256"]
            != value.proof.source_request_sha256
        ):
            raise ResearchError("The source differs from this immutable registration receipt")
        return value

    async def attempts(self):
        await self.sources.require_online()
        if self.store.root.is_symlink():
            raise ResearchError("Registration attempt paths cannot be symbolic links")
        rows = []
        committed = set(self.database.source_registration_ids())
        for identity in sorted(set(self.store.jobs()) | committed):
            if identity in committed:
                try:
                    receipt = self.database.source_registration(identity)
                    state, strategy = "registered", receipt.strategy_id
                except ResearchError:
                    state, strategy = "damaged_registry_receipt", None
            else:
                try:
                    state = self.store.state(identity)
                except ResearchError, ValueError, OSError:
                    state = "incomplete_or_damaged"
                strategy = None
            rows.append({"id": identity, "state": state, "strategy_id": strategy})
        return rows


def registration_markdown(value):
    proof = value.proof
    submitted = (
        getattr(proof.source_request, "submitted_url", None)
        or getattr(proof.source_request, "attribution_url", None)
        or "not supplied"
    )
    return "\n\n".join(
        [
            f"# Native source registration {value.id}",
            f"Strategy `{value.strategy_id}` · agent `{value.agent_id}` · {value.resolution}",
            f"Name: {value.registered_name}",
            f"Requested name: {value.request.name or '(source label)'} · "
            f"declared parent: {value.request.parent_strategy_id or 'none'}",
            f"Source `{value.request.source_id}` · snapshot `{proof.snapshot.digest}`",
            f"Acquisition request `{proof.source_request_sha256}` · "
            f"retained submitted URL/attribution `{submitted}`",
            f"Definition `{proof.strategy_sha256}` · piece `{proof.piece_sha256}`",
            f"Captured {proof.snapshot.retrieved_at.isoformat()} · "
            f"requested {value.requested_at.isoformat()} · "
            f"recorded {value.recorded_at.isoformat()}",
            "The whole native JSON matches the supported definition contract. Source attribution, "
            "external-code equivalence, executable identity and market performance "
            "are not verified.",
            "Registration did not approve or start a test, link an account or activate trading.",
            f"Select its agent: `/agent {value.strategy_id}`. "
            f"Prepare a separate reviewed study: `/study {value.strategy_id}`.",
            f"Receipt `{value.digest}` · reader `{proof.reader_sha256}` · "
            f"reviewed source implementation `{proof.reviewed_implementation_sha256}`",
            "## Retained source limitations",
            *proof.snapshot.limitations,
        ]
    )
