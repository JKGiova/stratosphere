"""Catalog projection and ownership invariants for a development harness, not auth."""

from dataclasses import dataclass
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field

from stratosphere.phase0.contracts import (
    Contract,
    Digest,
    StrategyDocument,
    StrategySpec,
    fingerprint,
)

PublicId = Annotated[str, Field(pattern=r"^[0-9]{24}$")]


class RegistrationRequest(Contract):
    """Client input cannot assign identity, ownership, readiness or publication authority."""

    schema_version: Literal["registration/1"]
    document: StrategyDocument


class CatalogProjection(Contract):
    schema_version: Literal["catalog/1"]
    public_id: PublicId
    name: str
    description: str
    formal_spec: StrategySpec
    spec_sha256: Digest
    status: Literal["specified", "retired"]
    validation: Literal["registration_does_not_imply_validation"]
    approved_source_refs: tuple[str, ...]


@dataclass(frozen=True)
class InternalStrategyFixture:
    """Internal example shape; private payload must never be copied to a catalog DTO."""

    public_id: str
    document: StrategyDocument
    spec_sha256: str
    state: Literal["draft", "registered", "retired"]
    approved_source_refs: tuple[str, ...]
    private_payload: dict


def project_catalog(record: InternalStrategyFixture) -> CatalogProjection:
    if record.state not in {"registered", "retired"}:
        raise ValueError("Unregistered draft or unknown state has no catalog projection")
    if fingerprint(record.document.formal_spec) != record.spec_sha256:
        raise ValueError("Stored behavior fingerprint mismatch")
    # Enumerate output fields; never serialize/copy an internal row and remove known secrets.
    return CatalogProjection(
        schema_version="catalog/1",
        public_id=record.public_id,
        name=record.document.name,
        description=record.document.description,
        formal_spec=record.document.formal_spec,
        spec_sha256=record.spec_sha256,
        status="retired" if record.state == "retired" else "specified",
        validation="registration_does_not_imply_validation",
        approved_source_refs=record.approved_source_refs,
    )


def assert_harness_owner(subject: UUID | None, owner: UUID, proposed_owner: UUID | None = None):
    """Pure policy fixture: subject must come from trusted context, never request JSON.

    This function does not validate tokens, establish a session, query a database or
    replace RLS. Production access must implement and test that separate boundary.
    """
    if subject is None or subject != owner:
        raise PermissionError("Owner context required")
    if proposed_owner is not None and proposed_owner != subject:
        raise PermissionError("Ownership reassignment forbidden")
