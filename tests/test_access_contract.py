import json
from dataclasses import replace
from uuid import UUID

import pytest
from pydantic import ValidationError

from stratosphere.phase0.access_contract import (
    CatalogProjection,
    InternalStrategyFixture,
    RegistrationRequest,
    assert_harness_owner,
    project_catalog,
)
from stratosphere.phase0.contracts import StrategyDocument, fingerprint, load_contract
from stratosphere.phase0.data_proof import EXAMPLES


@pytest.fixture
def record():
    document = load_contract(EXAMPLES / "strategy.json", StrategyDocument)
    return InternalStrategyFixture(
        public_id="000000000000000000000007",
        document=document,
        spec_sha256=fingerprint(document.formal_spec),
        state="registered",
        approved_source_refs=("approved-evidence-pointer",),
        private_payload={
            "submission": "PRIVATE_REQUEST_FIXTURE",
            "owner": "PRIVATE_OWNER_FIXTURE",
            "account": "PRIVATE_ACCOUNT_FIXTURE",
            "future_field": "PRIVATE_FUTURE_FIXTURE",
            "source_content": "PRIVATE_CONTENT_FIXTURE",
        },
    )


def test_catalog_uses_allowlist_and_approved_evidence_only(record):
    projected = project_catalog(record)
    data = projected.model_dump(mode="json")
    assert set(data) == {
        "schema_version",
        "public_id",
        "name",
        "description",
        "formal_spec",
        "spec_sha256",
        "status",
        "validation",
        "approved_source_refs",
    }
    serialized = projected.model_dump_json()
    for private_value in record.private_payload.values():
        assert private_value not in serialized
    assert record.document.source_refs[0] not in serialized
    assert projected.approved_source_refs == ("approved-evidence-pointer",)
    assert projected.validation == "registration_does_not_imply_validation"
    assert projected.public_id.startswith("000")
    other = project_catalog(replace(record, approved_source_refs=("additional-approved-pointer",)))
    assert other.spec_sha256 == projected.spec_sha256


def test_drafts_and_tampered_identity_are_not_projected(record):
    with pytest.raises(ValueError, match="draft"):
        project_catalog(replace(record, state="draft"))
    with pytest.raises(ValueError, match="fingerprint"):
        project_catalog(replace(record, spec_sha256="0" * 64))
    with pytest.raises(ValueError, match="unknown state"):
        project_catalog(replace(record, state="verified"))
    assert project_catalog(replace(record, state="retired")).status == "retired"


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner_id", "another-user"),
        ("public_id", "7" * 24),
        ("visibility", "private"),
        ("status", "verified"),
        ("user_metadata", {"role": "admin"}),
        ("approved_source_refs", ["unreviewed-source"]),
    ],
)
def test_client_cannot_assign_service_authority_fields(record, field, value):
    request = {
        "schema_version": "registration/1",
        "document": record.document.model_dump(mode="json"),
    }
    request[field] = value
    with pytest.raises(ValidationError):
        RegistrationRequest.model_validate(request)


def test_owner_invariants_reject_cross_user_anonymous_and_reassignment():
    alice = UUID("00000000-0000-0000-0000-000000000001")
    bob = UUID("00000000-0000-0000-0000-000000000002")
    assert_harness_owner(alice, alice)
    assert_harness_owner(alice, alice, alice)
    for subject, owner, proposed in ((None, alice, None), (bob, alice, None), (alice, alice, bob)):
        with pytest.raises(PermissionError):
            assert_harness_owner(subject, owner, proposed)


def test_numeric_id_is_string_and_catalog_cannot_smuggle_private_columns(record):
    data = project_catalog(record).model_dump(mode="json")
    data["account"] = "private"
    with pytest.raises(ValidationError):
        CatalogProjection.model_validate(data)
    del data["account"]
    data["public_id"] = 7
    with pytest.raises(ValidationError):
        CatalogProjection.model_validate(data)


def test_published_access_schemas_match_runtime():
    for name, cls in (("catalog", CatalogProjection), ("registration", RegistrationRequest)):
        schema = EXAMPLES.parents[1] / "schemas" / f"{name}-v1.json"
        assert json.loads(schema.read_text()) == cls.model_json_schema()
