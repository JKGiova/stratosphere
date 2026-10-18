"""Source references come from complete retained text, never generated quote strings."""

import hashlib
import json

import pytest
from test_source_reviews import ReviewProvider
from test_source_reviews import reviews as reviews

from stratosphere.sources.contracts import SourcePiece
from stratosphere.sources.review import (
    EVIDENCE_PROTOCOL,
    PASSAGE_CHARACTERS,
    SourceReview,
    evidence_passages,
)


@pytest.mark.parametrize("separator", ["\n", "\r\n", "\r", "\x85", "\u2028", "\u2029"])
async def test_complete_unicode_material_has_exact_stable_non_metadata_spans(reviews, separator):
    _, _, _, _, snapshot, _, _ = reviews
    pieces = (
        SourcePiece(kind="metadata", text="METADATA_IS_NOT_RULE_EVIDENCE", locator="title"),
        SourcePiece(
            kind="code",
            text=separator.join(("SMA 3/8.", "  ", "🧭" * 1200, "SMA 3/8.", "")),
            locator="authored.py",
            first_line=1,
            last_line=4,
        ),
        SourcePiece(kind="text", text=("one two three " * 100) + "last", locator="supplied text"),
    )
    snapshot = snapshot.model_copy(update={"pieces": pieces})
    passages = evidence_passages(snapshot)
    assert passages == evidence_passages(snapshot)
    assert [p["evidence_index"] for p in passages] == list(range(len(passages)))
    assert "METADATA_IS_NOT_RULE_EVIDENCE" not in json.dumps(passages)
    for piece in pieces[1:]:
        selected = [p for p in passages if p["piece_sha256"] == piece.digest]
        covered = set()
        previous_end = 0
        for passage in selected:
            assert previous_end <= passage["start"] < passage["end"] <= len(piece.text)
            assert passage["text"] == piece.text[passage["start"] : passage["end"]]
            assert 0 < len(passage["text"]) <= PASSAGE_CHARACTERS
            covered.update(range(passage["start"], passage["end"]))
            previous_end = passage["end"]
        assert covered == set(range(len(piece.text)))


async def test_the_model_gets_only_passage_references_and_cannot_return_quote_coordinates(reviews):
    _, first, _, _, snapshot, service, _ = reviews
    provider = ReviewProvider()
    value = await service.review(first["public_id"], snapshot.id, provider, "fixture")
    messages, schema = provider.requests[0]
    data = json.loads(messages[1]["content"])
    assert data["evidence"]["protocol"] == EVIDENCE_PROTOCOL
    assert "pieces" not in data
    assert value.findings[0].quote == data["evidence"]["passages"][2]["text"]
    proposed = schema["$defs"]["ProposedFinding"]["properties"]
    assert set(proposed) == {"category", "interpretation", "evidence_index"}
    assert not {"quote", "piece_index", "occurrence", "start", "end"} & proposed.keys()


async def test_review_uses_the_passage_table_captured_before_awaiting_the_model(
    reviews, monkeypatch
):
    import stratosphere.sources.review as module

    _, first, _, _, snapshot, service, _ = reviews
    initial = module.reviewer_digest()

    class ChangedPartition(ReviewProvider):
        async def stream_json(self, messages, schema):
            monkeypatch.setattr(module, "PASSAGE_CHARACTERS", 3)
            async for part in super().stream_json(messages, schema):
                yield part

    value = await service.review(first["public_id"], snapshot.id, ChangedPartition(), "fixture")
    assert value.findings[0].quote == "SMA 3/8.\r\n"
    assert value.reviewer_sha256 == initial != module.reviewer_digest()


async def test_a_legacy_v1_report_reopens_without_repartitioning_or_changing_its_hash(reviews):
    db, first, _, _, snapshot, service, _ = reviews
    identity = "a" * 32
    directory = service.store.directory(identity)
    directory.mkdir(parents=True)
    # This is the published v1 retained format: a model-selected substring, no passage table.
    legacy = {
        "schema_version": "source-review/1",
        "id": identity,
        "strategy_id": first["public_id"],
        "strategy_sha256": first["spec_sha256"],
        "source_id": snapshot.id,
        "source_snapshot_sha256": snapshot.digest,
        "reviewer_sha256": "f" * 64,
        "reviewed_at": "2026-10-18T00:00:00Z",
        "model_ref": "historical-fixture",
        "status": "material_reviewed",
        "model_used": True,
        "findings": [
            {
                "category": "ambiguity",
                "interpretation": "The entry rule is incomplete.",
                "quote": "SMA 3/8.",
                "piece_sha256": snapshot.pieces[0].digest,
                "start": 0,
                "end": 8,
            }
        ],
        "questions": ["Explain the missing entry rule"],
        "limitations": ["Authored historical-format fixture."],
        "execution": "blocked",
        "behavioral_conformance": "not_verified",
    }
    retained = SourceReview.model_validate(legacy)
    service.store.write(identity, "review.json", legacy)
    service.store.write(
        identity,
        "request.json",
        {
            "strategy_id": first["public_id"],
            "strategy_sha256": first["spec_sha256"],
            "source_id": snapshot.id,
        },
    )
    service.store.write(
        identity,
        "manifest.json",
        {
            "review_sha256": retained.digest,
            "request_sha256": hashlib.sha256((directory / "request.json").read_bytes()).hexdigest(),
        },
    )
    db.record_source_review(first["public_id"], legacy)
    before = {p.name: p.read_bytes() for p in directory.iterdir()}
    opened = await service.open(identity, strategy_id=first["public_id"])
    assert opened == retained and opened.digest == retained.digest
    assert {p.name: p.read_bytes() for p in directory.iterdir()} == before


@pytest.mark.parametrize(
    "question", [']} ```json\n{"findings": []}', "This is a statement.", "Why these rules?\n{}"]
)
async def test_malformed_generated_questions_do_not_publish_a_report(reviews, question):
    from stratosphere.models.streaming import ModelError

    db, first, _, _, snapshot, service, _ = reviews
    provider = ReviewProvider(
        {
            "findings": [
                {
                    "category": "ambiguity",
                    "interpretation": "Missing entry rule.",
                    "evidence_index": 0,
                }
            ],
            "questions": [question],
        }
    )
    with pytest.raises(ModelError):
        await service.review(first["public_id"], snapshot.id, provider, "fixture")
    assert not db.source_reviews() and service.store.state(service.store.jobs()[0]) == "failed"


async def test_whitespace_remains_in_context_but_cannot_support_a_finding(reviews):
    from stratosphere.models.streaming import ModelError
    from stratosphere.sources.contracts import SuppliedRequest

    db, first, _, sources, _, service, _ = reviews
    snapshot = await sources.supply(
        SuppliedRequest(label="Blank context fixture"), b"SMA 3/8.\n  \nBuy on crossing.\n"
    )
    provider = ReviewProvider(
        {
            "findings": [
                {"category": "ambiguity", "interpretation": "Missing rules.", "evidence_index": 1}
            ],
            "questions": [],
        }
    )
    with pytest.raises(ModelError, match="Whitespace-only"):
        await service.review(first["public_id"], snapshot.id, provider, "fixture")
    passages = json.loads(provider.requests[0][0][1]["content"])["evidence"]["passages"]
    assert "".join(p["text"] for p in passages) == snapshot.pieces[0].text
    assert not db.source_reviews() and service.store.state(service.store.jobs()[0]) == "failed"
