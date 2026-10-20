"""Exact caption file provenance, cue clocks, malformed input and historical contracts."""

import hashlib
import json
from datetime import UTC, datetime

import pytest
from test_source_reviews import ReviewProvider
from test_source_reviews import reviews as reviews
from test_sources import snapshot

from stratosphere.research.store import ResearchError
from stratosphere.sources.captions import PROTOCOL, CaptionError, CaptionRequest, parse_caption_v1
from stratosphere.sources.contracts import SourceRequest, SuppliedRequest
from stratosphere.sources.review import evidence_passages, review_markdown
from stratosphere.sources.service import SourceService, source_markdown
from stratosphere.sources.store import SourceStore, load_request

VTT = """WEBVTT authored cue fixture
Kind: captions
Language: en

NOTE fake instructions here are metadata, not strategy evidence

STYLE
::cue { color: lime; }

alpha
00:00.000 --> 00:02.000 align:start
<v Analyst>SMA 3/8.
  Buy only on the cross above.

beta
00:01.500 --> 01:01.025
Close on the cross below; use fixed size.
"""
SRT = """1
00:00:00,000 --> 00:00:02,000
SMA 3/8.
  Buy only on the cross above.

2
00:00:02,000 --> 00:01:01,025
Close on the cross below; use fixed size.
"""


async def online():
    pass


@pytest.fixture
def sources(tmp_path):
    return SourceService(
        SourceStore(tmp_path / "sources"),
        online_check=online,
        clock=lambda: datetime(2026, 10, 20, tzinfo=UTC),
    )


@pytest.mark.parametrize("format,text", [("webvtt", VTT), ("srt", SRT)])
@pytest.mark.parametrize("newline", ["\n", "\r\n", "\r"])
@pytest.mark.parametrize("bom", [False, True])
def test_every_character_and_exact_payload_clock_survives_partitioning(format, text, newline, bom):
    text = text.replace("\n", newline)
    raw = (b"\xef\xbb\xbf" if bom else b"") + text.encode()
    parsed = parse_caption_v1(raw, format)
    assert "".join(p.text for p in parsed.pieces) == text
    captions = [p for p in parsed.pieces if p.kind == "caption"]
    assert len(captions) == 2 and captions[0].start_ms == 0
    assert captions[0].end_ms == 2000 and captions[1].end_ms == 61025
    assert captions[0].text.endswith("Buy only on the cross above." + newline)
    for cue in captions:
        start, end = map(int, cue.locator.split("raw characters ")[1].split(";")[0].split(":"))
        assert text[start:end] == cue.text
        assert text.splitlines()[cue.first_line - 1 : cue.last_line] == cue.text.splitlines()
    assert any("metadata" in p.text for p in parsed.pieces if p.kind == "metadata") == (
        format == "webvtt"
    )


@pytest.mark.parametrize("suffix", ["", "\n", "\n\n\n"])
def test_eof_and_unicode_are_not_normalized_or_lost(suffix):
    text = "WEBVTT\n\n00:00.001 --> 00:03.002\ne\u0301, é, 星\u2028next\t<&amp;>" + suffix
    parsed = parse_caption_v1(text.encode(), "webvtt")
    assert "".join(p.text for p in parsed.pieces) == text
    cue = next(p for p in parsed.pieces if p.kind == "caption")
    assert cue.start_ms == 1 and cue.end_ms == 3002
    assert "e\u0301, é, 星\u2028next\t<&amp;>" in cue.text


def test_hours_overlap_duplicate_and_unordered_source_cues_are_retained_with_findings():
    text = (
        "WEBVTT\n\nrepeat\n100:00:00.001 --> 100:00:10.002\nfirst\n\n"
        "repeat\n00:01.000 --> 00:02.000\nsecond\n"
    )
    value = parse_caption_v1(text.encode(), "webvtt")
    cues = [p for p in value.pieces if p.kind == "caption"]
    assert [p.text for p in cues] == ["first\n", "second\n"]
    assert cues[0].start_ms == 360000001
    assert any("Repeated" in s for s in value.findings)
    assert any("decrease" in s for s in value.findings)
    assert any("overlap" in s for s in value.findings)
    duplicate = parse_caption_v1((SRT + "\n" + SRT).encode(), "srt")
    assert len([p for p in duplicate.pieces if p.kind == "caption"]) == 4
    assert any("counters" in s for s in duplicate.findings)


@pytest.mark.parametrize(
    "raw,format",
    [
        (b"", "webvtt"),
        (b"\xff", "webvtt"),
        (b"WEBVTT\0", "webvtt"),
        (b" WEBVTT\n\n00:00.000 --> 00:01.000\ntext", "webvtt"),
        (b"WEBVTT\n00:00.000 --> 00:01.000\ntext", "webvtt"),
        (b"WEBVTT\n\n00:61.000 --> 01:02.000\ntext", "webvtt"),
        (b"WEBVTT\n\n00:00,000 --> 00:01,000\ntext", "webvtt"),
        (b"WEBVTT\n\n00:00.00 --> 00:01.000\ntext", "webvtt"),
        (b"WEBVTT\n\n00:00.000-->00:01.000\ntext", "webvtt"),
        (b"WEBVTT\n\n00:01.000 --> 00:01.000\ntext", "webvtt"),
        (b"WEBVTT\n\n00:02.000 --> 00:01.000\ntext", "webvtt"),
        (b"WEBVTT\n\n00:00.000 --> 00:01.000\n", "webvtt"),
        (b"WEBVTT\n\n00:00.000 --> 00:01.000\ntext\n00:01.000 --> 00:02.000\ntext", "webvtt"),
        (
            b"WEBVTT\nX-TIMESTAMP-MAP=LOCAL:00:00.000,MPEGTS:100\n\n00:00.000 --> 00:01.000\ntext",
            "webvtt",
        ),
        (b"WEBVTT\n\n00:00.000 --> 00:01.000\ntext\n\nSTYLE\n::cue {}", "webvtt"),
        (b"0\n00:00:00,000 --> 00:00:01,000\ntext", "srt"),
        (b"1\n00:00:00.000 --> 00:00:01.000\ntext", "srt"),
        (b"1\n00:00:00,000 --> 00:00:01,000 X1:1 X2:2\ntext", "srt"),
        (b"1\n00:00:00,000 --> 00:00:01,000\ntext", "unknown"),
    ],
)
def test_malformed_or_unsupported_input_never_silently_repairs_or_skips_cues(raw, format):
    with pytest.raises(CaptionError):
        parse_caption_v1(raw, format)


async def test_snapshot_raw_provenance_attribution_private_restart_and_no_conversion(sources):
    attribution = SourceRequest(url="https://youtu.be/dQw4w9WgXcQ?t=20")
    request = CaptionRequest(
        label="explicit caption fixture.vtt", format="webvtt", attribution=attribution
    )
    value = await sources.supply_captions(request, VTT.encode())
    assert value.access_state == "acquired" and value.normalization == PROTOCOL
    assert value.provider == "supplied" and value.behavior_conformance == "not_verified"
    assert value.redistribution == "not_assessed"
    assert load_request(sources.store.read(value.id, "request.json")) == request
    assert sources.store.read(value.id, "request.json")["attribution"]["submitted_url"].endswith(
        "?t=20"
    )
    assert (
        sources.store.read(value.id, "request.json")["attribution"]["url"]
        == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    )
    (material,) = value.raw_materials
    assert material.sha256 == hashlib.sha256(VTT.encode()).hexdigest()
    assert (
        sources.store.directory(value.id) / ("blob_" + material.sha256 + ".bin")
    ).read_bytes() == VTT.encode()
    assert "fake instructions" not in value.analysis_text and "Buy only" in value.analysis_text
    assert "File-local cue bounds: 0–2000 ms" in source_markdown(value)
    assert SourceStore(sources.store.root).snapshot(value.id) == value
    assert all(p.stat().st_mode & 0o077 == 0 for p in sources.store.directory(value.id).iterdir())


@pytest.mark.parametrize(
    "raw",
    [
        b"WEBVTT bad file",
        b"\xffinvalid bytes",
        b"WEBVTT\n\n00:00.000 --> 00:01.000\nvalid\n\nbroken cue",
    ],
)
async def test_failed_parse_retains_original_bytes_and_no_partial_readable_subset(sources, raw):
    value = await sources.supply_captions(CaptionRequest(label="bad.vtt", format="webvtt"), raw)
    assert value.access_state == "unsupported_format" and not value.pieces
    assert len(value.raw_materials) == 1 and value.next_steps
    assert sources.store.snapshot(value.id) == value
    assert value.raw_materials[0].sha256 == hashlib.sha256(raw).hexdigest()


@pytest.mark.parametrize("spelling", ["raw", "entities", "unicode"])
async def test_known_credentials_are_rejected_before_raw_file_or_model_use(sources, spelling):
    secret = 'fixture-"key"-\\-é-😀'
    sources.store.known_secrets = (secret,)
    encoded = (
        secret
        if spelling == "raw"
        else "".join(f"&#{ord(c)};" for c in secret)
        if spelling == "entities"
        else json.dumps(secret, ensure_ascii=True)[1:-1]
    )
    # Decode ordinary JSON quote/backslash escapes too, as a caption may contain JSON data.
    content = VTT.replace("SMA 3/8.", encoded).encode()
    value = await sources.supply_captions(
        CaptionRequest(label="secret.vtt", format="webvtt"), content
    )
    assert value.access_state == "sensitive_material_rejected"
    assert not value.raw_materials and not value.pieces
    assert not list(sources.store.directory(value.id).glob("blob_*"))


async def test_online_gate_and_raw_corruption_block_reads(sources):
    value = await sources.supply_captions(CaptionRequest(label="c.srt", format="srt"), SRT.encode())
    path = sources.store.directory(value.id) / ("blob_" + value.raw_materials[0].sha256 + ".bin")
    path.write_text("changed")
    with pytest.raises(ResearchError, match="integrity"):
        await sources.open(value.id)

    async def offline():
        raise ValueError("offline")

    sources.online_check = offline
    with pytest.raises(ResearchError, match="online"):
        await sources.supply_captions(CaptionRequest(label="c.srt", format="srt"), SRT.encode())
    assert len(sources.store.jobs()) == 1


async def test_caption_review_quotes_are_exact_and_cue_bounds_do_not_become_word_timing(reviews):
    db, first, second, sources, old, service, _ = reviews
    before = sources.store.read(old.id, "snapshot.json")
    value = await sources.supply_captions(
        CaptionRequest(label="review.vtt", format="webvtt"), VTT.encode()
    )
    passages = evidence_passages(value)
    assert all(p["kind"] == "caption" and "NOTE" not in p["text"] for p in passages)
    for cue in (p for p in value.pieces if p.kind == "caption"):
        assert "".join(p["text"] for p in passages if p["piece_sha256"] == cue.digest) == cue.text
    provider = ReviewProvider(
        response={
            "findings": [
                {
                    "category": "rule_candidate",
                    "interpretation": (
                        "Tentative entry description; audio and behavior remain unverified."
                    ),
                    "evidence_index": 1,
                }
            ],
            "questions": [],
        }
    )
    report = await service.review(first["public_id"], value.id, provider, "fixture")
    shown = review_markdown(report, value)
    assert "caption-piece bounds 0–2000 ms" in shown and "Buy only on the cross above." in shown
    assert report.execution == "blocked" and not db.source_reviews(second["public_id"])
    assert sources.store.read(old.id, "snapshot.json") == before
    assert sources.store.snapshot(old.id).digest == old.digest
    assert not db.jobs(first["public_id"]) and not db.reports()


def test_historical_noncaption_snapshot_digest_is_unchanged(sources):
    old = snapshot(sources.store, SuppliedRequest(label="Legacy fixture"))
    before = {p.name: p.read_bytes() for p in sources.store.directory(old.id).iterdir()}
    assert SourceStore(sources.store.root).snapshot(old.id).digest == old.digest
    assert before == {p.name: p.read_bytes() for p in sources.store.directory(old.id).iterdir()}


def test_subrip_literal_arrow_is_payload_not_an_invented_cue():
    text = SRT.replace("SMA 3/8.", "Condition --> exit, preserving the literal arrow.")
    value = parse_caption_v1(text.encode(), "srt")
    assert "Condition --> exit" in value.pieces[1].text


def test_frozen_first_caption_protocol_retains_piece_shapes_and_hashes():
    from pathlib import Path

    data = json.loads((Path(__file__).parent / "fixtures/caption-cues-v1.json").read_text())
    parsed = parse_caption_v1(data["text"].encode(), data["format"])
    assert PROTOCOL == data["protocol"]
    assert [p.model_dump(mode="json") for p in parsed.pieces] == data["pieces"]
    assert [p.digest for p in parsed.pieces] == data["piece_sha256"]


async def test_retained_caption_cannot_invent_clock_or_payload_even_with_resealed_snapshot(sources):
    value = await sources.supply_captions(
        CaptionRequest(label="bound.vtt", format="webvtt"), VTT.encode()
    )
    directory = sources.store.directory(value.id)
    from stratosphere.sources.contracts import SourceSnapshot

    payload = value.model_dump(mode="json")
    payload["pieces"][1]["start_ms"] = 25
    changed = SourceSnapshot.model_validate(payload)
    (directory / "snapshot.json").write_text(changed.model_dump_json())
    manifest = sources.store.read(value.id, "manifest.json")
    manifest["snapshot_sha256"] = changed.digest
    (directory / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ResearchError, match="retained raw file"):
        sources.store.snapshot(value.id)


async def test_cancel_during_parse_retains_raw_attempt_and_never_publishes_late_cues(
    sources, monkeypatch
):
    import asyncio
    import threading

    import stratosphere.sources.service as module

    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = module.parse_caption_v1

    def pause(content, format):
        started.set()
        release.wait(10)
        value = original(content, format)
        finished.set()
        return value

    monkeypatch.setattr(module, "parse_caption_v1", pause)
    task = asyncio.create_task(
        sources.supply_captions(CaptionRequest(label="cancel.vtt", format="webvtt"), VTT.encode())
    )
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        (source_id,) = sources.store.jobs()
        value = sources.store.snapshot(source_id)
        assert value.access_state == "unavailable" and not value.pieces
        assert len(value.raw_materials) == 1
        before = (sources.store.directory(source_id) / "snapshot.json").read_bytes()
    finally:
        release.set()
    assert await asyncio.to_thread(finished.wait, 5)
    assert before == (sources.store.directory(source_id) / "snapshot.json").read_bytes()
