"""Source integrity and explicit partial acquisition, independent of external availability."""

import asyncio
import base64
import hashlib
from datetime import UTC, datetime

import httpx
import pytest

from stratosphere.research.store import ResearchError
from stratosphere.sources.contracts import (
    SourcePiece,
    SourceRequest,
    SourceSnapshot,
    SuppliedRequest,
)
from stratosphere.sources.service import SourceCredentials, SourceService, source_markdown
from stratosphere.sources.store import SourceStore, parser_digest


@pytest.mark.parametrize(
    "url,provider,canonical",
    [
        (
            "https://youtu.be/dQw4w9WgXcQ?si=tracking",
            "youtube",
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        ),
        (
            "https://www.youtube.com/shorts/dQw4w9WgXcQ",
            "youtube",
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        ),
        (
            "https://www.instagram.com/reel/Abc_123/?igsh=tracking",
            "instagram",
            "https://www.instagram.com/reel/Abc_123/",
        ),
        ("https://twitter.com/author/status/12345?s=20", "x", "https://x.com/author/status/12345"),
        (
            "https://www.tradingview.com/script/Abcd1234-example/",
            "tradingview",
            "https://www.tradingview.com/script/Abcd1234-example/",
        ),
        (
            "https://github.com/owner/repo/blob/main/a%3Fb.py",
            "github",
            "https://github.com/owner/repo/blob/main/a%3Fb.py",
        ),
    ],
)
def test_provider_urls_are_explicit_canonical_and_idempotent(url, provider, canonical):
    request = SourceRequest(url=url)
    assert request.provider == provider and request.url == canonical
    assert SourceRequest(url=request.url).url == request.url
    assert SourceRequest.model_validate(request.model_dump()) == request
    assert request.submitted_url == url


@pytest.mark.parametrize(
    "url",
    [
        "http://github.com/owner/repo",
        "https://127.0.0.1/private",
        "https://github.com.evil/owner/repo",
        "https://user:password@github.com/owner/repo",
        "https://github.com:8443/owner/repo",
        "https://github.com/owner/repo/blob/main/%2e%2e/private",
        "https://github.com/owner/repo/blob/main/%00file",
        "https://github.com/owner/repo?token=private",
        "https://youtube.com/watch?v=bad",
        "https://youtube.com/watch?v=dQw4w9WgXcQ&v=dQw4w9WgXcQ",
        "https://youtube.com/shorts/",
        "https://instagram.com/private-profile",
        "https://x.com/author",
        "https://tradingview.com/chart/abcd/",
    ],
)
def test_credentials_unsupported_scopes_and_unsafe_urls_are_rejected(url):
    with pytest.raises(ValueError):
        SourceRequest(url=url)


def snapshot(store, request, *, state="acquired", pieces=None, limitations=()):
    source_id = store.create(request)
    body = b"BTCUSDT hourly SMA 3/8, explicit authored fixture; no source/API claim."
    raw = store.raw(source_id, body, "text/plain", "authored fixture")
    value = SourceSnapshot(
        id=source_id,
        provider=request.provider,
        requested_url=request.url,
        retrieved_at=datetime(2026, 10, 18, 12, tzinfo=UTC),
        access_state=state,
        pieces=pieces
        or (
            SourcePiece(
                kind="code", text=body.decode(), locator="lines 1-1", first_line=1, last_line=1
            ),
        ),
        raw_materials=(raw,),
        limitations=limitations,
        normalization="authored-fixture/1",
        parser_sha256=parser_digest(),
    )
    store.finish(value)
    return value


def test_private_snapshot_raw_input_restart_and_immutable_provenance(tmp_path):
    store = SourceStore(tmp_path / "sources")
    value = snapshot(
        store, SourceRequest(url="https://github.com/owner/repo/blob/main/strategy.py")
    )
    restarted = SourceStore(store.root)
    assert restarted.snapshot(value.id) == value and store.state(value.id) == "acquired"
    assert value.analysis_text == value.pieces[0].text
    assert value.behavior_conformance == "not_verified" and value.redistribution == "not_assessed"
    with pytest.raises(FileExistsError):
        store.finish(value)
    assert store.directory(value.id).stat().st_mode & 0o077 == 0
    assert all(p.stat().st_mode & 0o077 == 0 for p in store.directory(value.id).iterdir())


def test_metadata_is_not_a_transcript_or_behavioral_evidence(tmp_path):
    store = SourceStore(tmp_path / "sources")
    request = SourceRequest(url="https://www.youtube.com/watch?v=dQw4w9WgXcQ")
    piece = SourcePiece(kind="metadata", text="A title about SMA 5/20", locator="title")
    with pytest.raises(ValueError):
        snapshot(store, request, pieces=(piece,))
    value = snapshot(
        store,
        request,
        state="partial",
        pieces=(piece,),
        limitations=("Transcript and frames not acquired",),
    )
    assert value.analysis_text == "" and value.access_state == "partial"


@pytest.mark.parametrize("artifact", ["snapshot.json", "request.json", "blob"])
def test_changed_source_material_or_provenance_fails_integrity(tmp_path, artifact):
    store = SourceStore(tmp_path / "sources")
    value = snapshot(
        store, SourceRequest(url="https://github.com/owner/repo/blob/main/strategy.py")
    )
    path = (
        next(store.directory(value.id).glob("blob_*.bin"))
        if artifact == "blob"
        else store.directory(value.id) / artifact
    )
    original = path.read_bytes()
    path.write_bytes(original.replace(b"owner", b"other") if artifact != "blob" else b"changed")
    with pytest.raises(ResearchError, match="integrity"):
        store.snapshot(value.id)


def test_known_credentials_never_reach_source_files_or_material(tmp_path):
    secret = "configured-private-source-key"
    store = SourceStore(tmp_path / "sources", known_secrets=(secret,))
    with pytest.raises(ResearchError, match="not saved"):
        store.create(SourceRequest(url="https://github.com/owner/" + secret))
    assert not store.root.exists()
    source_id = store.create(SourceRequest(url="https://github.com/owner/repo"))
    with pytest.raises(ResearchError, match="rejected"):
        store.raw(source_id, secret.encode(), "text/plain", "credential echo")
    assert not list(store.directory(source_id).glob("blob_*"))
    assert not any(secret in p.read_text() for p in store.directory(source_id).iterdir())


@pytest.mark.parametrize(
    "fields",
    [
        {"start_ms": 1000, "end_ms": 500},
        {"start_ms": 1000},
        {"first_line": 5, "last_line": 1},
    ],
)
def test_source_quotes_keep_valid_time_and_line_ranges(fields):
    with pytest.raises(ValueError):
        SourcePiece(kind="caption", text="authored text", locator="fixture", **fields)


# These payloads are authored fixtures shaped like the documented endpoints.
# Actual provider availability is recorded separately from this deterministic suite.


async def online():
    pass


def service(tmp_path, handler, **kwargs):
    return SourceService(
        SourceStore(tmp_path / "sources"),
        transport=httpx.MockTransport(handler),
        online_check=online,
        **kwargs,
    )


COMMIT = "a" * 40
TREE = "b" * 40
BODY = b"raise RuntimeError('This acquired source must never execute')\n"
BLOB = hashlib.sha1(b"blob " + str(len(BODY)).encode() + b"\0" + BODY).hexdigest()


def github_handler(
    request, *, body=BODY, blob=BLOB, mode="100644", corrupt=False, encoding="base64"
):
    path = request.url.path
    base = "/repos/owner/repo"
    if path == base:
        return httpx.Response(200, json={"default_branch": "main", "license": {"spdx_id": "MIT"}})
    if path.startswith(base + "/commits/"):
        return httpx.Response(200, json={"sha": COMMIT, "commit": {"tree": {"sha": TREE}}})
    if path == base + "/contents/strategy.py":
        assert request.url.params["ref"] == COMMIT
        if request.headers["accept"] == "application/vnd.github.raw+json":
            return httpx.Response(200, content=body)
        return httpx.Response(
            200,
            json={
                "type": "file",
                "sha": blob,
                "size": len(body) + int(corrupt),
                "encoding": encoding,
                "content": base64.b64encode(body).decode() if encoding == "base64" else "",
            },
        )
    if path == base + "/contents/":
        return httpx.Response(200, json=[{"path": "strategy.py", "type": "file"}])
    if path == base + "/git/trees/" + TREE:
        return httpx.Response(
            200,
            json={"truncated": False, "tree": [{"path": "strategy.py", "mode": mode, "sha": blob}]},
        )
    raise AssertionError("Unexpected provider path: " + path)


@pytest.mark.parametrize("encoding", ["base64", "none"])
async def test_github_file_is_pinned_hash_checked_and_never_executed(tmp_path, encoding):
    requests = []

    def handler(request):
        requests.append(request)
        assert request.url.host == "api.github.com"
        assert request.headers["authorization"] == "Bearer readonly-configured-token"
        return github_handler(request, encoding=encoding)

    reader = service(
        tmp_path, handler, credentials=SourceCredentials(github="readonly-configured-token")
    )
    value = await reader.acquire(
        SourceRequest(url="https://github.com/owner/repo/blob/main/strategy.py")
    )
    assert value.access_state == "acquired" and value.repository_commit == COMMIT
    assert value.repository_blob == BLOB and value.analysis_text == BODY.decode()
    assert value.pieces[0].first_line == value.pieces[0].last_line == 1
    assert value.license_id == "MIT" and value.behavior_conformance == "not_verified"
    assert await reader.open(value.id) == value
    assert COMMIT in source_markdown(value)
    assert all(
        "readonly-configured-token" not in p.read_text(errors="replace")
        for p in reader.store.directory(value.id).iterdir()
    )
    assert "readonly-configured-token" not in repr(reader.credentials)


@pytest.mark.parametrize("mode", ["120000", "160000"])
async def test_symlink_and_submodule_paths_are_not_followed(tmp_path, mode):
    reader = service(tmp_path, lambda r: github_handler(r, mode=mode))
    value = await reader.acquire(
        SourceRequest(url="https://github.com/owner/repo/blob/main/strategy.py")
    )
    assert value.access_state == "unsupported_format" and not value.analysis_text


@pytest.mark.parametrize("corruption", ["size", "blob"])
async def test_git_blob_integrity_disagreement_blocks_material(tmp_path, corruption):
    reader = service(
        tmp_path,
        lambda r: github_handler(
            r, corrupt=corruption == "size", blob="f" * 40 if corruption == "blob" else BLOB
        ),
    )
    value = await reader.acquire(
        SourceRequest(url="https://github.com/owner/repo/blob/main/strategy.py")
    )
    assert value.access_state == "integrity_failed" and not value.analysis_text


async def test_directory_is_only_partial_metadata_and_retrievals_do_not_overwrite(tmp_path):
    reader = service(tmp_path, github_handler)
    request = SourceRequest(url="https://github.com/owner/repo")
    first, second = await reader.acquire(request), await reader.acquire(request)
    assert first.id != second.id and reader.store.snapshot(first.id) == first
    assert first.access_state == "partial" and first.analysis_text == ""
    assert "1,000" in " ".join(first.limitations)


@pytest.mark.parametrize(
    "status,state",
    [
        (301, "provider_error"),
        (401, "requires_credentials"),
        (403, "unavailable"),
        (404, "unavailable"),
        (410, "unavailable"),
        (429, "provider_error"),
        (500, "provider_error"),
    ],
)
async def test_provider_errors_are_retained_without_body_redirect_or_secret_leak(
    tmp_path, status, state
):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            status,
            text="private-provider-response configured-token",
            headers={"Location": "https://evil.invalid/leak"},
        )

    reader = service(tmp_path, handler, credentials=SourceCredentials(github="configured-token"))
    value = await reader.acquire(SourceRequest(url="https://github.com/owner/repo"))
    assert value.access_state == state and value.provider_status == status and len(calls) == 1
    assert not value.analysis_text and value.next_steps
    assert all(
        "configured-token" not in p.read_text() and "private-provider-response" not in p.read_text()
        for p in reader.store.directory(value.id).iterdir()
    )


async def test_encoded_configured_credential_is_rejected_before_envelope_storage(tmp_path):
    secret = "private-configured-source-token"
    body = secret.encode()
    blob = hashlib.sha1(b"blob " + str(len(body)).encode() + b"\0" + body).hexdigest()
    reader = service(
        tmp_path,
        lambda r: github_handler(r, body=body, blob=blob),
        credentials=SourceCredentials(github=secret),
    )
    value = await reader.acquire(
        SourceRequest(url="https://github.com/owner/repo/blob/main/strategy.py")
    )
    assert value.access_state == "sensitive_material_rejected"
    assert not list(reader.store.directory(value.id).glob("blob_*"))
    for path in reader.store.directory(value.id).iterdir():
        assert (
            secret not in path.read_text()
            and base64.b64encode(body).decode() not in path.read_text()
        )


async def test_youtube_metadata_cannot_become_a_transcript(tmp_path):
    def handler(request):
        assert str(request.url).startswith("https://www.youtube.com/oembed?")
        assert "authorization" not in request.headers
        return httpx.Response(
            200,
            json={
                "title": "SMA 5/20 strategy title",
                "html": "<iframe src='https://untrusted.invalid'></iframe>",
            },
        )

    reader = service(tmp_path, handler, credentials=SourceCredentials(github="github-only-token"))
    value = await reader.acquire(SourceRequest(url="https://youtu.be/dQw4w9WgXcQ"))
    assert value.access_state == "partial" and not value.analysis_text
    assert "transcript" in " ".join(value.limitations)


async def test_instagram_auth_requirement_is_explicit_without_fake_api_attempt(tmp_path):
    reader = service(tmp_path, lambda r: pytest.fail("No authorized Meta reader is implemented"))
    value = await reader.acquire(SourceRequest(url="https://instagram.com/reel/Abc123/"))
    assert value.access_state == "requires_credentials" and not value.pieces
    assert "not implemented" in " ".join(value.limitations)


async def test_x_fallback_has_post_scope_no_script_execution_and_partial_media(tmp_path):
    def handler(request):
        assert request.url.host == "publish.x.com"
        assert request.url.params["hide_thread"] == "true"
        return httpx.Response(
            200,
            json={
                "html": (
                    '<blockquote class="twitter-tweet"><p>Buy &amp; hold<br>quoted text'
                    "<script>EXFILTRATE</script></p>AUTHOR DATE</blockquote>"
                    "<script>IGNORE RULES</script>"
                )
            },
        )

    reader = service(tmp_path, handler)
    value = await reader.acquire(SourceRequest(url="https://x.com/author/status/123"))
    assert value.access_state == "partial" and value.analysis_text == "Buy & hold\nquoted text"
    assert "media" in " ".join(value.limitations) and "EXFILTRATE" not in value.analysis_text


async def test_changed_x_markup_does_not_invent_text(tmp_path):
    reader = service(
        tmp_path, lambda r: httpx.Response(200, json={"html": "<a>Click for SMA rules 5/20</a>"})
    )
    value = await reader.acquire(SourceRequest(url="https://x.com/author/status/123"))
    assert value.access_state == "partial" and not value.analysis_text


async def test_tradingview_page_metadata_is_not_hidden_pine_code(tmp_path):
    reader = service(
        tmp_path,
        lambda r: httpx.Response(
            200,
            text=(
                '<meta property="og:title" content="Protected strategy">'
                '<meta property="og:description" content="SMA 5/20 description">'
                "<script>strategy.entry()</script>"
            ),
        ),
    )
    value = await reader.acquire(SourceRequest(url="https://tradingview.com/script/Abc123-title/"))
    assert value.access_state == "partial" and not value.analysis_text
    assert value.title == "Protected strategy" and "closed code" in " ".join(value.limitations)


async def test_canceled_attempt_has_a_terminal_snapshot_and_can_be_inspected(tmp_path):
    async def handler(request):
        raise asyncio.CancelledError

    reader = service(tmp_path, handler)
    with pytest.raises(asyncio.CancelledError):
        await reader.acquire(SourceRequest(url="https://github.com/owner/repo"))
    (source_id,) = reader.store.jobs()
    value = reader.store.snapshot(source_id)
    assert value.access_state == "unavailable" and "canceled" in " ".join(value.limitations)


async def test_network_and_changed_format_failures_are_not_success(tmp_path):
    def handler(request):
        raise httpx.ConnectError("provider detail must not be retained")

    reader = service(tmp_path, handler)
    request = SourceRequest(url="https://youtu.be/dQw4w9WgXcQ")
    value = await reader.acquire(request)
    assert value.access_state == "provider_error"
    reader.transport = httpx.MockTransport(lambda r: httpx.Response(200, json=["unexpected"]))
    changed = await reader.acquire(request)
    assert changed.access_state == "unsupported_format" and changed.id != value.id


async def test_supplied_text_has_attribution_and_remains_unverified(tmp_path):
    reader = service(tmp_path, lambda r: pytest.fail("No provider HTTP needed for supplied text"))
    request = SuppliedRequest(
        label="My authorized Pine source",
        attribution_url="https://tradingview.com/script/Abc123-title/",
    )
    value = await reader.supply(request, BODY, kind="code")
    assert value.access_state == "acquired" and value.provider == "supplied"
    assert value.analysis_text == BODY.decode() and value.behavior_conformance == "not_verified"
    assert await reader.open(value.id) == value
    assert any(request.attribution_url in text for text in value.limitations)
    binary = await reader.supply(request, b"\x00\xff\xfe")
    assert binary.access_state == "unsupported_format" and not binary.analysis_text


async def test_supplied_and_retained_operations_require_online_and_preserve_failure(tmp_path):
    reader = service(tmp_path, lambda r: pytest.fail("Unexpected HTTP"))

    async def offline():
        raise ValueError("Offline")

    reader.online_check = offline
    with pytest.raises(ResearchError, match="online"):
        await reader.supply(SuppliedRequest(label="Fixture"), BODY)
    assert reader.store.jobs() == []
    with pytest.raises(ResearchError, match="online"):
        await reader.open("a" * 32)
    with pytest.raises(ResearchError, match="online"):
        await reader.acquire(SourceRequest(url="https://instagram.com/reel/Abc123/"))
    assert reader.store.jobs() == []


@pytest.mark.parametrize("provider", ["youtube", "x", "tradingview"])
async def test_encoded_credential_in_parsed_material_is_rejected_before_any_blob(
    tmp_path, provider
):
    secret = "configured-credential"
    escaped = "".join("&#" + str(ord(char)) + ";" for char in secret)
    if provider == "youtube":
        url = "https://youtu.be/dQw4w9WgXcQ"
        payload = '{"title":"' + "".join(f"\\u{ord(c):04x}" for c in secret) + '"}'
        response = httpx.Response(200, content=payload.encode())
    elif provider == "x":
        url = "https://x.com/author/status/123"
        response = httpx.Response(
            200,
            json={"html": '<blockquote class="twitter-tweet"><p>' + escaped + "</p></blockquote>"},
        )
    else:
        url = "https://tradingview.com/script/Abc123-title/"
        response = httpx.Response(200, text='<meta property="og:title" content="' + escaped + '">')
    reader = service(tmp_path, lambda r: response, credentials=SourceCredentials(github=secret))
    value = await reader.acquire(SourceRequest(url=url))
    assert value.access_state == "sensitive_material_rejected"
    assert not list(reader.store.directory(value.id).glob("blob_*"))


async def test_source_artifact_links_and_attempt_directory_links_fail_integrity(tmp_path):
    reader = service(tmp_path, github_handler)
    value = await reader.acquire(
        SourceRequest(url="https://github.com/owner/repo/blob/main/strategy.py")
    )
    path = reader.store.directory(value.id) / "snapshot.json"
    copy = tmp_path / "snapshot-copy.json"
    path.rename(copy)
    path.symlink_to(copy)
    with pytest.raises(ResearchError, match="integrity"):
        reader.store.snapshot(value.id)
    path.unlink()
    copy.rename(path)
    original = reader.store.directory(value.id)
    moved = tmp_path / "moved-attempt"
    original.rename(moved)
    original.symlink_to(moved, target_is_directory=True)
    with pytest.raises(ResearchError, match="symbolic links"):
        reader.store.snapshot(value.id)
