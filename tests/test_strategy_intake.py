"""Real PDF parsing and source-grounded intake, with authored HTTP/model fixtures."""

import asyncio
import json

import httpx
import pytest

from stratosphere.models.streaming import ModelError
from stratosphere.research.store import ResearchError
from stratosphere.sources.contracts import SuppliedRequest
from stratosphere.sources.documents import supply_pdf
from stratosphere.sources.service import SourceService
from stratosphere.sources.store import SourceStore
from stratosphere.sources.web import PublicWeb, WebRequest, public_addresses
from stratosphere.workflow.discovery import StrategyDiscovery, brief_markdown


def pdf_fixture(texts):
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    kids = []
    for text in texts:
        page = len(objects) + 1
        kids.append(f"{page} 0 R")
        objects.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 600 800] "
                f"/Resources << /Font << /F1 3 0 R >> >> /Contents {page + 1} 0 R >>"
            ).encode()
        )
        stream = f"BT /F1 12 Tf 40 740 Td ({text}) Tj ET".encode()
        objects.append(
            b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream"
        )
    objects[1] = f"<< /Type /Pages /Count {len(texts)} /Kids [{' '.join(kids)}] >>".encode()
    data = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for i, obj in enumerate(objects, 1):
        offsets.append(len(data))
        data.extend(f"{i} 0 obj\n".encode() + obj + b"\nendobj\n")
    start = len(data)
    data.extend(f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode())
    for offset in offsets[1:]:
        data.extend(f"{offset:010} 00000 n \n".encode())
    data.extend(
        f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{start}\n%%EOF\n".encode()
    )
    return bytes(data)


async def online():
    pass


@pytest.fixture
def sources(tmp_path):
    return SourceService(SourceStore(tmp_path / "sources"), online_check=online)


class Replies:
    def __init__(self, *values):
        self.values = iter(values)
        self.requests = []

    async def stream(self, messages):
        self.requests.append(messages)
        value = next(self.values)
        if isinstance(value, BaseException):
            raise value
        yield json.dumps(value)


def reading(indices=(0,)):
    return {
        "name": "RSI reversal",
        "summary": "Tentative RSI strategy",
        "rules": [
            {
                "category": "entry",
                "interpretation": "Buy below RSI 30",
                "evidence_indices": list(indices),
            }
        ],
        "questions": ["When should the position exit?"],
    }


async def test_actual_pdf_worker_retains_bytes_pages_and_unicode_safe_quotes(sources):
    content = pdf_fixture(["Enter when RSI is below 30.", "Exit when RSI exceeds 70."])
    snapshot = await supply_pdf(sources, SuppliedRequest(label="paper.pdf"), content)
    assert snapshot.access_state == "acquired"
    assert snapshot.pieces[0].locator == "PDF physical page 1"
    assert snapshot.pieces[1].locator == "PDF physical page 2"
    assert "RSI is below 30" in snapshot.pieces[0].text
    assert sources.store.snapshot(snapshot.id) == snapshot
    raw = snapshot.raw_materials[0]
    assert (sources.store.directory(snapshot.id) / f"blob_{raw.sha256}.bin").read_bytes() == content


async def test_image_only_pdf_reports_missing_text_without_invention(sources):
    snapshot = await supply_pdf(sources, SuppliedRequest(label="scan.pdf"), pdf_fixture([""]))
    assert snapshot.access_state == "partial" and not snapshot.pieces
    assert "[1]" in " ".join(snapshot.limitations)


@pytest.mark.parametrize("content", [b"not a PDF", b"%PDF-1.4 corrupt"])
async def test_invalid_pdf_keeps_failed_attempt_without_text(sources, content):
    snapshot = await supply_pdf(sources, SuppliedRequest(label="bad.pdf"), content)
    assert snapshot.access_state == "unsupported_format" and not snapshot.pieces
    assert sources.store.snapshot(snapshot.id) == snapshot


async def test_known_secret_in_compressed_pdf_text_is_not_published(sources):
    sources.store.known_secrets = ("fixture-secret-value",)
    snapshot = await supply_pdf(
        sources, SuppliedRequest(label="private.pdf"), pdf_fixture(["fixture-secret-value"])
    )
    assert not snapshot.pieces and not snapshot.raw_materials
    assert not list(sources.store.directory(snapshot.id).glob("blob_*"))


async def test_pdf_worker_cancellation_cannot_publish_late_pages(sources, monkeypatch):
    entered = asyncio.Event()

    async def blocked(content):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("stratosphere.sources.documents.pdf_pages", blocked)
    task = asyncio.create_task(supply_pdf(sources, SuppliedRequest(label="paper.pdf"), b"%PDF-"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    identity = sources.store.jobs()[0]
    snapshot = sources.store.snapshot(identity)
    assert snapshot.access_state == "unavailable" and not snapshot.pieces


async def test_pdf_to_strategy_intake_constructs_page_citations(sources, tmp_path):
    path = tmp_path / "paper with spaces.pdf"
    path.write_bytes(pdf_fixture(["Buy when RSI is below 30."]))
    # Passage zero belongs to the user's brief, passage one to the PDF.
    provider = Replies(reading((1,)))
    service = StrategyDiscovery(sources)
    result = await service.create("Extract RSI rules", provider, "fixture", pdf=path)
    ref = result["rules"][0]["references"][0]
    assert ref["locator"] == "PDF physical page 1"
    assert "Buy when RSI" in ref["text"] and result["execution"] == "blocked"
    assert result["status"] == "needs_clarification"
    assert "missing risk" in " ".join(result["questions"])
    assert "SMA" not in result["summary"]
    assert service.open(result["id"]) == result
    assert "PDF physical page 1" in brief_markdown(result)


async def test_unavailable_citation_rejected_and_failed_attempt_retained(sources):
    snapshot = await sources.supply(SuppliedRequest(label="Source"), b"RSI below 30")
    service = StrategyDiscovery(sources)
    with pytest.raises(ModelError, match="unavailable evidence"):
        await service.create(
            "Extract RSI", Replies(reading((999,))), "fixture", source_id=snapshot.id
        )
    identity = service.store.jobs()[0]
    assert service.store.state(identity) == "failed"
    assert not (service.store.directory(identity) / "brief.json").exists()


async def test_source_or_brief_tampering_blocks_reader(sources):
    snapshot = await sources.supply(SuppliedRequest(label="Source"), b"RSI below 30")
    service = StrategyDiscovery(sources)
    result = await service.create(
        "Extract RSI", Replies(reading((1,))), "fixture", source_id=snapshot.id
    )
    path = service.store.directory(result["id"]) / "brief.json"
    value = json.loads(path.read_text())
    value["rules"] = []
    path.write_text(json.dumps(value))
    with pytest.raises(ResearchError, match="brief changed"):
        service.open(result["id"])


@pytest.mark.parametrize(
    "url",
    [
        "https://localhost/a",
        "https://127.0.0.1/a",
        "http://example.org/a",
        "https://name:password@example.org/a",
        "https://example.org/a?api_key=secret",
    ],
)
def test_private_or_credential_bearing_web_urls_rejected(url):
    with pytest.raises(ValueError):
        WebRequest(url=url)


async def test_private_dns_address_is_rejected(monkeypatch):
    async def resolve(*args, **kwargs):
        return [(2, 1, 6, "", ("192.168.1.1", 443))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolve)
    with pytest.raises(ResearchError, match="nonpublic"):
        await public_addresses("example.org")


async def no_dns(host):
    pass


async def test_public_page_retains_original_but_ignores_scripts(sources):
    html = b"<html><script>steal credentials</script><p>Buy below RSI 30</p></html>"
    sources.transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=html, headers={"content-type": "text/html"})
    )
    snapshot = await PublicWeb(sources, resolver=no_dns).acquire(
        WebRequest(url="https://example.org/strategy")
    )
    assert snapshot.provider == "web" and snapshot.access_state == "acquired"
    assert "steal credentials" not in snapshot.analysis_text
    assert sources.store.snapshot(snapshot.id) == snapshot


async def test_redirect_is_not_followed_to_private_target(sources):
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(302, headers={"location": "https://127.0.0.1/secret"})

    sources.transport = httpx.MockTransport(handler)
    snapshot = await PublicWeb(sources, resolver=no_dns).acquire(
        WebRequest(url="https://example.org/a")
    )
    assert snapshot.access_state == "unavailable" and len(seen) == 1


async def test_search_is_bounded_and_selection_cannot_invent_urls(sources):
    seen = []

    def handler(request):
        seen.append(str(request.url))
        if request.url.host == "en.wikipedia.org":
            if request.url.path.endswith("api.php"):
                return httpx.Response(200, json={"query": {"search": [{"title": "RSI"}]}})
            return httpx.Response(200, text="RSI below 30", headers={"content-type": "text/plain"})
        return httpx.Response(200, json={"message": {"items": []}})

    sources.transport = httpx.MockTransport(handler)
    service = StrategyDiscovery(sources, web=PublicWeb(sources, resolver=no_dns))
    result = await service.create(
        "RSI strategy",
        Replies({"query": "RSI trading strategy"}, {"indices": [0]}, reading((1,))),
        "fixture",
    )
    assert len(seen) == 3 and "wikipedia.org" in result["sources"][1]["url"]
    assert result["execution"] == "blocked"
    assert (service.store.directory(result["id"]) / "search.json").exists()
    with pytest.raises(ModelError, match="Invalid source selection"):
        await service.create(
            "RSI strategy", Replies({"query": "RSI trading strategy"}, {"indices": [99]}), "fixture"
        )


async def test_checked_public_peer_is_pinned_with_original_tls_hostname(monkeypatch):
    from stratosphere.sources.web import PublicTransport

    seen = []

    async def resolve(host):
        assert host == "example.org"
        return ["93.184.216.34"]

    async def capture(self, request):
        seen.append((request.url.host, request.headers["Host"], request.extensions["sni_hostname"]))
        return httpx.Response(200, text="public")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", capture)
    async with PublicTransport(resolve) as transport:
        await transport.handle_async_request(httpx.Request("GET", "https://example.org/paper"))
    assert seen == [("93.184.216.34", "example.org", "example.org")]


async def test_public_redirect_preserves_full_chain(sources):
    seen = []

    def handler(request):
        seen.append(str(request.url))
        if request.url.host == "doi.org":
            return httpx.Response(302, headers={"location": "https://example.org/paper"})
        return httpx.Response(200, text="Buy below RSI 30", headers={"content-type": "text/plain"})

    sources.transport = httpx.MockTransport(handler)
    snapshot = await PublicWeb(sources, resolver=no_dns).acquire(
        WebRequest(url="https://doi.org/10.1/example")
    )
    assert snapshot.access_state == "acquired" and len(seen) == 2
    assert "doi.org/10.1/example -> https://example.org/paper" in " ".join(snapshot.limitations)


async def test_followup_reuses_sealed_sources_and_records_parent(sources):
    source = await sources.supply(SuppliedRequest(label="Paper"), b"Buy when RSI is below 30.")
    service = StrategyDiscovery(sources)
    first = await service.create(
        "Read this paper", Replies(reading((1,))), "fixture", source_id=source.id
    )
    second = await service.create(
        "Read this paper\nUser clarification: Exit above RSI 70",
        Replies(reading((0,))),
        "fixture",
        prior=first["id"],
    )
    assert second["parent_intake_id"] == first["id"]
    assert source.id in {pin["id"] for pin in second["sources"]}
    assert service.open(first["id"]) == first
    assert service.open(second["id"]) == second


@pytest.mark.parametrize(
    "text,action",
    [
        ('/paper-read "/tmp/a paper.pdf" extract RSI', "paper-read"),
        ('/paper-read "unterminated', "message"),
        ('/paper-read ""', "message"),
        ("/discover explain turtle trading", "discover"),
        ("/intake " + "a" * 32 + " extract rules", "intake"),
        ("/intake-show " + "a" * 32, "intake-show"),
        ("/intake ../private", "message"),
        ("/intake-show " + "a" * 32 + " extra", "message"),
    ],
)
def test_intake_command_arguments(text, action):
    from stratosphere.commands import route_command

    assert route_command(text).action == action
