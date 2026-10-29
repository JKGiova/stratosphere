"""PDF intake retaining original bytes and exact physical-page text, without hosted upload."""

import asyncio
import json
import os
import sys

from stratosphere.research.store import ResearchError
from stratosphere.sources.contracts import SourcePiece
from stratosphere.sources.pdf_worker import MAX_BYTES


async def pdf_pages(content):
    if len(content) > MAX_BYTES or not content.startswith(b"%PDF-"):
        raise ResearchError("Use a PDF of at most 32 MiB")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "stratosphere.sources.pdf_worker",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        env={"PATH": os.defpath, "PYTHONUTF8": "1", "PYTHONNOUSERSITE": "1"},
    )
    try:
        async with asyncio.timeout(30):
            output, _ = await process.communicate(content)
        if process.returncode or len(output) > 16_000_000:
            raise ResearchError("PDF worker failed; no partial extraction published")
        result = json.loads(output)
        if "error" in result:
            raise ResearchError(result["error"])
        return result["pages"]
    except TimeoutError, asyncio.CancelledError:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise


async def supply_pdf(service, request, content, *, identity=None, retrieval_chain=()):
    await service.require_online()
    identity = identity or service.store.create(request)
    try:
        pages = await pdf_pages(content)
        pieces = tuple(
            SourcePiece(kind="text", text=p["text"], locator=f"PDF physical page {p['page']}")
            for p in pages
            if p["text"].strip()
        )
        if service.store.has_secret(json.dumps(pages).encode()):
            raise ResearchError("Credential-bearing PDF text rejected")
        raw = service.store.raw(identity, content, "application/pdf", "user-supplied PDF")
        empty = [p["page"] for p in pages if not p["text"].strip()]
        fields = dict(
            access_state="partial" if empty else "acquired",
            title=request.label,
            pieces=pieces,
            raw_materials=(raw,),
            limitations=(
                "PDF publication identity and behavioral conformance are unverified.",
                "Text extraction does not verify equations, tables or reading order; no OCR.",
                "Physical pages without extractable text: " + (str(empty) if empty else "none"),
                "Retrieval: " + (" -> ".join(retrieval_chain) or "user-supplied PDF"),
            ),
        )
    except asyncio.CancelledError:
        service._finish(
            identity,
            request,
            dict(
                access_state="unavailable",
                limitations=("PDF extraction canceled; no late result is published.",),
            ),
            normalization="pdf-pages/1",
        )
        raise
    except ResearchError, TimeoutError, ValueError, KeyError:
        fields = dict(
            access_state="unsupported_format",
            limitations=("PDF extraction failed; supply an unlocked text PDF or plain text.",),
        )
    return service._finish(identity, request, fields, normalization="pdf-pages/1")
