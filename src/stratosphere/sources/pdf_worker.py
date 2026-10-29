"""Bounded, credential-free PDF text worker; embedded actions and attachments are never run."""

import io
import json
import sys

MAX_BYTES = 32 * 1024 * 1024
MAX_PAGES = 250
MAX_TEXT = 2_000_000


def extract(content):
    from pypdf import PdfReader

    if not content.startswith(b"%PDF-") or len(content) > MAX_BYTES:
        raise ValueError("Use a PDF of at most 32 MiB")
    reader = PdfReader(io.BytesIO(content), strict=True)
    if reader.is_encrypted:
        raise ValueError("Encrypted PDFs are unsupported; supply an unlocked copy")
    if not 1 <= len(reader.pages) <= MAX_PAGES:
        raise ValueError("PDF page budget exceeded (250 pages)")
    pages, total = [], 0
    for index, page in enumerate(reader.pages, 1):
        text = page.extract_text() or ""
        total += len(text)
        if total > MAX_TEXT or "\0" in text:
            raise ValueError("PDF text budget exceeded or invalid extracted text")
        pages.append({"page": index, "text": text})
    return pages


def main():
    import resource

    resource.setrlimit(resource.RLIMIT_CPU, (20, 20))
    if sys.platform == "linux":
        resource.setrlimit(resource.RLIMIT_AS, (2 * 1024**3, 2 * 1024**3))
    try:
        pages = extract(sys.stdin.buffer.read(MAX_BYTES + 1))
        result = {"pages": pages}
    except Exception:
        result = {"error": "PDF extraction failed: invalid, encrypted or over-budget PDF"}
    sys.stdout.write(json.dumps(result))


if __name__ == "__main__":
    main()
