"""Checksum-only proposal acquisition, followed by exact monthly snapshot validation."""

import csv
import hashlib
import io
import zipfile

import httpx

from stratosphere.research.contracts import MarketBar
from stratosphere.research.data import Snapshot, bars_digest
from stratosphere.studies.contracts import HOUR_NS, ArchivePin


async def pin_month(month: str, *, transport=None) -> ArchivePin:
    candidate = ArchivePin(month=month, sha256="0" * 64)
    try:
        async with httpx.AsyncClient(
            timeout=30, follow_redirects=False, trust_env=False, transport=transport
        ) as client:
            response = await client.get(candidate.url + ".CHECKSUM")
            response.raise_for_status()
        parts = response.text.split()
        if len(parts) != 2 or parts[1] != candidate.filename:
            raise ValueError("Unexpected monthly checksum document")
        return ArchivePin(month=month, sha256=parts[0])
    except httpx.HTTPError:
        raise ValueError("Cannot pin the monthly archive online; no study was approved.") from None


def normalize_month(content: bytes, pin: ArchivePin) -> Snapshot:
    if hashlib.sha256(content).hexdigest() != pin.sha256:
        raise ValueError("Monthly archive differs from the reviewed checksum")
    member = pin.filename.removesuffix(".zip") + ".csv"
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        if archive.namelist() != [member]:
            raise ValueError("Unexpected monthly archive members")
        text = archive.read(member).decode("utf-8-sig")
    bars = []
    for row in csv.reader(io.StringIO(text)):
        if len(row) != 12 or not row[0].isdigit() or not row[6].isdigit() or not row[8].isdigit():
            raise ValueError("Malformed monthly kline row")
        bar = MarketBar(
            open_ns=int(row[0]) * 1000,
            close_ns=int(row[6]) * 1000,
            **dict(zip(("open", "high", "low", "close", "volume"), row[1:6], strict=True)),
        )
        if bar.open_ns != pin.first_ns + len(bars) * HOUR_NS:
            raise ValueError("Missing, duplicate, unordered or misaligned monthly candles")
        bars.append(bar)
    if len(bars) != pin.count:
        raise ValueError("Incomplete monthly archive")
    normalized = tuple(bars)
    return Snapshot(content, normalized, bars_digest(normalized))


async def fetch_month(pin: ArchivePin, *, transport=None) -> Snapshot:
    # Recheck the public pin: do not silently serve a cached month after a provider revision.
    if await pin_month(pin.month, transport=transport) != pin:
        raise ValueError("The provider revised this archive; review a new protocol")
    try:
        async with httpx.AsyncClient(
            timeout=30, follow_redirects=False, trust_env=False, transport=transport
        ) as client:
            response = await client.get(pin.url)
            response.raise_for_status()
        return normalize_month(response.content, pin)
    except httpx.HTTPError:
        raise ValueError("Monthly archive unavailable; no substitute data was used.") from None
