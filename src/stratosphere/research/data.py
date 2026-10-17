"""Pinned Binance spot archive acquisition; no silent cache or fixture fallback."""

import csv
import hashlib
import io
import json
import zipfile
from dataclasses import dataclass

import httpx

from stratosphere.research.contracts import ARCHIVE_SHA256, ARCHIVE_URL, MarketBar

FIRST_OPEN_NS = 1735689600 * 10**9
BAR_COUNT = 744


@dataclass(frozen=True)
class Snapshot:
    archive: bytes
    bars: tuple[MarketBar, ...]
    normalized_sha256: str


def bars_digest(bars: tuple[MarketBar, ...]) -> str:
    return hashlib.sha256(
        json.dumps(
            [bar.model_dump(mode="json") for bar in bars], sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()


def normalize_archive(content: bytes, checksum: str) -> Snapshot:
    if checksum.split() != [ARCHIVE_SHA256, "BTCUSDT-1h-2025-01.zip"]:
        raise ValueError("Archive checksum document differs from the approved pinned snapshot")
    if hashlib.sha256(content).hexdigest() != ARCHIVE_SHA256:
        raise ValueError("Archive content differs from the approved pinned snapshot")
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        if archive.namelist() != ["BTCUSDT-1h-2025-01.csv"]:
            raise ValueError("Unexpected archive members")
        # Read the known member in memory; never extract archive paths onto the filesystem.
        text = archive.read("BTCUSDT-1h-2025-01.csv").decode("utf-8-sig")
    bars = []
    for row in csv.reader(io.StringIO(text)):
        if len(row) != 12:
            raise ValueError("Expected twelve Binance kline columns")
        if not row[0].isdigit() or not row[6].isdigit():
            raise ValueError("Expected integer microsecond timestamps")
        if not row[8].isdigit() or int(row[8]) < 0:
            raise ValueError("Invalid trade count")
        bar = MarketBar(
            open_ns=int(row[0]) * 1000,
            close_ns=int(row[6]) * 1000,
            open=row[1],
            high=row[2],
            low=row[3],
            close=row[4],
            volume=row[5],
        )
        if bar.open_ns != FIRST_OPEN_NS + len(bars) * 3600 * 10**9:
            raise ValueError("Missing, duplicated, unordered or misaligned hourly candles")
        bars.append(bar)
    if len(bars) != BAR_COUNT:
        raise ValueError("Incomplete monthly snapshot")
    result = tuple(bars)
    return Snapshot(content, result, bars_digest(result))


async def fetch_snapshot(*, transport=None) -> Snapshot:
    try:
        async with httpx.AsyncClient(
            timeout=30, follow_redirects=False, trust_env=False, transport=transport
        ) as client:
            checksum = await client.get(ARCHIVE_URL + ".CHECKSUM")
            checksum.raise_for_status()
            response = await client.get(ARCHIVE_URL)
            response.raise_for_status()
        return normalize_archive(response.content, checksum.text)
    except httpx.HTTPError:
        raise ValueError(
            "Historical download failed; no backtest was executed. Retry online."
        ) from None


async def require_online(*, transport=None) -> None:
    """Keep retained-report access an online application operation."""
    try:
        async with httpx.AsyncClient(
            timeout=15, follow_redirects=False, trust_env=False, transport=transport
        ) as client:
            response = await client.get(ARCHIVE_URL + ".CHECKSUM")
            response.raise_for_status()
    except httpx.HTTPError:
        raise ValueError(
            "Online access is required to inspect research in the application."
        ) from None
