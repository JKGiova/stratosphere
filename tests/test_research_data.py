import io
import zipfile

import httpx
import pytest

from stratosphere.research.contracts import MarketBar
from stratosphere.research.data import bars_digest, fetch_snapshot, normalize_archive


def test_authored_archive_normalizes_microseconds_and_precision(research_snapshot, market_bars):
    assert research_snapshot.bars == market_bars
    assert research_snapshot.normalized_sha256 == bars_digest(market_bars)
    assert len(market_bars) == 744
    assert market_bars[0].close_ns < market_bars[1].open_ns


@pytest.mark.parametrize(
    "change",
    [
        {"high": "1"},
        {"volume": "-1"},
        {"close": "0"},
        {"open": "NaN"},
        {"open": "1.000000001"},
        {"close_ns": 1735693200000000000},
    ],
)
def test_invalid_candles_are_rejected(market_bars, change):
    with pytest.raises(ValueError):
        MarketBar.model_validate(market_bars[0].model_dump() | change)


def mutate_archive(content, change):
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        rows = archive.read(archive.namelist()[0]).decode().splitlines()
    rows = change(rows)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("BTCUSDT-1h-2025-01.csv", "\n".join(rows))
    return output.getvalue()


@pytest.mark.parametrize(
    "change",
    [
        lambda rows: rows[:-1],
        lambda rows: [rows[0], *rows],
        lambda rows: [rows[1], rows[0], *rows[2:]],
        lambda rows: [rows[0].replace("1735689600000000", "1735689600000"), *rows[1:]],
        lambda rows: [rows[0] + ",extra", *rows[1:]],
    ],
)
def test_missing_duplicate_unordered_wrong_units_and_malformed_data_fail(
    research_snapshot, pin_archive, change
):
    content = mutate_archive(research_snapshot.archive, change)
    with pytest.raises(ValueError):
        normalize_archive(content, pin_archive(content))


def test_checksum_mismatch_and_unexpected_zip_paths_fail(research_snapshot, pin_archive):
    with pytest.raises(ValueError, match="checksum"):
        normalize_archive(research_snapshot.archive, "0" * 64 + "  BTCUSDT-1h-2025-01.zip")
    checksum = pin_archive(research_snapshot.archive)
    with pytest.raises(ValueError, match="content"):
        normalize_archive(research_snapshot.archive + b"changed", checksum)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("../escape.csv", "untrusted")
    with pytest.raises(ValueError, match="members"):
        normalize_archive(output.getvalue(), pin_archive(output.getvalue()))


@pytest.mark.asyncio
async def test_online_fetch_verifies_both_files_and_does_not_follow_redirects(
    research_snapshot, pin_archive
):
    checksum = pin_archive(research_snapshot.archive)
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path.endswith(".CHECKSUM"):
            return httpx.Response(200, text=checksum)
        return httpx.Response(200, content=research_snapshot.archive)

    result = await fetch_snapshot(transport=httpx.MockTransport(handler))
    assert result.bars == research_snapshot.bars and len(requests) == 2
    assert all(request.url.host == "data.binance.vision" for request in requests)
    assert all("authorization" not in request.headers for request in requests)
    with pytest.raises(ValueError, match="download failed"):
        await fetch_snapshot(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(302, headers={"location": "https://other.invalid"})
            )
        )


@pytest.mark.asyncio
async def test_retained_reports_have_no_offline_application_mode():
    from stratosphere.research.data import require_online

    requests = []

    def online(request):
        requests.append(request)
        return httpx.Response(200, text="checksum")

    await require_online(transport=httpx.MockTransport(online))
    assert len(requests) == 1 and requests[0].url.path.endswith(".CHECKSUM")
    with pytest.raises(ValueError, match="Online access"):
        await require_online(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
