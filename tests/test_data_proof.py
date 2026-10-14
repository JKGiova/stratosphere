import csv
import io

import httpx
import pytest

from stratosphere.phase0.contracts import DatasetManifest, load_contract
from stratosphere.phase0.data_proof import EXAMPLES, fetch_ecb, normalize_ecb, quality_report


@pytest.fixture
def manifest():
    return load_contract(EXAMPLES / "dataset.json", DatasetManifest)


def test_real_snapshot_dates_and_availability(manifest):
    raw = (EXAMPLES / "ecb-reference.csv").read_bytes()
    observations = normalize_ecb(raw, manifest)
    assert len(observations) == 63
    assert observations[0].available_ns == 1735837200000000000
    assert observations[0].rate == "1.0321"
    assert observations[-1].date == "2025-03-31"
    report = quality_report(raw, manifest)
    assert report["raw_response_sha256"] == (
        "cb09229025b2aee48d7192ef6a633062538015e73d7d479e1a767094e53dc4e1"
    )
    assert not report["availability_verified"]


@pytest.mark.parametrize(
    "corruption",
    [
        "missing",
        "duplicate",
        "ordering",
        "changed_rate",
        "invalid_rate",
        "status",
        "series",
        "date",
    ],
)
def test_data_corruption_blocks_run(manifest, corruption):
    raw = (EXAMPLES / "ecb-reference.csv").read_text()
    reader = csv.DictReader(io.StringIO(raw))
    rows = list(reader)
    match corruption:
        case "missing":
            rows.pop(10)
        case "duplicate":
            rows[10] = rows[9]
        case "ordering":
            rows[10], rows[11] = rows[11], rows[10]
        case "changed_rate":
            rows[10]["OBS_VALUE"] = "1.0001"
        case "invalid_rate":
            rows[10]["OBS_VALUE"] = "NaN"
        case "status":
            rows[10]["OBS_STATUS"] = "E"
        case "series":
            rows[10]["CURRENCY"] = "GBP"
        case "date":
            rows[10]["TIME_PERIOD"] = "2025-01-04"
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=reader.fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    with pytest.raises(ValueError):
        normalize_ecb(buffer.getvalue().encode(), manifest)


def test_online_adapter_validates_response_and_fails_on_outage(manifest):
    content = (EXAMPLES / "ecb-reference.csv").read_bytes()
    transport = httpx.MockTransport(lambda _: httpx.Response(200, content=content))
    assert fetch_ecb(manifest, transport=transport) == content
    with pytest.raises(httpx.HTTPStatusError):
        fetch_ecb(manifest, transport=httpx.MockTransport(lambda _: httpx.Response(503)))

    def timeout(_):
        raise httpx.ReadTimeout("Provider unavailable")

    with pytest.raises(httpx.ReadTimeout):
        fetch_ecb(manifest, transport=httpx.MockTransport(timeout))
    with pytest.raises(ValueError):
        fetch_ecb(manifest, transport=httpx.MockTransport(lambda _: httpx.Response(200, text="{}")))
