"""One read-only ECB historical integration for the Phase 0 proof."""

import argparse
import csv
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path

import httpx

from stratosphere.phase0.contracts import DatasetManifest, decimal_text, load_contract

EXAMPLES = Path(__file__).resolve().parents[3] / "examples/phase0"


@dataclass(frozen=True)
class Observation:
    date: str
    rate: str
    available_ns: int


def reference_availability_ns(day: str) -> int:
    available = datetime.combine(date.fromisoformat(day), time(17), tzinfo=UTC)
    return int(available.timestamp()) * 10**9


def observation_digest(observations: tuple[Observation, ...]) -> str:
    content = json.dumps(
        [{"date": item.date, "rate": decimal_text(item.rate)} for item in observations],
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(content).hexdigest()


def normalize_ecb(content: bytes, manifest: DatasetManifest) -> tuple[Observation, ...]:
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    required = {
        "KEY",
        "FREQ",
        "CURRENCY",
        "CURRENCY_DENOM",
        "TIME_PERIOD",
        "OBS_VALUE",
        "OBS_STATUS",
    }
    if not reader.fieldnames or not required.issubset(reader.fieldnames):
        raise ValueError("Missing ECB columns")
    if len(reader.fieldnames) != len(set(reader.fieldnames)):
        raise ValueError("Duplicate ECB header")
    observations = []
    for row in reader:
        if None in row or any(row.get(key) is None for key in required):
            raise ValueError("Malformed ECB row")
        if (row["KEY"], row["FREQ"], row["CURRENCY"], row["CURRENCY_DENOM"]) != (
            "EXR.D.USD.EUR.SP00.A",
            "D",
            "USD",
            "EUR",
        ) or row["OBS_STATUS"] != "A":
            raise ValueError("Unexpected series or unaccepted observation status")
        day = date.fromisoformat(row["TIME_PERIOD"])
        if day.isoformat() != row["TIME_PERIOD"] or day.weekday() >= 5:
            raise ValueError("Invalid reference date")
        rate = decimal_text(row["OBS_VALUE"])
        if Decimal(rate) <= 0:
            raise ValueError("Reference rate must be positive")
        observations.append(
            Observation(day.isoformat(), rate, reference_availability_ns(day.isoformat()))
        )
    dates = tuple(item.date for item in observations)
    if dates != manifest.expected_dates:
        raise ValueError("Missing, duplicate, unexpected or out-of-order observation dates")
    if observation_digest(tuple(observations)) != manifest.content_sha256:
        raise ValueError("Historical observation checksum changed")
    return tuple(observations)


def fetch_ecb(manifest: DatasetManifest, *, transport=None) -> bytes:
    expected = (
        "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A"
        "?startPeriod=2025-01-01&endPeriod=2025-03-31&format=csvdata"
    )
    if manifest.source_url != expected:
        raise ValueError("Phase 0 fetch supports only the declared fixed ECB request")
    with httpx.Client(timeout=30, follow_redirects=False, transport=transport) as client:
        response = client.get(manifest.source_url)
        response.raise_for_status()
        content = response.content
    normalize_ecb(content, manifest)
    return content


def quality_report(content: bytes, manifest: DatasetManifest) -> dict:
    observations = normalize_ecb(content, manifest)
    return {
        "schema_version": "data-proof/1",
        "provider": "ECB",
        "attribution": manifest.attribution,
        "source_url": manifest.source_url,
        "raw_response_sha256": hashlib.sha256(content).hexdigest(),
        "normalized_sha256": manifest.content_sha256,
        "observations": len(observations),
        "first_date": observations[0].date,
        "last_date": observations[-1].date,
        "missing_dates": [],
        "duplicate_dates": [],
        "unaccepted_statuses": [],
        "availability_verified": False,
        "limitations": [
            "Reference rates are not tradable quotes.",
            "17:00 UTC availability is an assumption, not retained publication evidence.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true", help="Verify a fresh online response")
    parser.add_argument("--output", type=Path, default=Path("reports/data-proof.json"))
    args = parser.parse_args()
    manifest = load_contract(EXAMPLES / "dataset.json", DatasetManifest)
    content = fetch_ecb(manifest) if args.fetch else (EXAMPLES / "ecb-reference.csv").read_bytes()
    report = quality_report(content, manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
