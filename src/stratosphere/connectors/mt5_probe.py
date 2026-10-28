"""Inspect a bounded, credential-free MT5 diagnostic export. No terminal/order transport."""

import argparse
import csv
import hashlib
import io
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

SCHEMA = "stratosphere.mt5.readonly.v1"
FRAMES = ("PERIOD_M15", "PERIOD_H1", "PERIOD_H4", "PERIOD_D1", "PERIOD_W1")
HISTORY_SCHEMA = "stratosphere.mt5.history.v2"
HISTORY_FRAMES = tuple(
    f"PERIOD_{name}"
    for name in (
        "M1",
        "M2",
        "M3",
        "M4",
        "M5",
        "M6",
        "M10",
        "M12",
        "M15",
        "M20",
        "M30",
        "H1",
        "H2",
        "H3",
        "H4",
        "H6",
        "H8",
        "H12",
        "D1",
        "W1",
        "MN1",
    )
)
HEADER = ["server_epoch", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]


def _read(root: Path, name: str, limit: int = 1_000_000) -> bytes:
    path = root / name
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError(f"Missing, linked or oversized probe file: {name}")
    return path.read_bytes()


def _integer(value: str) -> int:
    if (
        not value
        or not value.removeprefix("-").isascii()
        or not (value.removeprefix("-").isdigit())
    ):
        raise ValueError("Invalid probe integer")
    return int(value)


def inspect_probe(root: Path) -> tuple[dict, dict[str, bytes]]:
    """Return validated findings and exact raw bytes; validation grants no account authority."""
    if root.is_symlink() or not root.is_dir():
        raise ValueError("Probe source must be an existing ordinary directory")
    raw = {"complete.txt": _read(root, "complete.txt"), "metadata.tsv": _read(root, "metadata.tsv")}
    schema = raw["complete.txt"].decode("utf-8").strip()
    if schema not in {SCHEMA, HISTORY_SCHEMA}:
        raise ValueError("Probe completion marker missing or unsupported")
    metadata = {}
    for row in csv.reader(io.StringIO(raw["metadata.tsv"].decode("utf-8")), delimiter="\t"):
        if len(row) != 2 or row[0] in metadata:
            raise ValueError("Malformed or duplicate metadata field")
        metadata[row[0]] = row[1]
    required = {
        "schema",
        "captured_utc_epoch",
        "finished_utc_epoch",
        "connected",
        "account_mode",
        "company",
        "server",
        "symbol",
        "time_semantics",
        "profile_verification",
    }
    if not required <= metadata.keys() or metadata["schema"] != schema:
        raise ValueError("Incomplete or unsupported probe metadata")
    captured = _integer(metadata["captured_utc_epoch"])
    finished = _integer(metadata["finished_utc_epoch"])
    if captured <= 0 or finished < captured or metadata["connected"] not in {"0", "1"}:
        raise ValueError("Invalid probe capture times or connection status")
    findings = []
    if metadata["connected"] != "1":
        findings.append("Terminal reported disconnected at capture")
    if metadata["account_mode"] != "ACCOUNT_TRADE_MODE_DEMO":
        findings.append("Account is not reported as demo; FTMO trial not verified")
    if "ftmo" not in (metadata["company"] + " " + metadata["server"]).lower():
        findings.append("Company/server do not identify FTMO")
    if not metadata["symbol"]:
        findings.append("Gold symbol missing or ambiguous")
    age = metadata.get("quote_server_age_seconds")
    if age is None or not 0 <= _integer(age) <= 60:
        findings.append("Gold quote missing, future-dated or older than 60 seconds at capture")
    history = schema == HISTORY_SCHEMA
    budget = 512
    if history:
        budget = _integer(metadata.get("bar_budget_per_interval", "0"))
        terminal_limit = _integer(metadata.get("terminal_max_bars", "0"))
        if not 1 <= budget <= min(100_000, terminal_limit):
            raise ValueError("Invalid history acquisition budget")
        if metadata.get("attempt_budget_per_interval") != "3":
            raise ValueError("Invalid history attempt budget")
    coverage = {}
    for frame in HISTORY_FRAMES if history else FRAMES:
        count = _integer(metadata.get(f"{frame}.copy_count", "-1"))
        if not -1 <= count <= budget:
            raise ValueError("Probe sample exceeds its fixed budget")
        if count <= 0:
            findings.append(f"{frame}: no retrieved sample")
            continue
        name = f"{frame}.tsv"
        raw[name] = _read(root, name, 32_000_000 if history else 1_000_000)
        rows = list(csv.reader(io.StringIO(raw[name].decode("utf-8")), delimiter="\t"))
        if not rows or rows[0] != HEADER or len(rows) - 1 != count:
            raise ValueError(f"{frame}: unexpected bar schema/count")
        previous = -1
        for row in rows[1:]:
            if len(row) != len(HEADER):
                raise ValueError(f"{frame}: malformed bar")
            timestamp = _integer(row[0])
            try:
                prices = [Decimal(value) for value in row[1:5]]
            except InvalidOperation as exc:
                raise ValueError(f"{frame}: invalid price") from exc
            if any(not price.is_finite() or price <= 0 for price in prices):
                raise ValueError(f"{frame}: nonpositive/nonfinite price")
            opening, high, low, closing = prices
            if low > min(opening, closing) or high < max(opening, closing) or low > high:
                raise ValueError(f"{frame}: inconsistent OHLC")
            if timestamp <= previous or any(_integer(value) < 0 for value in row[5:]):
                raise ValueError(f"{frame}: unordered bars or negative volume/spread")
            previous = timestamp
        coverage[frame] = {
            "retrieved_bars": count,
            "first_server_epoch": _integer(rows[1][0]),
            "last_server_epoch": previous,
            "reported_local_bars": metadata.get(f"{frame}.local_bars"),
            "reported_server_first_epoch": metadata.get(f"{frame}.server_first_epoch"),
            "copy_error": metadata.get(f"{frame}.copy_error"),
            "synchronized": metadata.get(f"{frame}.synchronized"),
        }
        if history:
            local_bars = _integer(metadata.get(f"{frame}.local_bars", "0"))
            coverage[frame].update(
                {
                    "first_server_label": datetime.fromtimestamp(
                        _integer(rows[1][0]), UTC
                    ).strftime("%Y-%m-%d %H:%M"),
                    "last_server_label": datetime.fromtimestamp(previous, UTC).strftime(
                        "%Y-%m-%d %H:%M"
                    ),
                    "span_days": round((previous - _integer(rows[1][0])) / 86400, 2),
                    "bar_budget_reached": count == budget,
                    "terminal_series_at_budget": local_bars >= budget,
                    "maximum_history_verified": False,
                }
            )
            if count == budget:
                findings.append(f"{frame}: bar budget reached; older history may exist")
            elif local_bars >= budget:
                findings.append(
                    f"{frame}: terminal series reached bar budget; older history may exist"
                )
        if metadata.get(f"{frame}.copy_error") != "0":
            findings.append(f"{frame}: MT5 reported a CopyRates error")
        if metadata.get(f"{frame}.synchronized") != "1":
            findings.append(f"{frame}: history not synchronized")
    report = {
        "schema": schema,
        "metadata": metadata,
        "coverage": coverage,
        "findings": findings,
        "limits": [
            "Local exported evidence; not independent account ownership/feed attestation",
            "Free Trial / two-step / Swing profile and evaluation phase not verified",
            (
                "Bounded maximum-window acquisition; neither a short result nor server dates prove "
                "that all available history was retrieved"
                if history
                else "At most 512 recent bars per interval; "
                "server dates do not prove full retrieval"
            ),
            "Last bar may be incomplete; historical server-to-UTC mapping not verified",
            "Current contract/swap values are not historical costs",
            "No backtest, strategy approval, risk verification or order authority",
        ],
        "sha256": {name: hashlib.sha256(data).hexdigest() for name, data in raw.items()},
    }
    return report, raw


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path, help="New private local evidence directory")
    args = parser.parse_args()
    report, raw = inspect_probe(args.source)
    args.output.mkdir(parents=True, exist_ok=False)
    for name, data in raw.items():
        (args.output / name).write_bytes(data)
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    if report["schema"] == HISTORY_SCHEMA:
        lines = [
            "# Gold MT5 maximum-window acquisition",
            "",
            "Raw server/chart date labels; UTC mapping and full-history completeness unverified.",
            "",
            "| Interval | Bars | First label | Last label | Days | Cap indicated |",
            "| --- | ---: | --- | --- | ---: | --- |",
        ]
        for frame in HISTORY_FRAMES:
            item = report["coverage"].get(frame)
            if item:
                lines.append(
                    f"| {frame.removeprefix('PERIOD_')} | {item['retrieved_bars']} | "
                    f"{item['first_server_label']} | {item['last_server_label']} | "
                    f"{item['span_days']} | "
                    f"{item['bar_budget_reached'] or item['terminal_series_at_budget']} |"
                )
            else:
                lines.append(f"| {frame.removeprefix('PERIOD_')} | 0 | unavailable | | | |")
        lines.extend(
            [
                "",
                "Findings:",
                "",
                *(f"- {item}" for item in report["findings"]),
                "",
                "Limits:",
                "",
                *(f"- {item}" for item in report["limits"]),
            ]
        )
        (args.output / "summary.md").write_text("\n".join(lines) + "\n")
    print(f"Retained read-only diagnostic: {args.output / 'report.json'}")
    print(f"Retrieved intervals: {len(report['coverage'])}; findings: {len(report['findings'])}")


if __name__ == "__main__":
    main()
