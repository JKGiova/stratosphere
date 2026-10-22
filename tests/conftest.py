"""Authored research fixtures; no remote requests, real credentials or venue data."""

import hashlib
import io
import json
import zipfile

import pytest

from stratosphere.research import contracts, data
from stratosphere.research.contracts import (
    EVIDENCE_FIELDS,
    Extraction,
    MarketBar,
    ResearchConfig,
    ResearchPlan,
    build_spec,
    implementation_digest,
)

SOURCE = (
    "BTCUSDT spot, 1-hour closed candles. Fast SMA 3 and slow SMA 8. "
    "Buy when fast SMA crosses above slow SMA; exit when it crosses below. "
    "Fixed quantity 0.01 BTC. No shorting, leverage, reinforcement, stops or other filters."
)


@pytest.fixture
def research_source():
    return SOURCE


@pytest.fixture
def research_draft():
    return Extraction(
        status="ready",
        name="Authored SMA 3/8",
        symbol="BTCUSDT",
        timeframe_seconds=3600,
        fast_period=3,
        slow_period=8,
        quantity="0.01",
        entry="fast_crosses_above_slow",
        exit="fast_crosses_below_slow",
        questions=(),
        evidence=tuple({"field": field, "quote": SOURCE} for field in sorted(EVIDENCE_FIELDS)),
    )


@pytest.fixture
def market_bars():
    prices = [100, 100, 100, 110, 120, 110, 90, 80]
    result = []
    for index in range(data.BAR_COUNT):
        opening, closing = prices[(index - 1) % len(prices)], prices[index % len(prices)]
        opened = data.FIRST_OPEN_NS + index * 3600 * 10**9
        result.append(
            MarketBar(
                open_ns=opened,
                close_ns=opened + 3600 * 10**9 - 1000,
                open=str(opening),
                high=str(max(opening, closing) + 1),
                low=str(min(opening, closing) - 1),
                close=str(closing),
                volume="100",
            )
        )
    return tuple(result)


def make_archive(bars):
    rows = []
    for bar in bars:
        rows.append(
            [
                str(bar.open_ns // 1000),
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                str(bar.close_ns // 1000),
                "10000",
                "10",
                "50",
                "5000",
                "0",
            ]
        )
    content = "\n".join(",".join(row) for row in rows) + "\n"
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            zipfile.ZipInfo("BTCUSDT-1h-2025-01.csv", date_time=(2020, 1, 1, 0, 0, 0)), content
        )
    return output.getvalue()


@pytest.fixture
def pin_archive(monkeypatch):
    def pin(content):
        digest = hashlib.sha256(content).hexdigest()
        monkeypatch.setattr(contracts, "ARCHIVE_SHA256", digest)
        monkeypatch.setattr(data, "ARCHIVE_SHA256", digest)
        return digest + "  BTCUSDT-1h-2025-01.zip\n"

    return pin


@pytest.fixture
def research_snapshot(market_bars, pin_archive):
    content = make_archive(market_bars)
    return data.normalize_archive(content, pin_archive(content))


@pytest.fixture
def research_plan(research_draft, research_snapshot):
    return ResearchPlan(
        source_sha256=hashlib.sha256(SOURCE.encode()).hexdigest(),
        draft=research_draft,
        specification=build_spec(research_draft),
        config=ResearchConfig(),
        implementation_sha256=implementation_digest(),
    )


class ResearchProvider:
    def __init__(self, draft):
        self.reply = json.dumps(draft.model_dump(mode="json"))
        self.requests = []

    async def stream(self, messages):
        self.requests.append(messages)
        yield self.reply[: len(self.reply) // 2]
        yield self.reply[len(self.reply) // 2 :]


@pytest.fixture
def research_provider(research_draft):
    return ResearchProvider(research_draft)


@pytest.fixture(autouse=True)
def isolate_workspace_database(tmp_path, monkeypatch):
    """Product startup now persists strategy records; tests must never touch user state."""
    monkeypatch.setenv("STRATOSPHERE_RESEARCH_DIR", str(tmp_path / "research"))


class MemoryCredentialBackend:
    """Explicit test double; never selected by the product's native backend selector."""

    def __init__(self):
        self.values = {}

    def get_password(self, service, name):
        return self.values.get((service, name))

    def set_password(self, service, name, value):
        self.values[service, name] = value

    def delete_password(self, service, name):
        self.values.pop((service, name), None)


@pytest.fixture(autouse=True)
def isolate_native_credentials(tmp_path, monkeypatch):
    """Legacy UI cases cannot read or overwrite the user's OS credential store."""
    from stratosphere.credentials import CredentialStore

    monkeypatch.setattr(
        "stratosphere.tui.CredentialStore",
        lambda: CredentialStore(
            backend=MemoryCredentialBackend(), lock_root=tmp_path / "credential-locks"
        ),
    )
