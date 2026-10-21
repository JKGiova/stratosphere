"""Paper-host-only metadata reads preserve account, currency and credential boundaries."""

import asyncio
import copy
import hashlib
import json
from datetime import UTC, datetime

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from stratosphere.connectors.alpaca import (
    PAPER_API,
    AlpacaError,
    AlpacaPaperConnection,
    PaperCredentials,
)

KEY = "paper-fixture-key-id-12345"
SECRET = "paper-fixture-secret-67890"
ACCOUNT = {
    "id": "11111111-1111-4111-8111-111111111111",
    "status": "ACTIVE",
    "crypto_status": "ACTIVE",
    "currency": "USD",
    "account_blocked": False,
    "trading_blocked": False,
    "account_number": "PRIVATE-IGNORED-NUMBER",
    "cash": "100000",
    "email": "private@example.test",
}
ASSET = {
    "id": "22222222-2222-4222-8222-222222222222",
    "class": "crypto",
    "exchange": "ALPACA",
    "symbol": "BTC/USD",
    "status": "active",
    "tradable": True,
    "fractionable": True,
    "min_order_size": "0.000000001",
    "min_trade_increment": "0.000000001",
    "price_increment": "0.00000001",
}
AT = datetime(2026, 10, 21, tzinfo=UTC)


def credentials():
    return PaperCredentials(key_id=SecretStr(KEY), secret_key=SecretStr(SECRET))


class Provider:
    def __init__(self):
        self.requests = []
        self.account = copy.deepcopy(ACCOUNT)
        self.assets = [copy.deepcopy(ASSET)]
        self.status = 200
        self.change = None
        self.headers = {"content-type": "application/json", "x-request-id": "fixture-request"}

    def handle(self, request):
        self.requests.append(request)
        assert request.method == "GET" and request.url.host == "paper-api.alpaca.markets"
        assert request.headers["APCA-API-KEY-ID"] == KEY
        assert request.headers["APCA-API-SECRET-KEY"] == SECRET
        assert not request.content
        if self.change:
            self.change(len(self.requests))
        body = self.assets if request.url.path == "/v2/assets" else self.account
        return httpx.Response(self.status, headers=self.headers, json=body)


@pytest.fixture
def paper():
    provider = Provider()
    client = AlpacaPaperConnection(transport=httpx.MockTransport(provider.handle), clock=lambda: AT)
    client.configure(credentials())
    return client, provider


async def test_fixed_host_projection_native_precision_and_private_export(
    paper, monkeypatch, tmp_path
):
    client, provider = paper
    monkeypatch.setenv("APCA_API_BASE_URL", "https://api.alpaca.markets")
    monkeypatch.setenv("HTTPS_PROXY", "http://untrusted.invalid")
    result = await client.inspect()
    assert [str(request.url) for request in provider.requests] == [
        PAPER_API + "/v2/account",
        PAPER_API + "/v2/assets?asset_class=crypto",
        PAPER_API + "/v2/account",
    ]
    assert result.asset.symbol == "BTC/USD" and result.asset.min_trade_increment == "0.000000001"
    assert result.account.id.hex == "1" * 12 + "4111" + "8111" + "1" * 12
    assert result.execution_authority == "none" and not result.feed_entitlement_verified
    payload = result.model_dump_json()
    assert all(
        value not in payload
        for value in (KEY, SECRET, "PRIVATE-IGNORED-NUMBER", "private@example.test", "100000")
    )
    path = client.save_check(tmp_path / "checks")
    before = path.read_bytes()
    assert client.save_check(path.parent.parent) == path and path.read_bytes() == before
    assert path.stat().st_mode & 0o777 == 0o600 and path.parent.stat().st_mode & 0o777 == 0o700
    client.disconnect()
    restarted = AlpacaPaperConnection()
    assert restarted.load_check(path.parent.parent, result.id) == result
    assert restarted.credentials is None and restarted.last_check is None
    assert len(provider.requests) == 3 and "no orders" in result.markdown()


@pytest.mark.parametrize("status", [301, 302, 401, 403, 429, 500])
async def test_http_errors_never_redirect_retry_or_echo_body(paper, status):
    client, provider = paper
    provider.status = status
    provider.headers["location"] = "https://api.alpaca.markets/v2/account"
    provider.account = {"error": SECRET}
    with pytest.raises(AlpacaError) as error:
        await client.inspect()
    assert SECRET not in str(error.value) and len(provider.requests) == 1
    assert client.last_check is None and not client.busy


@pytest.mark.parametrize(
    "kind", ["missing", "blocked", "inactive", "wrong_currency", "crypto_missing"]
)
async def test_account_blocks_and_absent_asset_are_not_execution_readiness(paper, kind):
    client, provider = paper
    if kind == "missing":
        provider.assets[0]["symbol"] = "BTC/USDT"
    elif kind == "blocked":
        provider.account["trading_blocked"] = True
    elif kind == "inactive":
        provider.assets[0]["tradable"] = False
    elif kind == "wrong_currency":
        provider.account["currency"] = "EUR"
    else:
        provider.account.pop("crypto_status")
    result = await client.inspect()
    assert "- " in result.markdown() and result.execution_authority == "none"
    if kind == "missing":
        assert result.asset is None and "BTC/USD is absent" in result.markdown()


@pytest.mark.parametrize(
    "kind",
    [
        "duplicate_asset",
        "float_increment",
        "wrong_class",
        "bool_string",
        "wrong_content_type",
        "changed_account",
    ],
)
async def test_unsupported_or_changed_responses_are_rejected(paper, kind):
    client, provider = paper
    if kind == "duplicate_asset":
        provider.assets.append(copy.deepcopy(ASSET))
    elif kind == "float_increment":
        provider.assets[0]["min_trade_increment"] = 0.000000001
    elif kind == "wrong_class":
        provider.assets[0]["class"] = "us_equity"
    elif kind == "bool_string":
        provider.account["account_blocked"] = "false"
    elif kind == "wrong_content_type":
        provider.headers["content-type"] = "text/html"
    else:

        def change(count):
            if count == 3:
                provider.account["id"] = "33333333-3333-4333-8333-333333333333"

        provider.change = change
    with pytest.raises(AlpacaError):
        await client.inspect()
    assert not client.busy and client.last_check is None and len(provider.requests) <= 3


@pytest.mark.parametrize("encoded", [False, True])
async def test_raw_and_escaped_credentials_in_ignored_fields_are_rejected(paper, encoded):
    client, provider = paper
    provider.account["ignored"] = (
        "".join(f"\\u{ord(char):04x}" for char in SECRET) if encoded else SECRET
    )
    with pytest.raises(AlpacaError, match="Credential-bearing"):
        await client.inspect()
    assert len(provider.requests) == 1 and client.last_check is None


async def test_disconnect_during_await_blocks_late_publication_and_concurrent_read(paper):
    client, provider = paper
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(request):
        entered.set()
        await release.wait()
        return provider.handle(request)

    client.transport = httpx.MockTransport(handler)
    task = asyncio.create_task(client.inspect())
    await entered.wait()
    with pytest.raises(AlpacaError, match="already running"):
        await client.inspect()
    client.disconnect()
    release.set()
    with pytest.raises(AlpacaError, match="disconnected"):
        await task
    assert client.last_check is None and not client.busy and len(provider.requests) == 1
    assert client.contains_credential(SECRET.encode())


async def test_cancellation_swallowed_by_transport_cannot_publish(paper):
    client, provider = paper
    entered = asyncio.Event()

    async def handler(request):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return provider.handle(request)

    client.transport = httpx.MockTransport(handler)
    task = asyncio.create_task(client.inspect())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.last_check is None and not client.busy


async def test_export_tampering_symlinks_and_secret_echo_rejected(paper, tmp_path):
    client, _ = paper
    value = await client.inspect()
    root = tmp_path / "checks"
    path = client.save_check(root)
    path.write_text("{}")
    with pytest.raises(AlpacaError, match="verified"):
        client.load_check(root, value.id)
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(AlpacaError, match="symbolic"):
        client.save_check(alias)


async def test_rehashed_export_with_duplicate_json_fields_is_rejected(paper, tmp_path):
    client, _ = paper
    value = await client.inspect()
    root = tmp_path / "checks"
    path = client.save_check(root)
    data = path.read_text().replace(
        '"schema_version":',
        '"schema_version": "alpaca-paper-check/1", "schema_version":',
        1,
    )
    path.write_text(data)
    (path.parent / "manifest.json").write_text(
        json.dumps({"check.json": hashlib.sha256(path.read_bytes()).hexdigest()})
    )
    with pytest.raises(AlpacaError, match="verified"):
        client.load_check(root, value.id)


@pytest.mark.parametrize("value", ["", "bad\nkey", " leading", "\u00e9", "a" * 2049])
def test_credentials_are_masked_and_header_safe(value):
    with pytest.raises(ValidationError):
        PaperCredentials(key_id=SecretStr(value), secret_key=SecretStr(SECRET))
    assert KEY not in repr(credentials()) and SECRET not in repr(credentials())


async def test_missing_credentials_make_no_request():
    client = AlpacaPaperConnection()
    with pytest.raises(AlpacaError, match="connect"):
        await client.inspect()
