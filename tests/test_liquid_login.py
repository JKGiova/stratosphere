"""Authored OAuth grants verify resume/rotation, not a real Liquid login."""

import asyncio
import json
import time
from urllib.parse import parse_qs

import httpx
import pytest
from conftest import MemoryCredentialBackend
from test_liquid import TOKEN, Provider

from stratosphere.credentials import CredentialError, CredentialStore
from stratosphere.legacy.liquid import RESOURCE, LiquidConnection, LiquidError


@pytest.fixture
def vault(tmp_path):
    return CredentialStore(backend=MemoryCredentialBackend(), lock_root=tmp_path / "locks")


class RefreshProvider(Provider):
    def __init__(self):
        super().__init__()
        self.renewed = {
            "access_token": "rotated-access-private",
            "refresh_token": "rotated-refresh-private",
            "expires_in": 3600,
            "token_type": "Bearer",
            "scope": "read",
        }
        self.refresh_failure = False
        self.refreshes = []

    def handle(self, request):
        if request.url.path == "/oauth/token":
            values = parse_qs(request.content.decode())
            if values.get("grant_type") == ["refresh_token"]:
                self.requests.append(request)
                self.refreshes.append(values)
                assert set(values) == {
                    "grant_type",
                    "client_id",
                    "refresh_token",
                    "scope",
                    "resource",
                }
                assert values["scope"] == ["read"] and values["resource"] == [RESOURCE]
                assert (
                    request.url.host == "coinvest.liquid.trade"
                    and "authorization" not in request.headers
                )
                if self.refresh_failure:
                    raise httpx.ReadTimeout("private refresh response")
                self.tokens = dict(self.renewed)
                return httpx.Response(200, json=self.tokens)
        return super().handle(request)

    def restarted(self, vault):
        async def forbidden(url):
            raise AssertionError("Resume must not open a browser")

        return LiquidConnection(
            transport=httpx.MockTransport(self.handle),
            open_browser=forbidden,
            credential_store=vault,
        )


def expire(vault):
    value = vault.read("liquid")
    value.update(issued_at=time.time() - 120, expires_at=time.time() - 60)
    vault.put("liquid", value)


async def saved(provider, vault):
    provider.connection.credential_store = vault
    await provider.connection.connect()
    provider.connection.disconnect()


async def test_saved_login_restarts_with_fresh_catalog_without_browser_or_account(vault):
    provider = RefreshProvider()
    await saved(provider, vault)
    old_count = len(provider.requests)
    connection = provider.restarted(vault)
    assert await connection.resume()
    assert len(provider.browser_urls) == 1 and not provider.refreshes
    methods = [
        json.loads(r.content)["method"]
        for r in provider.requests[old_count:]
        if r.url.path == "/mcp"
    ]
    assert methods == ["initialize", "notifications/initialized", "tools/list"]
    assert connection.session.tools == ("fixture_portfolio",)
    assert connection.contains_credential(b"discarded-refresh-token")
    assert "discarded-refresh-token" not in connection.status_markdown()


async def test_expired_grant_rotates_once_and_saves_before_fresh_catalog(vault):
    provider = RefreshProvider()
    await saved(provider, vault)
    expire(vault)
    connection = provider.restarted(vault)
    assert await connection.resume()
    assert len(provider.refreshes) == 1 and len(provider.browser_urls) == 1
    assert provider.refreshes[0]["refresh_token"] == ["discarded-refresh-token"]
    assert vault.read("liquid")["refresh_token"] == "rotated-refresh-private"
    assert connection.session.token.get_secret_value() == "rotated-access-private"
    assert all(
        connection.contains_credential(s.encode())
        for s in (TOKEN, "discarded-refresh-token", "rotated-refresh-private")
    )
    assert await provider.restarted(vault).resume()
    assert len(provider.refreshes) == 1


@pytest.mark.parametrize(
    "change",
    [
        {"scope": "read trade"},
        {"scope": None},
        {"expires_in": True},
        {"token_type": "MAC"},
        {"refresh_token": None},
        {"refresh_token": "discarded-refresh-token"},
    ],
)
async def test_invalid_refresh_scope_lifetime_or_rotation_cannot_restore_access(vault, change):
    provider = RefreshProvider()
    await saved(provider, vault)
    expire(vault)
    provider.renewed.update(change)
    connection = provider.restarted(vault)
    with pytest.raises(LiquidError):
        await connection.resume()
    assert connection.session is None and vault.read("liquid") is None
    assert not await provider.restarted(vault).resume()
    assert len(provider.refreshes) == 1


async def test_timeout_consumes_refresh_and_restart_cannot_replay_it(vault):
    provider = RefreshProvider()
    await saved(provider, vault)
    expire(vault)
    provider.refresh_failure = True
    with pytest.raises(LiquidError) as error:
        await provider.restarted(vault).resume()
    assert "private" not in str(error.value)
    assert not await provider.restarted(vault).resume() and len(provider.refreshes) == 1


async def test_changed_metadata_blocks_credentials_before_refresh(vault):
    provider = RefreshProvider()
    await saved(provider, vault)
    expire(vault)
    provider.server["token_endpoint"] = "https://untrusted.example/oauth/token"
    with pytest.raises(LiquidError):
        await provider.restarted(vault).resume()
    assert not provider.refreshes


async def test_rotated_grant_survives_catalog_failure_but_no_session_is_published(vault):
    provider = RefreshProvider()
    await saved(provider, vault)
    expire(vault)
    provider.initialized["protocolVersion"] = "unsupported"
    connection = provider.restarted(vault)
    with pytest.raises(LiquidError):
        await connection.resume()
    assert connection.session is None
    assert vault.read("liquid")["refresh_token"] == "rotated-refresh-private"


async def test_refresh_secret_echo_cannot_be_exported_as_catalog(vault):
    provider = RefreshProvider()
    await saved(provider, vault)
    provider.tools["tools"][0]["description"] = "discarded-refresh-token"
    connection = provider.restarted(vault)
    with pytest.raises(LiquidError):
        await connection.resume()
    assert connection.session is None


async def test_disconnect_memory_preserves_login_and_explicit_forget_removes_it(vault):
    provider = RefreshProvider()
    await saved(provider, vault)
    assert vault.read("liquid") is not None
    await provider.restarted(vault).forget()
    assert not await provider.restarted(vault).resume()


async def test_simultaneous_refresh_owner_cannot_consume_the_same_grant(vault):
    provider = RefreshProvider()
    await saved(provider, vault)
    expire(vault)
    with vault.lease("liquid"):
        with pytest.raises(CredentialError):
            await provider.restarted(vault).resume()
    assert not provider.refreshes


async def test_canceled_refresh_is_not_replayed_after_restart(vault):
    provider = RefreshProvider()
    await saved(provider, vault)
    expire(vault)
    entered = asyncio.Event()

    async def handler(request):
        if request.url.path == "/oauth/token":
            entered.set()
            await asyncio.Event().wait()
        return provider.handle(request)

    connection = LiquidConnection(transport=httpx.MockTransport(handler), credential_store=vault)
    task = asyncio.create_task(connection.resume())
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert vault.read("liquid") is None and connection.session is None
    assert not await provider.restarted(vault).resume()


async def test_expired_nonrenewable_login_is_forgotten_without_browser(vault):
    provider = RefreshProvider()
    provider.tokens.pop("refresh_token")
    await saved(provider, vault)
    expire(vault)
    assert not await provider.restarted(vault).resume()
    assert vault.read("liquid") is None and not provider.refreshes


async def test_wrong_resource_saved_grant_is_rejected_before_any_network(vault):
    provider = RefreshProvider()
    await saved(provider, vault)
    value = vault.read("liquid")
    value["resource"] = "https://untrusted.example/mcp"
    vault.put("liquid", value)
    count = len(provider.requests)
    with pytest.raises(LiquidError):
        await provider.restarted(vault).resume()
    assert len(provider.requests) == count and vault.read("liquid") is None


async def test_revoked_access_clears_cached_login_without_automatic_browser(vault):
    provider = RefreshProvider()
    await saved(provider, vault)

    def revoked(request):
        if request.url.path == "/mcp":
            return httpx.Response(401)
        return provider.handle(request)

    connection = LiquidConnection(transport=httpx.MockTransport(revoked), credential_store=vault)
    assert not await connection.resume()
    assert vault.read("liquid") is None and connection.session is None
