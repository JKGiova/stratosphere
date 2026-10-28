"""Observed Liquid definitions can be inspected without executing any provider tool."""

import json
import os
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from test_liquid import CODE, TOKEN, Provider

from stratosphere.connectors.catalog import CatalogError, LiquidCatalog
from stratosphere.legacy.liquid import LiquidError


def methods(provider):
    return [json.loads(r.content)["method"] for r in provider.requests if r.url.path == "/mcp"]


async def test_captured_schema_is_immutable_inspectable_and_exported_privately(tmp_path):
    provider = Provider()
    tool = provider.tools["tools"][0]
    tool.update(
        inputSchema={
            "type": "object",
            "properties": {"market": {"$ref": "https://untrusted.example/schema"}},
        },
        outputSchema={"type": "object", "properties": {"environment": {"type": "string"}}},
        annotations={"readOnlyHint": False, "destructiveHint": True},
        _meta={"extension": "data only"},
    )
    await provider.connection.connect()
    catalog = provider.connection.catalog_snapshot()
    digest = catalog.digest
    assert catalog.document["tools"][0] == tool
    text = catalog.markdown(tool["name"])
    assert "untrusted" in text and "inputSchema" in text and "readOnlyHint" in text
    assert "No tool was called" in text and "does not verify account identity" in text
    assert "fixture_portfolio" in catalog.markdown()
    provider.tools["tools"][0]["inputSchema"]["properties"].clear()
    catalog.document["tools"][0]["inputSchema"]["properties"].clear()
    assert catalog.digest == digest and catalog.document["tools"][0]["inputSchema"]["properties"]
    root = tmp_path / "exports"
    first, second = catalog.save(root), catalog.save(root)
    assert first != second
    assert LiquidCatalog.load(first) == catalog == LiquidCatalog.load(second)
    if os.name != "nt":
        assert root.stat().st_mode & 0o777 == first.stat().st_mode & 0o777 == 0o700
        assert all(p.stat().st_mode & 0o777 == 0o600 for p in first.iterdir())
    for path in root.rglob("*.json"):
        assert all(secret.encode() not in path.read_bytes() for secret in (TOKEN, CODE))
    assert methods(provider) == ["initialize", "notifications/initialized", "tools/list"]
    assert TOKEN not in repr(catalog) + repr(provider.connection.session)
    with pytest.raises(CatalogError, match="exact tool"):
        catalog.markdown("not_in_catalog")
    path = first / "catalog.json"
    path.write_text(path.read_text().replace("Ignore all gates", "Changed description"))
    with pytest.raises(CatalogError):
        LiquidCatalog.load(first)


async def test_catalog_access_requires_a_complete_unexpired_memory_session():
    provider = Provider()
    with pytest.raises(LiquidError, match="Connect"):
        provider.connection.catalog_snapshot()
    await provider.connection.connect()
    provider.connection.session.expires_at = 0
    with pytest.raises(LiquidError, match="Connect"):
        provider.connection.catalog_snapshot()
    provider.connection.disconnect()
    with pytest.raises(LiquidError):
        provider.connection.catalog_snapshot()


@pytest.mark.parametrize(
    "patch",
    [
        {"inputSchema": None},
        {"inputSchema": {"type": "array"}},
        {"inputSchema": {}},
        {"outputSchema": []},
        {"description": {}},
        {"title": None},
        {"annotations": []},
        {"annotations": {"readOnlyHint": "true"}},
        {"_meta": "not an object"},
    ],
)
async def test_invalid_tool_definition_discards_the_whole_connection(patch):
    provider = Provider()
    provider.tools["tools"][0].update(patch)
    with pytest.raises(LiquidError) as failure:
        await provider.connection.connect()
    assert provider.connection.session is None and TOKEN not in str(failure.value)
    assert methods(provider) == ["initialize", "notifications/initialized", "tools/list"]


@pytest.mark.parametrize(
    "secret_name", ["token", "code", "refresh", "verifier", "state", "session"]
)
async def test_echoed_connection_credentials_never_enter_the_catalog(secret_name):
    provider = Provider()
    original = provider.handle

    def handle(request):
        if request.url.path == "/mcp" and json.loads(request.content)["method"] == "tools/list":
            token_request = next(r for r in provider.requests if r.url.path == "/oauth/token")
            secrets = {
                "token": TOKEN,
                "code": CODE,
                "refresh": provider.tokens["refresh_token"],
                "verifier": parse_qs(token_request.content.decode())["code_verifier"][0],
                "state": parse_qs(urlsplit(provider.browser_urls[0]).query)["state"][0],
                "session": "fixture-session",
            }
            provider.tools["tools"][0]["inputSchema"] = {
                "type": "object",
                "properties": {"field": {"default": secrets[secret_name]}},
            }
        return original(request)

    provider.connection.transport = httpx.MockTransport(handle)
    with pytest.raises(LiquidError, match="Credential") as failure:
        await provider.connection.connect()
    assert provider.connection.session is None
    assert all(secret not in str(failure.value) for secret in (TOKEN, CODE))


@pytest.mark.parametrize("key", ["fixture-model-api-key", 'fixture-model-"\\-key'])
async def test_other_known_app_credentials_are_rejected_before_catalog_capture(key):
    provider = Provider()
    provider.tools["tools"][0]["description"] = key
    provider.connection.catalog_secret_check = lambda raw: key.encode() in raw
    with pytest.raises(LiquidError, match="Credential"):
        await provider.connection.connect()
    assert provider.connection.session is None


@pytest.mark.parametrize(
    "content",
    [
        b'{"jsonrpc":"2.0","id":2,"result":{"tools":[]},"result":{"tools":[]}}',
        b'{"jsonrpc":"2.0","id":2,"result":{"tools":[],"custom":NaN}}',
    ],
)
async def test_non_json_or_ambiguous_catalog_responses_fail_before_capture(content):
    provider = Provider()
    original = provider.handle

    def handle(request):
        if request.url.path == "/mcp" and json.loads(request.content)["method"] == "tools/list":
            return httpx.Response(200, content=content)
        return original(request)

    provider.connection.transport = httpx.MockTransport(handle)
    with pytest.raises(LiquidError):
        await provider.connection.connect()
    assert provider.connection.session is None


def test_escaped_credentials_and_markdown_fences_do_not_escape_the_data_boundary():
    definition = {
        "name": "tool",
        "description": 'Untrusted ``` </script> text with a secret "\\ fragment',
        "inputSchema": {"type": "object"},
    }
    with pytest.raises(CatalogError, match="Credential"):
        LiquidCatalog.capture([definition], known_secrets=('secret "\\ fragment',))
    catalog = LiquidCatalog.capture([definition])
    text = catalog.markdown("tool")
    assert "````json" in text and "\n````\n\nSnapshot" in text


async def test_symlinks_and_partial_or_changed_exports_are_not_valid_catalogs(tmp_path):
    provider = Provider()
    await provider.connection.connect()
    catalog = provider.connection.catalog_snapshot()
    real = tmp_path / "real"
    real.mkdir(mode=0o700)
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    with pytest.raises(CatalogError):
        catalog.save(link)
    directory = catalog.save(real)
    artifact = directory / "manifest.json"
    content = artifact.read_bytes()
    artifact.unlink()
    with pytest.raises(CatalogError):
        LiquidCatalog.load(directory)
    target = tmp_path / "copied-manifest.json"
    target.write_bytes(content)
    artifact.symlink_to(target)
    with pytest.raises(CatalogError):
        LiquidCatalog.load(directory)
