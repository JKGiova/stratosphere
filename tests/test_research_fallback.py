"""Authored public-reader failures, fallback ownership and subprocess/proxy boundaries."""

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from test_strategy_intake import sources as sources

from stratosphere.research.store import ResearchError
from stratosphere.sources.egress import PublicProxy
from stratosphere.sources.fallback import ResearchWeb, cli_data
from stratosphere.sources.process import MissingTool, PublicCLIs, ToolFailure, execute
from stratosphere.sources.web import WebRequest

TEXT = "Buy on a confirmed moving-average crossover and exit on the opposite signal."


async def public(host):
    return ["93.184.215.14"]


class CLIs:
    def __init__(self, **responses):
        self.responses, self.calls = responses, []

    def availability(self):
        return {
            name: name in self.responses for name in ("agent-reach", "opencli", "agent-browser")
        }

    async def call(self, name, operation, value):
        self.calls.append((name, operation, value))
        result = self.responses.get(name, MissingTool("Authored missing tool"))
        if isinstance(result, BaseException):
            raise result
        return result


def audit(sources, snap):
    raw = snap.raw_materials[-1]
    return json.loads((sources.store.directory(snap.id) / f"blob_{raw.sha256}.bin").read_bytes())


def web(sources, clis, response=None):
    sources.transport = httpx.MockTransport(
        lambda request: (
            response
            or httpx.Response(
                200, text="<script>empty app</script>", headers={"content-type": "text/html"}
            )
        )
    )
    return ResearchWeb(sources, clis=clis, resolver=public)


async def test_direct_success_does_not_start_optional_tools(sources):
    clis = CLIs()
    reader = web(
        sources, clis, httpx.Response(200, text=TEXT, headers={"content-type": "text/plain"})
    )
    snap = await reader.acquire(WebRequest(url="https://example.com/strategy"))
    assert snap.pieces[0].text == TEXT and not clis.calls
    assert sources.store.snapshot(snap.id) == snap
    assert audit(sources, snap)["attempts"] == [{"backend": "direct", "state": "completed"}]


async def test_fallback_order_retains_failures_and_real_browser_shape(sources):
    clis = CLIs(
        **{
            "agent-reach": ToolFailure("Authored network failure"),
            "agent-browser": [
                {"success": True, "result": {"url": "https://example.com/strategy"}},
                {"success": True, "result": {"text": TEXT}},
            ],
        }
    )
    snap = await web(sources, clis).acquire(WebRequest(url="https://example.com/strategy"))
    assert snap.access_state == "acquired" and snap.pieces[0].text == TEXT
    assert [c[0] for c in clis.calls] == ["agent-reach", "opencli", "agent-browser"]
    assert [a["state"] for a in audit(sources, snap)["attempts"]] == [
        "failed",
        "failed",
        "unavailable",
        "completed",
    ]


@pytest.mark.parametrize("status", [401, 403, 404, 429])
async def test_publisher_access_failure_is_terminal(sources, status):
    clis = CLIs()
    reader = web(sources, clis, httpx.Response(status))
    snap = await reader.acquire(WebRequest(url="https://example.com/strategy"))
    assert snap.access_state == "unavailable" and not clis.calls
    assert audit(sources, snap)["attempts"][0]["state"] == "access_blocked"


@pytest.mark.parametrize("code", ["AUTH_REQUIRED", "RATE_LIMITED", "CAPTCHA"])
async def test_cli_access_failure_does_not_escalate(sources, code):
    clis = CLIs(**{"agent-reach": {"ok": False, "error": {"code": code}}})
    snap = await web(sources, clis).acquire(WebRequest(url="https://example.com/strategy"))
    assert snap.access_state == "unavailable" and len(clis.calls) == 1


async def test_missing_tools_are_retained_without_fabricated_text(sources):
    snap = await web(sources, CLIs()).acquire(WebRequest(url="https://example.com/strategy"))
    assert snap.access_state == "unavailable" and not snap.pieces
    assert [a["state"] for a in audit(sources, snap)["attempts"]][1:] == ["unavailable"] * 3


async def test_secret_result_is_never_saved_or_forwarded(sources):
    sources.store.known_secrets = ("private-fixture-key",)
    clis = CLIs(**{"agent-reach": {"ok": True, "items": [{"text": TEXT + "private-fixture-key"}]}})
    snap = await web(sources, clis).acquire(WebRequest(url="https://example.com/strategy"))
    assert not snap.pieces and len(clis.calls) == 1
    for path in sources.store.directory(snap.id).iterdir():
        assert b"private-fixture-key" not in path.read_bytes()


async def test_private_dns_is_terminal_before_any_tool(sources):
    clis = CLIs()
    reader = web(sources, clis)
    reader.resolver = AsyncMock(side_effect=ResearchError("Private peer"))
    with pytest.raises(ResearchError):
        await reader.acquire(WebRequest(url="https://example.com/strategy"))
    assert not clis.calls


async def test_cancelled_read_seals_unavailable_attempt(sources):
    reader = web(sources, CLIs())
    entered = asyncio.Event()

    async def wait(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    reader.get = wait
    task = asyncio.create_task(reader.acquire(WebRequest(url="https://example.com/strategy")))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    snap = sources.store.snapshot(sources.store.jobs()[0])
    assert snap.access_state == "unavailable" and not snap.pieces
    assert audit(sources, snap)["attempts"][-1]["state"] == "cancelled"


async def test_search_falls_back_to_public_arxiv_and_normalizes_https(sources):
    clis = CLIs(
        opencli={"ok": True, "data": [{"title": "Paper", "url": "http://arxiv.org/abs/1234"}]}
    )
    reader = web(sources, clis)
    reader.get = AsyncMock(side_effect=httpx.ConnectError("Authored index outage"))
    hits, attempts = await reader.search("moving average research")
    assert hits == [{"title": "Paper", "url": "https://arxiv.org/abs/1234"}]
    assert [a["state"] for a in attempts] == ["failed", "failed", "unavailable", "completed"]


async def test_search_denied_index_does_not_use_cli(sources):
    clis = CLIs()
    reader = web(sources, clis, httpx.Response(429))
    hits, attempts = await reader.search("moving averages")
    assert not hits and not clis.calls and attempts[-1]["state"] == "access_blocked"


@pytest.mark.parametrize(
    "payload", [None, [], {"ok": False, "error": None}, {"ok": True, "items": [None]}]
)
def test_malformed_cli_results_are_not_text(payload):
    with pytest.raises(ValueError):
        cli_data("agent-reach", payload)


async def test_proxy_revalidates_peer_and_pins_checked_address():
    resolver = AsyncMock(return_value=["93.184.215.14"])
    proxy = PublicProxy({"example.com"}, resolver=resolver)
    assert await proxy.peer("example.com", 443) == "93.184.215.14"
    resolver.return_value = ["127.0.0.1"]
    with pytest.raises(ValueError):
        await proxy.peer("example.com", 443)
    with pytest.raises(ValueError):
        await proxy.peer("unapproved.com", 443)
    with pytest.raises(ValueError):
        await proxy.peer("example.com", 80)


async def test_proxy_rejects_plain_http_and_closes_connections():
    async with PublicProxy({"example.com"}) as proxy:
        port = int(proxy.url.rsplit(":", 1)[1])
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET http://example.com/ HTTP/1.1\r\n\r\n")
        await writer.drain()
        assert b"403 Forbidden" in await reader.read()
        writer.close()
        await writer.wait_closed()
    assert not proxy.tasks


async def test_process_environment_is_empty_of_operator_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "private-fixture-key")
    monkeypatch.setenv("AGENT_BROWSER_AUTO_CONNECT", "1")
    result = await execute(
        [sys.executable, "-c", "import os,json;print(json.dumps(dict(os.environ)))"],
        home=tmp_path,
        proxy="http://127.0.0.1:9999",
    )
    assert "OPENROUTER_API_KEY" not in result and "AGENT_BROWSER_AUTO_CONNECT" not in result
    assert result["HOME"] == str(tmp_path) and result["NO_PROXY"] == ""
    assert "private-fixture-key" not in json.dumps(result)
    assert result["SSL_CERT_FILE"].endswith("cacert.pem")


async def test_process_timeout_reaps_owned_child(tmp_path):
    code = "import os,time;open('pid','w').write(str(os.getpid()));time.sleep(30)"
    with pytest.raises(TimeoutError):
        await execute([sys.executable, "-c", code], home=tmp_path, proxy="", timeout=1)
    pid = int((tmp_path / "pid").read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def test_only_fixed_public_cli_adapters_are_exposed(tmp_path):
    path = Path(sys.executable)
    clis = PublicCLIs(
        binaries={name: path for name in ("agent-reach", "opencli", "agent-browser")},
        resolver=public,
    )
    with pytest.raises(MissingTool):
        await clis.call("opencli", "read", "https://example.com/article")
    with pytest.raises(ResearchError):
        await clis.call("agent-reach", "search", "--import-cookies")
    with pytest.raises(ResearchError):
        await clis.call("agent-browser", "eval", "alert(1)")


async def test_browser_adapter_never_attaches_a_personal_session(tmp_path, monkeypatch):
    import stratosphere.sources.process as adapter

    calls = []

    class Proxy:
        def __init__(self, hosts, **kwargs):
            assert hosts == {"example.com"}
            self.url = "http://127.0.0.1:1234"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def capture(argv, *, home, proxy, timeout=40, extra=None):
        calls.append(argv)
        assert home.is_absolute() and not home.is_symlink()
        assert json.loads((home / "policy.json").read_text()) == {
            "default": "deny",
            "allow": ["launch", "navigate", "gettext"],
        }
        assert extra["AGENT_BROWSER_PLUGINS"] == "{}"
        assert "--auto-connect" not in argv and "--profile" not in argv
        assert "--cdp" not in argv and "--restore" not in argv
        assert "<-loopback>" in argv and "--no-webmcp" in argv
        if "batch" in argv:
            return [{"success": True, "result": {"text": TEXT}}]
        return {"success": True}

    monkeypatch.setattr(adapter, "PublicProxy", Proxy)
    monkeypatch.setattr(adapter, "execute", capture)
    monkeypatch.setattr(adapter, "browser_executable", lambda: Path(sys.executable))
    clis = PublicCLIs(binaries={"agent-browser": Path(sys.executable)}, resolver=public)
    result = await clis.call("agent-browser", "read", "https://example.com/strategy")
    assert cli_data("agent-browser", result) == [{"text": TEXT}]
    assert calls[0][-2:] == ["open https://example.com/strategy", "get text body"]
    assert calls[-1][-1] == "close"


async def test_process_cancellation_reaps_owned_child(tmp_path):
    code = "import os,time;open('pid','w').write(str(os.getpid()));time.sleep(30)"
    task = asyncio.create_task(execute([sys.executable, "-c", code], home=tmp_path, proxy=""))
    async with asyncio.timeout(5):
        while not (tmp_path / "pid").exists():
            await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    with pytest.raises(ProcessLookupError):
        os.kill(int((tmp_path / "pid").read_text()), 0)


async def test_pdf_failure_preserves_document_reader_without_browser_fallback(sources):
    from test_strategy_intake import pdf_fixture

    clis = CLIs()
    reader = web(
        sources,
        clis,
        httpx.Response(
            200, content=pdf_fixture([TEXT]), headers={"content-type": "application/pdf"}
        ),
    )
    snap = await reader.acquire(WebRequest(url="https://example.com/paper.pdf"))
    assert snap.access_state == "acquired" and snap.normalization == "pdf-pages/1"
    assert snap.pieces[0].locator == "PDF physical page 1" and not clis.calls


async def test_proxy_connects_to_checked_ip_instead_of_resolving_again():
    class Writer:
        def __init__(self):
            self.data = bytearray()
            self.closed = False

        def write(self, data):
            self.data.extend(data)

        async def drain(self):
            pass

        def close(self):
            self.closed = True

        async def wait_closed(self):
            pass

    client, remote = asyncio.StreamReader(), asyncio.StreamReader()
    client.feed_data(b"CONNECT example.com:443 HTTP/1.1\r\n\r\n")
    client.feed_eof()
    remote.feed_eof()
    destination, response = Writer(), Writer()
    connector = AsyncMock(return_value=(remote, destination))
    proxy = PublicProxy({"example.com"}, resolver=public, connector=connector)
    await proxy.handle(client, response)
    connector.assert_awaited_once_with("93.184.215.14", 443)
    assert b"200 Connection Established" in response.data
    assert destination.closed and response.closed and not proxy.tasks


def test_linux_browser_discovery_and_missing_browser_capability(monkeypatch):
    import stratosphere.sources.process as adapter

    monkeypatch.setattr(
        adapter.shutil, "which", lambda name: "/usr/bin/chromium" if name == "chromium" else None
    )
    monkeypatch.setattr(Path, "is_file", lambda path: str(path) == "/usr/bin/chromium")
    assert adapter.browser_executable() == Path("/usr/bin/chromium")
    monkeypatch.setattr(adapter, "browser_executable", lambda: None)
    clis = PublicCLIs(binaries={"agent-browser": Path("/usr/bin/chromium")})
    assert clis.availability() == {"agent-browser": False}
