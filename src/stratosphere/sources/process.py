"""Bounded credential-free CLI calls, never a model-controlled command or environment."""

import asyncio
import json
import os
import shlex
import shutil
import signal
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlsplit

import certifi

from stratosphere.research.store import ResearchError
from stratosphere.sources.egress import PublicProxy

ROOT = Path(__file__).resolve().parents[3]
NPM = ROOT / "tools/research/node_modules/.bin"
BINARIES = {
    "agent-reach": ROOT / ".tools/agent-reach/bin/agent-reach",
    "opencli": NPM / "opencli",
    "agent-browser": NPM / "agent-browser",
}


class MissingTool(ResearchError):
    pass


class ToolFailure(ResearchError):
    pass


def browser_executable():
    candidates = [Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")]
    candidates.extend(
        Path(value)
        for name in ("google-chrome", "google-chrome-stable", "chromium", "chromium-browser")
        if (value := shutil.which(name))
    )
    return next((path for path in candidates if path.is_file()), None)


async def execute(argv, *, home, proxy, timeout=40, extra=None):
    env = {
        "PATH": os.pathsep.join(
            (
                str(Path(argv[0]).parent),
                str(Path(shutil.which("node") or "/usr/local/bin/node").parent),
                os.defpath,
            )
        ),
        "HOME": str(home),
        "XDG_CONFIG_HOME": str(home / "config"),
        "XDG_CACHE_HOME": str(home / "cache"),
        "TMPDIR": str(home),
        "PYTHONNOUSERSITE": "1",
        "PYTHONUTF8": "1",
        "SSL_CERT_FILE": certifi.where(),
        "HTTP_PROXY": proxy,
        "HTTPS_PROXY": proxy,
        "ALL_PROXY": proxy,
        "http_proxy": proxy,
        "https_proxy": proxy,
        "all_proxy": proxy,
        "NO_PROXY": "",
        "no_proxy": "",
        "NODE_USE_ENV_PROXY": "1",
    } | (extra or {})
    process = None
    spawn = asyncio.create_task(
        asyncio.create_subprocess_exec(
            *argv,
            cwd=home,
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
    )
    try:
        process = await asyncio.shield(spawn)
        async with asyncio.timeout(timeout):
            data = bytearray()
            while block := await process.stdout.read(65536):
                data.extend(block)
                if len(data) > 4_000_000:
                    raise ResearchError("Research CLI output exceeded its byte budget")
            await process.wait()
        try:
            result = json.loads(data)
        except ValueError:
            raise ToolFailure("Research CLI failed without a structured result") from None
        return result
    finally:
        if process is None:
            while not spawn.done():
                try:
                    await asyncio.shield(spawn)
                except asyncio.CancelledError:
                    continue
            if not spawn.cancelled() and spawn.exception() is None:
                process = spawn.result()
        if process is not None:
            # Reap the owned CLI group; the caller separately closes any detached browser daemon.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            await process.wait()


class PublicCLIs:
    def __init__(self, *, binaries=None, resolver=None):
        self.binaries = binaries or BINARIES
        self.resolver = resolver

    def availability(self):
        return {
            name: path.is_file() and (name != "agent-browser" or browser_executable() is not None)
            for name, path in self.binaries.items()
        }

    async def call(self, name, operation, value):
        if operation not in {"read", "search"} or name not in BINARIES:
            raise ResearchError("Unsupported public research operation")
        path = self.binaries.get(name)
        if path is None or not path.is_file():
            raise MissingTool(f"Optional {name} is not installed")
        if operation == "read":
            from stratosphere.sources.web import public_addresses, public_url

            public_url(value)
            await (self.resolver or public_addresses)(urlsplit(value).hostname)
        elif not value.strip() or value.startswith("-") or len(value) > 180:
            raise ResearchError("Invalid public search query")
        host = urlsplit(value).hostname if operation == "read" else None
        if name == "agent-reach":
            hosts = {"r.jina.ai", "mcp.exa.ai"}
            args = ["web", operation, value, "--timeout", "25"]
            if operation == "search":
                args += ["--limit", "3"]
        elif name == "opencli":
            if operation == "search":
                hosts = {"export.arxiv.org"}
                args = ["arxiv", "search", value, "--limit", "3"]
            elif host == "en.wikipedia.org" and urlsplit(value).path.startswith("/wiki/"):
                title = unquote(urlsplit(value).path[6:])
                if not title or title.startswith("-"):
                    raise ResearchError("Invalid public article title")
                hosts = {host}
                args = ["wikipedia", "page", title, "--lang", "en"]
            else:
                raise MissingTool("OpenCLI has no enabled public adapter for this URL")
        else:
            if operation != "read":
                raise MissingTool("Browser fallback supports retained public URLs only")
            chrome = browser_executable()
            if chrome is None:
                raise MissingTool("Public browser reading needs an installed Chrome/Chromium")
            hosts, args = {host}, None
        options = {"resolver": self.resolver} if self.resolver else {}
        async with PublicProxy(hosts, **options) as proxy:
            with tempfile.TemporaryDirectory(prefix="sr-", dir=Path("/tmp").resolve()) as temp:
                home = Path(temp).resolve()
                if args is not None:
                    result = await execute([str(path), *args], home=home, proxy=proxy.url)
                else:
                    policy = home / "policy.json"
                    policy.write_text(
                        json.dumps({"default": "deny", "allow": ["launch", "navigate", "gettext"]})
                    )
                    config = home / "config.json"
                    config.write_text("{}")
                    common = [
                        str(path),
                        "--json",
                        "--config",
                        str(config),
                        "--namespace",
                        home.name,
                        "--session",
                        "research",
                        "--allowed-domains",
                        host,
                        "--action-policy",
                        str(policy),
                        "--proxy",
                        proxy.url,
                        "--proxy-bypass",
                        "<-loopback>",
                        "--max-output",
                        "200000",
                        "--idle-timeout",
                        "15s",
                        "--no-webmcp",
                    ]
                    common += ["--executable-path", str(chrome)]
                    # Fixed batch arguments expose navigation and text extraction only.
                    try:
                        result = await execute(
                            [
                                *common,
                                "batch",
                                "--bail",
                                shlex.join(["open", value]),
                                "get text body",
                            ],
                            home=home,
                            proxy=proxy.url,
                            extra={
                                "AGENT_BROWSER_PLUGINS": "{}",
                                "AGENT_BROWSER_SOCKET_DIR": str(home / "s"),
                            },
                        )
                    finally:
                        try:
                            await asyncio.shield(
                                execute(
                                    [*common, "close"],
                                    home=home,
                                    proxy=proxy.url,
                                    timeout=5,
                                    extra={
                                        "AGENT_BROWSER_PLUGINS": "{}",
                                        "AGENT_BROWSER_SOCKET_DIR": str(home / "s"),
                                    },
                                )
                            )
                        except ResearchError, ValueError, OSError, TimeoutError:
                            pass  # Fresh anonymous daemon also has a 15-second idle limit.

                return result
