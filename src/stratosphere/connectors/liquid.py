"""Read-scope Liquid OAuth and MCP catalog inspection, with no tool-call path."""

import asyncio
import base64
import hashlib
import re
import secrets
import time
import webbrowser
from dataclasses import dataclass
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
from pydantic import SecretStr

from stratosphere.connectors.catalog import CatalogError, LiquidCatalog, canonical, strict_json
from stratosphere.research.store import ResearchError

ORIGIN = "https://coinvest.liquid.trade"
RESOURCE = ORIGIN + "/mcp"
PROTOCOL = "2025-06-18"
MAX_MESSAGE = 2 * 1024 * 1024  # Protocol metadata, not a user upload quota.


class LiquidError(ResearchError):
    """Safe connector error; never includes provider bodies, codes or tokens."""


def required(condition, message):
    if not condition:
        raise LiquidError(message)


def opaque(value):
    return (
        isinstance(value, str)
        and 0 < len(value) <= 8192
        and all(33 <= ord(c) <= 126 for c in value)
    )


def advertised(document, name, value):
    values = document.get(name)
    return isinstance(values, list) and all(isinstance(v, str) for v in values) and value in values


@dataclass(frozen=True)
class Discovery:
    """Fixed-origin, validated public metadata; not evidence of an account or paper mode."""

    resource: str = RESOURCE
    issuer: str = ORIGIN
    authorize: str = ORIGIN + "/oauth/authorize"
    token: str = ORIGIN + "/oauth/token"
    registration: str = ORIGIN + "/oauth/register"


@dataclass(repr=False)
class ReadSession:
    token: SecretStr
    expires_at: float
    catalog: LiquidCatalog | None = None
    mcp_session_id: SecretStr | None = None
    rpc_id: int = 0
    initialized: bool = False
    account_read_busy: bool = False
    redactions: tuple[SecretStr, ...] = ()

    @property
    def tools(self):
        return self.catalog.names if self.catalog is not None else ()

    def __repr__(self):
        return "ReadSession(credentials=redacted, account_identity=unverified)"


class LoopbackCallback:
    """One short-lived IPv4 listener. Invalid/local unsolicited requests cannot consume it."""

    def __init__(self, state: str):
        self.state = state
        self.server = None
        self.result = None
        self.redirect = None
        self.handlers = set()

    async def __aenter__(self):
        self.result = asyncio.get_running_loop().create_future()
        try:
            self.server = await asyncio.start_server(self._accept, "127.0.0.1", 0, limit=16384)
        except OSError:
            raise LiquidError("Cannot open the local Liquid authorization callback.") from None
        port = self.server.sockets[0].getsockname()[1]
        self.redirect = f"http://127.0.0.1:{port}/liquid/callback"
        return self

    async def __aexit__(self, *_):
        self.server.close()
        await self.server.wait_closed()
        for task in tuple(self.handlers):
            task.cancel()
        if self.handlers:
            await asyncio.gather(*self.handlers, return_exceptions=True)
        if not self.result.done():
            self.result.cancel()

    async def _accept(self, reader, writer):
        task = asyncio.current_task()
        self.handlers.add(task)
        status = "400 Bad Request"
        body = "This is not a valid stratosphere authorization response."
        try:
            raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
            text = raw.decode("ascii")
            lines = text.split("\r\n")
            method, target, version = lines[0].split(" ")
            headers = [line.partition(":") for line in lines[1:] if line]
            hosts = [v.strip() for k, _, v in headers if k.lower() == "host"]
            parsed = urlsplit(target)
            params = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
            valid = (
                method == "GET"
                and version == "HTTP/1.1"
                and target.startswith("/liquid/callback?")
                and not parsed.scheme
                and not parsed.netloc
                and not parsed.fragment
                and parsed.path == "/liquid/callback"
                and hosts == [urlsplit(self.redirect).netloc]
                and all(len(v) == 1 for v in params.values())
                and params.get("state", [""])[0].isascii()
                and secrets.compare_digest(params.get("state", [""])[0], self.state)
                and params.get("iss", [ORIGIN]) == [ORIGIN]
                and not self.result.done()
            )
            if valid and "error" in params and "code" not in params:
                self.result.set_result(None)
                status, body = "200 OK", "Liquid connection declined. You may close this tab."
            elif valid and opaque(params.get("code", [None])[0]) and "error" not in params:
                self.result.set_result(params["code"][0])
                status, body = "200 OK", "Return to stratosphere. You may close this tab."
            writer.write(
                (
                    f"HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\n"
                    "Cache-Control: no-store\r\nReferrer-Policy: no-referrer\r\n"
                    "Content-Security-Policy: default-src 'none'\r\nConnection: close\r\n"
                    f"Content-Length: {len(body.encode())}\r\n\r\n{body}"
                ).encode()
            )
            await writer.drain()
        except (
            TimeoutError,
            ValueError,
            UnicodeError,
            OSError,
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
        ):
            pass
        finally:
            writer.close()
            self.handlers.discard(task)


class LiquidConnection:
    """Explicit read-only connection, held in memory for the current process only."""

    def __init__(self, *, transport=None, open_browser=None, callback_factory=LoopbackCallback):
        self.transport = transport
        self.open_browser = open_browser or self._open_browser
        self.callback_factory = callback_factory
        self.session = None
        self._busy = False
        self.catalog_secret_check = None

    @staticmethod
    async def _open_browser(url):
        return await asyncio.to_thread(webbrowser.open, url, new=2)

    def disconnect(self):
        # The caller cancels any active connection worker. This erases local credentials;
        # provider-side OAuth consent must be revoked in the Liquid account separately.
        self.session = None

    def contains_credential(self, content: bytes) -> bool:
        if self.session is None:
            return False
        session = self.session
        values = (session.token, *session.redactions)
        if session.mcp_session_id is not None:
            values += (session.mcp_session_id,)
        return any(
            value.get_secret_value().encode() in content
            or canonical(value.get_secret_value())[1:-1].encode() in content
            for value in values
        )

    def client(self):
        return httpx.AsyncClient(
            transport=self.transport, timeout=20, follow_redirects=False, trust_env=False
        )

    async def _request(self, client, method, url, *, notification=False, **kwargs):
        # All destinations are constants or metadata already checked for exact equality.
        required(
            url
            in {
                RESOURCE,
                ORIGIN + "/.well-known/oauth-protected-resource",
                ORIGIN + "/.well-known/oauth-authorization-server",
                ORIGIN + "/oauth/register",
                ORIGIN + "/oauth/token",
            },
            "Liquid destination is not allowed.",
        )
        try:
            async with asyncio.timeout(25), client.stream(method, url, **kwargs) as response:
                required(
                    response.status_code in {200, 201, 202, 204},
                    "Liquid rejected the connection request; reconnect or check provider access.",
                )
                if notification:
                    return None, response.headers
                is_sse = (
                    response.headers.get("content-type", "").split(";")[0] == "text/event-stream"
                )
                content = bytearray()
                async for chunk in response.aiter_bytes():
                    content.extend(chunk)
                    required(
                        len(content) <= MAX_MESSAGE, "Liquid protocol response exceeded its limit."
                    )
                    if is_sse:
                        # JSON-RPC messages may arrive before an SSE connection closes.
                        normalized = bytes(content).replace(b"\r\n", b"\n")
                        blocks = normalized.split(b"\n\n")
                        for block in blocks[:-1]:
                            data = b"\n".join(
                                line[5:].lstrip(b" ")
                                for line in block.split(b"\n")
                                if line.startswith(b"data:")
                            )
                            if data:
                                payload = strict_json(data)
                                if isinstance(payload, dict) and "id" in payload:
                                    return payload, response.headers
                required(not is_sse, "Liquid event stream ended without a response.")
                payload = strict_json(content)
                required(isinstance(payload, dict), "Invalid Liquid protocol response.")
                return payload, response.headers
        except (
            httpx.HTTPError,
            TimeoutError,
            ValueError,
            UnicodeError,
            RecursionError,
            OverflowError,
        ):
            raise LiquidError(
                "Liquid connection failed; no provider response body was retained."
            ) from None

    async def discover(self):
        expected = Discovery()
        async with self.client() as client:
            resource, _ = await self._request(
                client, "GET", ORIGIN + "/.well-known/oauth-protected-resource"
            )
            server, _ = await self._request(
                client, "GET", ORIGIN + "/.well-known/oauth-authorization-server"
            )
        required(
            resource.get("resource") == RESOURCE
            and resource.get("authorization_servers") == [ORIGIN]
            and advertised(resource, "scopes_supported", "read")
            and advertised(resource, "bearer_methods_supported", "header"),
            "Liquid resource metadata changed; connection is blocked for review.",
        )
        for key, value in {
            "issuer": expected.issuer,
            "authorization_endpoint": expected.authorize,
            "token_endpoint": expected.token,
            "registration_endpoint": expected.registration,
        }.items():
            required(
                server.get(key) == value,
                "Liquid authorization endpoints changed; connection is blocked for review.",
            )
        required(
            advertised(server, "code_challenge_methods_supported", "S256")
            and advertised(server, "token_endpoint_auth_methods_supported", "none")
            and advertised(server, "grant_types_supported", "authorization_code")
            and advertised(server, "response_types_supported", "code")
            and advertised(server, "scopes_supported", "read"),
            "Liquid does not advertise the required public-client read flow.",
        )
        return expected

    async def connect(self, *, timeout=240):
        required(not self._busy, "Liquid authorization is already running.")
        required(
            self.session is None, "Disconnect the existing Liquid session before reconnecting."
        )
        self._busy = True
        try:
            metadata = await self.discover()
            state, verifier = secrets.token_urlsafe(32), secrets.token_urlsafe(64)
            challenge = (
                base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
                .decode()
                .rstrip("=")
            )
            async with self.callback_factory(state) as callback, self.client() as client:
                registration, _ = await self._request(
                    client,
                    "POST",
                    metadata.registration,
                    json={
                        "client_name": "stratosphere read-only inspection",
                        "application_type": "native",
                        "redirect_uris": [callback.redirect],
                        "token_endpoint_auth_method": "none",
                        "grant_types": ["authorization_code"],
                        "response_types": ["code"],
                        "scope": "read",
                    },
                )
                client_id = registration.get("client_id")
                required(
                    opaque(client_id)
                    and registration.get("token_endpoint_auth_method") == "none"
                    and registration.get("redirect_uris") == [callback.redirect],
                    "Liquid did not accept the public loopback client; no sign-in was opened.",
                )
                query = urlencode(
                    {
                        "response_type": "code",
                        "client_id": client_id,
                        "redirect_uri": callback.redirect,
                        "scope": "read",
                        "state": state,
                        "code_challenge": challenge,
                        "code_challenge_method": "S256",
                        "resource": RESOURCE,
                    }
                )
                try:
                    opened = await self.open_browser(metadata.authorize + "?" + query)
                except Exception:
                    raise LiquidError("Cannot open the Liquid sign-in browser.") from None
                required(opened, "Cannot open the Liquid sign-in browser.")
                try:
                    code = await asyncio.wait_for(callback.result, timeout)
                except TimeoutError:
                    raise LiquidError(
                        "Liquid sign-in timed out; the local callback was closed."
                    ) from None
                required(code is not None, "Liquid authorization was declined.")
                requested_at = time.monotonic()
                tokens, _ = await self._request(
                    client,
                    "POST",
                    metadata.token,
                    data={
                        "grant_type": "authorization_code",
                        "client_id": client_id,
                        "code": code,
                        "redirect_uri": callback.redirect,
                        "code_verifier": verifier,
                        "resource": RESOURCE,
                    },
                )
                required(
                    tokens.get("scope") == "read"
                    and isinstance(tokens.get("token_type"), str)
                    and tokens["token_type"].lower() == "bearer"
                    and opaque(tokens.get("access_token"))
                    and type(tokens.get("expires_in")) is int
                    and 0 < tokens["expires_in"] < 2**31,
                    "Liquid did not confirm a bounded read-only token; connection was discarded.",
                )
                session = ReadSession(
                    SecretStr(tokens["access_token"]), requested_at + tokens["expires_in"]
                )
                # Grant values are memory-only redaction guards, never refresh credentials in use.
                session.catalog = await self._catalog(
                    client,
                    session,
                    known_secrets=tuple(
                        value
                        for value in (
                            tokens["access_token"],
                            tokens.get("refresh_token"),
                            code,
                            verifier,
                            state,
                            registration.get("client_secret"),
                            registration.get("registration_access_token"),
                        )
                        if isinstance(value, str) and value
                    ),
                )
                self.session = session
                return self.status_markdown()
        finally:
            self._busy = False

    async def _catalog(self, client, session, *, known_secrets=()):
        headers = {
            "Accept": "application/json, text/event-stream",
            "Authorization": "Bearer " + session.token.get_secret_value(),
        }
        counter = 0

        async def rpc(method, params=None, notification=False):
            nonlocal counter
            required(
                method in {"initialize", "notifications/initialized", "tools/list"},
                "This Liquid client cannot call tools or change account state.",
            )
            required(time.monotonic() < session.expires_at, "Liquid token expired; reconnect.")
            message = {"jsonrpc": "2.0", "method": method}
            if not notification:
                counter += 1
                message["id"] = counter
            if params is not None:
                message["params"] = params
            payload, response_headers = await self._request(
                client, "POST", RESOURCE, json=message, headers=headers, notification=notification
            )
            if method == "initialize":
                if sid := response_headers.get("mcp-session-id"):
                    required(opaque(sid), "Invalid Liquid MCP session identifier.")
                    headers["Mcp-Session-Id"] = sid
            if notification:
                return None
            required(
                payload.get("jsonrpc") == "2.0"
                and type(payload.get("id")) is int
                and payload["id"] == counter
                and "error" not in payload
                and isinstance(payload.get("result"), dict),
                "Liquid returned an invalid MCP response; connection was discarded.",
            )
            return payload["result"]

        initialized = await rpc(
            "initialize",
            {
                "protocolVersion": PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "stratosphere", "version": "0.0.1"},
            },
        )
        capabilities = initialized.get("capabilities")
        required(
            initialized.get("protocolVersion") == PROTOCOL
            and isinstance(capabilities, dict)
            and isinstance(capabilities.get("tools"), dict),
            "Liquid did not negotiate the supported MCP tool catalog protocol.",
        )
        headers["Mcp-Protocol-Version"] = PROTOCOL
        await rpc("notifications/initialized", notification=True)
        definitions, names, cursors, cursor = [], set(), set(), None
        for _ in range(100):
            result = await rpc("tools/list", {"cursor": cursor} if cursor is not None else {})
            required(isinstance(result.get("tools"), list), "Invalid Liquid tool catalog.")
            for tool in result["tools"]:
                required(
                    isinstance(tool, dict)
                    and isinstance(tool.get("name"), str)
                    and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", tool["name"]),
                    "Invalid Liquid tool catalog name.",
                )
                required(tool["name"] not in names, "Liquid repeated a catalog tool.")
                names.add(tool["name"])
                definitions.append(tool)
            cursor = result.get("nextCursor")
            if cursor is None:
                try:
                    catalog = LiquidCatalog.capture(
                        definitions,
                        known_secrets=(*known_secrets, headers.get("Mcp-Session-Id", "")),
                    )
                    required(
                        not self.catalog_secret_check
                        or not catalog.contains_secret(self.catalog_secret_check),
                        "Credential-bearing Liquid catalog was rejected.",
                    )
                    session.mcp_session_id = (
                        SecretStr(headers["Mcp-Session-Id"])
                        if "Mcp-Session-Id" in headers
                        else None
                    )
                    session.rpc_id = counter
                    session.initialized = True
                    session.redactions = tuple(SecretStr(value) for value in known_secrets)
                    return catalog
                except CatalogError as error:
                    raise LiquidError(str(error)) from None
            required(opaque(cursor) and cursor not in cursors, "Invalid Liquid catalog pagination.")
            cursors.add(cursor)
        raise LiquidError("Liquid tool catalog pagination exceeded its protocol limit.")

    def catalog_snapshot(self):
        required(
            self.session is not None and time.monotonic() < self.session.expires_at,
            "Connect with /liquid-connect before inspecting the Liquid catalog.",
        )
        required(self.session.catalog is not None, "No complete Liquid catalog is available.")
        return self.session.catalog

    def status_markdown(self):
        active = self.session is not None and time.monotonic() < self.session.expires_at
        return (
            "# Liquid connection\n\n"
            + (
                "**Read-only catalog connected** (current process only).\n\n"
                if active
                else "**Not connected**. Use `/liquid-connect` for browser authorization.\n\n"
            )
            + "Public metadata is checked against Liquid's standard Co-Invest endpoint. "
            "The client requests only `read`; additional or unconfirmed scopes are rejected.\n\n"
            "Account identity, paper mode, instrument access and execution are **unverified**. "
            "A tool catalog is not account or paper-trading verification. "
            "Metadata/catalog inspection calls no tool; orders remain blocked. "
            "Only `/liquid-account` explicitly requests the reviewed address read.\n\n"
            "Use `/liquid-disconnect` to forget this process's credentials. "
            "Revoke provider consent separately in Liquid. No token is saved across restart.\n\n"
            + (
                f"Catalog tool count: {len(self.session.tools)}. "
                "Use `/liquid-tools` to inspect names and `/liquid-tool <name>` for definitions. "
                "Use `/liquid-catalog-save` for an explicit private export. "
                "Descriptions are not sent to a model."
                if active
                else "No account data has been loaded."
            )
        )
