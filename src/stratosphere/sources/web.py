"""Bounded public web/paper discovery. No browser sessions, credentials or paid search API."""

import asyncio
import ipaddress
import json
import socket
from html.parser import HTMLParser
from typing import Literal
from urllib.parse import quote, urljoin, urlsplit

import httpx
from pydantic import Field, model_validator

from stratosphere.phase0.contracts import Contract
from stratosphere.research.store import ResearchError
from stratosphere.sources.contracts import SourcePiece


def public_url(value):
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.port is not None
        or "\\" in value
        or any(ord(c) < 33 for c in value)
    ):
        raise ValueError("Use a public HTTPS source without credentials or custom ports")
    if parsed.fragment or parsed.query:
        raise ValueError("Use a canonical source URL without query credentials or fragments")
    host = parsed.hostname
    if host.lower() == "localhost" or host.lower().endswith((".local", ".localhost", ".internal")):
        raise ValueError("Local source hosts are forbidden")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise ValueError("Use a public source hostname, not an IP address")
    return value


class WebRequest(Contract):
    schema_version: Literal["public-web-source/1"] = "public-web-source/1"
    url: str = Field(min_length=1, max_length=2000)

    @model_validator(mode="after")
    def valid(self):
        public_url(self.url)
        return self

    @property
    def provider(self):
        return "web"

    @property
    def label(self):
        return self.url


async def public_addresses(host):
    entries = await asyncio.get_running_loop().getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    if not entries or any(not ipaddress.ip_address(e[4][0]).is_global for e in entries):
        raise ResearchError("Source resolves to a nonpublic address")
    return sorted({e[4][0] for e in entries}, key=lambda address: ":" in address)


class PublicTransport(httpx.AsyncHTTPTransport):
    """Pin the checked peer address while retaining the hostname for TLS and HTTP."""

    def __init__(self, resolver):
        super().__init__(retries=0)
        self.resolver = resolver

    async def handle_async_request(self, request):
        hostname = request.url.host
        peers = await self.resolver(hostname)
        if not peers or any(not ipaddress.ip_address(p).is_global for p in peers):
            raise ResearchError("No validated public source peer")
        request.headers["Host"] = hostname
        request.extensions["sni_hostname"] = hostname
        request.url = request.url.copy_with(host=peers[0])
        return await super().handle_async_request(request)


class ReadableHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.ignored = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style", "noscript", "svg"}:
            self.ignored += 1
        elif not self.ignored and tag in {"p", "div", "br", "li", "h1", "h2", "h3", "tr"}:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in {"script", "style", "noscript", "svg"}:
            self.ignored = max(0, self.ignored - 1)
        elif not self.ignored and tag in {"p", "div", "li", "tr"}:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.ignored:
            self.parts.append(data)


class PublicWeb:
    def __init__(self, sources, *, resolver=public_addresses):
        self.sources, self.resolver = sources, resolver

    async def get(self, url, *, params=None, limit=4_000_000):
        chain = []
        for _ in range(4):
            public_url(url)
            chain.append(url)
            if self.sources.transport is not None:
                await self.resolver(urlsplit(url).hostname)
            # Fresh pools prevent reusing a peer TLS session for another hostname.
            async with httpx.AsyncClient(
                transport=self.sources.transport or PublicTransport(self.resolver),
                timeout=20,
                trust_env=False,
                follow_redirects=False,
                headers={
                    "User-Agent": (
                        "stratosphere/0.0.1 "
                        "(https://github.com/JKGiova/stratosphere; personal research)"
                    )
                },
            ) as client:
                async with client.stream("GET", url, params=params) as response:
                    if response.is_redirect:
                        url = urljoin(url, response.headers.get("location", ""))
                        params = None
                        continue
                    response.raise_for_status()
                    content = bytearray()
                    async for chunk in response.aiter_bytes():
                        content.extend(chunk)
                        if len(content) > limit:
                            raise ResearchError("Public source exceeds the acquisition budget")
                    return bytes(content), response.headers.get("content-type", ""), chain
        raise ResearchError("Source redirect budget exceeded")

    async def acquire(self, request):
        from stratosphere.sources.documents import supply_pdf

        await self.sources.require_online()
        identity = self.sources.store.create(request)
        try:
            content, media, chain = await self.get(request.url, limit=32 * 1024 * 1024)
            if content.startswith(b"%PDF-"):
                return await supply_pdf(
                    self.sources, request, content, identity=identity, retrieval_chain=chain
                )
        except httpx.HTTPError, ResearchError, ValueError, OSError:
            return self.sources._finish(
                identity,
                request,
                dict(
                    access_state="unavailable",
                    limitations=(
                        "Public source was unavailable, blocked or exceeded its retrieval budget.",
                    ),
                ),
                normalization="public-web/1",
            )
        try:
            if len(content) > 4_000_000:
                raise ValueError("Public text exceeds its acquisition budget")
            text = content.decode("utf-8")
            if "html" in media:
                parser = ReadableHTML()
                parser.feed(text)
                text = "".join(parser.parts)
            elif not media.startswith("text/"):
                raise ValueError("Unsupported public document format")
            if not text.strip() or self.sources.store.has_secret(text.encode()):
                raise ResearchError("Empty or sensitive public material")
            raw = self.sources.store.raw(identity, content, media, "public source response")
            fields = dict(
                access_state="acquired",
                title=request.url,
                pieces=(SourcePiece(kind="text", text=text, locator=request.url),),
                raw_materials=(raw,),
                limitations=(
                    "Page text only; scripts, images, tables and formulas may be incomplete.",
                    "Retrieval does not verify publication, conversion or execution authority.",
                    "Retrieval chain: " + " -> ".join(chain),
                ),
            )
        except ValueError, UnicodeError, ResearchError:
            fields = dict(
                access_state="unsupported_format",
                limitations=("No usable safe text was extracted; supply the paper or plain text.",),
            )
        return self.sources._finish(identity, request, fields, normalization="public-web/1")

    async def search(self, query):
        """Two fixed read-only indexes, at most three results each; no model-authored URLs."""
        await self.sources.require_online()
        if not query.strip() or len(query) > 300 or self.sources.store.has_secret(query.encode()):
            raise ResearchError("Give a non-sensitive strategy search of at most 300 characters")
        results, attempts = [], []
        for name, endpoint, params in (
            (
                "wikipedia",
                "https://en.wikipedia.org/w/api.php",
                {
                    "action": "query",
                    "list": "search",
                    "srsearch": query,
                    "srlimit": 3,
                    "format": "json",
                },
            ),
            (
                "crossref",
                "https://api.crossref.org/works",
                {"query.bibliographic": query, "rows": 3},
            ),
        ):
            try:
                raw, _, chain = await self.get(endpoint, params=params)
                data = json.loads(raw)
                if name == "wikipedia":
                    hits = [
                        {
                            "title": r["title"],
                            "url": "https://en.wikipedia.org/wiki/"
                            + quote(r["title"].replace(" ", "_"), safe="()_-"),
                        }
                        for r in data["query"]["search"][:3]
                    ]
                else:
                    hits = [
                        {"title": r.get("title", ["Untitled paper"])[0], "url": r["URL"]}
                        for r in data["message"]["items"][:3]
                        if "URL" in r
                    ]
                results.extend(hits)
                attempts.append(
                    {
                        "index": name,
                        "state": "completed",
                        "raw": raw.decode("utf-8"),
                        "retrieval_chain": chain,
                    }
                )
            except httpx.HTTPError, ResearchError, ValueError, KeyError, TypeError, OSError:
                attempts.append({"index": name, "state": "failed"})
        return results, attempts
