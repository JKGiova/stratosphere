"""Finite public research fallback chain. Access/policy failures never authorize another route."""

import asyncio
import json
from urllib.parse import urlsplit

import httpx

from stratosphere.research.store import ResearchError
from stratosphere.sources.contracts import SourcePiece
from stratosphere.sources.documents import supply_pdf
from stratosphere.sources.process import MissingTool, PublicCLIs, ToolFailure
from stratosphere.sources.web import PublicWeb, ReadableHTML, public_url

BACKENDS = ("direct", "agent-reach", "opencli", "agent-browser")


class AccessBlocked(ResearchError):
    pass


def check_text(text):
    head = text[:1500].casefold()
    if any(
        p in head
        for p in (
            "sign in to continue",
            "subscribe to continue",
            "subscribe to read",
            "verify you are human",
            "access denied",
            "requiring captcha",
        )
    ):
        raise AccessBlocked("Source requires additional access; no fallback escalation")
    if len(text.strip()) < 32:
        raise ValueError("No substantial public text")
    return text


def cli_data(name, payload):
    if name != "agent-browser" and not isinstance(payload, dict):
        raise ValueError("Invalid CLI response")
    if name == "agent-reach":
        ok, data = payload.get("ok"), payload.get("items")
    elif name == "opencli":
        ok, data = payload.get("ok"), payload.get("data")
    else:
        if not isinstance(payload, list) or not payload:
            raise ValueError("Invalid browser batch response")
        if any(not isinstance(p, dict) or not p.get("success", False) for p in payload):
            raise ValueError("Browser batch failed")
        data = [payload[-1].get("result", {})]
        ok = True
    if ok is not True:
        error = payload.get("error") or {}
        code = str(error.get("code", "") if isinstance(error, dict) else error).upper()
        if any(p in code for p in ("AUTH", "LOGIN", "FORBIDDEN", "CAPTCHA", "RATE_LIMIT")):
            raise AccessBlocked("Upstream access/rate policy blocks this research request")
        raise ValueError("CLI did not return a successful result")
    if not isinstance(data, list) or any(not isinstance(i, dict) for i in data):
        raise ValueError("Invalid CLI records")
    return data


class ResearchWeb(PublicWeb):
    def __init__(self, sources, *, clis=None, **kwargs):
        super().__init__(sources, **kwargs)
        self.clis = clis or PublicCLIs()

    def capabilities(self):
        return {
            "direct": True,
            **self.clis.availability(),
            "scope": "Anonymous public research only; missing tools degrade explicitly",
            "fallback_order": list(BACKENDS),
        }

    def clean(self, value):
        if self.sources.store.has_secret(json.dumps(value).encode()):
            raise AccessBlocked("Sensitive source output rejected before storage or inference")

    async def search(self, query):
        if not query.strip() or query.startswith("-") or len(query) > 180:
            raise ResearchError("Use a nonempty public query of at most 180 characters")
        self.clean(query)
        hits, attempts = await super().search(query)
        if any(a.get("state") == "access_blocked" for a in attempts):
            return hits, attempts
        if hits:
            return hits, attempts
        for name in ("agent-reach", "opencli"):
            try:
                async with asyncio.timeout(45):
                    payload = await self.clis.call(name, "search", query)
                self.clean(payload)
                records = cli_data(name, payload)
                filtered = []
                for item in records[:6]:
                    url = item.get("url", "")
                    if not isinstance(url, str):
                        continue
                    if url.startswith("http://arxiv.org/"):
                        url = "https://" + url[7:]
                    try:
                        public_url(url)
                    except ValueError:
                        continue
                    filtered.append(
                        {"title": str(item.get("title", "Untitled"))[:1000], "url": url}
                    )
                attempts.append({"index": name, "state": "completed", "records": records[:6]})
                if filtered:
                    return filtered[:3], attempts
            except AccessBlocked:
                attempts.append({"index": name, "state": "access_blocked"})
                break
            except MissingTool:
                attempts.append({"index": name, "state": "unavailable"})
            except ResearchError, ValueError, OSError, TimeoutError:
                attempts.append({"index": name, "state": "failed"})
        return [], attempts

    async def acquire(self, request):
        await self.sources.require_online()
        public_url(request.url)
        # An invalid/private target is terminal; no CLI gets a second interpretation of it.
        await self.resolver(urlsplit(request.url).hostname)
        identity = self.sources.store.create(request)
        attempts = []
        materials = []
        fields = None
        for name in BACKENDS:
            try:
                async with asyncio.timeout(45):
                    if name == "direct":
                        content, media, chain = await self.get(request.url, limit=32 * 1024 * 1024)
                        if content.startswith(b"%PDF-"):
                            return await supply_pdf(
                                self.sources,
                                request,
                                content,
                                identity=identity,
                                retrieval_chain=chain,
                            )
                        if len(content) > 4_000_000:
                            raise AccessBlocked("Public text exceeds its frozen byte budget")
                        materials.append(
                            self.sources.store.raw(
                                identity, content, media, name + " public response"
                            )
                        )
                        text = content.decode("utf-8")
                        if "html" in media:
                            parser = ReadableHTML()
                            parser.feed(text)
                            text = "".join(parser.parts)
                        elif not media.startswith("text/"):
                            raise ValueError("Unsupported public text media")
                        payload = {"retrieval_chain": chain}
                    else:
                        payload = await self.clis.call(name, "read", request.url)
                        self.clean(payload)
                        content, media = json.dumps(payload).encode(), "application/json"
                        materials.append(
                            self.sources.store.raw(
                                identity, content, media, name + " public response"
                            )
                        )
                        records = cli_data(name, payload)
                        text = "\n\n".join(
                            str(item.get("text") or item.get("extract") or "")
                            for item in records[:3]
                        )
                    self.clean(text)
                    check_text(text)
                    attempts.append({"backend": name, "state": "completed"})
                    fields = dict(
                        access_state="acquired",
                        title=request.url,
                        pieces=(SourcePiece(kind="text", text=text, locator=request.url),),
                        limitations=(
                            f"Public reader: {name}; anonymous session, no account access.",
                            "Derived reader text is not source-conformance or execution evidence.",
                            "JavaScript rendering/formulas/session semantics remain unverified.",
                        ),
                    )
                    break
            except asyncio.CancelledError:
                if (self.sources.store.directory(identity) / "snapshot.json").exists():
                    raise  # PDF cancellation already sealed its own unavailable snapshot.
                attempts.append({"backend": name, "state": "cancelled"})
                audit = self.sources.store.raw(
                    identity,
                    json.dumps({"attempts": attempts}).encode(),
                    "application/json",
                    "cancelled public fallback audit",
                )
                self.sources._finish(
                    identity,
                    request,
                    dict(
                        access_state="unavailable",
                        raw_materials=(*materials, audit),
                        limitations=("Public acquisition cancelled; no late result published.",),
                    ),
                    normalization="research-fallback/1",
                )
                raise
            except AccessBlocked:
                attempts.append({"backend": name, "state": "policy_blocked"})
                break
            except httpx.HTTPStatusError as exc:
                terminal = exc.response.status_code in {401, 403, 404, 429}
                attempts.append(
                    {"backend": name, "state": "access_blocked" if terminal else "failed"}
                )
                if terminal:
                    break
            except MissingTool:
                attempts.append({"backend": name, "state": "unavailable"})
            except ToolFailure:
                attempts.append({"backend": name, "state": "failed"})
            except ResearchError:
                # DNS/peer validation and byte/secret guards are policy failures, not retry hints.
                attempts.append({"backend": name, "state": "policy_blocked"})
                break
            except httpx.HTTPError, ValueError, OSError, TimeoutError:
                attempts.append({"backend": name, "state": "failed"})
        audit = self.sources.store.raw(
            identity,
            json.dumps({"schema_version": "research-fallback/1", "attempts": attempts}).encode(),
            "application/json",
            "public fallback audit",
        )
        if fields is None:
            fields = dict(
                access_state="unavailable",
                raw_materials=(*materials, audit),
                limitations=(
                    "No safe public reader completed; access/rate policy is never escalated.",
                ),
            )
        else:
            fields["raw_materials"] = (*materials, audit)
        return self.sources._finish(identity, request, fields, normalization="research-fallback/1")
