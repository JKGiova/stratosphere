"""Read-only source acquisition. Snapshots are evidence inputs, never executable code."""

import asyncio
import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from html.parser import HTMLParser
from urllib.parse import quote, unquote, urlsplit

import httpx

from stratosphere.research.data import require_online
from stratosphere.research.store import ResearchError
from stratosphere.sources.contracts import (
    SourcePiece,
    SourceRequest,
    SourceSnapshot,
    SuppliedRequest,
)
from stratosphere.sources.store import SourceStore, parser_digest


@dataclass(frozen=True)
class SourceCredentials:
    # Read-only tokens are never inferred from Git/GitHub CLI or browser credentials.
    github: str = field(default="", repr=False)

    def secrets(self):
        return tuple(s for s in (self.github,) if s)


class AcquisitionError(Exception):
    def __init__(self, state, reason, *, status=None, next_steps=()):
        self.state, self.reason = state, reason
        self.status, self.next_steps = status, next_steps


class EmbedText(HTMLParser):
    """Only X fallback post paragraphs; no scripts, links, metadata or widget execution."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.in_quote = False
        self.in_paragraph = False
        self.ignored = 0
        self.parts = []
        self.quote_count = 0

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag in {"script", "style"}:
            self.ignored += 1
        elif tag == "blockquote" and "twitter-tweet" in attrs.get("class", "").split():
            self.in_quote = True
            self.quote_count += 1
        elif tag == "p" and self.in_quote:
            self.in_paragraph = True
        elif tag == "br" and self.in_paragraph and not self.ignored:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self.ignored = max(0, self.ignored - 1)
        elif tag == "p":
            self.in_paragraph = False
            self.parts.append("\n")
        elif tag == "blockquote":
            self.in_quote = False
            self.in_paragraph = False

    def handle_data(self, data):
        if self.in_quote and self.in_paragraph and not self.ignored:
            self.parts.append(data)

    @property
    def text(self):
        if self.quote_count != 1:
            return ""
        return "".join(self.parts).strip()


class PageMetadata(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.values = {}

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        name = attrs.get("property", attrs.get("name"))
        if tag == "meta" and name in {"og:title", "og:description"}:
            self.values[name] = attrs.get("content", "")


class SourceService:
    def __init__(
        self,
        store: SourceStore,
        *,
        credentials=None,
        transport=None,
        online_check=require_online,
        clock=lambda: datetime.now(UTC),
    ):
        credentials = credentials or SourceCredentials()
        self.store, self.credentials = store, credentials
        self.transport, self.online_check, self.clock = transport, online_check, clock
        self.store.known_secrets = tuple(set(store.known_secrets + credentials.secrets()))

    async def require_online(self):
        try:
            await self.online_check()
        except httpx.HTTPError, ValueError:
            raise ResearchError(
                "Source inspection requires online access; retry connected."
            ) from None

    async def open(self, source_id):
        await self.require_online()
        return self.store.snapshot(source_id)

    async def acquire(self, request: SourceRequest):
        await self.require_online()
        # There is no cached/fabricated fallback after a failed provider request.
        source_id = self.store.create(request)
        try:
            async with httpx.AsyncClient(
                timeout=30, follow_redirects=False, trust_env=False, transport=self.transport
            ) as client:
                reader = getattr(self, "_" + request.provider)
                fields = await reader(client, source_id, request)
        except asyncio.CancelledError:
            fields = dict(
                access_state="unavailable",
                limitations=(
                    "Retrieval canceled; material may be incomplete and was not analyzed.",
                ),
                next_steps=("Start a new retrieval; this attempt is retained.",),
            )
            self._finish(source_id, request, fields)
            raise
        except AcquisitionError as error:
            fields = dict(
                access_state=error.state,
                limitations=(error.reason,),
                next_steps=error.next_steps
                or ("Supply the available text/code with attribution.",),
                provider_status=error.status,
            )
        except httpx.HTTPError:
            fields = dict(
                access_state="provider_error",
                limitations=("Provider connection failed; no cached material was substituted.",),
                next_steps=("Retry online as a new retrieval attempt.",),
            )
        except ResearchError:
            fields = dict(
                access_state="sensitive_material_rejected",
                limitations=("Credential-bearing source material was rejected before storage.",),
                next_steps=("Remove credentials from the source and retry.",),
            )
        except ValueError, KeyError, TypeError, UnicodeError, binascii.Error, AttributeError:
            fields = dict(
                access_state="unsupported_format",
                limitations=("Provider returned an unrecognized or invalid material format.",),
                next_steps=("Supply the available text/code; no interpretation was invented.",),
            )
        return self._finish(source_id, request, fields)

    def _finish(self, source_id, request, fields):
        value = SourceSnapshot(
            id=source_id,
            provider=request.provider,
            requested_url=request.url,
            retrieved_at=self.clock(),
            normalization="source-readers/1",
            parser_sha256=parser_digest(),
            **fields,
        )
        if self.store.has_secret(value.model_dump_json().encode()):
            value = SourceSnapshot(
                id=source_id,
                provider=request.provider,
                requested_url=request.url,
                retrieved_at=self.clock(),
                access_state="sensitive_material_rejected",
                limitations=("Credential-bearing parsed material was rejected before storage.",),
                normalization="source-readers/1",
                parser_sha256=parser_digest(),
            )
        self.store.finish(value)
        return value

    async def supply(self, request: SuppliedRequest, content: bytes, *, kind="text"):
        await self.require_online()
        if kind not in {"text", "code"}:
            raise ResearchError(
                "Supply text or code; timed caption parsing is not implemented yet."
            )
        source_id = self.store.create(request)
        try:
            text = content.decode("utf-8-sig")
            if not text.strip() or "\0" in text:
                raise ValueError("Non-text source")
            raw = self.store.raw(source_id, content, "text/plain", "user-supplied material")
            fields = dict(
                access_state="acquired",
                title=request.label,
                pieces=(
                    SourcePiece(
                        kind=kind,
                        text=text,
                        locator="supplied lines 1-" + str(len(text.splitlines())),
                        first_line=1,
                        last_line=len(text.splitlines()),
                    ),
                ),
                raw_materials=(raw,),
                limitations=(
                    "User-supplied material; completeness and original publication identity are "
                    "unverified.",
                    "Attribution: " + (request.attribution_url or "not supplied"),
                    "Acquisition does not establish behavioral conformance or redistribution "
                    "rights.",
                ),
            )
        except UnicodeError, ValueError:
            fields = dict(
                access_state="unsupported_format",
                limitations=(
                    "Supply nonempty UTF-8 text or code; binary material was not interpreted.",
                ),
            )
        except ResearchError:
            fields = dict(
                access_state="sensitive_material_rejected",
                limitations=("Credential-bearing material was rejected before storage.",),
            )
        return self._finish(source_id, request, fields)

    async def _get(self, client, url, *, headers=None, params=None):
        response = await client.get(url, headers=headers, params=params)
        status = response.status_code
        if not 200 <= status < 300:
            state = (
                "requires_credentials"
                if status == 401
                else "unavailable"
                if status in {403, 404, 410}
                else "provider_error"
            )
            reason = f"Provider returned HTTP {status}; material was not acquired."
            if 300 <= status < 400:
                reason += " Redirects were not followed; use the canonical source address."
            if status == 403:
                reason += " This may reflect permissions or rate limits."
            if status == 404:
                reason += " Missing, private and deleted material cannot be distinguished here."
            raise AcquisitionError(state, reason, status=status)
        return response

    def _json(self, response):
        value = self._parsed(response)
        if not isinstance(value, dict):
            raise ValueError("Expected provider JSON object")
        return value

    def _parsed(self, response):
        value = response.json()
        self._check_parsed(value)
        return value

    def _check_parsed(self, value):
        if self.store.has_secret(json.dumps(value, ensure_ascii=False).encode()):
            raise ResearchError("Credential-bearing parsed source rejected")

    async def _github(self, client, source_id, request):
        parts = unquote(urlsplit(request.url).path).strip("/").split("/")
        owner, repo = parts[:2]
        base = f"https://api.github.com/repos/{quote(owner, safe='')}/{quote(repo, safe='')}"
        headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
        if self.credentials.github:
            headers["Authorization"] = "Bearer " + self.credentials.github
        response = await self._get(client, base, headers=headers)
        metadata = self._json(response)
        license_id = (metadata.get("license") or {}).get("spdx_id")
        path, ref = "", request.github_ref or metadata["default_branch"]
        if len(parts) > 2:
            suffix = "/".join(parts[3:])
            ref = request.github_ref or parts[3]
            if not suffix.startswith(ref + "/") and suffix != ref:
                raise AcquisitionError(
                    "unsupported_format",
                    "The explicit Git ref does not match the URL prefix; supply the exact "
                    "branch/tag or commit.",
                )
            path = suffix[len(ref) :].lstrip("/")
        resolved = self._json(
            await self._get(client, base + "/commits/" + quote(ref, safe=""), headers=headers)
        )
        commit = resolved["sha"]
        if not re.fullmatch(r"[a-f0-9]{40}", commit):
            raise ValueError("Invalid Git commit identity")
        response = await self._get(
            client,
            base + "/contents/" + quote(path, safe="/"),
            headers=headers,
            params={"ref": commit},
        )
        value = self._parsed(response)
        # File content may be base64-encoded in this JSON. Check decoded bytes before
        # persisting that envelope, so configured secrets cannot be saved encoded.
        if isinstance(value, dict) and value.get("encoding") == "base64":
            decoded = base64.b64decode("".join(value["content"].split()), validate=True)
            if self.store.has_secret(decoded):
                raise ResearchError("Credential-bearing file rejected")
        raw = self.store.raw(
            source_id, response.content, "application/json", "GitHub contents metadata at " + commit
        )
        common = dict(
            repository_commit=commit,
            license_id=license_id,
            raw_materials=(raw,),
            limitations=(
                "Only the selected file/directory was inspected; dependencies and behavior are "
                "unverified.",
                "Code acquisition does not authorize redistribution or execution.",
            ),
        )
        if isinstance(value, list):
            return dict(
                **{
                    **common,
                    "limitations": common["limitations"]
                    + (
                        "Directory listing only; no repository files acquired. GitHub Contents "
                        "listings have a 1,000-entry provider limit; completeness is not "
                        "established.",
                    ),
                },
                access_state="partial",
                pieces=(
                    SourcePiece(
                        kind="metadata",
                        text="\n".join(str(v.get("path", "")) for v in value) or "Empty directory",
                        locator="contents listing at " + commit,
                    ),
                ),
                next_steps=(
                    "Select an explicit blob URL; give github_ref for branch names containing "
                    "slashes.",
                ),
            )
        if (
            not isinstance(value, dict)
            or value.get("type") != "file"
            or value.get("submodule_git_url")
        ):
            raise AcquisitionError(
                "unsupported_format",
                "Submodules, symbolic links and non-file objects are not followed.",
            )
        # Contents can resolve symlinks. The Git tree identifies the path's actual object mode.
        tree_sha = resolved["commit"]["tree"]["sha"]
        path_parts = path.split("/")
        for index, part in enumerate(path_parts):
            tree = self._json(
                await self._get(client, base + "/git/trees/" + tree_sha, headers=headers)
            )
            if tree.get("truncated") is not False:
                raise AcquisitionError(
                    "unsupported_format",
                    "Git tree is incomplete; the selected file cannot be verified.",
                )
            entry = next((e for e in tree["tree"] if e["path"] == part), None)
            mode = "040000" if index < len(path_parts) - 1 else "100644"
            allowed = {mode} if index < len(path_parts) - 1 else {"100644", "100755"}
            if not entry or entry["mode"] not in allowed:
                raise AcquisitionError(
                    "unsupported_format",
                    "Symlinks, submodules and missing tree paths are not followed.",
                )
            tree_sha = entry["sha"]
        blob = value["sha"]
        if not re.fullmatch(r"[a-f0-9]{40}", blob) or tree_sha != blob:
            raise AcquisitionError(
                "integrity_failed", "Git tree and Contents blob identities disagree."
            )
        if value.get("encoding") == "base64":
            body = base64.b64decode("".join(value["content"].split()), validate=True)
        else:
            response = await self._get(
                client,
                base + "/contents/" + quote(path, safe="/"),
                headers={**headers, "Accept": "application/vnd.github.raw+json"},
                params={"ref": commit},
            )
            body = response.content
        actual = hashlib.sha1(b"blob " + str(len(body)).encode() + b"\0" + body).hexdigest()
        if actual != blob or type(value.get("size")) is not int or len(body) != value["size"]:
            raise AcquisitionError(
                "integrity_failed", "Retrieved bytes do not match the pinned Git blob and size."
            )
        text = body.decode("utf-8-sig")
        if not text.strip() or "\0" in text:
            raise AcquisitionError(
                "unsupported_format", "The selected file is empty or not supported UTF-8 text."
            )
        material = self.store.raw(source_id, body, "text/plain", "Git blob " + blob)
        return dict(
            **{**common, "raw_materials": (raw, material)},
            access_state="acquired",
            repository_blob=blob,
            title=path,
            pieces=(
                SourcePiece(
                    kind="code",
                    text=text,
                    locator=f"{request.url} @ {commit} lines 1-{len(text.splitlines())}",
                    first_line=1,
                    last_line=len(text.splitlines()),
                ),
            ),
        )

    async def _youtube(self, client, source_id, request):
        response = await self._get(
            client, "https://www.youtube.com/oembed", params={"url": request.url, "format": "json"}
        )
        metadata = self._json(response)
        title = metadata["title"]
        if not isinstance(title, str) or not title.strip():
            raise ValueError("Missing title")
        raw = self.store.raw(
            source_id, response.content, "application/json", "YouTube oEmbed metadata only"
        )
        return dict(
            access_state="partial",
            title=title,
            pieces=(SourcePiece(kind="metadata", text=title, locator="oEmbed.title"),),
            raw_materials=(raw,),
            limitations=(
                "Only oEmbed metadata acquired; no transcript, audio, frames or complete "
                "description.",
                "Official caption downloads require authorized video-edit permission; no public "
                "transcript extractor is implemented.",
            ),
            next_steps=(
                "Supply a transcript with source attribution and original time references.",
            ),
        )

    async def _instagram(self, client, source_id, request):
        raise AcquisitionError(
            "requires_credentials",
            "Instagram oEmbed requires a Meta access token. An authorized Meta client is not "
            "implemented; no caption, audio or frames were acquired.",
            next_steps=("Supply the available caption/transcript with source attribution.",),
        )

    async def _x(self, client, source_id, request):
        response = await self._get(
            client,
            "https://publish.x.com/oembed",
            params={
                "url": request.url,
                "omit_script": "true",
                "hide_thread": "true",
                "dnt": "true",
            },
        )
        value = self._json(response)
        parser = EmbedText()
        parser.feed(value["html"])
        self._check_parsed(parser.text)
        raw = self.store.raw(
            source_id, response.content, "application/json", "X oEmbed fallback markup"
        )
        text = parser.text
        pieces = (
            (
                SourcePiece(
                    kind="post", text=text, locator="oEmbed.html > blockquote.twitter-tweet > p"
                ),
            )
            if text
            else ()
        )
        return dict(
            access_state="partial",
            pieces=pieces,
            raw_materials=(raw,),
            limitations=(
                "Only one post's fallback markup was inspected; long posts may be truncated and "
                "full threads/media/linked content were not acquired.",
                "The fallback format may change; text completeness and strategy conformance are "
                "unverified.",
            ),
            next_steps=("Supply missing thread text or media transcript with attribution.",),
        )

    async def _tradingview(self, client, source_id, request):
        response = await self._get(client, request.url)
        parser = PageMetadata()
        parser.feed(response.text)
        self._check_parsed(parser.values)
        raw = self.store.raw(
            source_id,
            response.content,
            "text/html",
            "TradingView publication page; not source code",
        )
        pieces = tuple(
            SourcePiece(kind="metadata", text=text, locator="meta[property='" + key + "']")
            for key, text in parser.values.items()
            if text.strip()
        )
        return dict(
            access_state="partial",
            title=parser.values.get("og:title"),
            pieces=pieces,
            raw_materials=(raw,),
            limitations=(
                "Publication page metadata only; Pine source, complete rules and behavior were "
                "not acquired.",
                "Open/protected/invite-only visibility is separate from page access; no code "
                "retrieval is implemented and closed code cannot be inferred.",
            ),
            next_steps=(
                "For open code, supply the visible Pine source. For closed code, supply an "
                "authorized explicit definition; hidden logic cannot be converted faithfully.",
            ),
        )


def source_markdown(value: SourceSnapshot):
    lines = [
        f"# Source {value.id}",
        f"**{value.provider} · {value.access_state}**",
        f"Retrieved {value.retrieved_at.isoformat()}",
        f"Source: {value.requested_url}",
        f"Snapshot SHA-256: `{value.digest}`",
        "Behavioral conformance: **not verified**. Redistribution rights: **not assessed**.",
    ]
    if value.repository_commit:
        lines.append(f"Pinned Git commit: `{value.repository_commit}`")
    for piece in value.pieces:
        # Code fences/Markdown in external content stay literal through indentation.
        lines += [
            f"## {piece.kind}: {piece.locator}",
            "\n".join("    " + line for line in piece.text.splitlines()),
        ]
    lines += ["## Limitations", *("- " + text for text in value.limitations)]
    if value.next_steps:
        lines += ["## Next steps", *("- " + text for text in value.next_steps)]
    return "\n\n".join(lines)
