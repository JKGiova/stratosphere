"""Material availability and provenance are separate from strategy conformance."""

import re
from datetime import UTC
from typing import Literal, Self
from urllib.parse import parse_qs, quote, unquote, urlencode, urlsplit, urlunsplit

from pydantic import AwareDatetime, Field, model_validator

from stratosphere.phase0.contracts import Contract, Digest, fingerprint

Provider = Literal["youtube", "instagram", "x", "tradingview", "github", "supplied"]


def source_url(value: str) -> tuple[str, str]:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or parsed.username or parsed.password or parsed.port is not None:
        raise ValueError("Source URLs must use HTTPS without credentials or custom ports")
    if not parsed.hostname or "\\" in value or any(ord(c) < 32 for c in value):
        raise ValueError("Invalid source URL")
    host = parsed.hostname.lower()
    path = unquote(parsed.path)
    if any(ord(c) < 32 for c in path) or any(part in {".", ".."} for part in path.split("/")):
        raise ValueError("Source paths cannot contain traversal segments")
    query = parse_qs(parsed.query, keep_blank_values=True)
    if any(
        k.casefold() in {"access_token", "token", "key", "api_key", "authorization", "signature"}
        for k in query
    ):
        raise ValueError("Enter credentials only in the source credential fields")
    if host in {"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be"}:
        if host == "youtu.be":
            video = path.strip("/")
        elif path.startswith(("/shorts/", "/embed/", "/live/")):
            parts = path.strip("/").split("/")
            if len(parts) != 2:
                raise ValueError("Use a single YouTube video URL")
            video = parts[1]
        else:
            ids = query.get("v", [])
            if path != "/watch" or len(ids) != 1:
                raise ValueError("Use a YouTube video URL")
            video = ids[0]
        if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video):
            raise ValueError("Invalid YouTube video ID")
        return "youtube", "https://www.youtube.com/watch?" + urlencode({"v": video})
    if host in {"instagram.com", "www.instagram.com"}:
        if not re.fullmatch(r"/(p|reel|reels)/[A-Za-z0-9_-]+/?", path):
            raise ValueError("Use an Instagram post or Reel URL")
        return "instagram", "https://www.instagram.com" + path.rstrip("/") + "/"
    if host in {"x.com", "www.x.com", "twitter.com", "www.twitter.com", "mobile.twitter.com"}:
        match = re.fullmatch(
            r"/([A-Za-z0-9_]+)/status/([0-9]+)(?:/(?:photo|video)/[0-9]+)?/?", path
        )
        if not match:
            raise ValueError("Use a single X post URL")
        return "x", f"https://x.com/{match[1]}/status/{match[2]}"
    if host in {"tradingview.com", "www.tradingview.com"}:
        if not re.fullmatch(r"/script/[A-Za-z0-9_-]+/?", path):
            raise ValueError("Use a TradingView script publication URL")
        return "tradingview", "https://www.tradingview.com" + path.rstrip("/") + "/"
    if host == "github.com" or host == "www.github.com":
        parts = path.strip("/").split("/")
        if (
            len(parts) < 2
            or not re.fullmatch(r"[A-Za-z0-9_.-]+", parts[0])
            or not re.fullmatch(r"[A-Za-z0-9_.-]+", parts[1])
        ):
            raise ValueError("Use a GitHub repository or file URL")
        if len(parts) > 2 and (parts[2] not in {"blob", "tree"} or len(parts) < 4):
            raise ValueError("Use a GitHub repository, tree or blob URL")
        return "github", urlunsplit(
            ("https", "github.com", quote(path.rstrip("/"), safe="/-._~"), "", "")
        )
    raise ValueError("This source host is not supported; supply the available material instead")


class SourceRequest(Contract):
    url: str
    github_ref: str | None = None
    submitted_url: str | None = None

    @model_validator(mode="after")
    def canonical_url(self) -> Self:
        provider, url = source_url(self.url)
        submitted = self.submitted_url or self.url
        if source_url(submitted) != (provider, url):
            raise ValueError("Submitted and canonical source identities disagree")
        object.__setattr__(self, "submitted_url", submitted)
        object.__setattr__(self, "url", url)
        if self.github_ref is not None and provider != "github":
            raise ValueError("Repository refs apply only to GitHub sources")
        if self.github_ref is not None and (
            not self.github_ref.strip() or any(ord(c) < 32 for c in self.github_ref)
        ):
            raise ValueError("A repository ref must be explicit and nonempty")
        return self

    @property
    def provider(self) -> str:
        return source_url(self.url)[0]


class SuppliedRequest(Contract):
    label: str = Field(min_length=1)
    attribution_url: str | None = None

    @model_validator(mode="after")
    def attribution(self) -> Self:
        if self.attribution_url is not None:
            _, url = source_url(self.attribution_url)
            object.__setattr__(self, "attribution_url", url)
        return self

    @property
    def provider(self) -> str:
        return "supplied"

    @property
    def url(self) -> str:
        return "supplied://material/" + quote(self.label, safe="")


class SourcePiece(Contract):
    kind: Literal["description", "caption", "post", "code", "text", "metadata"]
    text: str = Field(min_length=1)
    locator: str
    language: str | None = None
    start_ms: int | None = Field(default=None, strict=True, ge=0)
    end_ms: int | None = Field(default=None, strict=True, gt=0)
    first_line: int | None = Field(default=None, strict=True, gt=0)
    last_line: int | None = Field(default=None, strict=True, gt=0)

    @model_validator(mode="after")
    def ranges(self) -> Self:
        if (self.start_ms is None) != (self.end_ms is None):
            raise ValueError("Caption references require both time boundaries")
        if self.start_ms is not None and self.end_ms <= self.start_ms:
            raise ValueError("Invalid caption time range")
        if (self.first_line is None) != (self.last_line is None):
            raise ValueError("Code references require both line boundaries")
        if self.first_line is not None and self.last_line < self.first_line:
            raise ValueError("Invalid code line range")
        return self

    @property
    def digest(self) -> str:
        return fingerprint(self)


class RawMaterial(Contract):
    sha256: Digest
    media_type: str
    scope: str
    byte_count: int = Field(strict=True, ge=0)


class SourceSnapshot(Contract):
    schema_version: Literal["source-snapshot/1"] = "source-snapshot/1"
    id: str = Field(pattern=r"^[a-f0-9]{32}$")
    provider: Provider
    requested_url: str
    retrieved_at: AwareDatetime
    access_state: Literal[
        "acquired",
        "partial",
        "unavailable",
        "requires_credentials",
        "provider_error",
        "unsupported_format",
        "integrity_failed",
        "sensitive_material_rejected",
    ]
    title: str | None = None
    pieces: tuple[SourcePiece, ...] = ()
    raw_materials: tuple[RawMaterial, ...] = ()
    limitations: tuple[str, ...]
    next_steps: tuple[str, ...] = ()
    provider_status: int | None = None
    repository_commit: str | None = Field(default=None, pattern=r"^([a-f0-9]{40}|[a-f0-9]{64})$")
    repository_blob: str | None = Field(default=None, pattern=r"^([a-f0-9]{40}|[a-f0-9]{64})$")
    license_id: str | None = None
    normalization: str
    parser_sha256: Digest
    redistribution: Literal["not_assessed"] = "not_assessed"
    behavior_conformance: Literal["not_verified"] = "not_verified"

    @model_validator(mode="after")
    def scope(self) -> Self:
        object.__setattr__(self, "retrieved_at", self.retrieved_at.astimezone(UTC))
        if self.provider != "supplied":
            provider, url = source_url(self.requested_url)
            if provider != self.provider or url != self.requested_url:
                raise ValueError("Snapshot source identity mismatch")
        if self.access_state == "acquired" and not any(p.kind != "metadata" for p in self.pieces):
            raise ValueError("Acquired material cannot be empty")
        if self.pieces and not self.raw_materials:
            raise ValueError("Material must reference retained raw input digests")
        if self.access_state != "acquired" and not self.limitations:
            raise ValueError("Partial/unavailable material requires explicit limitations")
        return self

    @property
    def digest(self):
        return fingerprint(self)

    @property
    def analysis_text(self):
        # Titles/counts/metadata are not quoted behavioral evidence.
        return "\n\n".join(piece.text for piece in self.pieces if piece.kind != "metadata")
