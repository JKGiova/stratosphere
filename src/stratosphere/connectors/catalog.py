"""Private Liquid tool-definition snapshots are diagnostic data, never permissions."""

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from stratosphere.research.store import ResearchError

RESOURCE = "https://coinvest.liquid.trade/mcp"
PROTOCOL = "2025-06-18"
VERSION = "liquid-catalog/1"
MAX_CATALOG_BYTES = 200 * 1024 * 1024 + 4096  # Existing 100-page protocol bound, not uploads.


class CatalogError(ResearchError):
    """Catalog errors exclude raw provider data and credentials."""


def strict_json(content):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    def nonfinite(_):
        raise ValueError("Non-finite JSON value")

    return json.loads(content, object_pairs_hook=unique, parse_constant=nonfinite)


def canonical(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def tool_definition(value):
    if not isinstance(value, dict) or not re.fullmatch(
        r"[A-Za-z0-9_.-]{1,128}",
        value.get("name", "") if isinstance(value.get("name"), str) else "",
    ):
        raise ValueError("Invalid tool name")
    for key in ("inputSchema", "outputSchema"):
        schema = value.get(key)
        if key == "outputSchema" and key not in value:
            continue
        if not isinstance(schema, dict) or schema.get("type") != "object":
            raise ValueError("Invalid tool schema")
    for key in ("title", "description"):
        if key in value and not isinstance(value[key], str):
            raise ValueError("Invalid tool display text")
    for key in ("annotations", "_meta"):
        if key in value and not isinstance(value[key], dict):
            raise ValueError("Invalid tool metadata")
    annotations = value.get("annotations", {})
    for key in ("readOnlyHint", "destructiveHint", "idempotentHint", "openWorldHint"):
        if key in annotations and type(annotations[key]) is not bool:
            raise ValueError("Invalid tool annotation")
    if "title" in annotations and not isinstance(annotations["title"], str):
        raise ValueError("Invalid annotation title")


@dataclass(frozen=True, repr=False)
class LiquidCatalog:
    """Canonical JSON prevents callers from mutating nested schemas after capture."""

    document_json: str

    def __post_init__(self):
        try:
            if not isinstance(self.document_json, str):
                raise ValueError("Invalid catalog encoding")
            if len(self.document_json.encode()) > MAX_CATALOG_BYTES:
                raise ValueError("Oversized catalog")
            value = strict_json(self.document_json)
            if not isinstance(value, dict) or set(value) != {
                "schema_version",
                "resource",
                "protocol",
                "scope",
                "captured_at",
                "tools",
            }:
                raise ValueError("Invalid catalog fields")
            if (
                value["schema_version"] != VERSION
                or value["resource"] != RESOURCE
                or value["protocol"] != PROTOCOL
                or value["scope"] != "read"
                or not isinstance(value["tools"], list)
            ):
                raise ValueError("Invalid catalog boundary")
            captured = datetime.fromisoformat(value["captured_at"])
            if captured.tzinfo is None or captured.utcoffset().total_seconds() != 0:
                raise ValueError("Catalog capture time must be UTC")
            names = set()
            for tool in value["tools"]:
                tool_definition(tool)
                if tool["name"] in names:
                    raise ValueError("Repeated tool")
                names.add(tool["name"])
            if canonical(value) != self.document_json:
                raise ValueError("Catalog JSON is not canonical")
        except ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError:
            raise CatalogError(
                "Invalid Liquid catalog snapshot; raw definitions were not exposed."
            ) from None

    def __repr__(self):
        return f"LiquidCatalog(tool_count={len(self.names)}, authority=none)"

    @classmethod
    def capture(cls, tools, *, known_secrets=(), captured_at=None):
        try:
            encoded = canonical(
                {
                    "schema_version": VERSION,
                    "resource": RESOURCE,
                    "protocol": PROTOCOL,
                    "scope": "read",
                    "captured_at": (captured_at or datetime.now(UTC)).isoformat(),
                    "tools": tools,
                }
            )
            if any(
                secret and (secret in encoded or canonical(secret)[1:-1] in encoded)
                for secret in known_secrets
            ):
                raise CatalogError("Credential-bearing Liquid catalog was rejected.")
            return cls(encoded)
        except ValueError, TypeError, RecursionError, OverflowError, UnicodeError:
            raise CatalogError(
                "Invalid Liquid tool definitions; no snapshot was retained."
            ) from None

    @property
    def document(self):
        return strict_json(self.document_json)

    @property
    def names(self):
        return tuple(tool["name"] for tool in self.document["tools"])

    @property
    def digest(self):
        return hashlib.sha256(self.document_json.encode()).hexdigest()

    def contains_secret(self, check):
        """Check both wire spelling and decoded leaves, including escaped credential text."""
        if check(self.document_json.encode()):
            return True
        pending = [self.document]
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                pending.extend(value.keys())
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)
            elif isinstance(value, str) and check(value.encode()):
                return True
        return False

    def markdown(self, name=None):
        value = self.document
        tools = value["tools"]
        if name is not None:
            tools = [tool for tool in tools if tool["name"] == name]
            if not tools:
                raise CatalogError("Choose an exact tool name from /liquid-tools.")
        header = (
            "# Liquid catalog inspection\n\n"
            f"Captured {value['captured_at']} · {len(value['tools'])} tool definitions\n\n"
            "**Provider metadata is untrusted.**\n\n"
            "No tool was called. This does not verify account identity or paper mode "
            "and grants no execution permission.\n\n"
        )
        if name is None:
            return (
                header
                + "\n\n".join(
                    f"- `{tool['name']}` · inspect with `/liquid-tool {tool['name']}`"
                    for tool in tools
                )
                + "\n\nUse `/liquid-catalog-save` to export these definitions privately."
            )
        # JSON escapes source newlines; a fence longer than every source run cannot be closed by it.
        body = json.dumps(tools[0], ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        fence = "`" * max(3, 1 + max((len(m[0]) for m in re.finditer(r"`+", body)), default=0))
        return header + f"{fence}json\n{body}\n{fence}\n\nSnapshot `{self.digest}`"

    def save(self, root: Path):
        """An explicit export creates new private files; it never replaces an earlier snapshot."""
        try:
            if root.is_symlink():
                raise ValueError("Symlink export root")
            root.mkdir(parents=True, mode=0o700, exist_ok=True)
            if os.name != "nt" and root.stat().st_mode & 0o077:
                raise ValueError("Export root is not private")
            directory = root / uuid.uuid7().hex
            directory.mkdir(mode=0o700)
            payloads = {
                "catalog.json": self.document_json,
                "manifest.json": canonical(
                    {"schema_version": VERSION, "catalog_sha256": self.digest}
                ),
            }
            for name, content in payloads.items():
                descriptor = os.open(
                    directory / name,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                with os.fdopen(descriptor, "wb") as file:
                    file.write(content.encode())
                    file.flush()
                    os.fsync(file.fileno())
            return directory.resolve()
        except OSError, ValueError:
            raise CatalogError(
                "Cannot save Liquid catalog privately; no existing file was replaced."
            ) from None

    @classmethod
    def load(cls, directory: Path):
        """Diagnostic artifact inspection never creates a connection or grants authority."""
        try:
            if directory.is_symlink() or any(
                (directory / name).is_symlink() for name in ("catalog.json", "manifest.json")
            ):
                raise ValueError("Symlink artifact")
            if (directory / "catalog.json").stat().st_size > MAX_CATALOG_BYTES:
                raise ValueError("Oversized artifact")
            if (directory / "manifest.json").stat().st_size > 1024:
                raise ValueError("Oversized manifest")
            result = cls((directory / "catalog.json").read_text(encoding="utf-8"))
            manifest = strict_json((directory / "manifest.json").read_text(encoding="utf-8"))
            if manifest != {"schema_version": VERSION, "catalog_sha256": result.digest}:
                raise ValueError("Digest mismatch")
            return result
        except OSError, ValueError, TypeError, UnicodeError, RecursionError:
            raise CatalogError("Liquid catalog integrity check failed.") from None
