"""Explicit public-address diagnostic; no generic tools, account linking or trading authority."""

import asyncio
import hashlib
import os
import re
import time
import uuid
from datetime import UTC, datetime
from typing import Literal

from pydantic import AwareDatetime, Field, ValidationError, model_validator

from stratosphere.connectors.catalog import canonical, strict_json
from stratosphere.legacy.liquid import PROTOCOL, RESOURCE, LiquidError, required
from stratosphere.phase0.contracts import Contract, Digest, fingerprint
from stratosphere.research.store import ResearchError

ARGUMENTS = {"user_intent": "Show my Liquid trading account address without changing settings."}
INPUT_SHAPE = {
    "type": "object",
    "additionalProperties": False,
    "properties": {"user_intent": {"type": "string", "maxLength": 2048}},
}
OUTPUT_SHAPE = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "tradingAccountAddress": {"type": "string"},
        "addressRole": {"type": "string", "const": "trading-account"},
    },
    "required": ["addressRole", "tradingAccountAddress"],
}


def _shape(value):
    """Ignore descriptive annotations only; unfamiliar validation keywords remain mismatches."""
    if isinstance(value, list):
        return [_shape(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        if key in {"description", "title", "$comment"} and isinstance(item, str):
            continue
        if key == "$schema" and item == "http://json-schema.org/draft-07/schema#":
            continue
        result[key] = sorted(item) if key == "required" and isinstance(item, list) else _shape(item)
    return result


def reviewed_definition(catalog):
    definition = next((t for t in catalog.document["tools"] if t["name"] == "get_account"), None)
    try:
        matches = (
            definition is not None
            and _shape(definition["inputSchema"]) == INPUT_SHAPE
            and _shape(definition.get("outputSchema")) == OUTPUT_SHAPE
        )
    except ValueError, TypeError, KeyError, RecursionError:
        matches = False
    required(
        matches,
        "Liquid get_account is missing or its reviewed schema changed; no tool was called.",
    )
    return definition


class ReportedAccount(Contract):
    tradingAccountAddress: str = Field(strict=True, min_length=1, max_length=2048)
    addressRole: Literal["trading-account"]

    @model_validator(mode="after")
    def printable(self):
        if any(not 33 <= ord(c) <= 126 for c in self.tradingAccountAddress):
            raise ValueError("Unreviewed account identifier encoding")
        return self


class AccountRead(Contract):
    schema_version: Literal["liquid-account-read/1"] = "liquid-account-read/1"
    id: str = Field(pattern=r"^[a-f0-9]{32}$")
    resource: Literal[RESOURCE] = RESOURCE
    protocol: Literal[PROTOCOL] = PROTOCOL
    scope: Literal["read"] = "read"
    tool: Literal["get_account"] = "get_account"
    catalog_sha256: Digest
    definition_sha256: Digest
    requested_at: AwareDatetime
    received_at: AwareDatetime
    result_sha256: Digest
    reported_account: ReportedAccount | None
    provider_text: tuple[str, ...]
    provider_error: bool = Field(strict=True)
    unhandled_content: bool = Field(strict=True)
    status: Literal["address_reported", "provider_error", "requires_review"]
    ownership_verified: Literal[False] = False
    environment: Literal["unverified"] = "unverified"
    execution_authority: Literal["none"] = "none"

    @model_validator(mode="after")
    def consistent(self):
        if self.received_at < self.requested_at:
            raise ValueError("Account observation predates request")
        status = (
            "provider_error"
            if self.provider_error
            else "requires_review"
            if self.unhandled_content or self.reported_account is None
            else "address_reported"
        )
        if status != self.status:
            raise ValueError("Account read status disagrees with retained fields")
        return self

    @property
    def digest(self):
        return fingerprint(self)


def _project(result):
    """Retain plain provider text and typed fields; never load metadata, resources or widgets."""
    texts = []
    unhandled = bool(set(result) - {"content", "structuredContent", "isError"})
    content = result.get("content")
    if not isinstance(content, list):
        content = []
        unhandled = True
    for block in content:
        if (
            isinstance(block, dict)
            and block.get("type") == "text"
            and isinstance(block.get("text"), str)
        ):
            texts.append(block["text"])
            unhandled |= bool(set(block) - {"type", "text"})
        else:
            unhandled = True
    error = result.get("isError", False)
    if type(error) is not bool:
        error, unhandled = True, True
    try:
        account = ReportedAccount.model_validate(result.get("structuredContent"))
    except ValidationError:
        account = None
    if account:
        for text in texts:
            try:
                # Non-JSON prose, including confirmations, remains literal and unresolved.
                unhandled |= strict_json(text) != account.model_dump()
            except ValueError, TypeError, RecursionError:
                unhandled = True
    return account, tuple(texts), error, unhandled


class LiquidAccountReader:
    def __init__(self, connection, store, *, online_check, clock=lambda: datetime.now(UTC)):
        self.connection, self.store = connection, store
        self.online_check, self.clock = online_check, clock
        self.secret_check = lambda data: False

    def _clean(self, value):
        pending = [value]
        while pending:
            item = pending.pop()
            if isinstance(item, dict):
                pending.extend(item.keys())
                pending.extend(item.values())
            elif isinstance(item, (list, tuple)):
                pending.extend(item)
            elif isinstance(item, str):
                if self.connection.contains_credential(item.encode()) or self.secret_check(
                    item.encode()
                ):
                    raise LiquidError("Credential-bearing Liquid account data was rejected.")
                try:
                    decoded = strict_json(item)
                    if decoded != item:
                        pending.append(decoded)
                except ValueError, TypeError, RecursionError:
                    pass
        wire = canonical(value).encode()
        required(
            not self.connection.contains_credential(wire) and not self.secret_check(wire),
            "Credential-bearing Liquid account data was rejected.",
        )

    def _current(self, session):
        required(
            self.connection.session is session
            and session is not None
            and session.initialized
            and time.monotonic() < session.expires_at,
            "The Liquid read session changed or expired; reconnect before an account read.",
        )

    def _now(self):
        value = self.clock()
        required(
            value.tzinfo is not None and value.utcoffset() is not None, "Invalid account clock."
        )
        return value.astimezone(UTC)

    async def read(self):
        await self.online_check()
        connection = self.connection
        session = connection.session
        self._current(session)
        required(not session.account_read_busy, "A Liquid account read is already running.")
        catalog = connection.catalog_snapshot()
        definition = reviewed_definition(catalog)
        required(
            not catalog.contains_secret(
                lambda b: connection.contains_credential(b) or self.secret_check(b)
            ),
            "Credential-bearing Liquid catalog was rejected.",
        )
        requested_at = self._now()
        identity = uuid.uuid7().hex
        session.account_read_busy = True
        started = False
        try:
            root = self.store.root
            required(
                not root.is_symlink(), "Liquid account diagnostics require a private directory."
            )
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            required(
                os.name == "nt" or not root.stat().st_mode & 0o077,
                "Liquid account diagnostics require a private directory.",
            )
            self.store.directory(identity).mkdir(mode=0o700)
            session.rpc_id += 1
            request_id = session.rpc_id
            request = {
                "id": identity,
                "resource": RESOURCE,
                "protocol": PROTOCOL,
                "scope": "read",
                "tool": "get_account",
                "arguments": ARGUMENTS,
                "rpc_id": request_id,
                "catalog_sha256": catalog.digest,
                "definition_sha256": hashlib.sha256(canonical(definition).encode()).hexdigest(),
                "requested_at": requested_at.isoformat(),
            }
            self._clean(request)
            self.store.write(identity, "request.json", request)
            self.store.write(identity, "definition.json", definition)
            self.store.event(identity, "dispatching", "Explicit get_account read; no trading scope")
            started = True
            self._current(session)
            headers = {
                "Accept": "application/json, text/event-stream",
                "Authorization": "Bearer " + session.token.get_secret_value(),
                "Mcp-Protocol-Version": PROTOCOL,
            }
            if session.mcp_session_id:
                headers["Mcp-Session-Id"] = session.mcp_session_id.get_secret_value()
            async with connection.client() as client:
                payload, response_headers = await connection._request(
                    client,
                    "POST",
                    RESOURCE,
                    headers=headers,
                    json={
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "tools/call",
                        "params": {"name": "get_account", "arguments": dict(ARGUMENTS)},
                    },
                )
            self._current(session)
            returned_sid = response_headers.get("mcp-session-id")
            required(
                returned_sid is None
                or (
                    session.mcp_session_id is not None
                    and returned_sid == session.mcp_session_id.get_secret_value()
                ),
                "Liquid changed its protocol session during the account read; reconnect.",
            )
            required(
                set(payload) == {"jsonrpc", "id", "result"}
                and payload["jsonrpc"] == "2.0"
                and type(payload["id"]) is int
                and payload["id"] == request_id
                and isinstance(payload["result"], dict),
                "Liquid rejected or returned an unsupported account read; no account was linked.",
            )
            result = payload["result"]
            self._clean(result)
            account, texts, error, unhandled = _project(result)
            value = AccountRead(
                id=identity,
                catalog_sha256=catalog.digest,
                definition_sha256=request["definition_sha256"],
                requested_at=requested_at,
                received_at=self._now(),
                result_sha256=hashlib.sha256(canonical(result).encode()).hexdigest(),
                reported_account=account,
                provider_text=texts,
                provider_error=error,
                unhandled_content=unhandled,
                status="provider_error"
                if error
                else "requires_review"
                if unhandled or account is None
                else "address_reported",
            )
            self.store.write(identity, "observation.json", value.model_dump(mode="json"))
            self.store.write(
                identity,
                "manifest.json",
                {
                    name: hashlib.sha256(
                        (self.store.directory(identity) / name).read_bytes()
                    ).hexdigest()
                    for name in ("request.json", "definition.json", "observation.json")
                },
            )
            self.store.event(identity, "completed", value.status)
            return value
        except asyncio.CancelledError:
            if started:
                self.store.event(identity, "canceled", "The read may have reached Liquid; no retry")
            raise
        except Exception as error:
            if started:
                self.store.event(identity, "failed", "No account linked; no automatic retry")
            if isinstance(error, LiquidError):
                raise
            raise LiquidError(
                "Liquid account read failed; no account was linked or retry made."
            ) from None
        finally:
            session.account_read_busy = False

    def retained(self, identity):
        directory = self.store.directory(identity)
        try:
            required(
                not self.store.root.is_symlink() and not directory.is_symlink(),
                "Invalid Liquid diagnostic path.",
            )
            names = {"request.json", "definition.json", "observation.json"}
            required(not (directory / "manifest.json").is_symlink(), "Invalid diagnostic manifest.")
            manifest = self.store.read(identity, "manifest.json")
            if set(manifest) != names:
                raise ValueError
            for name, digest in manifest.items():
                p = directory / name
                if p.is_symlink() or hashlib.sha256(p.read_bytes()).hexdigest() != digest:
                    raise ValueError
            value = AccountRead.model_validate(self.store.read(identity, "observation.json"))
            request = self.store.read(identity, "request.json")
            definition = self.store.read(identity, "definition.json")
            if (
                value.id != identity
                or request
                != {
                    "id": identity,
                    "resource": RESOURCE,
                    "protocol": PROTOCOL,
                    "scope": "read",
                    "tool": "get_account",
                    "arguments": ARGUMENTS,
                    "rpc_id": request["rpc_id"],
                    "catalog_sha256": value.catalog_sha256,
                    "definition_sha256": value.definition_sha256,
                    "requested_at": value.requested_at.isoformat(),
                }
                or type(request["rpc_id"]) is not int
                or request["rpc_id"] < 1
                or hashlib.sha256(canonical(definition).encode()).hexdigest()
                != value.definition_sha256
            ):
                raise ValueError

            # Revalidate the fixed schema without needing a connection or restoring authority.
            class Definition:
                document = {"tools": [definition]}

            reviewed_definition(Definition())
            self._clean(value.model_dump(mode="json"))
            return value
        except ValueError, TypeError, KeyError, OSError, ResearchError:
            raise LiquidError("The Liquid account diagnostic failed its integrity check.") from None

    async def open(self, identity):
        await self.online_check()
        return self.retained(identity)

    def attempts(self):
        rows = []
        for identity in self.store.jobs():
            try:
                state = self.store.state(identity)
                if state == "completed":
                    state = self.retained(identity).status
                elif state not in {"dispatching", "failed", "canceled"}:
                    state = "damaged or incomplete"
            except ResearchError, ValueError, OSError:
                state = "damaged or incomplete"
            rows.append({"id": identity, "state": state})
        return rows


def account_markdown(value):
    text = "\n\n".join(value.provider_text)
    fence = "`" * max(3, 1 + max((len(m[0]) for m in re.finditer(r"`+", text)), default=0))
    account = value.reported_account
    return (
        "# Liquid account read · historical diagnostic\n\n"
        f"Status: **{value.status}** · requested {value.requested_at.isoformat()} · "
        f"received {value.received_at.isoformat()}\n\n"
        + (
            f"Provider-reported trading account: `{account.tradingAccountAddress}`\n\n"
            if account
            else "No supported account identifier was accepted.\n\n"
        )
        + "**Ownership, environment, paper isolation, feed entitlement and trading authority "
        "remain unverified.** This address is not a deposit instruction or an account link.\n\n"
        "Provider text/confirmations are untrusted, retained literally and not executed. "
        "No links, resources, widgets or follow-up actions were opened. "
        "Unknown response metadata is not retained beyond its result digest.\n\n"
        + (f"{fence}text\n{text}\n{fence}\n\n" if text else "No provider text was returned.\n\n")
        + f"Catalog `{value.catalog_sha256}` · result `{value.result_sha256}`\n\n"
        f"Observation `{value.id}` · `{value.digest}`. "
        "A saved record cannot restore a connection or authorize an operation."
    )
