"""Fixed paper-account and BTC/USD asset reads; no order or market-data transport."""

import asyncio
import hashlib
import json
import os
import re
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal
from uuid import UUID

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, SecretStr, model_validator

from stratosphere.connectors.catalog import strict_json
from stratosphere.phase0.contracts import Contract, Digest, decimal_text, fingerprint
from stratosphere.research.store import ResearchError, ResearchStore

PAPER_API = "https://paper-api.alpaca.markets"


class AlpacaError(ResearchError):
    """A fixed user-facing diagnostic failure, without provider bodies or headers."""


class PaperCredentials(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    key_id: SecretStr
    secret_key: SecretStr

    @model_validator(mode="after")
    def valid_headers(self):
        for value in (self.key_id, self.secret_key):
            raw = value.get_secret_value()
            if not raw or len(raw) > 2048 or any(not 33 <= ord(char) <= 126 for char in raw):
                raise ValueError("Enter both paper credentials without whitespace or controls")
        return self


class PaperAccount(Contract):
    id: UUID
    status: str = Field(pattern=r"^[A-Z_]{1,64}$")
    crypto_status: str | None = Field(default=None, pattern=r"^[A-Z_]{1,64}$")
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    account_blocked: bool = Field(strict=True)
    trading_blocked: bool = Field(strict=True)


class BitcoinAsset(Contract):
    id: UUID
    asset_class: Literal["crypto"]
    exchange: str = Field(pattern=r"^[A-Z0-9_]{1,64}$")
    symbol: Literal["BTC/USD"]
    status: str = Field(pattern=r"^[a-z_]{1,64}$")
    tradable: bool = Field(strict=True)
    fractionable: bool = Field(strict=True)
    min_order_size: str
    min_trade_increment: str
    price_increment: str

    @model_validator(mode="after")
    def native_increments(self):
        for key in ("min_order_size", "min_trade_increment", "price_increment"):
            value = decimal_text(getattr(self, key))
            if Decimal(value) <= 0:
                raise ValueError("A positive exact native increment is required")
            object.__setattr__(self, key, value)
        return self


class ResponseReference(Contract):
    path: Literal["/v2/account", "/v2/assets?asset_class=crypto"]
    body_sha256: Digest
    request_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_.:-]{1,256}$")


class PaperCheck(Contract):
    schema_version: Literal["alpaca-paper-check/1"] = "alpaca-paper-check/1"
    id: str = Field(pattern=r"^[a-f0-9]{32}$")
    endpoint: Literal[PAPER_API] = PAPER_API
    requested_at: AwareDatetime
    received_at: AwareDatetime
    account: PaperAccount
    asset: BitcoinAsset | None
    responses: tuple[ResponseReference, ResponseReference, ResponseReference]
    account_control_verified: Literal[False] = False
    feed_entitlement_verified: Literal[False] = False
    execution_authority: Literal["none"] = "none"

    @model_validator(mode="after")
    def consistent(self):
        if self.received_at < self.requested_at or tuple(row.path for row in self.responses) != (
            "/v2/account",
            "/v2/assets?asset_class=crypto",
            "/v2/account",
        ):
            raise ValueError("Invalid paper check references or capture clock")
        return self

    @property
    def digest(self):
        return fingerprint(self)

    def markdown(self):
        blockers = []
        if self.account.status != "ACTIVE":
            blockers.append("The reported paper account status is not ACTIVE.")
        if self.account.crypto_status != "ACTIVE":
            blockers.append("Active crypto permission has not been reported.")
        if self.account.account_blocked or self.account.trading_blocked:
            blockers.append("The provider reports an account or trading block.")
        if self.account.currency != "USD":
            blockers.append("The reported account currency is not this USD profile.")
        if self.asset is None:
            blockers.append("BTC/USD is absent from the reported crypto asset list.")
        elif (
            self.asset.status != "active" or not self.asset.tradable or not self.asset.fractionable
        ):
            blockers.append("BTC/USD is not reported active, tradable and fractionable.")
        return "\n\n".join(
            [
                "# Alpaca · paper endpoint diagnostic",
                f"Captured {self.received_at.isoformat()} · `{self.endpoint}`",
                "Credentials accessed the paper endpoint. These account/asset observations do not "
                "verify legal account control, data entitlement, strategy approval "
                "or execution readiness.",
                "## Reported account\n\n```json\n"
                + self.account.model_dump_json(indent=2)
                + "\n```",
                "## BTC/USD asset\n\n"
                + (
                    "```json\n" + self.asset.model_dump_json(indent=2) + "\n```"
                    if self.asset
                    else "No supported BTC/USD asset was reported."
                ),
                "## Observed blockers\n\n"
                + (
                    "\n".join("- " + item for item in blockers)
                    if blockers
                    else "No blockers in these limited account/asset fields. "
                    "Feed and execution remain unverified."
                ),
                "Native increments are retained exactly; "
                "they are not fixture-ledger rounding rules. "
                "BTC/USD is distinct from the previous BTC/USDT strategy and test evidence.",
                "This operation submits no orders, reads no prices and does not fund, "
                "reset or alter an account.",
                f"Check `{self.id}` · `{self.digest}`. "
                "Raw response digests do not reconstruct omitted bytes.",
            ]
        )


class AlpacaPaperConnection:
    def __init__(self, *, transport=None, clock=lambda: datetime.now(UTC)):
        self.transport, self.clock = transport, clock
        self.credentials = None
        self.last_check = None
        self.busy = False
        self.secret_check = lambda data: False
        self._redactions = []

    def configure(self, credentials):
        credentials = PaperCredentials.model_validate(credentials.model_dump())
        self._redactions.extend((credentials.key_id, credentials.secret_key))
        self.credentials, self.last_check = credentials, None

    def disconnect(self):
        self.credentials, self.last_check = None, None

    def save_check(self, root: Path):
        value = self.last_check
        if value is None:
            raise AlpacaError("Complete an Alpaca paper check before exporting it.")
        self._clean(value.model_dump(mode="json"))
        if any(path.is_symlink() for path in (root, *root.parents)):
            raise AlpacaError("Paper diagnostic exports cannot use symbolic links.")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if os.name != "nt" and root.stat().st_mode & 0o077:
            raise AlpacaError("Paper diagnostic exports require a private directory.")
        store = ResearchStore(root)
        directory = store.directory(value.id)
        if directory.exists():
            if self.load_check(root, value.id) != value:
                raise AlpacaError("An existing paper check export cannot be replaced.")
            return directory / "check.json"
        directory.mkdir(mode=0o700)
        store.write(value.id, "check.json", value.model_dump(mode="json"))
        digest = hashlib.sha256((directory / "check.json").read_bytes()).hexdigest()
        store.write(value.id, "manifest.json", {"check.json": digest})
        return directory / "check.json"

    def load_check(self, root: Path, identity):
        store = ResearchStore(root)
        directory = store.directory(identity)
        try:
            if any(path.is_symlink() for path in (root, *root.parents, directory)):
                raise ValueError
            for name in ("check.json", "manifest.json"):
                if (directory / name).is_symlink():
                    raise ValueError
            digest = hashlib.sha256((directory / "check.json").read_bytes()).hexdigest()
            if strict_json((directory / "manifest.json").read_bytes()) != {"check.json": digest}:
                raise ValueError
            value = PaperCheck.model_validate(strict_json((directory / "check.json").read_bytes()))
            if value.id != identity:
                raise ValueError
            self._clean(value.model_dump(mode="json"))
            return value
        except ValueError, TypeError, KeyError, OSError:
            raise AlpacaError("The paper diagnostic export could not be verified.") from None

    def contains_credential(self, data):
        return any(
            value.get_secret_value().encode() in data
            or json.dumps(value.get_secret_value())[1:-1].encode() in data
            for value in self._redactions
        )

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
                decoded = re.sub(r"\\u([a-fA-F0-9]{4})", lambda match: chr(int(match[1], 16)), item)
                for text in (item, decoded):
                    data = text.encode(errors="replace")
                    if self.contains_credential(data) or self.secret_check(data):
                        raise AlpacaError("Credential-bearing Alpaca response was rejected.")

    def _current(self, credentials):
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise asyncio.CancelledError
        if self.credentials is not credentials:
            raise AlpacaError(
                "The paper credentials changed or were disconnected; no check was accepted."
            )

    async def _get(self, client, path, credentials):
        if path not in {"/v2/account", "/v2/assets?asset_class=crypto"}:
            raise AlpacaError("Unsupported paper diagnostic read.")
        self._current(credentials)
        async with client.stream(
            "GET",
            PAPER_API + path,
            headers={
                "APCA-API-KEY-ID": credentials.key_id.get_secret_value(),
                "APCA-API-SECRET-KEY": credentials.secret_key.get_secret_value(),
                "Accept": "application/json",
            },
        ) as response:
            self._current(credentials)
            if response.status_code != 200:
                if response.status_code in (401, 403):
                    raise AlpacaError(
                        "Paper endpoint access denied. Check your Alpaca paper credentials."
                    )
                if response.status_code == 429:
                    raise AlpacaError("Alpaca rate limit reached; no automatic retry was made.")
                raise AlpacaError("Alpaca paper read failed; no redirect or retry was followed.")
            if (
                response.headers.get("content-type", "").split(";", 1)[0].strip()
                != "application/json"
            ):
                raise AlpacaError("Alpaca returned an unsupported paper response type.")
            body = bytearray()
            async for part in response.aiter_bytes():
                body.extend(part)
                if len(body) > 8 * 1024 * 1024:
                    raise AlpacaError("Alpaca paper response exceeded the technical read limit.")
            self._current(credentials)
            value = strict_json(body)
            reference = ResponseReference(
                path=path,
                body_sha256=hashlib.sha256(body).hexdigest(),
                request_id=response.headers.get("x-request-id"),
            )
            self._clean([value, reference.model_dump(mode="json")])
            return value, reference

    @staticmethod
    def _account(value):
        fields = ("id", "status", "crypto_status", "currency", "account_blocked", "trading_blocked")
        return PaperAccount.model_validate({key: value[key] for key in fields if key in value})

    async def inspect(self):
        if self.credentials is None:
            raise AlpacaError("Connect Alpaca paper credentials with /alpaca-connect first.")
        if self.busy:
            raise AlpacaError("An Alpaca paper check is already running.")
        credentials = self.credentials
        self.busy = True
        self.last_check = None
        try:
            started = self.clock()
            async with asyncio.timeout(60):
                async with httpx.AsyncClient(
                    timeout=20, transport=self.transport, follow_redirects=False, trust_env=False
                ) as client:
                    first, first_ref = await self._get(client, "/v2/account", credentials)
                    account = self._account(first)
                    assets, asset_ref = await self._get(
                        client, "/v2/assets?asset_class=crypto", credentials
                    )
                    if not isinstance(assets, list) or any(
                        not isinstance(row, dict) for row in assets
                    ):
                        raise ValueError("Unsupported asset list")
                    matches = [row for row in assets if row.get("symbol") == "BTC/USD"]
                    if len(matches) > 1:
                        raise ValueError("Ambiguous asset identity")
                    asset = None
                    if matches:
                        raw = matches[0]
                        asset = BitcoinAsset(
                            asset_class=raw["class"],
                            **{
                                key: raw[key]
                                for key in BitcoinAsset.model_fields
                                if key != "asset_class"
                            },
                        )
                    last, last_ref = await self._get(client, "/v2/account", credentials)
                    if self._account(last) != account:
                        raise AlpacaError(
                            "The reported paper account changed during inspection; "
                            "retry explicitly."
                        )
            self._current(credentials)
            value = PaperCheck(
                id=uuid.uuid7().hex,
                requested_at=started,
                received_at=self.clock(),
                account=account,
                asset=asset,
                responses=(first_ref, asset_ref, last_ref),
            )
            self._clean(value.model_dump(mode="json"))
            if asyncio.current_task().cancelling():
                raise asyncio.CancelledError
            self.last_check = value
            return value
        except AlpacaError:
            raise
        except httpx.HTTPError, TimeoutError, ValueError, KeyError, TypeError:
            raise AlpacaError(
                "The Alpaca paper response could not be verified; no account was changed."
            ) from None
        finally:
            self.busy = False
