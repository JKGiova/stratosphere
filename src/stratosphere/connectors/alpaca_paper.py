"""Fixed-origin Alpaca paper transport. No live URL, generic dispatcher or retry path."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import quote

import httpx
from pydantic import Field, SecretStr

from stratosphere.connectors.catalog import strict_json
from stratosphere.phase0.contracts import Contract, decimal_text
from stratosphere.research.store import ResearchError

PAPER = "https://paper-api.alpaca.markets"
DATA = "https://data.alpaca.markets"


class PaperKeys(Contract):
    key: SecretStr = Field(repr=False)
    secret: SecretStr = Field(repr=False)

    def headers(self):
        values = (self.key.get_secret_value(), self.secret.get_secret_value())
        if any(
            not s or len(s) > 512 or not s.isascii() or any(c.isspace() for c in s) for s in values
        ):
            raise ResearchError("Enter valid paper API credentials in the masked form")
        return dict(zip(("APCA-API-KEY-ID", "APCA-API-SECRET-KEY"), values, strict=True))

    def contains(self, data):
        import json

        return any(
            value and (value.encode() in data or json.dumps(value)[1:-1].encode() in data)
            for value in (self.key.get_secret_value(), self.secret.get_secret_value())
        )


def number(value, *, positive=False):
    try:
        value = decimal_text(value)
        if Decimal(value) < 0 or (positive and Decimal(value) == 0):
            raise ValueError
        return value
    except ValueError, TypeError, ArithmeticError:
        raise ResearchError("Provider returned an invalid monetary amount") from None


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return int(parsed.timestamp() * 10**9)
    except ValueError, TypeError, AttributeError:
        raise ResearchError("Provider returned an invalid timestamp") from None


class AlpacaPaper:
    provider = "alpaca"
    mode = "paper"
    supports_automatic_orders = True

    def __init__(self, keys, *, transport=None):
        self.keys, self.transport = keys, transport

    async def _request(self, method, origin, path, *, params=None, body=None, missing=False):
        # All callers below construct paths from allowlisted operations and encoded symbols/IDs.
        try:
            async with httpx.AsyncClient(
                transport=self.transport, timeout=15, trust_env=False, follow_redirects=False
            ) as client:
                r = await client.request(
                    method, origin + path, params=params, json=body, headers=self.keys.headers()
                )
            if missing and r.status_code == 404:
                return None
            if r.status_code >= 300 or len(r.content) > 4_000_000 or self.keys.contains(r.content):
                raise ValueError
            return strict_json(r.content)
        except httpx.HTTPError, ValueError, TypeError:
            # Never echo response bodies, credentials or exception representations.
            raise ResearchError(
                "Alpaca paper request failed or returned an unsafe response"
            ) from None

    async def account(self):
        value = await self._request("GET", PAPER, "/v2/account")
        try:
            import uuid

            identity = str(uuid.UUID(value["id"]))
            if (
                value["status"] != "ACTIVE"
                or value["currency"] != "USD"
                or value["trading_blocked"] is not False
                or value["account_blocked"] is not False
            ):
                raise ValueError
            return {
                "account_id": identity,
                "currency": "USD",
                "equity": number(value["equity"], positive=True),
                "cash": number(value["cash"]),
                "last_equity": number(value["last_equity"], positive=True),
            }
        except KeyError, ValueError, TypeError:
            raise ResearchError("Alpaca paper account is unavailable or ineligible") from None

    async def asset(self, symbol):
        v = await self._request("GET", PAPER, "/v2/assets/" + quote(symbol, safe=""))
        try:
            if v["symbol"] != symbol or v["status"] != "active" or v["tradable"] is not True:
                raise ValueError
            if v["class"] != "crypto":
                raise ResearchError("Automatic feed processing currently supports crypto spot only")
            return {
                "symbol": symbol,
                "asset_class": "crypto_spot",
                "increment": number(v["min_trade_increment"], positive=True),
                "minimum": number(v["min_order_size"], positive=True),
                "price_increment": number(v["price_increment"], positive=True),
            }
        except KeyError, TypeError, ValueError:
            raise ResearchError("Alpaca paper instrument mapping was not verified") from None

    async def positions(self):
        v = await self._request("GET", PAPER, "/v2/positions")
        try:
            if not isinstance(v, list) or len(v) > 1000:
                raise ValueError
            result = {}
            for p in v:
                if p["side"] != "long" or p["symbol"] in result:
                    raise ValueError
                result[p["symbol"]] = number(p["qty"], positive=True)
            return result
        except KeyError, TypeError, ValueError:
            raise ResearchError(
                "Paper positions are not a supported long-only account state"
            ) from None

    async def open_orders(self):
        v = await self._request("GET", PAPER, "/v2/orders", params={"status": "open", "limit": 500})
        if not isinstance(v, list) or len(v) >= 500:
            raise ResearchError("Paper order list is incomplete or invalid")
        return [self.order(v) for v in v]

    @staticmethod
    def order(v):
        try:
            import uuid

            status = v["status"]
            allowed = {
                "new",
                "accepted",
                "pending_new",
                "partially_filled",
                "filled",
                "canceled",
                "expired",
                "rejected",
                "done_for_day",
                "pending_cancel",
                "pending_replace",
                "accepted_for_bidding",
                "stopped",
                "suspended",
                "calculated",
                "replaced",
            }
            if status not in allowed or v["side"] not in {"buy", "sell"}:
                raise ValueError
            return {
                "id": str(uuid.UUID(v["id"])),
                "client_order_id": v["client_order_id"],
                "symbol": v["symbol"],
                "side": v["side"],
                "status": status,
                "qty": number(v["qty"], positive=True),
                "filled_qty": number(v["filled_qty"]),
                "type": v["type"],
                "time_in_force": v["time_in_force"],
                "limit_price": number(v["limit_price"], positive=True),
            }
        except KeyError, ValueError, TypeError:
            raise ResearchError("Paper order confirmation is unsupported or incomplete") from None

    async def find_order(self, identity):
        value = await self._request(
            "GET",
            PAPER,
            "/v2/orders:by_client_order_id",
            params={"client_order_id": identity},
            missing=True,
        )
        return None if value is None else self.order(value)

    async def submit(self, body):
        if set(body) != {
            "client_order_id",
            "symbol",
            "qty",
            "side",
            "type",
            "time_in_force",
            "limit_price",
        }:
            raise ResearchError("Only an exact reserved paper limit order is supported")
        if body["type"] != "limit" or body["time_in_force"] != "ioc":
            raise ResearchError("Paper entries/exits use bounded-price IOC orders")
        # Deliberately no retry: timeouts and even rejections require read-back reconciliation.
        return self.order(await self._request("POST", PAPER, "/v2/orders", body=body))

    async def bars(self, symbol, seconds, count, now_ns):
        now = datetime.fromtimestamp(now_ns / 10**9, UTC)
        v = await self._request(
            "GET",
            DATA,
            "/v1beta3/crypto/us/bars",
            params={
                "symbols": symbol,
                "timeframe": "1Hour" if seconds == 3600 else "1Day",
                "start": (now - timedelta(seconds=seconds * (count + 2))).isoformat(),
                "end": now.isoformat(),
                "sort": "asc",
                "limit": count + 3,
            },
        )
        try:
            if v.get("next_page_token"):
                raise ValueError
            raw = v["bars"][symbol]
            result = []
            for row in raw:
                opened = timestamp(row["t"])
                if opened + seconds * 10**9 <= now_ns:
                    result.append(
                        {"open_ns": opened, "close": number(str(row["c"]), positive=True)}
                    )
            if len(result) < count:
                raise ValueError
            for a, b in zip(result, result[1:], strict=False):
                if b["open_ns"] - a["open_ns"] != seconds * 10**9:
                    raise ValueError
            return result[-count:]
        except KeyError, TypeError, ValueError:
            raise ResearchError(
                "Closed paper feed bars are incomplete, revised or discontinuous"
            ) from None

    async def quote(self, symbol):
        v = await self._request(
            "GET", DATA, "/v1beta3/crypto/us/latest/quotes", params={"symbols": symbol}
        )
        try:
            q = v["quotes"][symbol]
            result = {
                "bid": number(str(q["bp"]), positive=True),
                "ask": number(str(q["ap"]), positive=True),
                "as_of_ns": timestamp(q["t"]),
            }
            if Decimal(result["bid"]) > Decimal(result["ask"]):
                raise ValueError
            return result
        except KeyError, TypeError, ValueError:
            raise ResearchError("Paper quote is invalid") from None


class LiquidPaper:
    """Explicit capability boundary, rather than a fake adapter or global mode toggle."""

    provider = "liquid"
    mode = "paper"
    supports_automatic_orders = False
    blocker = (
        "The available Liquid read catalog supplies paper review links, not a paper-only order "
        "submission API. Automatic execution requires a separately verified provider capability. "
        "Yahoo spot evidence cannot be relabeled as Liquid perpetual research."
    )

    async def submit(self, body):
        raise ResearchError(self.blocker)
