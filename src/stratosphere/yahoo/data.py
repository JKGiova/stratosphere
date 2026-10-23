"""Only historical yfinance reads; retain returned values before explicit normalization."""

import hashlib
import json
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, DecimalException
from zoneinfo import ZoneInfo

from stratosphere.yahoo.contracts import VERSION, YahooBar, YahooRequest, YahooSnapshot


class YahooValidationError(ValueError):
    """A fixed application validation explanation, safe to display."""


class YahooDataError(YahooValidationError):
    """Safe application quality code without raw provider error bodies."""

    def __init__(self, timestamp_ns):
        self.timestamp_ns = timestamp_ns
        super().__init__("invalid_ohlcv")


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def normalize(raw, request: YahooRequest):
    if raw.get("schema_version") != "yfinance-returned-frame/1" or (
        raw.get("yfinance_version") != VERSION
        or raw.get("request") != request.model_dump(mode="json")
    ):
        raise YahooValidationError("Yahoo returned-frame version or request identity mismatch")
    metadata = raw["metadata"]
    expected_type = {"equity": "EQUITY", "crypto_spot": "CRYPTOCURRENCY", "forex": "CURRENCY"}
    if (
        metadata["symbol"] != request.symbol
        or metadata["instrument_type"] != expected_type[request.asset_class]
    ):
        raise YahooValidationError("Yahoo returned a different instrument or asset class")
    currency = metadata["currency"]
    base = None
    if request.asset_class == "crypto_spot":
        base, quote = request.symbol.split("-")
        if quote != currency:
            raise YahooValidationError("Yahoo crypto quote currency mismatch")
    elif request.asset_class == "forex":
        if len(request.symbol) != 8 or not request.symbol.endswith("=X"):
            raise YahooValidationError("Use an explicit six-letter Yahoo FX pair, e.g. EURUSD=X")
        base, quote = request.symbol[:3], request.symbol[3:6]
        if currency != quote:
            raise YahooValidationError("Yahoo FX quote currency mismatch")
    timezone = ZoneInfo(metadata["timezone"])
    bars, previous = [], None
    for row in raw["rows"]:
        opening = int(row["timestamp_ns"])
        if previous is not None and opening <= previous:
            raise YahooValidationError("Yahoo returned duplicate or unordered observations")
        previous = opening
        local_date = datetime.fromtimestamp(opening / 10**9, timezone).date()
        if not request.start <= local_date < request.end:
            raise YahooValidationError("Yahoo returned observations outside the requested window")
        if any(Decimal(row[k]) != 0 for k in ("dividends", "splits", "capital_gains")):
            raise YahooValidationError(
                "This window contains corporate actions; their accounting is not supported yet"
            )
        closing = opening + request.seconds * 10**9 - 1
        if closing >= raw["observed_at_ns"]:
            raise YahooValidationError("A still-forming observation cannot enter a backtest")
        try:
            prices = {
                k: format(
                    Decimal(row[k]).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP), "f"
                )
                for k in ("open", "high", "low", "close")
            }
            bars.append(YahooBar(open_ns=opening, close_ns=closing, **prices, volume=row["volume"]))
        except ValueError, DecimalException, TypeError:
            raise YahooDataError(opening) from None
    if not bars:
        raise YahooValidationError("Yahoo returned no historical observations")
    if request.asset_class == "crypto_spot":
        expected_start = int(datetime.combine(request.start, datetime.min.time(), UTC).timestamp())
        expected_end = int(datetime.combine(request.end, datetime.min.time(), UTC).timestamp())
        if bars[0].open_ns != expected_start * 10**9 or (
            bars[-1].close_ns + 1 != expected_end * 10**9
        ):
            raise YahooValidationError("Yahoo did not return the complete requested crypto history")
    if request.asset_class != "crypto_spot":
        first_date = datetime.fromtimestamp(bars[0].open_ns / 1e9, timezone).date()
        last_date = datetime.fromtimestamp(bars[-1].open_ns / 1e9, timezone).date()
        if (first_date - request.start).days > 7 or (request.end - last_date).days > 7:
            raise YahooValidationError(
                "Yahoo returned insufficient coverage at the requested window boundaries"
            )
    gaps = tuple(
        (p.open_ns, c.open_ns)
        for p, c in zip(bars, bars[1:], strict=False)
        if c.open_ns - p.open_ns > request.seconds * 10**9
    )
    return YahooSnapshot(
        request=request,
        returned_frame_sha256=hashlib.sha256(encoded(raw)).hexdigest(),
        currency=currency,
        base_currency=base,
        exchange=metadata["exchange"],
        timezone=metadata["timezone"],
        bars=tuple(bars),
        gaps=gaps,
        observed_at_ns=raw["observed_at_ns"],
    )


def acquire(request: YahooRequest, cache):
    # Called only inside a separate, credential-free process; cookies live in a temporary directory.
    from importlib.metadata import version

    import yfinance as yf

    if version("yfinance") != VERSION:
        raise YahooValidationError("The pinned yfinance version is required")
    if request.interval == "1h" and (datetime.now(UTC).date() - request.start).days > 729:
        raise YahooValidationError(
            "The requested hourly start is outside Yahoo's recent history window"
        )
    yf.set_tz_cache_location(str(cache))
    ticker = yf.Ticker(request.symbol)
    frame = ticker.history(
        start=request.start.isoformat(),
        end=request.end.isoformat(),
        interval=request.interval,
        auto_adjust=False,
        back_adjust=False,
        actions=True,
        repair=False,
        keepna=True,
        rounding=False,
        prepost=False,
        timeout=20,
        raise_errors=True,
    )
    metadata = ticker.history_metadata
    if frame.index.tz is None:
        raise YahooValidationError("Yahoo observations have no timezone")
    rows = []
    for timestamp, values in frame.iterrows():
        row = {"timestamp_ns": int(timestamp.value)}
        for target, source in (
            ("open", "Open"),
            ("high", "High"),
            ("low", "Low"),
            ("close", "Close"),
            ("volume", "Volume"),
            ("dividends", "Dividends"),
            ("splits", "Stock Splits"),
            ("capital_gains", "Capital Gains"),
        ):
            row[target] = (
                str(values[source]) if source in values else (None if target == "volume" else "0")
            )
        rows.append(row)
    return {
        "schema_version": "yfinance-returned-frame/1",
        "yfinance_version": VERSION,
        "request": request.model_dump(mode="json"),
        "observed_at_ns": int(datetime.now(UTC).timestamp() * 10**9),
        "metadata": {
            "symbol": metadata.get("symbol"),
            "currency": metadata.get("currency"),
            "instrument_type": metadata.get("instrumentType"),
            "exchange": metadata.get("exchangeName", "unknown"),
            "timezone": metadata.get("exchangeTimezoneName"),
        },
        "rows": rows,
    }
