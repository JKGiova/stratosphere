"""Explicit paper benchmark/broker CFD mappings; no inferred ticker or index equivalence."""

from stratosphere.mt5.bridge import SYMBOL, BridgeError

# US100 is available as an explicit operator variant, but is NOT the paper's Nasdaq Composite.
MARKETS = {
    "US500.cash": ("S&P 500 (paper: SPDR ETF)", "XNYS", "America/New_York", True),
    "AUS200.cash": ("S&P/ASX 200 (paper: SPDR fund)", "XASX", "Australia/Sydney", True),
    "FRA40.cash": ("CAC 40", "XPAR", "Europe/Paris", True),
    "GER40.cash": ("DAX (membership changed since the paper)", "XETR", "Europe/Berlin", True),
    "UK100.cash": ("FTSE 100 (paper: iShares ETF)", "XLON", "Europe/London", True),
    "JP225.cash": ("Nikkei 225", "XTKS", "Asia/Tokyo", True),
    "HK50.cash": ("Hang Seng", "XHKG", "Asia/Hong_Kong", True),
    "N25.cash": ("AEX", "XAMS", "Europe/Amsterdam", True),
    "IT40.cash": ("FTSE MIB (paper: iShares ETF)", "XMIL", "Europe/Rome", True),
    "CAD60.cash": ("TSX 60 (paper: iShares ETF)", "XTSE", "America/Toronto", True),
    "US100.cash": (
        "Nasdaq 100; NOT the paper's Nasdaq Composite",
        "XNYS",
        "America/New_York",
        False,
    ),
}
UNMAPPED_PAPER_MARKETS = (
    "Nasdaq Composite (US100 is Nasdaq 100, a different index)",
    "Brazil iShares MSCI Brazil Capped ETF (US-listed ETF, not a Brazil CFD index)",
    "Mexico iShares MSCI Mexico Capped ETF (US-listed ETF, not a Mexico CFD index)",
    "Norway OBX",
    "Israel TA-125",
    "India NIFTY 50",
    "India S&P BSE SENSEX",
    "Singapore Straits Times",
    "Korea KOSPI",
    "Taiwan TSEC",
    "China SSE Composite",
)
FX_TO_USD = {
    "EUR": ("EURUSD", False),
    "GBP": ("GBPUSD", False),
    "AUD": ("AUDUSD", False),
    "JPY": ("USDJPY", True),
    "HKD": ("USDHKD", True),
    "CAD": ("USDCAD", True),
}


def market(symbol):
    try:
        benchmark, calendar, timezone, common = MARKETS[symbol]
    except KeyError:
        raise BridgeError("No reviewed paper/CFD/calendar mapping for this exact symbol") from None
    return {
        "symbol": symbol,
        "paper_benchmark": benchmark,
        "calendar": calendar,
        "timezone": timezone,
        "common_benchmark": common,
        "conversion": "Broker CFD adaptation; not the paper instrument or auction fills",
    }


def intersection(catalog):
    """Use a connected catalog, never a marketing page or guessed symbol as availability proof."""
    try:
        count = int(catalog["count"])
        if not 0 <= count <= 5000:
            raise ValueError
        names = [catalog[f"s.{i}.symbol"] for i in range(1, count + 1)]
        if len(set(names)) != count or any(not SYMBOL.fullmatch(s) for s in names):
            raise ValueError
    except KeyError, ValueError, TypeError:
        raise BridgeError("Malformed connected MT5 symbol catalog") from None
    matched = [market(s) for s in MARKETS if s in names and MARKETS[s][3]]
    return {
        "matched": matched,
        "not_available": [market(s) for s in MARKETS if s not in names and MARKETS[s][3]],
        "not_equivalent_or_unmapped": list(UNMAPPED_PAPER_MARKETS),
        "scope": "Benchmark-family intersection only; CFD prices/costs differ from paper data",
    }
