"""Strict Pine 6 mapping to the packaged SMA kernel, without evaluating source code."""

import hashlib
import json
import re
from pathlib import Path
from typing import Literal

from pydantic import Field

from stratosphere.phase0.contracts import Contract, Digest
from stratosphere.research.contracts import Extraction, RuleEvidence, SmaParameters
from stratosphere.sources.contracts import SourceSnapshot


class PineChartContext(Contract):
    # This is an explicit user declaration, never inferred from a title or script name.
    asset_class: Literal["crypto_spot"]
    symbol: Literal["BTCUSDT"]
    timeframe_seconds: Literal[3600]
    chart_type: Literal["regular_ohlcv"]
    properties: Literal["source_defaults"]

    @property
    def evidence_text(self):
        return (
            "User-declared source chart context: crypto_spot BTCUSDT; "
            "timeframe_seconds 3600; regular_ohlcv; source_defaults properties."
        )


class SourceRuleReference(Contract):
    field: str
    piece_sha256: Digest | None = None
    first_line: int | None = Field(default=None, strict=True, gt=0)
    last_line: int | None = Field(default=None, strict=True, gt=0)
    quote: str
    origin: Literal["source_piece", "user_chart_context"]


def converter_digest():
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


class ConversionBlocked(ValueError):
    """A safe actionable limitation, with no copied untrusted instruction or source body."""


def _uncomment(line):
    quoted, escaped = False, False
    for index, char in enumerate(line):
        if escaped:
            escaped = False
        elif char == "\\" and quoted:
            escaped = True
        elif char == '"':
            quoted = not quoted
        elif not quoted and line[index : index + 2] == "//":
            return line[:index].rstrip()
    return line.rstrip()


def _arguments(text):
    # This subset accepts only JSON string literals or scalar Pine tokens, not calls.
    token = r'"(?:[^"\\]|\\.)*"|[A-Za-z0-9_.]+(?:\s*=\s*(?:"(?:[^"\\]|\\.)*"|[A-Za-z0-9_.]+))?'
    values = []
    remaining = text.strip()
    while remaining:
        match = re.match(token, remaining)
        if not match:
            raise ConversionBlocked(
                "Unsupported strategy declaration arguments; preserve all properties."
            )
        values.append(match[0].strip())
        remaining = remaining[match.end() :].strip()
        if not remaining:
            break
        if not remaining.startswith(",") or not remaining[1:].strip():
            raise ConversionBlocked("Malformed or unsupported strategy declaration.")
        remaining = remaining[1:].strip()
    return values


def map_pine_rules(snapshot: SourceSnapshot, context: PineChartContext):
    if snapshot.access_state != "acquired" or len(snapshot.pieces) != 1:
        raise ConversionBlocked(
            "Supply one complete source file; partial/multiple pieces cannot be faithfully "
            "converted."
        )
    piece = snapshot.pieces[0]
    if piece.kind not in {"code", "text"}:
        raise ConversionBlocked("A title, description or post is not complete Pine source code.")
    numbered = [(n, line) for n, line in enumerate(piece.text.splitlines(), 1) if line.strip()]
    if not numbered or numbered[0][1].strip() != "//@version=6":
        raise ConversionBlocked("Only explicit Pine version 6 source is mapped by this converter.")
    statements = []
    for number, original in numbered[1:]:
        line = _uncomment(original)
        if not line.strip():
            continue
        if "\t" in line or "\r" in line:
            raise ConversionBlocked(
                f"Use explicit four-space Pine blocks; unsupported indentation at line {number}."
            )
        statements.append((number, line))
    if len(statements) != 9:
        raise ConversionBlocked(
            "The complete script must match the closed-bar long-only SMA subset. Extra/missing "
            "statements, filters, plots, stops and helpers need a separate verified converter; "
            "none were dropped."
        )
    declaration = re.fullmatch(r"strategy\((.*)\)", statements[0][1])
    if not declaration:
        raise ConversionBlocked("An explicit single-line strategy declaration is required.")
    args = _arguments(declaration[1])
    if not args or not args[0].startswith('"'):
        raise ConversionBlocked("An explicit quoted strategy name is required.")
    name = json.loads(args[0])
    properties = {}
    for arg in args[1:]:
        match = re.fullmatch(r"([A-Za-z_]+)\s*=\s*([A-Za-z0-9_.]+)", arg)
        if not match or match[1] in properties:
            raise ConversionBlocked(
                "Every strategy property must be a unique supported scalar value."
            )
        properties[match[1]] = match[2]
    required = {
        "overlay": "true",
        "pyramiding": "0",
        "default_qty_type": "strategy.fixed",
        "calc_on_every_tick": "false",
        "calc_on_order_fills": "false",
        "process_orders_on_close": "false",
        "margin_long": "100",
    }
    if {k: v for k, v in properties.items() if k != "default_qty_value"} != required:
        raise ConversionBlocked(
            "Preserve all strategy properties: require explicit fixed sizing, pyramiding=0, "
            "margin_long=100 and closed-bar/default delayed fills. Other properties or overrides "
            "are unsupported."
        )
    quantity = properties.get("default_qty_value", "")
    variables = []
    periods = []
    for index in (1, 2):
        match = re.fullmatch(
            r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*ta\.sma\(close\s*,\s*([0-9]+)\)", statements[index][1]
        )
        if not match:
            raise ConversionBlocked(
                f"Require a literal close-based SMA period at line {statements[index][0]}; "
                "input overrides/other indicators are unsupported."
            )
        variables.append(match[1])
        periods.append(int(match[2]))
    if len(set(variables)) != 2 or any(v in {"close", "strategy", "ta"} for v in variables):
        raise ConversionBlocked("SMA variables must be distinct and cannot shadow Pine built-ins.")
    try:
        parameters = SmaParameters(
            fast_period=periods[0], slow_period=periods[1], quantity=quantity
        )
    except ValueError:
        raise ConversionBlocked(
            "Require 0 < fast < slow < 744 and an explicit positive fixed BTC quantity with at "
            "most eight decimals."
        ) from None
    fast, slow = map(re.escape, variables)
    condition_names = []
    for index, function in ((3, "crossover"), (4, "crossunder")):
        match = re.fullmatch(
            rf"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*ta\.{function}\({fast}\s*,\s*{slow}\)",
            statements[index][1],
        )
        if not match:
            raise ConversionBlocked(
                f"Require the exact fast/slow {function} at line {statements[index][0]}; "
                "filters, offsets and lookahead are unsupported."
            )
        condition_names.append(match[1])
    if len(set(variables + condition_names)) != 4 or any(
        v in {"close", "strategy", "ta"} for v in condition_names
    ):
        raise ConversionBlocked("Conditions must have distinct non-shadowing variable names.")
    expected_entry = (
        rf"if\s+{re.escape(condition_names[0])}\s+and\s+strategy\.position_size\s*==\s*0"
    )
    expected_exit = rf"if\s+{re.escape(condition_names[1])}\s+and\s+strategy\.position_size\s*>\s*0"
    if not re.fullmatch(expected_entry, statements[5][1]) or not re.fullmatch(
        expected_exit, statements[7][1]
    ):
        raise ConversionBlocked(
            "Require explicit flat-only entry and long-only exit guards; no extra conditions are "
            "mapped."
        )
    entry = re.fullmatch(
        r'    strategy\.entry\(("(?:[^"\\]|\\.)+")\s*,\s*strategy\.long\)', statements[6][1]
    )
    close = re.fullmatch(
        r'    strategy\.close\(("(?:[^"\\]|\\.)+")\s*,\s*immediately\s*=\s*false\)',
        statements[8][1],
    )
    if not entry or not close or json.loads(entry[1]) != json.loads(close[1]):
        raise ConversionBlocked(
            "Require one matching long entry/close ID, no quantity/stop/limit override, and "
            "immediately=false for the closing market order."
        )
    context_text = context.evidence_text
    evidence = [
        RuleEvidence(field="symbol", quote=context_text),
        RuleEvidence(field="timeframe_seconds", quote=context_text),
    ]
    refs = [
        SourceRuleReference(field=f, quote=context_text, origin="user_chart_context")
        for f in ("symbol", "timeframe_seconds")
    ]
    for field, first, last in (
        ("fast_period", 1, 1),
        ("slow_period", 2, 2),
        ("quantity", 0, 0),
        ("entry", 3, 6),
        ("exit", 4, 8),
    ):
        start, end = statements[first][0], statements[last][0]
        quote = "\n".join(piece.text.splitlines()[start - 1 : end])
        evidence.append(RuleEvidence(field=field, quote=quote))
        refs.append(
            SourceRuleReference(
                field=field,
                piece_sha256=piece.digest,
                first_line=start,
                last_line=end,
                quote=quote,
                origin="source_piece",
            )
        )
    draft = Extraction(
        status="ready",
        name=name,
        symbol=context.symbol,
        timeframe_seconds=context.timeframe_seconds,
        fast_period=parameters.fast_period,
        slow_period=parameters.slow_period,
        quantity=parameters.quantity,
        entry="fast_crosses_above_slow",
        exit="fast_crosses_below_slow",
        evidence=tuple(evidence),
        questions=(),
    )
    source = piece.text + "\n\n" + context_text
    draft.validate_source(source)
    return source, draft, tuple(refs)
