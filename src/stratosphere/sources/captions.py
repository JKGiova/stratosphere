"""Versioned structural caption reader; payloads remain exact unrendered source text."""

import re
from bisect import bisect_right
from dataclasses import dataclass
from typing import Literal
from urllib.parse import quote

from pydantic import Field

from stratosphere.phase0.contracts import Contract
from stratosphere.sources.contracts import SourcePiece, SourceRequest

PROTOCOL = "supplied-caption-cues/1"


class CaptionRequest(Contract):
    schema_version: Literal["supplied-captions/1"] = "supplied-captions/1"
    label: str = Field(min_length=1, pattern=r"\S")
    format: Literal["webvtt", "srt"]
    attribution: SourceRequest | None = None

    @property
    def provider(self):
        return "supplied"

    @property
    def url(self):
        return f"supplied://captions/{self.format}/" + quote(self.label, safe="")


class CaptionError(ValueError):
    """A fixed parser reason/location, never an echoed caption or provider response."""

    def __init__(self, reason, line=None):
        super().__init__(f"Caption reader: {reason}" + (f" (physical line {line})" if line else ""))


@dataclass(frozen=True)
class CaptionParse:
    pieces: tuple[SourcePiece, ...]
    findings: tuple[str, ...]


def decode_caption(content):
    try:
        text = content.decode("utf-8-sig")
    except UnicodeError:
        raise CaptionError("UTF-8 input is required; no encoding was guessed") from None
    if not text.strip() or "\0" in text:
        raise CaptionError("nonempty text without NUL characters is required")
    return text


def _timestamp(token, format):
    pattern = (
        r"(?:[0-9]{2,}:)?[0-5][0-9]:[0-5][0-9]\.[0-9]{3}"
        if format == "webvtt"
        else r"[0-9]{2,}:[0-5][0-9]:[0-5][0-9],[0-9]{3}"
    )
    if not re.fullmatch(pattern, token):
        raise CaptionError("unsupported timestamp syntax or range")
    parts = token.replace(",", ".").split(":")
    hours = int(parts[0]) if len(parts) == 3 else 0
    minutes, seconds = parts[-2:]
    whole, millis = seconds.split(".")
    return ((hours * 60 + int(minutes)) * 60 + int(whole)) * 1000 + int(millis)


def parse_caption_v1(content: bytes, format: str) -> CaptionParse:
    """Preserve all decoded characters and cue order, including metadata and separators."""
    if format not in {"webvtt", "srt"}:
        raise CaptionError("choose webvtt or srt explicitly")
    text = decode_caption(content)
    # Only CR/LF delimit caption syntax; Unicode line characters remain payload characters.
    lines = list(re.finditer(r"[^\r\n]*(?:\r\n|\r|\n|$)", text))
    lines = [line for line in lines if line.start() != line.end()]
    bodies = [line.group().rstrip("\r\n") for line in lines]
    blocks = []
    position = 0
    while position < len(lines):
        if not bodies[position]:
            position += 1
            continue
        first = position
        while position < len(lines) and bodies[position]:
            position += 1
        blocks.append((first, position))
    if not blocks:
        raise CaptionError("no caption blocks")
    if format == "webvtt":
        if not re.fullmatch(r"WEBVTT(?:[ \t][^\r\n]*)?", bodies[0]) or "-->" in bodies[0]:
            raise CaptionError("a WebVTT signature at the start is required", 1)
        header_first, header_end = blocks.pop(0)
        if header_first != 0 or any("-->" in line for line in bodies[:header_end]):
            raise CaptionError("separate the WebVTT header from cues with a blank line", 1)
        if any("X-TIMESTAMP-MAP" in line for line in bodies[:header_end]):
            raise CaptionError("HLS timestamp maps need a separate media-clock reader", 1)
    spans, findings, identifiers = [], [], set()
    previous_start, previous_end, previous_counter = None, None, None
    for first, end in blocks:
        initial = bodies[first]
        if format == "webvtt" and re.match(r"^NOTE(?:[ \t]|$)", initial):
            continue
        if format == "webvtt" and initial.rstrip(" \t") in {"STYLE", "REGION"}:
            if spans or any("-->" in line for line in bodies[first:end]):
                raise CaptionError("unsupported or misplaced presentation block", first + 1)
            continue
        timing = first
        if format == "srt":
            if not re.fullmatch(r"[0-9]+", initial):
                raise CaptionError("a positive SubRip counter is required", first + 1)
            counter = int(initial)
            if counter <= 0:
                raise CaptionError("a positive SubRip counter is required", first + 1)
            if counter != (previous_counter + 1 if previous_counter is not None else 1):
                findings.append("Nonconsecutive/duplicate SubRip counters; file order retained.")
            previous_counter = counter
            timing += 1
        elif "-->" not in initial:
            if initial in identifiers:
                findings.append("Repeated WebVTT cue identifier; each occurrence retained.")
            identifiers.add(initial)
            timing += 1
        if timing >= end:
            raise CaptionError("a cue timing line is missing", first + 1)
        match = re.fullmatch(r"(\S+)[ \t]+-->[ \t]+(\S+)(?:[ \t]+(.*))?", bodies[timing])
        if not match:
            raise CaptionError("unsupported cue timing syntax", timing + 1)
        try:
            start_ms, end_ms = (_timestamp(match[index], format) for index in (1, 2))
        except CaptionError:
            raise CaptionError("unsupported cue timestamp syntax or range", timing + 1) from None
        if end_ms <= start_ms:
            raise CaptionError("cue end must follow its start", timing + 1)
        settings = match[3] or ""
        if "-->" in settings or format == "srt" and settings.strip():
            raise CaptionError("unsupported timing suffix", timing + 1)
        if previous_start is not None and start_ms < previous_start:
            findings.append("Cue start times decrease; file order retained without sorting.")
        if previous_end is not None and start_ms < previous_end:
            findings.append("Cue intervals overlap; all original cues retained.")
        previous_start, previous_end = start_ms, max(previous_end or 0, end_ms)
        if timing + 1 >= end:
            raise CaptionError("empty cue payload is not supported", timing + 1)
        start, stop = lines[timing + 1].start(), lines[end - 1].end()
        payload = text[start:stop]
        unseparated = (
            "-->" in payload
            if format == "webvtt"
            else any(
                re.match(r"^[0-9]+:[0-9]{2}:[0-9]{2},[0-9]{3}[ \t]+-->", line)
                for line in bodies[timing + 1 : end]
            )
        )
        if not payload.strip() or unseparated:
            raise CaptionError("empty payload or an unseparated cue boundary", timing + 2)
        spans.append((start, stop, start_ms, end_ms, timing + 1))
    if not spans:
        raise CaptionError("no readable timed cue payloads")
    # Match the existing source-review logical-line convention without rewriting Unicode.
    logical_starts = [0]
    offset = 0
    for line in text.splitlines(keepends=True):
        offset += len(line)
        logical_starts.append(offset)
    pieces = []

    def append(start, end, *, cue=None, cue_index=None):
        if start == end:
            return
        first_line = bisect_right(logical_starts, start)
        last_line = bisect_right(logical_starts, end - 1)
        bounds = {} if cue is None else {"start_ms": cue[2], "end_ms": cue[3]}
        locator = f"{format} raw characters {start}:{end}"
        if cue is not None:
            locator += f"; cue {cue_index}; timing physical line {cue[4]}"
        pieces.append(
            SourcePiece(
                kind="metadata" if cue is None else "caption",
                text=text[start:end],
                locator=locator,
                first_line=first_line,
                last_line=last_line,
                **bounds,
            )
        )

    cursor = 0
    for index, span in enumerate(spans):
        append(cursor, span[0])
        append(span[0], span[1], cue=span, cue_index=index)
        cursor = span[1]
    append(cursor, len(text))
    assert "".join(piece.text for piece in pieces) == text
    return CaptionParse(tuple(pieces), tuple(dict.fromkeys(findings)))
