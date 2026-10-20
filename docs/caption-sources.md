# Supplied timed caption sources

Status: implemented structural ingestion for explicit UTF-8 WebVTT and SubRip/SRT files, connected to the existing source inspector and strategy-owned source reviews. This acquires supplied text only. It does not download provider captions, transcribe audio, render a video track, verify the original publication or convert a strategy. The full [Phase 3–5 ledger](phase3-5.md) remains in progress.

## Intake and review

```text
/source-captions webvtt "/explicit/path/captions.vtt" https://youtu.be/VIDEO_ID
/source-captions srt "/explicit/path/captions.srt"
/source-file "/explicit/path/captions.vtt"
/source-show <source-id>
/source-review <source-id> [strategy-id]
```

Replace `VIDEO_ID` and paths with actual supplied values; attribution is optional and unverified. Quote paths containing spaces. `/source-captions` fixes the format explicitly. `/source-file` selects this reader for `.vtt` and `.srt` extensions, case-insensitively; a malformed file does not fall back to plain text. Symlinks/directories are rejected. Intake/inspection require online access and make no model request or media-provider call. No application upload/cue quota is added; files and normalized material currently remain memory-buffered, and existing provider/model limits still apply to later operations.

A `supplied-captions/1` request retains the selected format, file label and optional attribution as a full source request with original/canonical URLs. Attribution is not fetched and does not establish who authored the captions or whether they correspond to the linked media. The supplied URL uses a distinct `supplied://captions/…` identity; it is not a YouTube/Instagram/X acquisition claim.

The snapshot remains `source-snapshot/1`, using its existing caption/time/line fields and the new `supplied-caption-cues/1` normalization. Existing plain-source formats, source-review contracts, passage partitioning and historical hashes are unchanged. The raw file retains its byte digest, byte count and media type. One optional UTF-8 BOM is excluded from decoded text, but remains in the raw bytes. No encoding or Unicode normalization is guessed.

## Payloads, clocks and quality findings

The reader extracts integer-millisecond outer cue bounds and exact raw payload strings. It partitions every decoded character into caption pieces or non-behavioral metadata, preserving line endings, whitespace, Unicode, markup and original order. Comments, headers, counters/identifiers, timing/settings lines, presentation blocks and separators remain retained metadata. They do not become behavioral passages. Locators identify exact decoded raw-character ranges and physical timing lines; source-review line references follow the existing logical-line convention.

WebVTT supports its signature/header, optional cue identifiers, two- or three-component timestamps, cue settings and preceding STYLE/REGION blocks. NOTE blocks may occur between cues. The reader preserves settings and raw markup rather than interpreting CSS, voice tags, entities, inline timestamps or browser visibility. Consequently its payload is not a verified browser-rendered transcript. HLS timestamp maps require a separate clock-aware reader and remain explicitly unsupported.

SRT uses positive numeric counters and `HH:MM:SS,mmm` timing, with multiline raw payloads. Literal arrows in subtitle text are retained; an unseparated new timing line is rejected. Coordinate suffixes, non-UTF-8 encodings, malformed/missing bounds and empty payloads are unsupported. Errors retain the original raw file and a fixed reason/location, with no silently repaired or partially extracted cue subset.

Repeated identifiers/counters, nonconsecutive counters, decreasing starts and overlapping intervals produce retained findings. None is sorted, deduplicated or dropped. Overlap can be intentional; the finding is not a claim that the track is invalid. `acquired` means readable supplied material was retained, not that all authoring, rendering or semantic requirements of either format have been verified.

All cue bounds are file-local. They do not establish audio/video synchronization, publication time, full-video coverage or word-level precision. An exact short quotation receives the complete enclosing cue interval, not an invented finer timestamp. Material can be incomplete or inaccurately transcribed even when structural ingestion succeeds.

## Persistence, cancellation and authority

Raw/request/snapshot manifests and private permissions use the existing source store. Known configured secrets are checked in raw text and common JSON/Unicode/entity spellings before raw retention; this does not establish that arbitrary material contains no unknown secrets. Restart verifies raw bytes and re-derives v1 caption pieces to check the retained payload/clock mapping. The initial normalization fixture pins piece shapes and hashes for future compatibility.

Parsing runs in a worker thread without artifact writes. Canceling the await retains the raw attempt with `unavailable` state and no interpreted cues; a late parser return cannot publish a completed snapshot. Attempts do not resume implicitly.

A later explicit `/source-review` delegates to the exact selected specialist through the existing validated operation. The application constructs quotations from caption passage indices and shows enclosing cue bounds alongside immutable snapshot/piece references. Raw metadata is excluded, private strategy histories remain separate and model interpretations remain unverified. Neither ingestion nor review registers, approves, simulates or activates a strategy. The [conversion gate](source-conformance.md) remains separate and blocked where behavioral equivalence is missing.

## References and verification

The structural reader was checked against the [W3C WebVTT Candidate Recommendation Draft, 20 May 2026](https://www.w3.org/TR/2026/CRD-webvtt1-20260520/#syntax) and the [Library of Congress SRT description](https://www.loc.gov/preservation/digital/formats/fdd/fdd000569.shtml). These references describe the formats; they do not certify this reader or provide media acquisition/redistribution permission. The reader is intentionally not a browser-conformance implementation.

Tests use authored captions, including CR/LF/CRLF, BOM/Unicode, multiline/raw spans, EOF, timestamps, quality findings, metadata separation, malformed/no-fallback cases, credential spellings, cancellation, resealed mapping mismatch, historical snapshots and exact passage/cue references. Responsive TUI checks cover forced intake, explicit review and Reports at 180×48 and 45×12. Measured suite/package/local-model outcomes are recorded in the [acceptance ledger](phase3-5.md). A successful fixture or local model response does not verify real-provider captions, audio accuracy, faithful strategy conversion or market performance.
