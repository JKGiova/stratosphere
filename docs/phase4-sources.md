# Phase 4 source intake increment

Status: **partial implementation**. Read-only snapshots and inspection are implemented locally. This does not complete Phase 4, verify every provider, convert arbitrary strategies, or enable execution. Account linking, commercial market-data coverage and the Phase 5 paper runtime remain separate work.

## Available operations

Use `/source <url> [github-ref]`, `/sources` and `/source-show <source-id>` to acquire and inspect retained source attempts. `/source-text <text>` retains supplied text. `/source-file <path>` reads only the explicitly named regular UTF-8 file; quote paths containing spaces. No application upload quota is added. Current ingestion buffers material in memory; provider endpoint limits and supported formats still apply.

Forced source commands need no model request. They do not register a strategy, populate an executable plan, approve tests, invoke acquired code or send source contents to an agent. The separate, explicit [source-review operation](source-reviews.md) now delegates available material to an existing strategy specialist. Source inspection uses the existing scrollable Focus panel. Background acquisition preserves Chat focus. The application still requires online access, including inspection/supplied material operations; no offline fallback is introduced.

The original submitted URL, canonical retrieval URL and explicit Git ref are preserved in the request. Video time/playlist parameters do not constrain retrieval: the YouTube reader retrieves metadata for the whole video and explicitly acquires no timed content.

## Provider scope

| Provider | Implemented reader | Explicit limitation |
| --- | --- | --- |
| GitHub | Repository/directory metadata, or a selected UTF-8 file at an API-resolved immutable commit; the selected Git tree path, file size and Git blob hash are checked. | Directory listings are metadata only, with a 1,000-entry provider limit. Dependencies are not acquired. Symlinks/submodules are blocked. No repository code is imported. |
| YouTube | Public oEmbed metadata. | No transcript, audio, frames or complete description. Metadata is excluded from behavioral evidence. Authorized caption downloads/public transcript retrieval remain unimplemented. |
| X | One post's public oEmbed fallback paragraph text, with scripts excluded. | Always partial: no complete thread, media transcript, linked content or proof that a long post is untruncated. Changed fallback markup can yield no text. |
| TradingView | Public publication-page metadata. | No Pine source retrieval or visibility inference. An accessible page does not establish access to open/protected/invite-only source. |
| Instagram | Explicit `requires_credentials` result. | The authorized Meta client is not implemented. No API access, caption/audio/frames or publication existence is claimed. |
| Supplied | Explicit UTF-8 text/code, with optional source attribution through the service contract. | Supplied material's original identity/completeness remain unverified. Timed caption parsing is not implemented yet. |

GitHub branch names containing slashes require an explicit `github-ref` matching the URL prefix. Public read requests work without a token. The service accepts an explicitly supplied read-only GitHub token in memory; it does not retrieve browser, Git or GitHub CLI credentials. The TUI does not yet have a source-token configuration form. Only the fixed GitHub API host receives that token. Redirects and download URLs are not followed; raw Contents requests remain pinned to the same commit.

GitHub's Contents endpoint supports files up to 1 MB with all features, raw/object modes for 1–100 MB, and does not support files above 100 MB. These are [provider endpoint capabilities](https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28), not product upload quotas. A rate-limited, private, missing or deleted source produces an explicit access failure; a 404 is not proof which condition applies.

## Evidence and integrity

Each retrieval gets a new private attempt directory, immutable request/snapshot/manifest and content-addressed raw blobs. SHA-256 protects request/snapshot/raw references; GitHub file bytes also match the pinned Git blob identity and tree path. Retrieval timestamps are aware UTC. Parser implementation hashes, normalized pieces, line references, provider/access states, limitations and next steps are retained. Repeated retrievals do not replace an earlier snapshot. Mutation is detected during inspection; a retrieval interrupted by cancellation gets an explicit terminal unavailable snapshot. Crash-abandoned/incomplete artifacts remain visible as incomplete rather than being silently resumed or interpreted.

Titles, counters, directory names and page metadata never enter `analysis_text`. Acquired code/text remains untrusted data. A snapshot always states `behavior_conformance=not_verified` and `redistribution=not_assessed`. Reading source code and recognizing a repository license are separate from permission to redistribute it or proof that it matches an executable strategy.

Configured credentials are checked before request/material persistence, including decoded GitHub base64 bodies, JSON Unicode escapes and normalized HTML text. Provider error bodies, redirect destinations and headers are not retained. Keys stay outside snapshots/model prompts/workers. This exact configured-key check does not establish that arbitrary third-party content contains no unknown credentials.

## Official access references

- [GitHub Contents](https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28) and [Git trees](https://docs.github.com/en/rest/git/trees?apiVersion=2022-11-28) define file/directory objects and symlink/submodule modes.
- [YouTube caption downloads](https://developers.google.com/youtube/v3/docs/captions/download) require authorization and permission to edit the video; they are not a general public transcript API.
- [X oEmbed](https://docs.x.com/x-for-websites/oembed-api) returns fallback HTML without authentication; its markup can change. The reader neither runs widget JavaScript nor follows media links.
- [Instagram oEmbed](https://developers.facebook.com/docs/graph-api/reference/instagram-oembed/) requires a Meta token. A future authorized adapter must verify current fields and permissions with an actual account.
- [TradingView publishing](https://www.tradingview.com/pine-script-docs/writing/publishing/) distinguishes publication privacy from open/protected/invite-only code visibility. Closed logic cannot be inferred from page text.

## Verification and remaining work

Deterministic fixtures exercise canonical URLs and credential rejection, immutable retention/restart, raw/request/snapshot corruption, Git commit/tree/blob checks, symlink/submodule refusal, partial metadata/text scope, provider/auth/rate/redirect/network failures, changed formats, cancellation, supplied material and online gates. TUI checks cover explicit file paths with spaces, configured-key rejection, no model/simulation/registration side effects and responsive inspection at 45 × 12, 60 × 36 and 180 × 48.

The full local regression passed **525 tests**, zero skips, including nine native-engine tests (33 upstream Pandas deprecation warnings). A subsequent focused run passed **69 source/TUI checks**, including the final online-gate and artifact-symlink checks. Ruff and whitespace checks passed. Wheel/sdist inspection matched all Python/CSS sources and excluded configured credentials, private reports/state and build caches. Built-wheel Focus views were inspected at 180 × 48, 60 × 36 and 45 × 12 using actual retained public GitHub material with an injected online fixture gate for rendering only.

Real public reads on October 18, 2026 acquired the official [NautilusTrader quickstart file](https://github.com/nautechsystems/nautilus_trader/blob/6875261266758e379d7af4213b327ea78fa8da01/docs/getting_started/quickstart.py), pinned to commit `6875261266758e379d7af4213b327ea78fa8da01` and Git blob `91b5c5f7af721fe4dd8d3d56bdc5448fb1575774`. Its current upstream API is not claimed compatible with our pinned engine; no acquired code was executed. The original older example path and an intentionally missing file returned retained HTTP 404 outcomes.

YouTube's public metadata and X's documented example post returned partial snapshots. A public TradingView publication returned page metadata only, also partial. Production online gating and GitHub/TradingView acquisition passed together. The Instagram no-client result was verified locally without making an unauthorized API request; it does not verify publication existence or account access. Acceptance metadata is retained privately under `reports/p345/source-*-acceptance.json`. A fixture test is not proof of authenticated/provider capability. Unavailable official paths are retained as failed attempts, not rewritten as successful acquisition.

The subsequent [source-conformance checks](source-conformance.md) retain strict Pine rule mappings and concrete blockers, including a numerical mismatch with the exact-rational SMA kernel. They do not enable converted execution. Remaining Phase 4 work includes verified complete transcripts/media/Pine or authorized provider acquisition, verified complete caption acquisition and fine-grained timing, source-to-plan evidence binding, verified behavioral conversion, source-driven strategy creation, format maturation and verified commercial data adapters. The full acceptance ledger remains [phases 3–5](phase3-5.md).

The [source-review increment](source-reviews.md) connects retained material to an existing strategy agent, exact quote references and private Reports. It does not complete source-driven creation or source-to-plan conformance. Source capture and review times are distinct; a new review does not make an old publication snapshot current.
