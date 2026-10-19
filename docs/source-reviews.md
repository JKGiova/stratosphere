# Strategy-owned source reviews

Implemented as an experimental private local workflow. A registered strategy's specialist can review a retained source snapshot against its existing rules, retain tentative findings with exact quotes, and show the result in Reports. This is not source-to-plan conversion, behavioral conformance, empirical validation, test approval or paper activation.

## Use

1. Acquire material with `/source <url>`, `/source-text <text>` or `/source-file <path>`; `/sources` lists retained attempts.
2. Select an existing strategy with `/agent <full-strategy-id>` or its Library row.
3. Run `/source-review <source-id> [strategy-id]`. Readable material uses the configured model; metadata-only, blank or unavailable material produces explicit questions without a model request.
4. Open Reports or use `/source-review-show <review-id>` to inspect a completed review in Focus. `/source-reviews` lists retained attempts, including failures and cancellations.

Chat reaches the same application operation. For example, `Review source <source-id> against the selected strategy` routes directly to that specialist. Explicit review/analyze/inspect requests with one source reference and one unambiguous strategy target do not need a model call merely to choose the operation. A bare HTTPS URL acquires material without creating a strategy. Ambiguous requests retain validated model routing; unknown or conflicting explicit targets fail rather than choosing silently. A source-review link records comparison context, not proof that the source authored or derived the strategy.

Both Reports and Focus support Page Down/Page Up for long reviews, including the 45-column, 12-row layout. Acquisition, review and inspection retain the application's online requirement. No application upload quota is introduced; supported formats, provider limits and memory requirements still apply.

## Evidence boundary

The application partitions every readable non-metadata piece into ordered exact passages under `source-review-passages/1`. Passages follow original lines and contain at most 512 Python characters, preferring word boundaries within a long line. Whitespace-only passages remain as context but cannot support a finding; all source characters remain available, without normalization, truncation or an upload cap. Unicode, repeated text and original line endings retain their coordinates.

The model receives the selected strategy and passage table as untrusted data. Each proposed finding supplies an integer `evidence_index`; it cannot generate quote text, offsets or occurrence counts. The application looks up that exact passage and constructs its quote, piece digest and character range. Invalid indices and additional quote/coordinate fields are rejected rather than repaired. The captured table is used across the model await, so a later partition-setting change cannot reinterpret an in-flight response.

Inspection verifies the original source seal and exact retained substring again. Line references account for original line endings; caption timestamps describe the entire acquired caption piece, not an invented timing for an individual quote. A valid reference proves where the quote came from; it does not prove that the model's interpretation follows from it.

A new model review requires at least one quoted finding. New proposed questions must be single-line strings ending in a question mark. Blank/multiline strings and strings without that closing mark are rejected before publication. This rejects the observed multiline JSON-fragment question; it does not detect every possible malformed or irrelevant question. This is a format check, not proof of relevance or factual correctness. A source without usable rules can produce a quoted ambiguity; it cannot become an empty successful report. Metadata and unavailable content never become invented findings. Original quotes retain their language; interpretations and questions use English. Finding categories are tentative rule candidates, assumptions, ambiguities and unsupported features. None grants execution authority or establishes that an interpretation is correct.

Each `source-review/1` record pins the strategy ID/specification digest, source ID/snapshot digest, model reference, review time and reviewer digest. The reviewer digest identifies the loaded prompt, proposal/report schemas, evidence protocol and passage size captured before the request; it is not executable attestation. Snapshot capture time is displayed separately from review time. Reviewing retained material does not verify current publication content or current market conditions.

Source and strategy references are checked again after model completion. Private artifacts include the request, report, hash manifest and attempt events. Immutable local SQLite rows bind published reviews to their strategy and payload digest. Reopening verifies both the database record and files, source identity and quote spans. A completed artifact without its database publication is not a published report. Failed/canceled attempts remain inspectable and do not append a completed specialist conversation turn. Detected configured/session credentials are rejected before persistence.

Only the owning strategy's latest reviews enter its subsequent analysis context; histories and review ownership remain separate across strategies. The local `source_reviews` table is not deployed to Supabase. The existing private hosted schema, authenticated application service and future shared projections must not be confused with this local workflow.

## Verification and remaining work

Tests cover complete Unicode passage coverage, repeated-text/span resolution, line/caption coordinates, ownership, snapshot and artifact changes, reviewer identity capture, metadata/blank/unavailable material, invalid and empty output, credential rejection, cancellation, restart, chat/slash routing and responsive report inspection. Final measured suite, local-model and package results are recorded in the Phase 3–5 acceptance ledger.

The local Ollama harness uses authored descriptions for two already registered strategies. It checks actual quoted findings, separate model context and restored histories; it performs no price acquisition, simulation or trade call. Earlier attempts exposed wrong routing, empty output and a blank question; these failed attempts are retained privately. A questions-only intermediate result verified storage/isolation only. Successful bounded checks do not establish general model reliability or source interpretation quality.

Complete authorized media/Pine acquisition, source-to-plan evidence binding, faithful executable conversion and empirical testing of extracted rules remain pending. The [conformance diagnostic](source-conformance.md) continues to block unsupported conversion. Full requirements remain in the [Phase 3–5 acceptance ledger](phase3-5.md).

On the earlier quote-copying prompt/schema revision, both installed models (Spark-X2.5 1.7B and Qwopus3.5 4B) produced quotes absent from the supplied pieces. Validation rejected these attempts before report publication; no automatic retry, simulated test or trade followed. An earlier revision completed a two-agent quoted-review check in 22.762 seconds, but that result does not establish reliability of the final revision. That result motivated the passage-selection protocol; it remains part of the historical verification record.

The earlier quote-copying increment’s local regression passed **746 tests with zero skips**, including the native-engine checks, with 36 upstream Pandas deprecation warnings. Ruff lint/formatting, built wheel/sdist source and credential exclusion, and chat/slash Reports inspection at 180×48 and 45×12 passed. The built-package source reviews used an explicit fixture model; they do not replace the failed final live-model verification above.

## Retained compatibility

The persisted `source-review/1` report and manifest formats remain unchanged. They contain final exact quotes and source spans, not generated passage indices. Historical substring-based reviews reopen with their original hashes and bytes; inspection does not repartition or reinterpret them. The new proposal schema is identified by its reviewer digest and the explicit passage protocol in model context. Future partition behavior changes require a new protocol identity. This local change adds no Supabase schema, shared-service synchronization or execution authority.

## Passage-reference evaluation

The first passage-selection revision was exercised against two already registered strategies and multiple source passages with both installed models. Spark-X2.5 1.7B completed two requests in 46.399 seconds (3 and 4 findings); Qwopus3.5 4B completed two requests in 120.212 seconds (5 and 6 findings). Both selected actual rule passages, retained exact source spans, separated private contexts and restored completed histories. No prices, simulations or orders were requested.

Those reference checks did **not** establish useful interpretation quality. Inspection found incorrect associations between findings and passages, contradictory claims about matching position sizes, and a Qwopus question containing a multiline JSON fragment. The captured fragment is rejected by the subsequent plain-question proposal contract and covered by regression tests. Other inaccurate interpretations can still pass structural checks. The reports remain experimental, and broader extraction/interpretation reliability is not verified. A valid citation must not be presented as proof of its interpretation.

Two actual retained reports from the earlier substring-based revision also reopened against their original source/registry artifacts without changing report hashes or artifact bytes. The compatibility check used an explicit fixture online gate; it does not verify a provider connection or an offline application mode.

After the plain-question validation change, the Spark two-strategy reference/isolation check passed again in 46.044 seconds with two requests (3 and 4 findings). Response inspection still found inaccurate interpretations, so this is a bounded reference/provenance check, not validation of factual extraction. The separate full-character tests cover blank lines, long Unicode lines and six line-separator forms; whitespace-only references are rejected before publication.

The final passage-protocol regression passed **759 tests with zero skips**, with 36 upstream Pandas warnings; the focused source/agent/UI suite passed 85 checks. Ruff lint/formatting, built-package source/credential exclusion and responsive chat/slash report inspection are checked separately. These software checks do not upgrade the experimental interpretation-quality status above.

## Schema-delivery follow-up

The configured adapters now include the exact interpretation schema as explicit system-prompt data, with the shared `request-json-schema-text/1` contract included in new reviewer provenance. Native Ollama constraints remain additional; other providers are not assumed to enforce a native format. Persisted readers and old hashes remain unchanged. Two actual prior source reviews reopened with their old hashes and artifact bytes intact.

Local Spark then completed two strategy-owned reviews in 37.623 seconds, with three valid passage references each, private context isolation and restored histories. Manual inspection still found inaccurate interpretations: a period-only citation was described as supporting crossing behavior, and another finding introduced nonsensical terminology. A valid reference remains insufficient evidence of semantic entailment, faithful conversion, empirical performance or approval. The full [acceptance ledger](phase3-5.md) stays in progress.
