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

The model receives the selected strategy and available non-metadata pieces as untrusted data. It proposes interpretations and selects literal quotes by piece index and occurrence. The application resolves each non-overlapping occurrence into Python character offsets, verifies the exact substring and retains the piece digest. Inspection checks the original source seal again. Line references account for original line endings; caption timestamps describe the entire acquired caption piece, not an invented timing for an individual quote.

A new model review requires at least one quoted finding. A source without usable rules can produce a quoted ambiguity; it cannot become an empty successful report. Metadata and unavailable content never become invented findings. Original quotes retain their language; interpretations and questions use English. Finding categories are tentative rule candidates, assumptions, ambiguities and unsupported features. None grants execution authority or establishes that an interpretation is correct.

Each `source-review/1` record pins the strategy ID/specification digest, source ID/snapshot digest, model reference, review time and reviewer digest. The reviewer digest identifies the loaded prompt and proposal/report schemas captured before the request; it is not executable attestation. Snapshot capture time is displayed separately from review time. Reviewing retained material does not verify current publication content or current market conditions.

Source and strategy references are checked again after model completion. Private artifacts include the request, report, hash manifest and attempt events. Immutable local SQLite rows bind published reviews to their strategy and payload digest. Reopening verifies both the database record and files, source identity and quote spans. A completed artifact without its database publication is not a published report. Failed/canceled attempts remain inspectable and do not append a completed specialist conversation turn. Detected configured/session credentials are rejected before persistence.

Only the owning strategy's latest reviews enter its subsequent analysis context; histories and review ownership remain separate across strategies. The local `source_reviews` table is not deployed to Supabase. The existing private hosted schema, authenticated application service and future shared projections must not be confused with this local workflow.

## Verification and remaining work

Tests cover quote occurrence/span resolution, line/caption coordinates, ownership, snapshot and artifact changes, reviewer identity capture, metadata/blank/unavailable material, invalid and empty output, credential rejection, cancellation, restart, chat/slash routing and responsive report inspection. Final measured suite, local-model and package results are recorded in the Phase 3–5 acceptance ledger.

The local Ollama harness uses authored descriptions for two already registered strategies. It checks actual quoted findings, separate model context and restored histories; it performs no price acquisition, simulation or trade call. Earlier attempts exposed wrong routing, empty output and a blank question; these failed attempts are retained privately. A questions-only intermediate result verified storage/isolation only. Successful bounded checks do not establish general model reliability or source interpretation quality.

Complete authorized media/Pine acquisition, source-to-plan evidence binding, faithful executable conversion and empirical testing of extracted rules remain pending. The [conformance diagnostic](source-conformance.md) continues to block unsupported conversion. Full requirements remain in the [Phase 3–5 acceptance ledger](phase3-5.md).

On the final prompt/schema revision, both installed models (Spark-X2.5 1.7B and Qwopus3.5 4B) produced quotes absent from the supplied pieces. Validation rejected these attempts before report publication; no automatic retry, simulated test or trade followed. An earlier revision completed a two-agent quoted-review check in 22.762 seconds, but that result does not establish reliability of the final revision. The current live-model limitation remains unresolved and must not be presented as a dependable end-to-end extraction capability.

The final local regression passed **746 tests with zero skips**, including the native-engine checks, with 36 upstream Pandas deprecation warnings. Ruff lint/formatting, built wheel/sdist source and credential exclusion, and chat/slash Reports inspection at 180×48 and 45×12 passed. The built-package source reviews used an explicit fixture model; they do not replace the failed final live-model verification above.
