# Registering retained native definitions

Status: an explicit path from a retained, complete stratosphere `strategy/1` JSON definition to one stable strategy/agent and a separately reviewed study. This supports the existing BTCUSDT hourly, long-only fixed-quantity, exact-rational SMA kernel. It does not convert arbitrary Python, Pine, captions or media descriptions. The [full Phase 3–5 ledger](phase3-5.md) remains in progress.

## User workflow

The repository includes an authored engineering input at [examples/strategies/sma-close-cross.json](../examples/strategies/sma-close-cross.json). It is not market-performance evidence. From the repository checkout:

```text
/source-file "examples/strategies/sma-close-cross.json"
/source-register <complete-source-id> "Example SMA definition"
/source-registrations
/source-registration-show <complete-registration-id>
/agent <complete-strategy-id>
/study <complete-strategy-id>
```

The source-file operation retains material only. Registration requires the separate explicit command. `Register source <complete-source-id>` (optionally prefixed with `Please`) invokes the same registration operation without a model-generated route. Optional positional arguments are a name and a local parent strategy ID: `/source-register <source-id> [name|-] [parent-id]`. Use `-` to keep the source label while providing a parent. The active Library selection and existing private histories remain unchanged; select the returned strategy explicitly for analysis.

Registration creates no research plan, approval, native start, paper mandate, account connection or order. `/study` opens the existing native-definition study review, with an exact baseline, explicit candidate parameters, costs, windows and native-start budget. Only the later exact-plan approval may start tests. Current commercial market-data entitlement remains unresolved; development validation uses authored fixtures. Import does not make a data source licensed or an arbitrary window sufficient for warmup.

An exact duplicate reuses its strategy and agent, preserves their name/history and accumulates the new submission/source binding. The requested name remains in the receipt; it does not rename an existing definition. A changed definition has a different identity. An explicit parent records declared local variant lineage; it is not proof of external authorship or source derivation. Self-parenting and cycles are rejected in the same registry transaction. No parent is inferred from whichever strategy happens to be selected.

## Accepted material and complete-definition checks

Accepted inputs are one acquired supplied text/code piece, or one acquired GitHub file with retained commit/blob references. GitHub acquisition still requires appropriate existing read access; this increment adds no token discovery or credential flow. Directory listings, metadata-only/partial material, caption pieces, executable source, inferred descriptions and unavailable sources cannot be registered through this operation.

The entire piece must be JSON representing the complete existing `strategy/1` contract. Duplicate object keys and nonfinite values are rejected. No fields/defaults, value coercions, stops, filters, exit behavior, arithmetic or timeframe semantics are silently inserted/dropped. Validation compares the complete canonical input to the supported compiled SMA definition, not just indicator names/periods. Every behavior field must agree, including data/execution requirements and assumptions. Root/multiline whitespace and one leading BOM may differ without changing the declaration; raw input remains retained.

Unsupported input stays in its source snapshot, and the separate registration attempt records failure. The parser never imports or evaluates acquired code. The [external-source conformance gate](source-conformance.md), including Pine's arithmetic/runtime gap and the `/new` format guard, remains in force. Matching this native data contract is not a faithful external-language conversion claim.

## Atomic provenance and receipts

The application builds a `native-source-binding/1` proof over the exact source snapshot, retained acquisition request and its byte digest, whole-piece digest, complete strategy digest and review source pins. Original/canonical URLs are preserved where present in the retained acquisition request; unavailable historical attribution is not reconstructed. A `source-registration/1` receipt binds that proof, registration request, stable strategy/agent, actual name, resolution and separate requested/recorded times. Source capture time is retained separately. All interpretation/source pins are local consistency references, not executable or provider attestation.

One SQLite transaction creates/reuses the definition/agent, accumulates provenance/submission/lineage and inserts the immutable receipt. Failure rolls back all those changes. Replaying the same registration ID with the exact original inputs returns its receipt without another submission; reusing an ID with different inputs is rejected. An intentional new user request gets a new attempt/receipt and can accumulate duplicate-source provenance.

The database receipt is authoritative after commit. A crash or filesystem failure before an auxiliary completion event cannot require re-registration. Receipt listing unions committed records and retained filesystem attempts; even a missing attempt directory does not hide a committed action. Inspection never retries registration or mutates registry/approval state. Failed/canceled/uncommitted attempts remain distinct. A damaged/missing source prevents verification of its receipt without undoing an already registered immutable definition.

Known configured secrets are rejected before request storage or registry insertion. Native registration makes no model request and appends no model history. A model's JSON/text claiming a registration action is not an operation; only the explicit deterministic request grammar or slash command authorizes this path. Subsequent analyses/studies use the existing application operations and privacy boundaries.

The registry anchor in a study pins the exact full definition. Provenance accumulates separately: an association/registration receipt does not make an existing report an empirical validation of that source or original program. A selected variant's holdout still belongs only to that exact variant. Source rights, current publication content and broker/runtime readiness remain unverified.

## Verification boundaries

Tests cover whole-source acceptance, malformed/duplicate/defaulted/coerced/unsupported input, media/code rejection, source/request mutation, rollback at receipt insertion, immutable receipts, duplicate and concurrent delivery, declared lineage/cycle rejection, late/missing completion markers, missing auxiliary directories, cancellation, online/credential gates, active-selection/history separation and model-generated action rejection. The source-to-native study test requires a separate protocol approval and compares actual pinned NautilusTrader signals, fills, costs, balances and equity against an independent batch oracle on authored observations.

The [acceptance ledger](phase3-5.md) records measured full-suite, installed-package and responsive interface results. Protocol fixtures and local SQLite ownership are not an authenticated live GitHub check, shared-user service deployment, broad strategy conversion, current-market applicability or sustained paper execution.

When a study revisits an already registered candidate (including an ancestor), it reuses that identity and records comparison provenance instead of adding reverse/new derivation edges. New candidate definitions still record their creation lineage. This preserves acyclic declared lineage without disqualifying valid existing behavior from a reviewed comparison. The application-owned study operation selects this mode; imported metadata cannot select it. The study implementation source pin changes with this workflow update, so an older paused approval cannot silently execute the revised implementation; historical report readers retain their original evidence.
