# Dated applicability and local research candidates

Status: implemented local increment, October 18, 2026. It extends the [strategy-agent workflow](strategy-agents.md) and [bounded studies](phase3-studies.md). It does not complete the broader [Phase 3–5 ledger](phase3-5.md), connect a live market-context provider or publish a community leaderboard.

## Use it

Select an exact agent with `/agent <strategy-id>` or Library, then enter:

```text
/assess
/assess crypto_spot BTCUSDT 3600
/recommend crypto_spot BTCUSDT 3600
/assessment <assessment-id>
```

A bare `/assess` uses the selected strategy's single fixed market/symbol/interval; ambiguous or deployment-choice definitions require an explicit target. Market names are `crypto_spot`, `crypto_perpetual`, `equity`, `etf` and `forex`. A request does not establish support for that market. Symbols are uppercase and intervals are positive integer seconds. Instrument aliases are not assumed equivalent execution contracts.

A natural-language applicability request routes to the selected specialist; an explicit target can be extracted by the orchestrator. A catalog request delegates each candidate's assessment to its own stable agent. A missing recommendation target prompts for market/symbol/interval. Forced commands perform these deterministic operations without model calls. Read-only assessment cancellation preserves completed snapshots, excludes a partial record and audits the canceled agent operation.

Assessments appear in Reports and retain a content digest, strategy identity, assessment timestamp, target/preferences, rule checks, data/implementation status, evidence references, current-context state, missing verification and next research steps. Opening a retained record requires online access and verifies its stored digest, strategy and timestamp. It displays a historical snapshot rather than silently refreshing or replacing past conclusions. The SQLite development harness and private Supabase schema both protect assessments against update/deletion; the TUI still has no authenticated Supabase synchronization.

## Three separate questions

1. **Rule compatibility:** requested asset class, fixed/deployment-choice symbol, interval and overnight preference. Incompatibilities remain explicit. The initial data adapter's narrow BTCUSDT spot/hourly support is not general instrument support. [Commercial dataset rights](data-source-rights.md) remain unverified.
2. **Retained empirical evidence:** original artifact seals are verified again. Single-plan January research remains continuous development/validation, without a final holdout. A study result is attributed by exact strategy ID and behavioral digest. Only the selected exact variant can receive its final holdout metrics; its parent and siblings keep their own validation results. Each reference retains the entire relevant study's candidate denominator. Summed candidate evaluations are not a count of unique strategy identities. Failed/incomplete candidates and holdouts remain labeled.
3. **Dated current context:** the default is explicitly unavailable because no verified provider is connected. The adapter contract carries event/availability timestamps, target, source references and snapshot digest. Future, stale, mismatched, incorrectly ordered and user-supplied/unverified context are distinct from a current provider snapshot. Configured provider errors become an unavailable record without a substituted observation or leaked provider body. Assessment time is taken after retrieval. Even a fresh snapshot alone does not establish regime suitability.

Artifact integrity establishes retained software evidence, not market representativeness, data rights or success on another instrument. New protocol/native checks use authored fixtures while commercial data entitlement remains unresolved. Assessments do not turn fixtures, imported claims or unverified metrics into market-performance validation.

A requested drawdown preference is compared with retained measured drawdown, with breaches visible. It is an assessment preference, not an operational risk mandate. Overnight holding remains allowed by default as confirmed. No empirical sample/promotion thresholds or portfolio risk weights are invented.

## Local candidate output

Candidates remain in stable public-ID order. The list shows compatibility, evidence count/attempt denominators and context status, with inspectable dated assessment IDs. It does not score by P&L, invent ranking weights, label registration as validation or authorize paper/live operations. Paper/live evidence, account/environment/scopes and deterministic execution controls remain missing. Each selected specialist can use its own dated findings/evidence in later analysis; another strategy's private turns are excluded.

Current evidence lookup is scoped to this private local workspace. A hosted implementation must return only owner-authorized or explicitly publishable evidence projections before exposing shared users. Authentication, shared catalog publication/retention rules and live data-provider rights remain pending.

## Verification scope

Controlled tests cover exact-variant holdout attribution, incompatible markets/timeframes, missing/stale/future/user-supplied context, provider failure/redaction, immutable identity-bound snapshots, restart, cancellation, drawdown/holding preferences and online-only empty catalog queries. TUI checks cover forced commands, actual Reports records, responsive sizes and no model call for deterministic operations.

Live local Ollama routed one selected-strategy assessment and one request across two catalog strategies in 11.426 seconds with two model calls. The router received no private source or completed private conversation text. Three dated records were retained, context stayed explicitly unavailable, promotion stayed `none`, and no simulation or price download ran. This verifies the routing/operation boundary, not actual current-market data or investment suitability.

The remote SQL harness verifies private assessment ownership, spoofed-owner rejection and immutable payload/timestamp/deletion, then rolls back every fixture and temporary grant. Final checks showed zero retained profiles/strategies/assessments, 13 RLS-enabled tables, no anonymous/direct-client schema access and no security-advisor findings. Applied migration history remains authoritative for filenames.

The full local regression suite passed **457 tests with zero skips**, including nine native-engine tests. The 33 upstream Pandas deprecation warnings remain visible. Package/visual inspection is recorded with the checkpoint; this does not establish live data or account capability.

Subsequent responsive verification confirms that Reports reserves preview space at 45 × 12, scrolls the selected row into view after resizing, preserves the selected snapshot across refresh and retains chat focus when a background assessment completes. Rebuilt wheel/sdist source/CSS equality and configured-credential/cache/state exclusion checks passed. Isolated packaged views render original sealed native fixture evidence and the new dated assessment at small/large sizes. These are authored fixture previews with an injected online test gate, not a live-market observation.
