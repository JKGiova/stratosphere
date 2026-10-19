# Bounded SMA studies

Status: implemented local increment, October 18, 2026. This is part of the [Phase 3–5 acceptance ledger](phase3-5.md), not completion of Phase 3 or a release. The native workflow is verified with authored market fixtures. Binance Vision is not a verified commercial data entitlement; see [data-source rights](data-source-rights.md).

## Try the workflow

Prepare a complete supported SMA plan with `/new`, then select its strategy agent. Use `/study`, `/study <strategy-id>` or `/study <parent-research-job-id>`. The [registry-anchored path](phase3-registry-studies.md) also supports definitions and derived variants without a standalone parent research job. A natural-language request for a bounded experiment routes to the selected specialist, which can propose an editable hypothesis and distinct candidate pairs including the baseline. The application calculates the minimum run budget. Model suggestions are proposals; their explanatory quality still needs human review.

The inline form lets you set the hypothesis, exact comma-separated fast/slow pairs, development month and maximum native starts. Fixed BTC quantity and simulated costs come from the parent plan on the legacy path. Registry-anchored studies pin fixed quantity from the definition and expose costs in the form. Review the displayed protocol before approving. The parent plan need not have been run, but it must have a complete conforming specification. A registered variant without an original `/new` job can now use the version-2 registry anchor, with editable simulation costs and its exact baseline included. No replacement source extraction or parent research job is invented.

`/approve <study-id> <protocol-sha256>` authorizes exactly the frozen protocol. `/pause`, `/resume`, `/cancel`, `/show` and `/report` use the same controls as single-strategy research. Tests includes studies; Reports displays the retained comparisons and final holdout status. Specialist agents invoke the same validated service. Study cells and audit operations are attributed to their exact variant identity and stable agent.

## Frozen protocol

A protocol binds the parent behavior/source/plan references, explicit hypothesis and candidate list, costs and sizing, checksum-pinned monthly archives, benchmark, selection rule, run budget, native implementation sources and window/reset conventions. A changed implementation requires a new review. Previously completed sealed reports remain inspectable.

The adapter currently supports BTCUSDT spot hourly archives from January 2025 onward, with eight-decimal SIM semantics. It validates real calendar month lengths, microsecond timestamps, exact row counts, hourly alignment, continuity, OHLCV, checksums and the single expected ZIP member. It follows no redirects, silently substitutes no provider and uses no cached-data fallback after provider revision. Pinning a proposal retrieves checksums, not prices.

Development is the first `floor(0.7 × monthly observation count)` bars; validation is the remainder. Every valid candidate has base-cost and doubled-cost simulations in each window. Invalid candidates stay visible without native execution. Failed, canceled and interrupted native starts consume their slot and are retained. Slots are never automatically rerun, including after process recovery. A candidate is eligible only if all four simulations complete. An all-invalid/incomplete study produces a report with no fabricated winner or holdout metrics.

Each window starts with flat exposure and the reviewed starting cash. Prior observations warm indicators only. Crossings must occur at a measured closed candle, with fills at the following opening quote; no pre-window crossing creates an order at the first measured open. Positions are marked at the end rather than forced closed. Native quote fills retain the existing synthetic liquidity, fee/spread/slippage and instrument-filter limitations.

## Selection and final holdout

Select the highest validation doubled-cost net P&L; break ties by lower validation doubled-cost drawdown, then original candidate order. Development results are descriptive. Selection is written immutably before acquiring holdout prices. No holdout metric can change it.

The final holdout is the next separate calendar-month archive. Only the selected candidate gets one base and one doubled-cost run on that month. A local transactional exposure ledger claims archives before download. It blocks a previously exposed development archive from becoming a holdout, a consumed holdout from another study, and optimization on a consumed holdout. Different clients cannot concurrently claim the same holdout for different studies. A failed acquisition conservatively consumes the claim; this is deliberate. Resume can continue unconsumed slots in the same study, without rerunning a consumed cell.

This ledger is private to the current workspace. It cannot establish that public data was unseen by the user, another program, an earlier workspace or another device. It is not a shared-user contamination service. A fresh study needs fresh chronological windows rather than another search against a consumed holdout.

## Retained evidence

Study artifacts live under the existing private research directory's `studies/` subdirectory. Each candidate retains identity, parameters, status, all four cell outcomes and validation selection disposition. Native responses retain equity, fills, signals, account balances, costs, trade distributions and runtime versions. Cash and same-fixed-quantity buy-and-hold baselines are calculated for each measured window, at base and doubled costs. Benchmark capital insufficiency stays explicit.

Reports retain the complete denominator, invalid/ineligible counts, native-start count, frozen selection, holdout completion/failure, normalized data digests, original archives, implementation sources and a final artifact manifest. Checkpoint digests detect changed pending evidence; final seals cover every top-level evidence artifact. Workers use the existing credential-free packaged subprocess and independently normalize the pinned archives before selecting their window. Retrieved code, model output and arbitrary SQL are not executed.

The outer study report belongs to the parent study, but its headline holdout metrics explicitly identify the selected variant. They must not be transferred to the parent or another candidate. Local registration, source lineage and per-cell audit operations do not constitute Supabase synchronization, community publication or trading activation.

## Verification

The tests cover approval/budget checks, immutable checkpoints, complete attempt retention, failed/invalid candidates, pause/restart, interrupted slots, validation-only selection, failed holdouts, exclusive exposure claims, reuse blocking, malformed/provider-changed archives, native warm-up and independent batch SMA/cash reconciliation. A full authored two-candidate study executes ten actual native subprocess simulations. Separate TUI checks exercise the inline form, approval, specialist execution, Reports and responsive layouts.

Live local Ollama checks verify routing and schema-constrained proposal production without approval or price downloads. They do not verify hypothesis quality or empirical performance. The full regression suite passed 436 tests with zero skips, including nine native-engine tests; 33 upstream Pandas deprecation warnings remain visible. A subsequent focused test verified opening the form at 45 × 12 and scrolling the focused budget into view. Rebuilt wheel/sdist checks passed: packaged Python sources match the checkout, configured API keys and cache/state files are absent, and isolated packaged Reports/form previews passed at small and large sizes. The live Ollama proposal check completed in 12.572 seconds with two model calls and three proposed candidates. Hypothesis quality remains for human review.

The [dated local applicability workflow](phase3-applicability.md) is now implemented. Still pending: authenticated community recommendations, actual current-context providers, broader strategy families/instruments, authenticated registry synchronization, commercial data entitlement/provider selection, all five content-source adapters and the complete paper runtime.
