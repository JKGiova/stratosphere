# Research and reports

The historical Yahoo path is described in [Yahoo backtesting](yahoo-backtesting.md). It uses
separate versioned plans/snapshots for equity, crypto and fully funded FX price research; the
legacy Binance behavior and evidence below remain unchanged.

## Supported behavior

The trusted implementation supports long-only fast/slow SMA crossover on BTCUSDT spot hourly
closed candles, fixed BTC quantity and at most one position. A cross above opens while flat; a
cross below closes while long. Signals execute at the next candle's open. Initial SMA alignment
alone does not enter. No leverage, reinforcement, stop loss, take profit, other filters or intrabar
logic is silently added. End-of-sample exposure is not forcibly closed.

NautilusTrader 1.231.0 runs the trusted implementation in a separate process. Arbitrary imported
Python/Pine/generated code is not supported execution. The exact-rational kernel does not establish
Pine floating-point or runtime equivalence.

## Review and execution

`/new` validates source evidence and supported fields, then shows a complete specification and cost
plan. Approval pins the entire plan, implementation and finite native-start budget. The initial
research path runs a base case and doubled-cost stress case. Prices, timestamps, precision and
quality checks must pass before execution; no offline/fixture fallback can become market evidence.

The first archive profile is BTC/USDT hourly January 2025 from Binance Vision. New runs/resume
verify the pinned archive and checksum online. See [data rights](data-source-rights.md).

`/pause` retains completed checkpoints. `/resume` requires the same approved unchanged plan and
fresh verification. Cancellation/failure retains attempts; consumed native slots are not retried
silently. Changed code or plans require review through the application's validation rules.

## Bounded studies

`/study` starts from a supported registered definition or parent job. The application fixes the
baseline, explicit requested pairs, full definition, simulation costs, chronological windows,
selection rule and native-start budget before approval. Variants have distinct identities/lineage.

Retain invalid, failed, canceled and nonselected attempts. Select on development/validation only.
Acquire final holdout prices after immutable selection and evaluate it once. Local exposure claims
survive restart but cannot prove data was unseen elsewhere. A selected variant's holdout does not
belong to its parent or siblings.

## Reports

Reports retains measured jobs and interpretation receipts. `/report` displays a verified report;
`/report-data` inspects complete paged cells/rows without another simulation or changed approval.
Preserve exact candidate, window, cost, row order, denominators, failures and immutable references.

`/analyze-report` resolves the owning specialist and supplies complete application-owned measured
facts, not other histories or raw tables. Source/report narratives remain unverified even with
valid citations. Dated applicability distinguishes rule compatibility, measured evidence, current
context and missing verification; profitable results alone cannot activate a strategy.
