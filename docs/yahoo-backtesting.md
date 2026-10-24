# Yahoo historical backtesting

## Scope

`/yahoo` opens a historical-only research form. Use an explicit Yahoo symbol, market type,
start date, exclusive end date, hourly/daily interval, SMA periods, fixed units and simulation
capital/costs. Defaults cover the last twelve complete calendar months. No model or Yahoo key
is required. Yahoo is not a live quote source, trading connector or fallback for Liquid.

Supported price-research models are long-only SMA crossings for equities (including exchange-traded funds), crypto spot and fully
funded FX. Examples: `AAPL` (whole shares), `BTC-USD` (BTC units), `EURUSD=X` or `USDJPY=X`
(base-currency units with at most two decimals). Currency support is limited to the native engine's
known currencies. ETFs retain their Yahoo ETF metadata and use whole-share, cash-funded equity
accounting; futures remain unsupported. Yahoo identity and quote currency remain distinct from Binance BTCUSDT and
Liquid perpetual contracts; no strategy/result is silently relabeled across markets.

The manual `/yahoo` path is also used by the replacement `/create` research campaigns, whose
strategy agents receive their own verified Yahoo results. `/new`, legacy studies and their
historical readers retain existing Binance behavior. Yahoo jobs and campaigns appear in Tests
and Reports. Autonomous repeated optimization, generic tool loops, regime analysis and final
holdout selection are not implemented. See [agent workflow](agent-workflow.md).

## Review and run

1. Complete `/yahoo`, then select **Fetch and review**. Historical acquisition happens before
   simulation approval, without paid capacity or account/trading access.
2. Inspect the exact requested/observed window, currency, gaps, rules, costs and limitations.
3. Approve the displayed plan through its button or `/yahoo-approve <job-id> <plan-sha256>`.
4. Inspect with `/yahoo-show <job-id>` or select its Tests/Reports row. `/yahoo-cancel <job-id>`
   cancels a pending or running job. Escape closes the form; Ctrl+K cancels an active worker.

Two native NautilusTrader 1.231.0 simulations run in separate credential-free processes: base
costs and doubled costs. Quotes model synthetic full fills at the next observed opening price,
with spread/slippage, quote-rounded fees and native cash accounting. Closed-trade P&L comes from
native cash deltas. Final exposure is marked, not forcibly closed. There is no leverage, shorting,
reinforcement, optimization, independent holdout, automatic promotion or live execution.

Approval binds the dataset, complete supported strategy, costs, implementation and two-start
budget. Each start is consumed before dispatch. Failed/canceled/interrupted native work is never
silently retried or resumed; prepare and review a new plan. Restart preserves evidence and marks
active interrupted work without restarting it. Completed report inspection consumes no simulation.

## Data and limitations

The pinned `yfinance==1.7.0` acquisition runs outside the TUI with an allowlisted environment;
model/Liquid credentials are not inherited. Anonymous Yahoo cookie/timezone caches use a temporary
directory removed when acquisition ends. No Yahoo account login or durable credentials are added.
No Firecrawl, Twelve Data, paid downloads or alternate-provider fallback is used.

Preserve the exact frame returned by yfinance, selected instrument metadata, corporate actions,
request and observation time. This is not a raw Yahoo HTTP-response archive or provider attestation.
Disable automatic adjustment, repair, rounding and dropping of missing rows in the library.
Provider split-adjusted OHLC is normalized explicitly to eight-decimal simulation prices, while
returned values remain retained. Null/invalid prices, bad OHLC ranges, duplicate/out-of-order data,
forming bars and mismatched identities fail validation; crypto additionally requires full continuous
window coverage. Invalid OHLCV produces a retained `quality.json` diagnostic, with no price repair.

Equity/FX session calendars and publication times are not independently verified. Observation labels
and interval ends are timing assumptions; next-observation opening fills do not prove broker fills.
Equity/FX snapshots with more than seven calendar days missing at either requested boundary are
blocked; passing this check does not verify intervening sessions. SMA warmup uses the requested
observations and initial alignment alone cannot enter a position. All observed gaps are retained
and displayed. No missing prices are invented, and sparse Yahoo data
cannot establish complete exchange coverage. FX volume is not a consolidated global volume series.

Windows containing reported dividends, splits or capital gains are blocked pending proper corporate-
action accounting. Fractional shares, borrowing/rollover, futures/perpetual funding, margin,
liquidation, historical venue increments and actual liquidity are not implemented. Consequently,
not every stock/window or Liquid instrument is eligible for this research path.

Yahoo access is unofficial, may be rate-limited and is subject to its underlying data terms.
Hourly acquisition conservatively rejects starts older than 729 days. Daily depth and actual symbol
coverage are provider-dependent. No snapshot hash or successful download grants broader data rights.
Inspecting/running retained Yahoo research requires an online connectivity check; the approved
snapshot remains fixed and is not replaced with newly downloaded prices.

## Verified behavior

Authored fixtures exercise all three asset classes at hourly/daily resolution, an 8,760-bar window,
independent fill/equity and quote-rounding calculations, real native subprocesses, secure environment
boundaries, TUI approval/report controls, restart, tampering, cancellation and consumed-start gates.
Fixture performance is not market performance.

Actual acquisition checks returned 720 valid hourly BTC-USD bars for September 2026 and 365 valid
daily bars for October 2025 through September 2026. A separate twelve-month hourly request returned
8,760 slots containing missing OHLC prices and was correctly blocked. No real-data plan was approved
or simulated during these checks. Evidence is under ignored `reports/yahoo-backtests/`.
