# Data, testing, and validation

## Evaluation modes

| Mode | Data | Orders | Objective |
| --- | --- | --- | --- |
| Backtest | Versioned historical data | Simulated | Reconstruct past behavior |
| Replay | Historical events reproduced in order | Simulated | Verify the runtime and operational cases |
| Forward test | Real-time feed | None | Observe signals, delays, and data availability |
| Paper trading | Real-time feed | Simulated or provider paper environment | Evaluate the portfolio and simulated execution |
| Live | Real-time feed | Real | Operate within the activated mandate |

Simulation must expose its assumptions: paper trading and backtesting do not necessarily reproduce liquidity, order priority, and actual fills.

## Data contract

Record provider, instruments, interval, timezone, frequency, event and availability timestamps, transformations, and a hash or immutable snapshot. Check duplicates, ordering, gaps, incomplete bars, and symbol changes. Strategies use only information available at signal time.

Account for sessions and corporate actions where relevant; include funding, securities borrowing, expiries, or rollovers when applicable. A dataset unsuitable for the selected market blocks validation or explicitly limits the report's interpretation.

## Historical protocol

1. Fix hypotheses, benchmarks, metrics, costs, and criteria before experiments.
2. Split development, validation, and final out-of-sample holdout data chronologically.
3. Apply warm-up and rules to avoid contamination between windows; use walk-forward evaluation where appropriate.
4. Simulate relevant commissions, spreads, slippage, latency, and execution constraints.
5. Compare the strategy with simple baselines and previous versions under the same protocol.
6. Check sensitivity to parameters, regimes, periods, and increased costs.
7. Retain configuration, code, dataset, trades, and results for every attempt.

The final holdout is not an optimization tool. If it is used to select new variants, it is no longer an independent assessment and a new protocol is needed. Account for the number of attempts when interpreting the selected variant.

## Errors to check

- Look-ahead: using prices or indicators that were not yet available.
- Survivorship bias and opportunistic selection of instruments or periods.
- Repainting, especially in TradingView conversions.
- Differences between a close-of-bar signal and a fill at the same price.
- Stop loss and take profit touched within the same bar without enough data to reconstruct their order.
- Omitted costs, unrealistic liquidity, and trades exceeding market capacity.
- Parameter overfitting and repeated selection on out-of-sample data.

When historical data cannot determine a fill, adopt a declared, conservative convention or require more granular data.

## Minimum report

Every report contains the version, sources, assumptions, dataset, windows, cost model, baseline, gross/net returns, equity curve, drawdown, trade count, turnover, exposure, and result distribution. Include risk-adjusted return metrics only with explicit conventions and frequency; flag insufficient samples and results concentrated in a few trades.

Include inspectable trades, development versus out-of-sample comparisons, stress tests, limitations, and the validator's proposal rationale. Store values calculated by the engine: the model comments on them without reconstructing them from text.

## Promotion criteria

Numerical thresholds depend on the market, strategy, and mandate and must be defined before tests. Promotion nevertheless requires correct rules, suitable data, reproducible results net of costs, out-of-sample evidence, robustness, and no unresolved critical issues.

For paper → live promotion, define duration or minimum observation count, tolerances between expected and observed behavior, and latency and reliability requirements in advance. Also verify reconciliation, limits, interruptions, restarts, and shutdown procedures. Do not automatically promote a strategy based on returns alone.

## Shared evidence and short-term multi-market testing

Store each run independently with its strategy identity, exact implementation, data snapshot, engine version, cost assumptions, instruments, and evaluation protocol. Reusing an existing strategy permits retrieving past runs, not assigning their returns to a different asset, timeframe, provider, or configuration.

Short-term tests must allow overnight and multi-day holdings when the rules allow them. Do not automatically liquidate at a session boundary. Apply session calendars, corporate actions, funding, financing, and overnight gaps where relevant, and explicitly test holding policies.

Record observed regime suitability in findings with regime definitions, samples, windows, and limitations. Recommendations must distinguish reproducible backtests, observed paper results, and verified live evidence. Missing verification or short samples must remain visible; shared registration is not a validation badge.

Optional alternative-data sources such as Quiver require publication/availability timestamps and snapshot retention. Transaction dates alone must not grant earlier knowledge to the backtest. Dataset access, licenses, and freshness remain connector-specific decisions.

## Authentication and connector acceptance tests

Before shared/private access, verify Supabase OAuth callbacks, token validation/expiry, safe refresh and logout, identity-to-profile mapping, and cross-user authorization isolation. An external ID without a valid verified session must not grant access.

Test research without any broker connection. Linking a provider must not enable live orders. Verify account/environment isolation, least-privilege scopes, revocation, reconnects, required confirmations, and provider-specific reconciliation before each connector is enabled.

Verify that budget proposals and risk-agent assessments cannot bypass deterministic checks or send direct orders. Simulate concurrent intents, reserved capital, partial fills, ambiguous timeouts, and duplicate retries. Backtests must model the actual tradable instrument; a perpetual referencing a stock cannot inherit an ordinary equity test without additional validation.

## Release and commercial-cost refinements

The first public/commercial release follows extensive testing without a fixed deadline. Define measurable release gates rather than interpreting a successful internal demo as release readiness. Testing must cover all five Phase 1 sources, converted strategy behavior, Linux/macOS onboarding, online outages, user isolation, research repeatability and eventual paper/live recovery. Required durations, samples and operating thresholds remain open.

When the commercial fee policy is finalized, include the stratosphere opening-trade fee in appropriate cost models, separately from broker charges. The target is 0.5% per opening trade, capped at $1, but fee basis and grouping have not been approved.

## Release-gate discussion

The user requested clarification of release gates rather than approving numerical thresholds. The [research and release proposals](research-and-release-proposals.md) suggest an initial 30-day paper stability exercise, representative source/client tests, behavior conformance, reproducibility, security/billing checks and crash/reconnect/exit scenarios. The duration and specific criteria remain proposals, and operational stability does not establish a strategy's profitability or authorize live trading.
