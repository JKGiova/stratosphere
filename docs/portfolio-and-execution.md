# Portfolio, positions, and live execution

## Management mandate

The portfolio agent coordinates active strategies: it proposes capital allocation, decides which signals to accept, and proposes position openings, increases, reductions, or closures according to overall state. User-configured limits are enforced by deterministic checks outside the model.

The mandate includes the account, permitted instruments, capital, active strategies/versions, per-position and per-strategy limits, overall exposure, leverage where supported, concentration, maximum loss and drawdown, rebalancing frequency, and order approval mode. Thresholds and allocation methodology remain open decisions.

## From agents to positions

1. Strategies produce structured signals with a version, timestamp, and expiry.
2. The system collects capital, positions, open orders, and feed health.
3. The portfolio agent proposes allocations and exposure changes.
4. Checks validate the proposal, state freshness, available capital, and limits.
5. The gateway translates approved intents into provider-compatible orders.
6. Fills and reconciliation update the ledger; the portfolio is reevaluated against actual state.

Signals from different strategies on the same instrument must not create incompatible independent orders. Define a policy for netting, opposing signals, capital attribution, and priority. Maintain virtual positions per strategy and aggregate account positions, with fill and P&L attribution.

## Openings and closures

Exits may be requested by strategy logic, rebalancing, or a risk limit. The system identifies the decision's origin and applies the precedence defined in the mandate. The agent cannot override a risk constraint or autonomously expand its mandate.

Essential operational protections must work even when the model is unavailable. Where the provider supports them, evaluate broker-side protective orders; otherwise, state the dependency on the runtime and verify the corresponding emergency procedure.

## Execution requirements

- Idempotency identifiers and deduplication to avoid duplicate orders after retries.
- Handling of accepted, rejected, partially filled, canceled, and expired orders.
- Validation of quantity, tick size, minimums, balance/margin, and market state.
- Reconciliation with the provider after timeouts, disconnections, and restarts.
- Ambiguous order state treated as requiring reconciliation, without blind automatic resubmission.
- Tracking of slippage, costs, fills, and deviations from target.
- Separate paper and live credentials with access limited to required operations.

## Activation and shutdown

Live trading is disabled by default. Activation explicitly identifies the version, account, and mandate. This is a requirement of the future platform; writing these documents does not configure accounts or enable trades.

Provide at least three distinct actions: **block new entries**, **cancel pending orders**, and **request position closure**. A closure request does not guarantee an immediate fill: monitor residual exposure and notify the user of failures.

Stale data, reconciliation errors, limit violations, and provider anomalies block new openings according to policies defined in advance. Separately establish how existing positions are managed. A new version cannot replace the active version without checking pending orders and the transition plan.

## Observability

The operational view shows capital, exposures, positions, orders, realized/unrealized P&L, strategy contributions, model and service health, utilized limits, and block reasons. Every order is linked to signals, the portfolio proposal, checks, and fills. Alerts include a clear operational action and a verifiable record.

## Terminal control and holding horizon

Operate through the permanent TUI and shared application operations. Chief-agent chat may request portfolio actions, but research job controls do not bypass live authorization or silently change active strategy definitions. Display target job/instance, scope, and resulting state for each intervention.

Short-term trading includes overnight and multi-day positions. A week-long position is allowed if its strategy and mandate allow it; no universal day-end closure applies. An instrument's session ending is distinct from a strategy exit. Model calendars, financing, funding, margin, and provider restrictions as applicable.

The PostgreSQL registry holds shared strategy definitions; personal instances pin strategy identity and implementation, selected instruments, account reference, capital, and limits. Changing personal capital/account does not create a strategy. Changing behavioral sizing or exit rules does. Credentials, account details, chats, personal limits, and raw live records are not automatically published with the strategy.
