# Portfolio, positions, and live execution

## Management mandate

The portfolio agent coordinates active strategies: it proposes capital allocation, decides which signals to accept, and proposes position openings, increases, reductions, or closures according to overall state. User-configured limits are enforced by deterministic checks outside the model.

The mandate includes the account, permitted instruments, capital, active strategies/versions, per-position and per-strategy limits, overall exposure, leverage where supported, concentration, maximum loss and drawdown, rebalancing frequency, and order approval mode. Thresholds and allocation methodology remain open decisions.

## From agents to positions

1. Strategies produce structured signals with a version, timestamp, and expiry.
2. The system collects capital, positions, open orders, and feed health.
3. The portfolio coordinator identifies candidate exposure changes; the budget management agent proposes capital allocation and sizing.
4. The risk valuation agent assesses portfolio impact, concentration, liquidity, and the assumptions behind each intent.
5. Deterministic checks validate the resulting intent, state freshness, available capital, permissions, and mandate limits. An agent's approval cannot override these checks.
6. The future execution gateway and operational runtime submit approved intents through Liquid, after any provider-required confirmation and verified adapter/isolation. Current diagnostics cannot submit orders.
7. Fills and reconciliation update the ledger; the portfolio is reevaluated against actual state.

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

The private local SQLite library holds strategy definitions; personal instances pin strategy identity and implementation, selected instruments, account reference, capital, and limits. Changing personal capital/account does not create a strategy. Changing behavioral sizing or exit rules does. Credentials, account details, chats, personal limits, and raw live records are not automatically published with the strategy.

## Optional provider authorization

The personal application has no hosted login or users database. Liquid is the only trading connector and its connection is optional for research. Actual feed/execution capabilities require separate verification. No broker account is required for research or backtesting. Broker scopes/tokens are distinct from a stratosphere execution mandate.

Budget management and risk valuation run before execution. Keep a single authoritative order/reconciliation path per account; neither the chief agent nor a provider-side assistant may independently bypass the gateway. Risk controls must account for reserved capital and pending orders as well as filled positions. Define concurrency, netting, and failure policies before live use.

Liquid perpetual exposure to an equity/FX reference is not ownership of the underlying security or currency and is not interchangeable with an equity/ETF broker instrument. Verify funding, margin, liquidation, sessions, and historical price semantics for the actual instrument. Provider capability and confirmation requirements are hard constraints, not prompts agents can override.

Exact Liquid execution integration, native versus custom NautilusTrader adapters, per-request paper isolation and unattended execution compatibility remain to be verified. Selecting Liquid in the plan does not authorize orders. See [authentication and connectors](authentication-and-connectors.md).

## Latest confirmed operating decisions

Liquid is the only trading connector under D58. Perpetuals need their own strategy/instrument, funding/margin and fee evidence; existing spot studies cannot transfer to them. Execution uses configuration or explicit permission, subject to provider confirmations. The runtime operates on the user's computer. On TUI exit, users choose retaining positions or requesting closure of all positions; this user-facing choice is settled and must not be reopened. A computer shutdown stops local runtime logic. Specify and test pending-order handling and actual residual exposure rather than treating exit as guaranteed liquidation.
