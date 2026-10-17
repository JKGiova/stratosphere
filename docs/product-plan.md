# Product vision and plan

## Objective

stratosphere enables users to create, develop, modify, and improve quantitative strategies through agents, test them on historical or real-time data, and activate them with live trades. Portfolio management coordinates the capital and positions of selected strategies.

Its role within the tradingsphere suite is to turn trading ideas into strategies that can be verified and managed over time. Interfaces with the other suite components will be defined separately; stratosphere must not depend on integrations that do not yet exist.

## Confirmed scope

The permanent product interface is a terminal TUI, centered on a chat with a chief agent and complemented by slash commands. Support crypto, equities/ETFs, and forex through explicit instrument, data, and provider capabilities.

The focus is short-term indicator-driven trading, not multi-year holding. Positions can last minutes, hours, days, or around a week; the strategy determines exits. Day trading is supported but does not define the entire product, and end-of-day liquidation is not a universal rule.

Supabase Auth provides OAuth application login. Alpaca and Liquid accounts are optional operational connections, not required identities. Users can research, use the catalog, and backtest without connecting a trading account. Account linking alone does not grant permission to place live orders.

Registered strategies are automatically shared in a community catalog. An exactly matching submission adds provenance to the existing strategy; even a small behavioral variation receives a new strategy ID. Recommendations surface relevant strategies with inspectable evidence, not just the highest advertised returns.

## Primary use cases

| Use case | Expected result |
| --- | --- |
| Import a Reel, video, article, or post | Extracted rules, source references, and visible ambiguities |
| Import a TradingView or GitHub strategy | Analysis of accessible material, dependencies, and semantics to verify |
| Describe an original strategy | Complete specification and initial implementation |
| Intervene through chief-agent chat | Redirect, clarify, pause, or choose work without losing workflow state |
| Invoke a slash command | Explicitly launch the same operation available to the chief agent |
| Discover community strategies | Recommendations with rules, provenance, and labeled evidence |
| Submit an existing strategy from another source | Reuse its identity and append the new source association |
| Submit a small rule variation | Register a distinct strategy linked to its predecessor |
| Modify a strategy | New version and comparison with its predecessor |
| Request improvements | Justified variants, a research budget, and separate results for each experiment |
| Test on historical data | Reproducible report with trades, costs, and test limitations |
| Test with live data | Observed signals or paper trading without submitting live orders |
| Sign in without a trading account | Supabase Auth session and access to application-owned records |
| Link an optional Alpaca/Liquid account | Separate account access with explicit permissions and environment |
| Activate multiple strategies | Budget management and risk valuation proposals, deterministic checks, and execution within an explicit mandate |
| Manage a production strategy | Monitoring, pausing, deactivation, and controlled updates |

## Planned experience

Inside the TUI, the user works through the chief-agent chat, supplies ideas or sources, and sees what has been reconstructed and what is missing. Chat requests and slash commands use a common operation layer. They can correct the rules before implementation, launch experiments, and compare versions under the same testing protocol.

Each strategy has a profile containing its origin, rules, versions, results, operational state, and decision rationale. The portfolio view shows capital, allocations, aggregate positions, strategy contributions, limits, and orders. Agent explanations are linked to available evidence.

Before live activation, the user selects the version, account, capital, instruments, and operational limits. The platform clearly distinguishes historical results, simulated live results, and actual live results.

## Cross-cutting requirements

- Traceability from source to rule, rule to signal, and signal to order.
- Reproducible tests through strategy, dataset, and configuration versions.
- Separation between research and the operational runtime: an experiment cannot modify an active strategy.
- Configurable budgets for model calls, job duration, and the number of variants.
- Explicit handling of incomplete data, inaccessible sources, and failed operations.
- Credentials kept outside research documents and model inputs.

## Initial boundaries

The vision includes all listed sources, the shared catalog, and live trading, with all five external source integrations required in Phase 1 alongside the TUI and registry; research and backtesting follow as internal increments. Crypto, equities/ETFs, and forex are in scope; integrations will be verified progressively. Alpaca and Liquid are selected as optional operational connectors; provider account eligibility, instrument coverage, adapters, application-service hosting, and exact operating frequencies remain open. Supabase is selected for managed PostgreSQL and OAuth authentication; other Supabase products remain undecided. High-frequency trading, managing third-party accounts, and selling strategies through a paid marketplace are outside the initial plan. The community catalog is in scope and is not a paid marketplace.

Initial success is measured by a complete, verifiable workflow, the quality of reconstructed rules, and experiment reproducibility. High returns in a single test are not a sufficient criterion.

## Release and commercial refinement

Release only after extensive testing, with no target date. The repository remains private; open sourcing is a possible fallback requiring a later explicit decision. The application is online-only and English-only, starting on Linux/macOS with all-OS support as the eventual goal. Onboarding covers model setup, hosted browser login and optional trading connections. Chat/slash commands coexist with strategy, test and actual-position panels. User-selectable providers are required; OpenRouter Nemotron 3 Ultra (free) is the current testing selection; NVIDIA NIM remains an explicit alternative. External strategy code is converted into conforming NautilusTrader implementations. No application-imposed upload limits are requested.

Monetization targets a 0.5% opening-trade fee capped at $1. This is application service monetization, separate from a paid strategy marketplace. Fee basis, event grouping, collection and operational acceptance remain open in the [commercial model](commercial-model.md).
