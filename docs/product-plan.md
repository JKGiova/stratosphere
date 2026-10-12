# Product vision and plan

## Objective

stratosphere enables users to create, develop, modify, and improve quantitative strategies through agents, test them on historical or real-time data, and activate them with live trades. Portfolio management coordinates the capital and positions of selected strategies.

Its role within the tradingsphere suite is to turn trading ideas into strategies that can be verified and managed over time. Interfaces with the other suite components will be defined separately; stratosphere must not depend on integrations that do not yet exist.

## Primary use cases

| Use case | Expected result |
| --- | --- |
| Import a Reel, video, article, or post | Extracted rules, source references, and visible ambiguities |
| Import a TradingView or GitHub strategy | Analysis of accessible material, dependencies, and semantics to verify |
| Describe an original strategy | Complete specification and initial implementation |
| Modify a strategy | New version and comparison with its predecessor |
| Request improvements | Justified variants, a research budget, and separate results for each experiment |
| Test on historical data | Reproducible report with trades, costs, and test limitations |
| Test with live data | Observed signals or paper trading without submitting live orders |
| Activate multiple strategies | Coordinated allocations and positions within an explicit mandate |
| Manage a production strategy | Monitoring, pausing, deactivation, and controlled updates |

## Planned experience

The user creates a strategy project, adds sources or a description, and sees what has been reconstructed and what is missing. They can correct the rules before implementation, launch experiments, and compare versions under the same testing protocol.

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

The vision includes all listed sources and live trading, but the first MVP focuses on research and backtesting. Asset classes, brokers, operating frequencies, interface, and hosting have not yet been selected. High-frequency trading, managing third-party accounts, and a strategy marketplace are outside the initial plan.

Initial success is measured by a complete, verifiable workflow, the quality of reconstructed rules, and experiment reproducibility. High returns in a single test are not a sufficient criterion.
