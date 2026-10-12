# Roadmap and open decisions

## Proposed phases

Phases describe dependencies and verifiable outcomes, not delivery dates. All phases are currently **not implemented**; only the initial documentation has been created.

| Phase | Scope | Completion criterion |
| --- | --- | --- |
| 0 — Foundations | Select the first market, data, stack, and contracts | Justified decisions, an example specification, and a defined testing protocol |
| 1 — Research MVP | Text/article → extraction → specification → code → backtest → report | One reproducible end-to-end case with rule evidence, costs, and logs |
| 2 — Iterative research | Versions, modifications, variants, and out-of-sample validation | Reproducible comparisons and tracking of all attempts within a budget |
| 3 — External sources | YouTube, Instagram, X, TradingView, and GitHub | Individually verified connectors, preserved provenance, and explicit access limitations |
| 4 — Live data and paper | Feeds, signals, simulated portfolio, and first paper adapter | Sustained operation with reconciliation, restarts, and anomaly handling |
| 5 — Live trading | First live provider, mandate, checks, and position management | Verifiable activation and completed idempotency, risk, and shutdown checks |
| 6 — Evolution | Multiple strategies/providers, drift monitoring, and continuous improvement | Consistent aggregate portfolio and controlled promotion of new versions |

## MVP: concrete scope

Support one textual source or accessible article, one market, one historical data format, and one backtesting engine. Allow specification review before generation, isolated execution, and a report with trades and complete configuration.

The first example must be simple and chosen to verify rules: explicit entry and exit conditions, limited parameters, and checkable expected cases. Verification covers specification/code alignment, signal timing, cost calculation, handling of data gaps, and repeatability. The criterion does not require the example to be profitable.

## Future code structure

This proposal should be adapted after choosing the stack. Only README files, documentation, and repository development instructions currently exist.

| Proposed logical area | Responsibility |
| --- | --- |
| app | User interface, onboarding, and configuration |
| agents | Roles, prompts, orchestration, and tools |
| sources | Connectors and source normalization |
| strategies | Specifications, versions, and signal runtime |
| data | Historical data, live feeds, and quality |
| research | Runner, experiments, backtests, and reports |
| portfolio | Allocation, ledger, and position attribution |
| risk | Mandate and deterministic checks |
| execution | Paper/live adapters, orders, and reconciliation |
| tests / examples | Meaningful checks and reproducible cases |

## Open decisions

| Decision | Why it matters | When to resolve it |
| --- | --- | --- |
| First market and instruments | Determine data, costs, sessions, and order semantics | Phase 0 |
| Initial interface: CLI, web, or desktop | Determines onboarding and the review workflow | Phase 0 |
| Language and runtime version | Compatibility with engines, providers, and quant libraries | Phase 0 |
| Agent framework and models | Tool calling, structured outputs, checkpoints, and costs | Phase 0 |
| Historical provider and backtesting engine | Coverage, quality, licensing, and reproducibility | Phase 0 |
| Storage, jobs, and hosting | Experiment persistence and isolation | Phases 0–1 |
| Validation thresholds and research budget | Avoid criteria chosen after seeing results | Before experiments |
| Video and social content retrieval | Accessibility, credentials, transcription, and vision | Before each connector |
| Live feed and first broker/exchange | Paper trading, orders, limits, and reconciliation | Before phase 4 |
| Allocation method and strategy conflicts | Translate signals into consistent exposures | Before phase 4 |
| Mandate and operational approval | Define autonomy, limits, and shutdown actions | Before phase 5 |
| API to tradingsphere | Shared identities and exchange of results/signals | When the first integration is needed |

## Next step

Define the first market, interface, and stack; then formalize an example strategy and the data contract. These choices make phase 1 implementable without prematurely enabling live execution or building all integrations at once.
