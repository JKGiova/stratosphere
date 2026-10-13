# Roadmap and open implementation decisions

Last updated: October 13, 2026. Phases describe dependencies and verifiable outcomes, not delivery dates. All application phases are **not implemented**; only documentation exists.

## Selected foundation

Python, LangGraph, PostgreSQL, NautilusTrader, and the permanent terminal TUI are confirmed. Crypto, equities/ETFs, and forex are product scope, with short-term strategies that may hold positions overnight or for days/a week. Registered strategy definitions are automatically shared.

See the [decision log](decisions.md) for confirmed requirements and remaining proposals. Do not treat interface, framework, database, or automatic sharing as undecided.

## Proposed phases

| Phase | Scope | Completion criterion |
| --- | --- | --- |
| 0 — Compatibility and contracts | Select runtime/engine versions, first test adapters/data, TUI library, canonical spec, and access boundary | Verified compatibility matrix and example strategy/data protocol |
| 1 — TUI and shared registry | Chief-agent chat, common operation layer, initial commands, PostgreSQL registration and source links | Exact duplicate/source accumulation, distinct variants, automatic catalog inclusion, and private-data boundary tested |
| 2 — Research MVP | Text/article → specification → conforming implementation → NautilusTrader backtest → report | Reproducible end-to-end case with costs, rule evidence, immutable references, and user intervention |
| 3 — Iteration and recommendations | Bounded variants, out-of-sample validation, evidence-aware catalog recommendations, dated applicability | All attempts retained; recommendations distinguish compatibility, empirical evidence, and current context |
| 4 — External sources and market coverage | YouTube, Instagram, X, TradingView, GitHub; progressively verified crypto/equity/ETF/forex data integrations | Source provenance, access limits, behavioral conversion checks, and capability-based routing tested |
| 5 — Live data and paper | Feed/runtime, portfolio proposals, paper adapter, personal instances | Sustained operation with restart/reconciliation tests and overnight/multi-day holding checks |
| 6 — Live execution | Stable verified engine/adapters, mandate, risk checks, order gateway | Explicit activation plus idempotency, partial-fill, outage, risk, and shutdown acceptance tests |
| 7 — Operational evolution | Broader adapters, attribution, drift monitoring, controlled implementation promotion | Consistent aggregate portfolio and no silent changes to pinned user deployments |

An initial integration verifies the workflow; it does not restrict product scope to that market. Do not claim all markets are live-ready until their data, instruments, and broker adapters are verified.

## First usable increment

Provide the TUI and chief-agent interaction, a shared registry, one accessible textual source, a complete strategy specification, one verified historical dataset integration, isolated code execution, and a reproducible report.

The example strategy should have explicit entry/exit rules and limited parameters. Test specification/code alignment, signal timing, costs, missing data, repeatability, exact deduplication, small variants, source accumulation, and job intervention. Profitability is not a completion requirement.

Recommended dependency order: register identities and provenance before community ranking; validate paper operations before real orders. Detailed scheduling is a proposal and may be refined.

## Proposed code areas

| Area | Responsibility |
| --- | --- |
| tui / commands | Permanent terminal interface, chat, forms, and slash command routing |
| operations | Shared validated actions used by agents and manual commands |
| agents / workflows | LangGraph chief agent, specialist roles, checkpoints, and budgets |
| sources | Acquisition, snapshots, and evidence maps |
| registry | PostgreSQL identities, canonicalization, provenance, and submissions |
| catalog | Shared discovery, recommendations, and publication projections |
| strategies | Formal specs, conforming implementations, and signal logic |
| data / research | Data quality, snapshots, workers, NautilusTrader tests, and reports |
| portfolio / risk | Allocation, personal instances, deterministic controls, and attribution |
| execution | Engine adapters, order handling, reconciliation, and lifecycle |
| tests / examples | Conformance, integration, restart, deduplication, and reproducible cases |

## Open details

| Detail | Required before |
| --- | --- |
| Python runtime, stable NautilusTrader version, platform and adapters | Engine integration |
| First instruments, historical/live providers, broker/exchange | Corresponding data/paper/live phase |
| TUI library, LLM providers, structured output support, model budgets | TUI/research implementation |
| Public ID length, internal keys, canonical format, physical schema/migrations | Registry implementation |
| Catalog hosting/API, authentication, local persistence, outages | Shared service release |
| Generated-code isolation and resource/network restrictions | Executing user/model code |
| Ranking weights, verified evidence, moderation and source-use policy | Community recommendation release |
| Private/public evidence projection and deletion/retention | Catalog release |
| Validation thresholds, portfolio allocation, netting and risk mandate | Experiments/paper/live as applicable |
| Activation confirmations, permissions and emergency behavior | Live execution |
| APIs to other tradingsphere components | First suite integration |

## Next step

Run a compatibility proof for selected Python/NautilusTrader versions and one data integration; finalize the canonical specification and registry constraints; choose the TUI library and shared catalog access boundary. These are remaining implementation decisions, not a request to reconsider the confirmed stack.

Quiver remains a possible optional alternative-data integration. VectorBT remains a possible optional research accelerator. Neither is required for the first usable increment.
