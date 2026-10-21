# Roadmap and open implementation decisions

Last updated: October 21, 2026. Phases describe dependencies and verifiable outcomes, not delivery dates. [Phase 0 compatibility/contract proofs](phase0.md) are **complete within their stated scope**. [Phase 1](phase1.md) is **in progress**, beginning with terminal/model chat; [Phase 2](phase2.md) is **in progress** with its first reviewed text-to-native-backtest workflow; The user has authorized **Phases 3–5**, now in progress under the [full acceptance ledger](phase3-5.md). Their agent/database foundation and bounded local SMA study increment are implemented; local dated applicability/candidate operations are implemented; authenticated community recommendations, broader iteration, source/data maturation and paper runtime remain pending. Commercial market-data rights are unresolved; see [data-source rights](data-source-rights.md). Phases 6–7 remain not implemented. An internal proof is not a release.

## Selected foundation

Python, LangGraph, Supabase-managed PostgreSQL, Supabase Auth OAuth login, NautilusTrader, and the permanent terminal TUI are confirmed. Alpaca and Liquid are optional operational connectors. Crypto, equities/ETFs, and forex are product scope, with short-term strategies that may hold positions overnight or for days/a week. Registered strategy definitions are automatically shared.

See the [decision log](decisions.md) for confirmed requirements and remaining proposals. Do not treat interface, framework, database, authentication platform, optional connector choices, or automatic sharing as undecided.

## Proposed phases

| Phase | Scope | Completion criterion |
| --- | --- | --- |
| 0 — Compatibility and contracts | Select runtime/engine versions, first test adapters/data, TUI library, canonical spec, and access boundary | Verified compatibility matrix and example strategy/data protocol |
| 1 — Online TUI, onboarding, registry and sources | English Linux/macOS TUI, chief-agent chat, slash commands and panels; selectable model setup; registry contracts and internal development harness; YouTube, Instagram, X, TradingView and GitHub acquisition | Model setup, private ownership contracts, exact deduplication, source accumulation, distinct variants and catalog projection in controlled development tests and all five source integrations tested against accessible examples; unavailable material explicitly reported |
| [2 — Research MVP](phase2.md), in progress | Text/article → specification → conforming implementation → NautilusTrader backtest → report | Reproducible end-to-end case with costs, rule evidence, immutable references, and user intervention |
| 3 — Iteration and recommendations | Bounded variants, out-of-sample validation, evidence-aware catalog recommendations, dated applicability | All attempts retained; recommendations distinguish compatibility, empirical evidence, and current context |
| 4 — Source maturity and market coverage | Improve the Phase 1 source integrations and conversion conformance; progressively verify crypto/equity/ETF/forex data integrations | Source provenance, format/provider failure cases, behavioral conversion and capability-based routing tested |
| 5 — Live data and paper | Feed/runtime, budget and risk agents, optional account linking, verified paper adapter, personal instances | Sustained operation with restart/reconciliation tests and overnight/multi-day holding checks |
| 6 — Live execution and commercial billing | Stable verified Alpaca/Liquid adapters, mandate, deterministic checks, single order gateway and commission accounting/collection | Explicit activation plus idempotency, partial-fill, outage, risk and shutdown tests; approved opening-trade fee rules, cap, collection and ledger reconciliation tested before commercial release |
| 7 — Operational evolution | Broader adapters, attribution, drift monitoring, controlled implementation promotion | Consistent aggregate portfolio and no silent changes to pinned user deployments |

An initial integration verifies the workflow; it does not restrict product scope to that market. Do not claim all markets are live-ready until their data, instruments, and broker adapters are verified.

## Internal development increment

The research proof uses one textual source, a complete specification, a verified historical dataset integration, isolated code execution and a reproducible report. This narrow proof is not a release and does not defer the confirmed Phase 1 scope: all five external source integrations, Linux/macOS English TUI panels, onboarding and shared registry must also be delivered and tested.

The example strategy should have explicit entry/exit rules and limited parameters. Test specification/code alignment, signal timing, costs, missing data, repeatability, exact deduplication, small variants, source accumulation, and job intervention. Profitability is not a completion requirement.

Recommended dependency order: register identities and provenance before community ranking; validate paper operations before real orders. Detailed scheduling is a proposal and may be refined.

## Proposed code areas

| Area | Responsibility |
| --- | --- |
| tui / commands | Permanent terminal interface, chat, forms, and slash command routing |
| operations | Shared validated actions used by agents and manual commands |
| agents / workflows | LangGraph chief agent, specialist roles, checkpoints, and budgets |
| sources | Acquisition, snapshots, and evidence maps |
| registry | Supabase PostgreSQL identities, canonicalization, provenance, and submissions |
| catalog | Shared discovery, recommendations, and publication projections |
| strategies | Formal specs, conforming implementations, and signal logic |
| data / research | Data quality, snapshots, workers, NautilusTrader tests, and reports |
| portfolio / risk | Allocation, personal instances, deterministic controls, and attribution |
| auth / connections | Supabase OAuth session/ownership and separate optional Alpaca/Liquid consent |
| execution | Selected connector/engine adapters, order handling, reconciliation, and lifecycle |
| tests / examples | Conformance, integration, restart, deduplication, and reproducible cases |

## Open details

| Detail | Required before |
| --- | --- |
| Tradable-data and operational engine adapters beyond the verified Phase 0 proof | Corresponding research/paper/live integration |
| Additional instruments/data profiles and Alpaca/Liquid account capabilities/adapters beyond the selected BTC/USDT hourly Binance research profile | Corresponding data/paper/live phase |
| LLM providers/structured output and user-approved job budgets (meaning explained; values unconfirmed); Textual is selected | TUI/research implementation |
| Physical schema/migrations, canonical extensions and transactional constraints; IDs/canonical v1 selected in Phase 0 | Registry implementation |
| Supabase provisioning and project plan (organization/Frankfurt region selected), OAuth provider/callback/session configuration, API access, RLS, hosting, secure session storage and online outage handling | Shared service release |
| Generated-code isolation and resource/network restrictions | Executing user/model code |
| Ranking weights, verified evidence, moderation and source-use policy | Community recommendation release |
| Private/public evidence projection and deletion/retention | Catalog release |
| Validation thresholds, portfolio allocation, netting and risk mandate | Experiments/paper/live as applicable |
| Activation confirmations, permissions and emergency behavior | Live execution |
| Fee basis, USD/currency policy, opening event grouping, partial fills, retries, collection and disputes | Billing implementation/commercial release |
| Measurable extensive-testing gates, scale and operating budget | Public/commercial release |
| APIs to other tradingsphere components | First suite integration |

## Deferred login and collection

Login implementation/configuration and fee-collection selection are deferred to later pre-release work. Google/GitHub, browser-hosted login and the opening-order fee rules remain confirmed. They do not block the current compatibility, TUI, research, source-acquisition or backtesting proof. The planned Phase 1 registry uses a controlled development harness, not an unauthenticated public deployment; real shared-user access still requires verified authentication and ownership controls before release. Final onboarding continues to include login and optional trading connections. Do not reopen payment-collection or login questions during the current planning pass.

## Next step

The [Phase 0 audit](phase0.md) is complete. Runtime/version matrix, Textual, canonical v1 contracts, ECB historical integration/native-engine proof and hosted catalog access boundary have been selected and tested within their stated scope. The user authorized Phase 1 and subsequently [Phase 2](phase2.md). Phase 1 remains unfinished: its [increment record](phase1.md) distinguishes implemented client behavior from pending shared registry, source and broader agent-operation work. Phase 2 begins with a reviewed BTC/USDT hourly SMA research path using a pinned Binance archive; this does not defer or claim completion of the five Phase 1 content adapters.

Quiver remains a possible optional alternative-data integration. VectorBT remains a possible optional research accelerator. Neither is required for the first usable increment.

## Roadmap refinement

The [planning questionnaire](planning-questions.md) covers remaining product, runtime, agent, authentication, source, data, catalog, execution, operational and development decisions. Record confirmed answers in decisions.md, retain deferred choices with the phase they block and expand each phase into tasks/dependencies/acceptance criteria. Runtime and provider capabilities require verification; do not treat unanswered product questions as approved defaults. Phase 0 engineering selections/evidence are tracked separately; Phase 1 has started; Phase 2 has begun within the verified scope in its increment record; Phases 3–5 are authorized and in progress; their full completion requires the acceptance ledger. Phases 6–7 remain unimplemented. AGENTS.md rules remain in force.

## Release policy and confirmed experience

There is no target release date. Release follows extensive testing rather than the completion of a narrow MVP. Define release gates after the remaining questionnaire answers: user acceptance, security/ownership, source acquisition/conversion, reproducibility, paper stability, operational recovery and commercial fee reconciliation. Duration, sample sizes, scale and thresholds are not yet approved. Internal testing may proceed incrementally; a commercial trading release depends on validated execution and billing.

The product is online-only, English-only and private-source. Start client support with Linux and macOS; retain eventual support for all OSes. Use an agent chat and explicit slash commands with strategy, ongoing/completed test and actual-position panels. Onboarding covers model selection/configuration, browser-based hosted login and optional trading connections. Use OpenRouter `cohere/north-mini-code:free` during development/testing, selected through the user-authorized live free-model comparison. NVIDIA NIM remains an explicit alternative; model selection must not silently fall back to a paid variant.

The service fee target is 0.5% per opening trade with a $1 cap. The basis is opening-order trade value, with one cap per opening order and reinforcement exempt; execution edge cases remain to be specified. Collection is deferred and no billing is implemented. See [commercial requirements](commercial-model.md) and [Supabase setup](supabase-setup.md) for outstanding dependencies.

## Local-runtime and commercial refinements

Research/trading compute runs on users' computers; shared auth/catalog/billing remains online. Users bring model API keys. Google and GitHub login are selected, but hosted-login deployment/domain is not. Alpaca is first for Bitcoin-spot paper integration; Liquid follows later for perpetuals, as confirmed on October 21. Research follows one plan approval with ambiguity questions; live operation follows its own configuration/permission and provider restrictions. TUI exit has the confirmed choice to keep positions open or close them all. This user-facing decision is settled; internal lifecycle implementation must honor it without asking the user to redefine the choice. Opening-order fees exclude reinforcement of existing positions.

See [research and release proposals](research-and-release-proposals.md) for first data options, job-budget explanations and proposed testing gates. Payment alternatives in that document are deferred. These proposals are not confirmed defaults.
