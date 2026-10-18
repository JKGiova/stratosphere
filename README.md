# stratosphere

**Quantitative strategies created, tested, and managed through agents.**

stratosphere is the component of the **tradingsphere** suite dedicated to the complete lifecycle of a quantitative trading strategy: starting from an idea or external source, turning it into executable rules, evaluating it on historical and real-time data, improving it, and activating it on a live account with coordinated portfolio management.

Planned sources include YouTube videos, Instagram Reels, articles, X posts and threads, TradingView strategies, and GitHub repositories. Agents reconstruct the rules, highlight missing information, and propose implementations and variants. A portfolio management agent evaluates the signals from active strategies together and proposes allocations and position openings, reductions, and closures within limits enforced by the risk system.

## Project status

**Phase 0 complete.** The repository contains the plan plus verified compatibility/contract proofs: pinned runtime dependencies, a Textual framework probe, canonical examples, a read-only ECB historical integration and a bounded native NautilusTrader simulation. See [Phase 0 gates and evidence](docs/phase0.md). **Phase 1 is in progress:** the first terminal increment adds model onboarding, streaming model chat through LangGraph ([12 model providers](docs/model-providers.md), with OpenRouter Cohere North Mini Code free by default), deterministic slash commands and capability panels. See [Phase 1 progress and run instructions](docs/phase1.md). Live streaming chat and multi-turn context are verified with the selected Cohere free model; see the bounded test evidence in Phase 1. The first [Phase 2 research workflow](docs/phase2.md) now turns pasted rules into a reviewed SMA specification, approved native backtests and reproducible reports. The first [bounded-study increment](docs/phase3-studies.md) adds approved SMA comparisons, validation-only selection, retained attempts and a separate final holdout protocol, verified with authored data. [Commercial dataset rights](docs/data-source-rights.md) remain unresolved. The [dated applicability increment](docs/phase3-applicability.md) adds private assessment snapshots and local research candidates with explicit compatibility, evidence and missing-context status. A [strategy-agent foundation](docs/strategy-agents.md) now provides stable per-strategy agents, local registry records and a Reports panel; the [Supabase private schema](docs/supabase-setup.md) is provisioned and ownership-tested. The [source intake increment](docs/phase4-sources.md) adds read-only snapshots, a commit-pinned GitHub file reader, partial YouTube/X/TradingView material and explicit Instagram access limitations. [Strategy-owned source reviews](docs/source-reviews.md) add quote-grounded private reports without enabling conversion or tests. Complete media/Pine acquisition, source-bound conversion, authenticated shared registry service, general generated-code execution, login, paper/live execution and billing remain unimplemented. The initial proof does not verify tradable market data or broker capabilities.

The terminal follows [Gloomberb](https://github.com/gloom-sh/gloomberb)'s monochrome style: black/charcoal surfaces, white focus accents, square dividers and compact title bars. The responsive workspace shows six, four, two side-by-side, two stacked or one panel according to terminal size. **Tab / Shift+Tab** cycle through Library, Focus, Chat, Tests, Positions and Reports; F1–F4 are direct shortcuts. Model setup lives inside agent chat, and assistant replies render Markdown, including code and tables. See [layout thresholds and keyboard controls](docs/terminal-interface.md#workspace-layout-and-navigation).

The app uses the terminal’s existing window size; automatic maximization and the clickable brand menu are removed.

Use `/new` in chat to review the first research profile: BTC/USDT spot, hourly SMA crossover, a pinned January 2025 dataset, editable simulated costs and one approval for base/stress backtests. `/jobs`, `/show` and `/report` expose actual local evidence; `/pause` and `/resume` retain completed checkpoints. This is an internal increment, not a completed phase or release. See [Phase 2 run instructions and limits](docs/phase2.md).

## Confirmed direction

- **Permanent terminal interface:** a TUI with a chief-agent chat and slash commands; no planned migration to a web UI.
- **Short-term, multi-market strategies:** crypto, equities/ETFs, and forex. Positions may remain open for days or a week; there is no mandatory daily liquidation.
- **Python + LangGraph:** backend and agent orchestration.
- **NautilusTrader:** selected backtesting and execution engine, subject to version and adapter verification.
- **Supabase (PostgreSQL + Auth):** managed database and OAuth-based application login, independent of trading accounts.
- **Optional Alpaca and Liquid connectors:** link accounts only when needed; connecting an account does not activate live trading.
- **Controlled execution:** budget management → risk valuation → deterministic checks → execution gateway and selected connector.
- **Automatic community sharing:** registered strategies enter the shared catalog, with exact deduplication and distinct IDs for even small rule changes.

The [decision log](docs/decisions.md) separates confirmed choices from implementation proposals and open questions.

## Planned capabilities

- Collaborate with a chief agent in the TUI, intervene in research workflows, and invoke the same operations through slash commands.
- Discover evidence-backed community strategy recommendations and use existing strategies as starting points.
- Import sources and preserve references to the evidence behind extracted rules.
- Create, modify, and improve strategies through specialized agents.
- Version strategy specifications and implementations, with explicit assumptions and parameters.
- Run reproducible backtests with costs, data quality checks, and out-of-sample validation.
- Run forward tests and paper trading on real-time data.
- Coordinate capital, exposures, and positions across multiple strategies.
- Activate validated versions for live trading, with monitoring, reconciliation, and operational shutdown controls.

## Strategy workflow

1. Import a source or describe an idea.
2. Extract the rules and resolve ambiguities.
3. Generate a verifiable specification and implementation.
4. Run backtests, validation, and comparisons between versions.
5. Evaluate the selected version with live data and simulated capital.
6. Explicitly activate it for live trading within a defined risk mandate.
7. Monitor results and positions; develop improvements as new versions.

Backtest results and claims made by a source do not guarantee future returns.

## Documentation

| Document | Contents |
| --- | --- |
| [Plan index](docs/README.md) | Reading order and document status |
| [Product vision and plan](docs/product-plan.md) | Objectives, use cases, boundaries, and requirements |
| [Architecture and agents](docs/architecture.md) | Components, responsibilities, and contracts |
| [Sources and strategy lifecycle](docs/strategy-lifecycle.md) | Importing, specifications, versions, and improvements |
| [Data and validation](docs/testing-and-validation.md) | Backtests, live evaluation, and promotion criteria |
| [Portfolio and execution](docs/portfolio-and-execution.md) | Allocation, positions, risk, and orders |
| [Authentication and connectors](docs/authentication-and-connectors.md) | Supabase OAuth login, optional accounts, authorization, and security |
| [Liquid paper testing](docs/liquid-paper-testing.md) | Read-only browser connection, test options and unverified paper execution |
| [Paper ledger foundation](docs/phase5-paper-ledger.md) | Internal authored-paper reservations, fill attribution and native accounting checks |
| [Database model](docs/database-model.md) | Tables, fields, exact strategy identity, evidence, and personal instances |
| [Source intake](docs/phase4-sources.md) | Retained material, provider access limits, provenance and inspection |
| [Source reviews](docs/source-reviews.md) | Strategy-owned interpretations, exact quote references and private reports |
| [Data capabilities](docs/market-data-capabilities.md) | Exact profile/purpose matching, freshness and unresolved provider requirements |
| [Source conformance](docs/source-conformance.md) | Rule mapping, numerical blockers and limits on converted execution |
| [Terminal interface](docs/terminal-interface.md) | Chief-agent chat, slash commands, jobs, and user intervention |
| [Community catalog](docs/community-catalog.md) | Automatic sharing, recommendations, provenance, and reuse |
| [Decision log](docs/decisions.md) | Confirmed decisions, proposals, and unresolved details |
| [Roadmap](docs/roadmap.md) | MVP, subsequent phases, and completion criteria |
| [Phase 0 verification](docs/phase0.md) | Sequential gates, commands, platform limits and measured evidence |
| [Canonical contracts](docs/phase0-contracts.md) | Exact identities, schemas, example strategy/data/experiment protocol |
| [Access boundary](docs/phase0-access-boundary.md) | Hosted API decision and shared/private contract scope |

## Subsequent development objective

Complete a reproducible workflow from **article or user-provided text → specification → executable strategy → backtest → report**, using one initial verified dataset/provider integration inside a multi-market architecture. Include the TUI and shared strategy registration; the first integration does not restrict the product to one asset class. YouTube, Instagram, X, TradingView, and GitHub source integrations are required in Phase 1 alongside the online English Linux/macOS TUI, onboarding, and registry. This narrow research proof is an internal increment, not a public release. Paper trading and live execution follow in the [roadmap](docs/roadmap.md).

## Development conventions

GitHub is the shared source of truth for development. Code, comments, documentation, commit messages, and GitHub development discussions must be written in English. The product and suite names are always written as **stratosphere** and **tradingsphere**, respectively.

## Launch and commercial direction

The repository remains private. Release follows extensive testing, without a target date. The application is online-only and English-only, starting on Linux/macOS with eventual support for all OSes. The commercial fee target is 0.5% per opening trade, capped at $1; calculation/collection details remain open. See [commercial requirements](docs/commercial-model.md) and the [updated decisions](docs/decisions.md).
