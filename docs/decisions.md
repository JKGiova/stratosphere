# Decisions and alignment

Last updated: October 13, 2026. This file records product decisions from the planning discussion. It does not claim implementation or integration readiness.

## Confirmed decisions

| ID | Decision | Consequence |
| --- | --- | --- |
| D01 | Product name is stratosphere; suite name is tradingsphere | Both are lowercase; tradingsphere is one word |
| D02 | Development stays on GitHub and repository material is in English | Code, documentation, comments, and development records use English |
| D03 | Permanent terminal product with a TUI | No planned replacement by a web UI |
| D04 | Chief-agent chat with workflow intervention and slash commands | Users can contribute ideas, clarify rules, choose tests, and control jobs |
| D05 | Short-term indicator-driven trading, not multi-year holding | Overnight and multi-day positions, including about a week, are allowed; daily liquidation is not mandatory |
| D06 | Crypto, equities/ETFs, and forex are in scope | Design market-neutral contracts and validate providers progressively |
| D07 | Python backend and LangGraph orchestration | Framework alternatives are no longer unresolved choices |
| D08 | NautilusTrader for backtests and execution | Exact version, Python compatibility, and adapters still need verification |
| D09 | Supabase-managed PostgreSQL strategy registry and related records | Store precise specifications, provenance, experiments, and personal configuration separately |
| D10 | Every strategy has a long random numeric public ID, explanatory name, description, and formal rules | Include mathematical definitions where possible |
| D11 | Deduplicate only identical strategies | New sources attach to an existing identity; even small behavioral variations get new IDs |
| D12 | Sources are separate from description and accumulate | Preserve source snapshots and rule-level evidence through many-to-many links |
| D13 | Community catalog recommends relevant, successful user-built strategies | Keep evidence type, robustness, risk, and compatibility visible; ranking formula remains open |
| D14 | Registered strategies are shared automatically | No private-by-default/voluntary-publication workflow for registered strategy definitions |
| D15 | Separate mandatory conditions, empirical evidence, user constraints, and current context | Store rules/evidence and reevaluate applicability using dated snapshots |
| D16 | Support strategy creation, modification, improvement, historical/live testing, and live portfolio management | Stage implementation; agents propose within deterministic risk controls |
| D17 | Supabase Auth with OAuth-based application login | Delegate authentication; do not build a proprietary password system or require a broker account to log in |
| D18 | Alpaca and Liquid are optional operational connectors | Research/catalog/backtests work without them; linking and live activation are separate |
| D19 | Execution follows budget management and risk valuation agents | Enforce deterministic controls before the execution gateway and selected connector |

D07, D08, and D14 were explicitly confirmed on October 13. D09 was subsequently refined the same day to select Supabase as the managed PostgreSQL platform. D17–D19 were then confirmed to separate application authentication from optional operational connectors. The other entries summarize the product requirements and registry model developed in the preceding discussion.

## Implementation proposals, not confirmed vendor choices

- Pydantic for structured contracts; Parquet for versioned market data; DuckDB for analysis.
- Separate TUI, research workers, and trading runtime within a modular repository.
- A shared catalog service/API with client authentication; application-service hosting and transport remain open. Supabase database hosting is selected.
- Database fields and constraints in the [database model](database-model.md); these are a logical design, not applied migrations.
- Isolated generated-code execution, durable job records, and structured intervention audit.
- Semantic candidate search followed by conservative exact-spec comparison.
- A later optional VectorBT research accelerator and optional Quiver alternative-data connector.
- The slash command vocabulary in the [TUI plan](terminal-interface.md).
- Google/GitHub as initial OAuth provider candidates; neither is individually confirmed yet.
- Connection fields and account/environment authorization boundaries in the [authentication plan](authentication-and-connectors.md).

Automatic strategy sharing does not grant permission to publish secrets, account state, private conversations, personal configuration, or third-party material without appropriate rights. Define a publishable strategy projection and ingestion notices before launch.

## Still open

- Python runtime, stable NautilusTrader version, platform compatibility, and verified adapters.
- First smoke-test instruments, historical/live data providers, Alpaca/Liquid connector order, supported accounts/instruments, and exact engine adapters.
- TUI library, models/providers, queue/scheduler, application-service hosting, and local persistence.
- Supabase OAuth provider selection, terminal redirect/PKCE flow, token storage, linking, and revocation details.
- Supabase region/plan, project configuration, client access path, RLS policies, migrations, and any adoption of Storage, Realtime, or Edge Functions. Auth is selected.
- Public numeric ID length, internal primary-key format, canonical schema, and implementation packaging.
- Recommendation weights, evidence verification, moderation, content-use policy, and user deletion handling.
- Risk thresholds, portfolio allocation, netting/attribution, and operational approval policy.
- Publication scope of performance evidence; raw private account/trade data is excluded by default.
- Offline behavior and how an interrupted catalog connection affects registration.
- APIs to other tradingsphere components.

Do not reopen confirmed choices as if undecided. When a new proposal changes a confirmed requirement, obtain user confirmation and update this log.
