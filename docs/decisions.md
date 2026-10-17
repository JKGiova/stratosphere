# Decisions and alignment

Last updated: October 17, 2026. Product decisions and Phase 0 implementation selections are separate below. Measured scope/limits are recorded in phase0.md; selections alone do not establish product or live readiness.

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
| D20 | Release only after extensive testing; no target delivery date | Internal development increments are not public releases; define measurable release gates |
| D21 | Commercial product with 0.5% commission on each opening trade, capped at $1 | Opening-order trade value and per-order cap confirmed in D34; collection, rounding and execution-event edge cases remain open |
| D22 | Keep the repository private | Open sourcing is only a potential fallback if monetization fails, requiring a later explicit decision |
| D23 | Eventually support all operating systems; Linux and macOS first | Windows follows; versions, packaging and SSH behavior require verification |
| D24 | Online-only product; no offline functionality | No offline research, backtests, viewing or queued registration mode |
| D25 | English-only application | Localized application UI is outside current scope |
| D26 | Agent chat and forced slash commands plus inspectable panels | Panels expose strategies, ongoing/completed tests and actual trading positions |
| D27 | First-run onboarding covers model setup, application login and trading connections | Existing optional-connector requirement remains; onboarding introduces connecting or skipping trading accounts |
| D28 | Browser-based hosted login | Google/GitHub selected in D38; hosting domain and secure terminal callback remain open |
| D29 | User-selectable models from multiple providers; initial testing choice was NVIDIA NIM DeepSeek V4.1 Flash | The current development selection is recorded in D44; provider availability and tool conformance require separate verification |
| D30 | YouTube, Instagram, X, TradingView and GitHub integrations all belong in Phase 1 | Include accessible material acquisition and explicit unavailable-source handling; source-specific credentials and acceptance examples remain open |
| D31 | No application-imposed upload size/type/count limits requested | Do not silently add product quotas; actual provider, infrastructure and format capabilities must be verified and reported |
| D32 | Develop runnable strategies for NautilusTrader; convert externally authored strategies | Conversion must preserve behavior or surface unresolved differences; engine selection does not replace application-owned strategy generation |
| D33 | Create a dedicated stratosphere Supabase project in Matti Barbershop, Frankfurt (eu-central-1) | User selected organization and region; cost-check capability currently blocks provisioning; unrelated mattibarbershop project must not be modified |
| D34 | Fee is based on opening-order trade value, not profit; existing-position increases are exempt | Cap applies per opening order; partial fills, reversals, cross-strategy position attribution and rounding need explicit handling |
| D35 | Research and trading compute runs on users' computers | Shared online services still provide auth/catalog/billing; local runtime cannot operate while its host is off |
| D36 | On TUI exit, offer keeping positions or requesting closure of all positions | The user-facing keep/close choice is settled; implementation must preserve it and track actual closure outcomes |
| D37 | Research proceeds after one plan approval and asks questions for ambiguities | Per-job spend/runtime/search budgets remain undecided; research approval does not authorize live trading |
| D38 | Google and GitHub OAuth login; users provide their own model API keys | User has no hosted-login domain yet; provider app registration and secure key storage remain open |
| D39 | Liquid is the first operational connector | Alpaca remains optional later; custom-client permissions, engine adapter and execution capability must be verified |
| D40 | Live approval behavior depends on configuration or explicit permission | Per-order review and mandate-based modes are intended; provider-required confirmations remain binding |
| D41 | Defer login and fee-collection planning/implementation to later roadmap work | Preserve confirmed auth/pricing direction; focus now on core TUI, sources, strategies, research and backtesting |
| D42 | Do not reopen the TUI-exit choice | Offer exactly the confirmed keep-positions or close-all-positions decision; lifecycle internals are implementation work |
| D43 | Initial OpenRouter testing choice: Nemotron 3 Ultra free | Its route reported upstream overload; subsequently replaced through the user-authorized comparison in D44. Retain explicit provider choice and no paid fallback |
| D44 | Test several free text models and choose one that completes live conversations | Selected cohere/north-mini-code:free after seven successful zero-cost live requests, including two TUI conversation checks. The requirement is a working free model; this is a bounded development selection, not research or tool-conformance validation |
| D45 | Rework the UI in the style of agentic-trading-workflow | Use its navy/cyan four-pane terminal direction for the strategy library, strategy focus, chat and tests/positions workspace. Preserve current capability boundaries and explicit unavailable states; see the [implemented layout](terminal-interface.md#workspace-layout-and-navigation) |
| D46 | Use Tab for switching views and further polish the TUI with agent-terminal inspiration | Tab/Shift+Tab cycle all six workspace views, including Tests/Positions/Session; preserve normal form navigation in onboarding. Keep a visible navigation strip, a simple composer, readable status and literal messages, with no change to implemented trading/research capabilities |
| D47 | Final visual rework follows Gloomberb, including home/onboarding | Use Gloomberb's default White Phosphor direction: black/charcoal surfaces, white focus, square dividers and compact pane title bars. Apply the same style to first-run welcome and model settings. This supersedes the navy/cyan palette in D45 while preserving D46 navigation and current capability boundaries |
| D48 | Start the CLI in a maximized terminal window; the user uses macOS Terminal | Superseded by D49. The previous implementation: local Apple_Terminal sessions resize the window containing the caller's TTY to the current display's usable area before the TUI starts. Other hosts use advisory XTWINOPS. Native permissions and unsupported hosts can prevent resizing; tests verify targeting/geometry/dispatch, while actual desktop acceptance remains manual |
| D49 | Remove automatic maximization; use responsive panels, formatted replies, an inert brand and onboarding inside chat | Six independent panels adapt to six/four/two horizontal/two vertical/one layouts; initial thresholds are documented in the [terminal interface](terminal-interface.md#workspace-layout-and-navigation). Preserve drafts, model state and navigation through resizes. Render assistant Markdown while keeping keys masked and user/command text literal. Replace the separate setup screen with an inline chat form; remove the brand menu. This supersedes D48 and the screen/layout details in D45–D47 |



D07, D08, and D14 were explicitly confirmed on October 13. D09 was subsequently refined the same day to select Supabase as the managed PostgreSQL platform. D17–D19 were then confirmed to separate application authentication from optional operational connectors. The other entries summarize the product requirements and registry model developed in the preceding discussion.

## Implementation proposals, not confirmed vendor choices

- Parquet for versioned market data and DuckDB for analysis remain proposed; Pydantic is now selected for Phase 0 contracts.
- Separate TUI, research workers, and trading runtime within a modular repository.
- A shared catalog service/API with client authentication; HTTPS/JSON transport is selected in Phase 0, while application framework/hosting remain open. Supabase database hosting is selected.
- Database fields and constraints in the [database model](database-model.md); these are a logical design, not applied migrations.
- Isolated generated-code execution, durable job records, and structured intervention audit.
- Semantic candidate search followed by conservative exact-spec comparison.
- A later optional VectorBT research accelerator and optional Quiver alternative-data connector.
- The slash command vocabulary in the [TUI plan](terminal-interface.md).
- Google and GitHub OAuth providers are now confirmed; hosted-login deployment and terminal callback details remain implementation decisions.
- Connection fields and account/environment authorization boundaries in the [authentication plan](authentication-and-connectors.md).

Automatic strategy sharing does not grant permission to publish secrets, account state, private conversations, personal configuration, or third-party material without appropriate rights. Define a publishable strategy projection and ingestion notices before launch.

## Still open

- Tradable historical/live data providers, supported accounts/instruments and operational engine adapters. Phase 0 selects an ECB EURUSD reference-rate compatibility sample, not a production market-data provider. Liquid's first-connector order remains confirmed.
- Production model provider coverage, queue/scheduler, application-service hosting and secure local session storage. Testing model and initial client OSes are confirmed; Textual is selected in Phase 0.
- Supabase OAuth provider selection, terminal redirect/PKCE flow, token storage, linking, and revocation details.
- Supabase project provisioning, project plan, RLS policies, migrations and any adoption of Storage, Realtime or Edge Functions. Organization/Frankfurt and Auth remain confirmed; a hosted application API is the selected client access path.
- Physical registry schema/constraints, canonical-format extensions and implementation packaging. Phase 0 selects strategy/1, 24-digit public strings and UUIDv7 internal keys.
- Recommendation weights, evidence verification, moderation, content-use policy, and user deletion handling.
- Risk thresholds, portfolio allocation, netting/attribution, and operational approval policy.
- Publication scope of performance evidence; raw private account/trade data is excluded by default.
- Online outage behavior for jobs and operational positions; offline product functionality is excluded.
- APIs to other tradingsphere components.

Do not reopen confirmed choices as if undecided. When a new proposal changes a confirmed requirement, obtain user confirmation and update this log.

## Planning refinement status

D20–D33 reflect the subsequent roadmap discussion on October 13, 2026. No application functionality, billing collection or Supabase configuration is implemented. The repository was verified private. D34–D40 record the next answer batch; payment collection, first market/data and numerical release/job criteria remain open. NVIDIA model catalog availability was verified against [NVIDIA NIM](https://build.nvidia.com/deepseek-ai/deepseek-v4.1-flash); actual calls remain untested.

## Phase 0 implementation selections

These engineering decisions were made within the authorized Phase 0 work; they do not change the confirmed product requirements or approve release. Historical planning entries above retain their original context.

| ID | Selection | Verified scope / consequence |
| --- | --- | --- |
| I01 | Python 3.14.7, NautilusTrader 1.231.0, LangGraph 1.2.15, Pydantic 2.14.1, HTTPX 0.28.1, uv lock | Actual imports on Linux x86_64/ARM64 and macOS 26 ARM64; Intel macOS core only, no stable engine wheel |
| I02 | Textual 8.2.8 | Headless input, literal rendering, resize, focus and exit proof on four platforms; no product UI |
| I03 | strategy/1 and versioned dataset/instrument/experiment contracts | Full structural behavioral identity, exact decimal strings, collision checks, schema examples; no general strategy compiler |
| I04 | 24 random decimal digits for public IDs; UUIDv7 for internal keys | String zeroes preserved; persistence, uniqueness and concurrent retry remain Phase 1 work |
| I05 | ECB EURUSD daily references, January–March 2025; SIM currency-pair and next-reference event adapter | Online acquisition, checksums and bounded native conformance proof; availability/liquidity are declared assumptions, not tradable-market verification |
| I06 | Hosted HTTPS/JSON application API; explicit shared/private DTOs | Pure projection/ownership contract proof; no deployed API, authentication, Supabase schema or RLS verification |

See [Phase 0](phase0.md), [canonical contracts](phase0-contracts.md) and [access boundary](phase0-access-boundary.md) for exact evidence and deferred responsibilities.

## Phase 1 implementation progress

October 17, 2026: the user authorized Phase 1. The first sequential increment implements the terminal and conversational model workflow described in [phase1.md](phase1.md). It does not complete the registry, source adapters, model execution tools or live provider verification. NVIDIA's current documentation lists V4 Flash while the selected testing model remains V4.1 Flash; the client preserves explicit model selection without substitution. Login and fee collection remain deferred.
