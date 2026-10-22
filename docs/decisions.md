# Decisions and alignment

Last updated: October 22, 2026. Product decisions and Phase 0 implementation selections are separate below. Measured scope/limits are recorded in phase0.md; selections alone do not establish product or live readiness.

Current scope: **personal use only**, confirmed in D57 and reflected in the current roadmap, UI notices and acceptance ledger. Earlier commercial, hosted-login and multi-user catalog decisions below are historical where superseded; no hosted resource deletion is implied. The user's current checkpoint is to finish this cleanup and then move to an intermediate bug-fixing session, without starting a new feature increment.

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
| D18 | Alpaca and Liquid were optional operational connectors | Historical choice, superseded by D58: only Liquid remains. Research/backtests do not require a provider connection; linking and activation remain separate |
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
| D39 | Liquid was selected first | D56 temporarily changed paper priority; D58 restores Liquid as the only trading connector. Custom-client permissions, perpetual semantics and execution capability still require verification; no live integration is implemented |
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
| D50 | Start Phase 2; use BTC/USDT spot hourly candles from Binance public archives for the first case | Phase 1 registry/content adapters remain unfinished. Implement and verify a bounded pasted-text → reviewed SMA specification → approved native backtests → reproducible report path; further strategy families/data profiles remain work |
| D51 | Add Claude, OpenAI, Grok, Groq, DeepSeek, Gemini, Kimi, Qwen, GLM and Ollama APIs | Explicit provider/key/address selection for chat and validated research extraction. Local Ollama is an authorized exception to the earlier hosted-model-only assumption; online research/data checks remain required. OpenRouter remains the default and free-only. Protocol verification and authenticated live verification are separate; see [model providers](model-providers.md) |
| D52 | Proceed with Phases 3, 4 and 5; use local Ollama testing where useful | Preserve the full iteration/recommendation, source/data and live-data/paper scope in the [acceptance ledger](phase3-5.md). This does not authorize Phase 6 live orders or billing |
| D53 | The orchestrator bridges the user to one AI agent per strategy; agents analyze, use the database, test and report; replace Session with Reports | Stable per-strategy identity and separate private contexts; shared validated operations retain exact-plan approval and deterministic runtime gates. See [strategy agents](strategy-agents.md) |
| D54 | Create a new Supabase organization named tradingsphere | Supersedes Matti Barbershop as the project target. The organization and stratosphere project were created on the quoted $0 allowance, retaining Frankfurt. The old unrelated project remains untouched |
| D55 | Use Liquid paper mode for the first real-feed paper test | Superseded by D56 after actual catalog inspection and the documented absence of spot paper trading. The original selection did not verify an account, authorize funding or grant trading scope; see [Liquid paper testing](liquid-paper-testing.md) |
| D56 | Alpaca first for Bitcoin spot; Liquid later for perpetuals | Historical 21 October choice, explicitly superseded by D58. The diagnostic implementation is removed; its [historical verification record](https://github.com/JKGiova/stratosphere/blob/053a19733d6241dfe419b0eec5b483ed63cc1d90/docs/alpaca-paper-testing.md) and private observations do not grant connection, feed or execution authority. BTC/USD evidence cannot transfer to BTC/USDT or Liquid perpetuals |
| D57 | stratosphere is for the user's personal use, without commercialization or multi-user databases | Supersedes commercial pricing/billing, hosted application login, automatic community publication and hosted user-database requirements in D09, D13–D14, D17, D21, D27–D28, D33–D35, D38, D41 and D54 where they depend on a commercial/shared service. Keep the private repository, local strategy/agent/history/report storage, provider credentials and deterministic testing/trading gates. Model keys and optional broker connections remain necessary for their respective operations. Local SQLite is not a hosted user database. This decision does not delete existing local evidence or Supabase resources, change repository visibility/licensing, or establish provider data/execution entitlement. |
| D58 | Liquid is the only trading connector; remove Alpaca entirely | Confirmed on 22 October 2026. Remove its runtime adapter, credential form, commands, data profile, dedicated tests and active setup guide. Keep Liquid's reviewed read-only connection/catalog/address operations and existing restrictions. Preserve historical transcript/decisions and private diagnostic evidence without restoring access or implying a working perpetual feed, paper adapter or live orders |



D07, D08, and D14 were explicitly confirmed on October 13. D09 was subsequently refined the same day to select Supabase as the managed PostgreSQL platform. D17–D19 were then confirmed to separate application authentication from optional operational connectors. The other entries summarize the product requirements and registry model developed in the preceding discussion.

## Historical proposals preceding D57

The hosted, login and publication proposals in this historical section are superseded by [personal-use scope](personal-use.md). Remaining source, research and provider decisions are tracked in the current [roadmap](roadmap.md); retired billing/hosting questions need no further answers.

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

## Historical open items preceding D57

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

D57 explicitly supersedes hosted login, community publication and commercial prerequisites above. The current personal roadmap tracks remaining choices; historical questions do not require new answers.

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

## Phase 2 implementation selection

| ID | Selection | Verified scope / consequence |
| --- | --- | --- |
| I07 | First research profile: January 2025 archive, long-only fixed-quantity SMA template, base/doubled-cost pair, private local evidence and native subprocess | Engineering defaults and limitations are shown in each plan. Execution requires one exact-plan approval; no arbitrary generated-code execution, shared registration, broker/live integration or release is implied. See [Phase 2](phase2.md) |

## Phase 4 source implementation increment

Read-only source snapshots are separate from strategy registration, conversion and execution. Original/canonical URL, raw/request/snapshot digests, acquisition time, access state and precise material scope are retained. Metadata is excluded from behavioral evidence. Public GitHub files are resolved to an immutable commit and checked against Git tree/blob identity; acquired code is never imported or executed. Partial public readers and unimplemented authorized-provider paths remain labeled explicitly. Supplied material does not prove original-source completeness or behavioral conformance. See [source intake](phase4-sources.md) for verified operations, live endpoint results and remaining requirements; this increment does not complete Phase 4.
