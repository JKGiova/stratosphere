# Architecture and agents

Current scope: the user's **personal application**, as confirmed in [D57](decisions.md). Hosted login, user databases, automatic sharing and platform billing are retired. The [personal-use scope](personal-use.md) and [Phase 3–5 ledger](phase3-5.md) distinguish the current design from historical proofs.

## Selected foundation

Python, LangGraph, Pydantic, Textual and NautilusTrader remain selected. The local SQLite workspace is the strategy/agent registry; private artifact stores and account/service journals retain evidence and operational state. The application needs no Supabase client, application account or hosted API. Existing hosted migrations/access contracts remain historical, without deployment or deletion implied.

[Phase 0](phase0.md) pins runtime/engine versions and verifies its bounded compatibility scope. Intel macOS uses a verified custom wheel; see [Intel verification](intel-macos.md). Version compatibility is not proof of a broker adapter. Broader storage/analysis formats such as Parquet/DuckDB remain proposals.

## Operation and agent boundaries

The orchestrator bridges the user to one stable agent per exact strategy. Each agent has separate private history, retained evidence and validated operations. Exact duplicates reuse their identity/agent; variants receive distinct identities and lineage. The user can inspect jobs and Reports without exposing another strategy's private conversation to the selected model.

| Role | Responsibility |
| --- | --- |
| Orchestrator | Route user requests and return specialist results |
| Strategy specialist | Analyze its rules/evidence, propose bounded work, inspect approved tests and reports |
| Source/research/development roles | Acquire actual material, formalize rules and establish behavioral conformance |
| Validator | Compare complete measured evidence, failures, costs and holdout attribution |
| Portfolio coordinator | Coordinate candidate intents across personal strategies |
| Budget and risk roles | Produce advisory proposals over explicit frozen account context |
| Operational supervisor | Inspect health and propose review/pause actions |

These logical roles do not imply an always-running model/process for each role. Models cannot grant plan approval, execute acquired code/SQL, mutate allocations or submit orders independently.

## Processes and durable state

The TUI invokes validated application operations; slash commands and natural-language routes share those boundaries. Local registry records, job checkpoints, evidence, approvals and account journals are authoritative, rather than conversation text.

Research runs separately from operational processing. The reviewed [authored observation service](observation-service.md) adds one owner per account, durable queue recovery and silence/heartbeat stops. It processes authored inputs only. Its existence does not establish a real feed, broker transport, production soak or the complete keep/close exit workflow.

[Research workers](phase2.md) receive only validated inputs and a minimal credential-free environment. The supported exact SMA compiler and pinned native simulations are implemented; arbitrary generated/acquired-code execution remains disabled. Risk controls, reservations, idempotency and reconciliation must remain deterministic even when models fail.

## Storage and privacy

- Local SQLite: canonical strategies, provenance, stable agents, separate histories, jobs and report references.
- Private artifacts: source snapshots, immutable approvals, native results, reports and review receipts.
- Account/service journals: observations, reservations, actual reported economics, faults and processing state.
- Process memory: active model/broker/session credentials and their redaction guards.

The application has no multi-user profile, app-login or public-publication requirement. Historical identifiers named public IDs remain stable local identifiers. Preserve old wire readers, source pins and sealed evidence; a presentation/scope cleanup must not rewrite historical metrics, fee assumptions or approvals. See [local database model](database-model.md).

## Providers and online operation

Model adapters remain explicit and selectable, including local Ollama; no paid/model/provider fallback is inferred. Online gates for research/reporting remain. Linux/macOS and English are the initial client targets.

Alpaca is first for personal Bitcoin spot paper development; Liquid perpetual support follows later. Optional provider authentication and permissions remain separate from strategy approval. Account metadata does not establish feed access, paper isolation, native precision/fee handling or order authority. Match each actual venue/instrument/quote/timing profile and permitted personal use.

There is no stratosphere service fee or billing ledger requirement. Broker/exchange commissions, spread, slippage, funding and currency rounding still belong in simulation and operational accounting.

## Operational lifecycle still required

The confirmed exit choice is to keep positions or request closure of all. Closing the TUI, stopping a worker, disconnecting credentials, canceling entries and closing positions are distinct actions. Closure must retain actual outcomes, failures and residual exposure; no immediate fill is assumed.

Provider feed/authenticity, native fee/precision handling, a single broker order gateway, complete portfolio reconciliation, sustained operation and executable binding remain required before personal paper/live readiness. Live activation needs separate explicit authorization. The current checkpoint finishes existing personal-use cleanup and then reserves further work for the user-led bug-fixing session.
