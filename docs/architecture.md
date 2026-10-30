# Architecture

## Components

| Component | Responsibility | Code |
| --- | --- | --- |
| TUI | Panels, inline forms, chat, explicit commands and job controls | `src/stratosphere/tui.py`, `commands.py`, `workspace.py` |
| Model transport | Provider-specific text streaming and exact workflow-schema delivery | `src/stratosphere/models/`, `chat.py` |
| Orchestrator/strategy sessions | Route requests to stable strategy owners with isolated histories | `src/stratosphere/workflow/` (active), `agents/` (compatibility helpers) |
| Local library | Canonical identities, duplicate reuse, provenance, lineage, jobs and reports | `src/stratosphere/registry/` |
| Strategy development | Typed formalization, application code generation, stable-agent research, finite native comparisons/revisions | `src/stratosphere/development/` |
| Yahoo historical research | Isolated yfinance acquisition, separate approved price-research plans and native runs | `src/stratosphere/yahoo/` |
| Research/studies | Reviewed plans, finite native starts, variants and retained results | `src/stratosphere/research/`, `studies/` |
| Source/evidence services | Public discovery, local PDF extraction, cited strategy briefs, immutable snapshots, reviews, registration and report inspection | `src/stratosphere/sources/`, `report_evidence.py`, `report_analysis.py` |
| Provider boundary | Typed local MT5 EA/client data and order gateway; retired provider compatibility | `src/stratosphere/mt5/`, `tools/mt5/`, `src/stratosphere/legacy/` |
| Portfolio role | MT5 / FTMO, unbound until exact account read; no implicit activation | `src/stratosphere/workflow/portfolio_setup.py` |
| Historical paper workflow | Retained packages/approval, journal, reviews, signals and fixtures; no active broker routing | `src/stratosphere/workflow/portfolios.py`, `execution.py` |
| Authored account/runtime | Journals, reservations, risk gates, observation processing and inspection | `src/stratosphere/paper/` |

Application services are authoritative. Natural-language and slash routes must reuse their
validation and approval boundaries. Models cannot grant permission or mutate an active strategy.
The replacement research-to-portfolio lifecycle is described in [agent workflow](agent-workflow.md).

The persistent specialist separates typed decisions (`development/agent.py`), allowlisted tools
(`tools.py`), deterministic continuation criteria (`progress.py`) and checkpoint execution
(`runner.py`). Historical v1 services/readers remain the native single-program execution boundary.

## State

- Local SQLite stores definitions, provenance, private strategy histories and job/report references.
- Private files retain source bytes, plans, approvals, native artifacts and reports.
- Separate account/service journals retain observations, reservations, fills, faults and worker state.
- Native OS secure storage holds selected model settings/key. Historical broker entries are not restored.
- MT5 owns broker login; the local bridge journal/policies contain no passwords or API keys.
- The personal paper portfolio journal uses a fixed native state directory across research-root overrides.
- Callback codes, PKCE/state, active MCP session identifiers and active credentials stay in memory.

`STRATOSPHERE_RESEARCH_DIR` selects a research root. Defaults are the user's Application Support
folder on macOS and XDG state directory on Linux. Operational data is not committed to Git.

## Process boundaries

The TUI invokes validated services. Trusted native research runs in separate credential-free
workers. Imported source is never executed just because it was acquired. Strategy development executes only exact application-compiled Python specializations in
credential-free children, with pinned definition/code/input/runtime and finite reservations.
Arbitrary model/acquired Python is unsupported; this is not independent executable attestation.

The authored observation worker is separate from the TUI and requires its own approved streams,
queue and OS ownership lock. It has no real provider feed or order transport. Reading status never
starts or modifies it. The MT5 EA runs inside the existing terminal; the application invokes its local typed interface.
Exact deployment approval and native enablement are separate from data access. See the
[connector guide](mt5-bridge.md) for current lifecycle and verification limits. Historical Alpaca
runner code is compatibility material and is no longer routed by the TUI.

## Compatibility

Versioned schemas, complete definition hashes, frozen approval pins and immutable report readers
must survive upgrades. Do not recalculate old results or reinterpret old spot evidence as perpetual
performance. See [contracts](contracts.md) and [invariants](invariants.md).
