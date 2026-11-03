# Architecture

## Components

| Component | Responsibility | Code |
| --- | --- | --- |
| Headless dispatcher | Help, MT5 connector commands and runtime compatibility inspection | `src/stratosphere/__main__.py` |
| Compatibility model transport | Explicit one-shot services and exactly pinned MT5 model/risk adapters | `src/stratosphere/models/` |
| Historical campaign readers | Validate old v1/v2 reports, action chains and consumed reservations without resuming | `src/stratosphere/development/history.py`, `history_contracts.py` |
| Local library | Canonical identities, duplicate reuse, provenance, lineage, jobs and reports | `src/stratosphere/registry/` |
| Strategy development | Explicit validated definitions, application compilation, single-program native comparisons and sealed evidence | `src/stratosphere/development/` |
| Yahoo historical research | Isolated yfinance acquisition, separate approved price-research plans and native runs | `src/stratosphere/yahoo/` |
| Research/studies | Reviewed plans, finite native starts, variants and retained results | `src/stratosphere/research/`, `studies/` |
| Source/evidence services | Public discovery, local PDF extraction, cited strategy briefs, immutable snapshots, reviews, registration and report inspection | `src/stratosphere/sources/`, `report_evidence.py`, `report_analysis.py` |
| Provider boundary | Typed local MT5 EA/client data and order gateway; retired provider compatibility | `src/stratosphere/mt5/`, `tools/mt5/`, `src/stratosphere/legacy/` |
| Portfolio role | MT5 / FTMO, unbound until exact account read; no implicit activation | `src/stratosphere/workflow/portfolio_setup.py` |
| Historical paper workflow | Retained packages/approval, journal, reviews, signals and fixtures; no active broker routing | `src/stratosphere/workflow/portfolios.py`, `execution.py` |
| Authored account/runtime | Journals, reservations, risk gates, observation processing and inspection | `src/stratosphere/paper/` |

Application services are authoritative. External-agent adapters must reuse their validation and
approval boundaries. Models cannot grant permission or mutate an active strategy. The TUI, its
widgets/layout/style code and slash-command router have been removed. There is no general
external-agent/MCP interface yet. The internal research crew, chat sessions, automatic definition/
revision prompts and persistent execution loop are removed. LangGraph is no longer required.
[Strategy development](strategy-development.md) owns the explicit v3 engine operations and legacy
readers. [Agent workflow](agent-workflow.md) owns external role responsibilities.

Existing source/report interpretation helpers still accept explicitly supplied model transports.
The operator-approved MT5 overnight and retired Gold adapters retain their pinned implementation
and protective-exit behavior. Those compatibility modules are not the general research crew;
their extraction must preserve exact deployment identities and exit access.

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

The headless MT5 commands invoke validated services. Trusted native research runs in separate credential-free
workers. Imported source is never executed just because it was acquired. Strategy development executes only exact application-compiled Python specializations in
credential-free children, with pinned definition/code/input/runtime and finite reservations.
Arbitrary model/acquired Python is unsupported; this is not independent executable attestation.

The authored observation worker requires its own approved streams,
queue and OS ownership lock. It has no real provider feed or order transport. Reading status never
starts or modifies it. The MT5 EA runs inside the existing terminal; the application invokes its local typed interface.
Exact deployment approval and native enablement are separate from data access. See the
[connector guide](mt5-bridge.md) for current lifecycle and verification limits. Historical Alpaca
runner code is compatibility material and has no active broker routing.

## Compatibility

Versioned schemas, complete definition hashes, frozen approval pins and immutable report readers
must survive upgrades. Do not recalculate old results or reinterpret old spot evidence as perpetual
performance. See [contracts](contracts.md) and [invariants](invariants.md).
