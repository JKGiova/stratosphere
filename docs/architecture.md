# Architecture

## Components

| Component | Responsibility | Code |
| --- | --- | --- |
| TUI | Panels, inline forms, chat, explicit commands and job controls | `src/stratosphere/tui.py`, `commands.py`, `workspace.py` |
| Model transport | Provider-specific text streaming and exact workflow-schema delivery | `src/stratosphere/models/`, `chat.py` |
| Orchestrator/strategy sessions | Route requests to stable strategy owners with isolated histories | `src/stratosphere/workflow/` (active), `agents/` (compatibility helpers) |
| Local library | Canonical identities, duplicate reuse, provenance, lineage, jobs and reports | `src/stratosphere/registry/` |
| Yahoo historical research | Isolated yfinance acquisition, separate approved price-research plans and native runs | `src/stratosphere/yahoo/` |
| Research/studies | Reviewed plans, finite native starts, variants and retained results | `src/stratosphere/research/`, `studies/` |
| Source/evidence services | Acquisition, immutable snapshots, reviews, registration and report inspection | `src/stratosphere/sources/`, `report_evidence.py`, `report_analysis.py` |
| Provider boundaries | Fixed Liquid read diagnostics, Alpaca paper/data HTTP and Liquid paper capability blocker | `src/stratosphere/connectors/` |
| Operational paper workflow | Final packages/approval, global paper journal, binary/wallet reviews, signals, reservations and order reconciliation | `src/stratosphere/workflow/portfolios.py`, `execution.py` |
| Authored account/runtime | Journals, reservations, risk gates, observation processing and inspection | `src/stratosphere/paper/` |

Application services are authoritative. Natural-language and slash routes must reuse their
validation and approval boundaries. Models cannot grant permission or mutate an active strategy.
The replacement research-to-portfolio lifecycle is described in [agent workflow](agent-workflow.md).

## State

- Local SQLite stores definitions, provenance, private strategy histories and job/report references.
- Private files retain source bytes, plans, approvals, native artifacts and reports.
- Separate account/service journals retain observations, reservations, fills, faults and worker state.
- Native OS secure storage holds selected model settings/key, the fixed Liquid read grant and Alpaca paper keys.
- The personal paper portfolio journal uses a fixed native state directory across research-root overrides.
- Callback codes, PKCE/state, active MCP session identifiers and active credentials stay in memory.

`STRATOSPHERE_RESEARCH_DIR` selects a research root. Defaults are the user's Application Support
folder on macOS and XDG state directory on Linux. Operational data is not committed to Git.

## Process boundaries

The TUI invokes validated services. Trusted native research runs in separate credential-free
workers. Imported source is never executed just because it was acquired. Generated-code isolation
and executable verification remain unfinished.

The authored observation worker is separate from the TUI and requires its own approved streams,
queue and OS ownership lock. It has no real provider feed or order transport. Reading status never
starts or modifies it. The separate Alpaca paper runner lives in the TUI process and starts only
from an exact final approval or an explicit run command. Liquid submission and all live transport
remain unavailable.

## Compatibility

Versioned schemas, complete definition hashes, frozen approval pins and immutable report readers
must survive upgrades. Do not recalculate old results or reinterpret old spot evidence as perpetual
performance. See [contracts](contracts.md) and [invariants](invariants.md).
