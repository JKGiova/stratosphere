# Documentation

[plan.md](../plan.md) owns scope, status, priorities and completion criteria.
These are focused reference guides, not separate roadmaps.

[Headless usage](usage.md) owns current entry points. The TUI and slash-command router are removed.
Guides retaining slash-command examples describe historical interfaces and internal contracts;
those examples are unavailable. Internal research runners are removed; historical checkpoints are
read-only. External-agent/MCP tools and remaining compatibility-adapter separation are pending.

| Guide | Purpose |
| --- | --- |
| [Usage](usage.md) | Installation, headless entry points and rebuild limits |
| [Architecture](architecture.md) | Components, state ownership and code locations |
| [Strategy development](strategy-development.md) | Explicit program jobs, compiled code, finite backtests and historical v1/v2 readers |
| [Research tools](research-tools.md) | Deterministic inspection helpers, optional public readers and verification |
| [Agent workflow](agent-workflow.md) | External-agent responsibilities, engine operations and retained approval contracts |
| [Yahoo backtesting](yahoo-backtesting.md) | Historical Yahoo setup, supported models and data limitations |
| [Research](research.md) | Supported strategy, approval, studies and report evidence |
| [Sources](sources.md) | Actual intake capabilities, provenance and conversion limits |
| [Paper foundations](paper.md) | Internal accounting/runtime and missing provider integration |
| [Legacy provider evidence](liquid.md) | Retained Liquid records; active connector retired |
| [MT5 connector](mt5-bridge.md) | Active local data/order bridge, exact approvals, commands and limits |
| [Overnight agents](mt5-overnight.md) | Paper-grounded CFD adaptation, local calendars, continuous bounded agents and exact activation |
| [Retired Gold pilot](mt5-gold.md) | Historical XAUUSD golden-cross identity, reports and compatibility |
| [MT5 history probes](mt5.md) | Historical diagnostics, coverage exports and validation limits |
| [Credentials](credentials.md) | Saving, restoring and forgetting access |
| [Model providers](model-providers.md) | Selection, configuration and failure behavior |
| [Data rights](data-source-rights.md) | Data-purpose and entitlement boundaries |
| [Contracts](contracts.md) | Versioned formats and compatibility rules |
| [Invariants](invariants.md) | Mandatory subsystem rules for development and bug fixes |
| [Development](development.md) | Test commands, verification limits and defect workflow |
| [Intel macOS](intel-macos.md) | Pinned external wheel dependency, installation and acceptance |

Retired hosted database code remains under [supabase](../supabase/README.md), outside application
requirements. Previous plans and development histories are available through Git history.
