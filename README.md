# stratosphere

A personal terminal workspace for quantitative strategy research, part of **tradingsphere**.
Linux and macOS are the initial targets. The repository is private.

## Start

From the configured project environment:

```sh
.venv/bin/python -m stratosphere
```

Set up a model in the inline form or with `/model`. Saved credentials use the native OS store.
Use `/help` for commands and Tab/Shift+Tab to switch panels. The terminal keeps its window size.
See [setup and usage](docs/usage.md) for installation and the research workflow.

## Current state

- Responsive TUI, selectable model providers and a local library with separate strategy-agent histories.
- Approved NautilusTrader research for the supported BTC/USDT hourly, long-only SMA crossover family.
- [Yahoo historical research](docs/yahoo-backtesting.md), with bounded agent campaigns via `/create` and manual runs via `/yahoo`.
- Bounded variant studies and retained reports with explicit costs, failures and holdout attribution.
- Partial source intake and reviews; arbitrary strategy conversion is unfinished.
- Liquid read-only connection, catalog inspection and an address diagnostic.
- Final-strategy approval and an Alpaca paper runner for crypto spot, tested with authored accounts/models/HTTP.
- Portfolio roles: Alpaca paper, future Liquid live perpetuals and FTMO two-step Swing via MT5 or a verified equivalent. Liquid/FTMO execution is unavailable; Alpaca live is deferred. Historical Liquid paper state is retained.

The [agent workflow rebuild](docs/agent-workflow.md) is in progress. New research is automatic;
one final strategy approval binds paper limits. Real provider verification and operational recovery/exit
controls remain unfinished. Software tests,
actual model behavior and provider readiness are separate checks. There is no release deadline.

## Development

Start with [plan.md](plan.md): current scope, unfinished work, priorities and completion criteria.
The [documentation index](docs/README.md) links the focused guides.
The current priority is the requested research-to-portfolio workflow rebuild, with tests at each stage.
