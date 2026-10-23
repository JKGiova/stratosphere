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
- Bounded variant studies and retained reports with explicit costs, failures and holdout attribution.
- Partial source intake and reviews; arbitrary strategy conversion is unfinished.
- Liquid read-only connection, catalog inspection and an address diagnostic.
- Internal simulated-account and observation-worker foundations; no connected paper or live trading.

The recent tool-using agent runtime remains work in progress pending stabilization. Software tests,
actual model behavior and provider readiness are separate checks. There is no release deadline.

## Development

Start with [plan.md](plan.md): current scope, unfinished work, priorities and completion criteria.
The [documentation index](docs/README.md) links the focused guides.
The current priority is bug fixing and reproducibility before further feature expansion.
