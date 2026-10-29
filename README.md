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
- [Source-grounded strategy intake](docs/agent-workflow.md) from public references and local PDFs.
- [Strategy-agent development](docs/strategy-development.md): compiled SMA/EMA/RSI expression programs, native backtests, bounded revisions and sampled market comparisons. Arbitrary source conversion remains unfinished.
- [MT5 local data/order connector](docs/mt5-bridge.md), with a compiled Expert Advisor, exact account/session pins, typed data reads and guarded market entry/owned exits.
- Alpaca/Liquid active commands and automatic credential restoration are retired; historical formats, journals and readers remain separate.
- Gold MT5 history acquired and structurally validated across all 21 standard intervals; coarse intervals reach 2004.

The [agent workflow rebuild](docs/agent-workflow.md) remains in progress. Automatic research and
the headless MT5 connector are implemented separately: faithful broker-CFD research, final report
ownership validation and automatic strategy/risk-agent execution are not connected yet.
MT5 trading requires an exact user-approved deployment and native enablement. One demo connector
round-trip passed; strategy-driven broker execution, FTMO rules, sustained operation and recovery
remain verification gates. There is no
release deadline.

## Development

Start with [plan.md](plan.md): current scope, unfinished work, priorities and completion criteria.
The [documentation index](docs/README.md) links the focused guides.
The current priority is the requested research-to-portfolio workflow rebuild, with tests at each stage.
