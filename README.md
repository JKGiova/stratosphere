# stratosphere

A personal headless strategy research engine and guarded MT5 connector, part of **tradingsphere**.
Linux and macOS are the initial targets. The repository is public; credentials and operational
evidence stay private.

## Start

From the configured project environment:

```sh
.venv/bin/python -m stratosphere --help
.venv/bin/python -m stratosphere mt5 --help
.venv/bin/python -m stratosphere compatibility --help
```

See [setup and headless usage](docs/usage.md) for installation and available operations.

## Rebuild scope

The target is one engine for strategy validation, market data, reproducible backtesting, evidence
and approved MT5 deployment. Agents run in an external framework and call engine operations.
The TUI, inline forms and slash-command router have been removed. A general external-agent/MCP
interface is still pending. The internal research crew, chat sessions and persistent tool runner
are removed. New Python research jobs accept an explicit typed definition and run one program
within frozen application limits. Historical v1/v2 campaigns remain readable without resuming them.

Existing code retains validated SMA/EMA/RSI expression programs, application-generated Python,
bounded NautilusTrader/Yahoo research, stable strategy identities and versioned report readers.
The [MT5 local connector](docs/mt5-bridge.md) supports typed data reads, exact deployment approval,
guarded market entries and owned exits. The separately approved
[overnight adapter](docs/mt5-overnight.md) and its pinned model/risk compatibility code remain
available; they are separate from the removed research crew.

The general research-to-MT5 pipeline remains unfinished. Source conformance, faithful broker-CFD
research, final report ownership validation, sustained operation, recovery and FTMO rules remain
verification gates. One demo connector round-trip is not release readiness.

MT5 trading requires an exact user-approved deployment and native enablement. Connecting never
activates trading. Historical Alpaca/Liquid readers, approvals and journals remain separate and
cannot grant MT5 authority. Credentials use native OS secure storage; broker login stays inside MT5.

## Development

[plan.md](plan.md) is the single development plan. The [documentation index](docs/README.md) links
the focused guides. NautilusTrader wheel production belongs to
`JKGiova/nautilus-trader-macos-intel`; this repository consumes pinned artifacts.
