# Setup and headless usage

stratosphere is being rebuilt as a strategy research engine and MT5 connector for external
agents. The terminal UI, inline setup forms and slash-command router have been removed.
Agent coordination and conversation belong to an external framework. A general external-agent
interface, including MCP, is not implemented yet.

## Environment

The project targets Python 3.14 and uses `uv.lock`. On supported Linux/macOS platforms:

```sh
uv sync --locked --extra engine --no-build-package nautilus-trader
.venv/bin/python -m stratosphere --help
```

Intel macOS selects the [separate project's pinned engine wheel](intel-macos.md); Linux and Apple
Silicon use upstream wheels. Engine build/release tooling stays in that separate repository.

## Available entry points

Running the package without arguments prints help and starts no operation. The headless dispatcher
exposes the existing MT5 connector and runtime compatibility inspector:

```sh
.venv/bin/python -m stratosphere mt5 --help
.venv/bin/python -m stratosphere compatibility --help
.venv/bin/python -m stratosphere compatibility --require-engine
```

The original module entry points remain available:

```sh
.venv/bin/python -m stratosphere.mt5 --help
.venv/bin/python -m stratosphere.phase0.compatibility --help
```

Compatibility inspection checks package versions/imports; it does not run a backtest or connect
to MT5. MT5 read commands require a running terminal and the configured local EA bridge:

```sh
.venv/bin/python -m stratosphere mt5 account
.venv/bin/python -m stratosphere mt5 rates XAUUSD H1 --count 512
.venv/bin/python -m stratosphere mt5 positions
```

Broker login stays inside MT5. Account diagnostics are private operator operations, not strategy-model
tools. Connection grants no trading authority. See [MT5 connector](mt5-bridge.md) for installation,
exact deployment approval, finite limits and owned exits. Existing overnight deployment entry points
remain documented in [overnight agents](mt5-overnight.md).

## Research and compatibility

Validated signal programs, application-generated Python, isolated native workers, registry identities,
sealed reports and historical readers remain available in the Python modules. See
[architecture](architecture.md) for their locations. There is no replacement general research CLI
or external-agent tool server yet. Historical guides that describe slash commands document the
removed interface; those commands are unavailable.

The internal model transports and research-agent runners remain while engine operations are
extracted from them. Their presence does not mean the external-agent rebuild is complete.
The full historical research-to-MT5 deployment path also remains unfinished. Historical Yahoo
results or approvals cannot silently become CFD execution authority.

Private SQLite journals and artifacts remain in their existing native state locations.
`STRATOSPHERE_RESEARCH_DIR` overrides the research root, not the account-wide MT5 journal.
Removing the UI does not erase stored model credentials, reports, approvals or broker journals.
No background worker or MT5 position is started, stopped or closed by displaying package help.
