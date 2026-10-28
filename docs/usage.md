# Setup and usage

## Environment

The project targets Python 3.14 and uses `uv.lock`. On supported Linux/macOS platforms:

```sh
uv sync --locked --extra engine --no-build-package nautilus-trader
.venv/bin/python -m stratosphere
```

Intel macOS selects the [separate project's pinned engine wheel](intel-macos.md); Linux and Apple
Silicon use upstream wheels. The external Intel release must be published and downloadable.

## First run

Configure the provider/model/key in the inline form. There is no application login. Model settings
and the optional Liquid read grant use the [native credential store](credentials.md). Research
works without Liquid. Escape dismisses model setup when inspecting non-model operations.

Tab/Shift+Tab switch panels. Layout adapts to terminal dimensions; forms use their own field
navigation. The app does not resize, maximize or make the terminal enter native fullscreen.
Replies render Markdown. Chat, Library, Focus, Tests, Positions and Reports are the main views.

## Legacy manual research path

For new automatic campaigns, see the agent workflow below. The legacy path remains available:

1. Use `/new` with a complete supported SMA description. Include exact periods, quantity, instrument,
   closed-bar entry/exit and next-open execution assumptions.
2. Review the specification, source evidence, capital and costs. Unsupported behavior must be resolved.
3. Approve the exact displayed plan. No model response substitutes for approval.
4. Inspect jobs, reports and detailed evidence. Use `/study` for a bounded variant comparison with a
   separate final holdout. Read [research](research.md) before interpreting results.

## Commands by purpose

`/help` is generated from the application command router and is the full command reference.

| Purpose | Commands |
| --- | --- |
| Models/chat | `/model`, `/model-forget`, `/cancel`, `/clear` |
| Views/selection | `/strategies`, `/agent [id]`, `/jobs`, `/reports`, `/portfolio` |
| Yahoo historical research | `/yahoo`, `/yahoo-show`, `/yahoo-approve`, `/yahoo-cancel` ([guide](yahoo-backtesting.md)) |
| Agent research | `/create <brief>`, `/flow`, `/flow-show`, `/flow-cancel` (v1 `/flow-approve` only for compatibility) |
| Legacy research | `/new`, `/study`, `/approve <job> <digest>`, `/pause <job>`, `/resume <job>`, `/cancel <job>`, `/show <job>` |
| Evidence | `/report <job>`, `/report-data`, `/analyze-report`, `/report-analyses`, `/report-analysis-show` |
| Sources | `/source`, `/source-text`, `/source-file`, `/source-captions`, `/sources`, `/source-show` |
| Source interpretation/import | `/conformance`, `/source-review`, `/source-register` and their list/show commands |
| Dated local assessments | `/assess`, `/recommend`, `/assessment` |
| Data capabilities | `/data` |
| Internal account inspection | `/paper [positions|orders|fills|health] [page]`, `/paper-review`, `/paper-reviews`, `/paper-review-show` |
| Exit | `/quit` |

`/clear` clears the selected conversation, including saved strategy chat where applicable; immutable
research/source evidence remains. The legacy `/paper` commands inspect authored journals. The automatic Alpaca runner updates Positions
with reconciled provider quantities. `/portfolios` shows Alpaca paper, planned Liquid live perpetuals
and planned FTMO evaluation via MT5 or an equivalent. Liquid and FTMO execution remain unavailable;
historical Liquid paper journals are retained outside the active setup. Missing accounts are
unavailable, not zero balances.

To propose research, use `/create` with the strategy rules and requested Yahoo symbols, intervals
and historical windows. The supported family and approval sequence are described in
[agent workflow](agent-workflow.md).

## Quit and restart

Approved research pauses on quit and retains checkpoints; interrupted jobs are not automatically
started on launch. Saved credentials remain until forgotten. An actual trading-position keep/close
exit workflow is planned but not implemented. Disconnecting Liquid never closes a position.

## Replacement agent research and paper approval

Use `/create <brief>` for an automatically executed bounded Yahoo SMA campaign. All attempts appear
in Tests and Reports; no research approval is requested for new v2 campaigns. After reviewing a
successful exact candidate, prepare its final package with `/strategy-finish`, then approve it once.
That approval allows automatic paper orders within its displayed limits. See
[agent workflow](agent-workflow.md) for exact command arguments, native paper-key setup, scope and
recovery limitations. `/portfolio-stop` and quitting keep positions and stop monitoring; no live
execution exists. Existing v1/manual research commands retain their original approval protocols.

## MT5 connector

MT5 is the active broker connector; use `/mt5` and `/mt5-status` for setup/read-only diagnostics.
Headless data and exact-deployment order commands: [MT5 connector](mt5-bridge.md). Alpaca/Liquid
commands and their automatic execution setup are retired. Historical reports remain readable.
