# Setup and usage

## Environment

The project targets Python 3.14 and uses `uv.lock`. On a supported platform with a published engine wheel:

```sh
uv sync --locked --extra engine
.venv/bin/python -m stratosphere
```

Intel macOS requires the [custom engine installation](intel-macos.md), not an ARM wheel or the
normal upstream-wheel path. Use the existing configured environment if it is already installed.

## First run

Configure the provider/model/key in the inline form. There is no application login. Model settings
and the optional Liquid read grant use the [native credential store](credentials.md). Research
works without Liquid. Escape dismisses model setup when inspecting non-model operations.

Tab/Shift+Tab switch panels. Layout adapts to terminal dimensions; forms use their own field
navigation. The app does not resize, maximize or make the terminal enter native fullscreen.
Replies render Markdown. Chat, Library, Focus, Tests, Positions and Reports are the main views.

## Supported research path

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
| Agent research | `/create <brief>`, `/flow`, `/flow-show`, `/flow-approve`, `/flow-cancel` |
| Legacy research | `/new`, `/study`, `/approve <job> <digest>`, `/pause <job>`, `/resume <job>`, `/cancel <job>`, `/show <job>` |
| Evidence | `/report <job>`, `/report-data`, `/analyze-report`, `/report-analyses`, `/report-analysis-show` |
| Sources | `/source`, `/source-text`, `/source-file`, `/source-captions`, `/sources`, `/source-show` |
| Source interpretation/import | `/conformance`, `/source-review`, `/source-register` and their list/show commands |
| Dated local assessments | `/assess`, `/recommend`, `/assessment` |
| Data capabilities | `/data` |
| Liquid read access | `/liquid`, `/liquid-connect`, `/liquid-disconnect`, `/liquid-tools`, `/liquid-tool`, `/liquid-catalog-save` |
| Explicit Liquid diagnostic | `/liquid-account`, `/liquid-account-reads`, `/liquid-account-show` |
| Internal account inspection | `/paper [positions|orders|fills|health] [page]`, `/paper-review`, `/paper-reviews`, `/paper-review-show` |
| Exit | `/quit` |

`/clear` clears the selected conversation, including saved strategy chat where applicable; immutable
research/source evidence remains. Positions and paper commands currently inspect authored local
journals, not a Liquid trading account. Missing accounts are unavailable, not zero balances.

To propose research, use `/create` with the strategy rules and requested Yahoo symbols, intervals
and historical windows. The supported family and approval sequence are described in
[agent workflow](agent-workflow.md).

## Quit and restart

Approved research pauses on quit and retains checkpoints; interrupted jobs are not automatically
started on launch. Saved credentials remain until forgotten. An actual trading-position keep/close
exit workflow is planned but not implemented. Disconnecting Liquid never closes a position.

## Replacement agent research

Use `/create <brief>` for a bounded Yahoo SMA research campaign. Review all candidate plans and
blocked datasets, then approve the full campaign once. `/flow`, `/flow-show <id>`,
`/flow-approve <id> <sha256>` and `/flow-cancel <id>` inspect/control it. Each eligible candidate
uses two native starts, base and stress; canceled/consumed work never silently resumes. Campaigns
appear in Tests and Reports. Final strategy deployment and connected portfolios remain unfinished.
See [agent workflow](agent-workflow.md) for the canonical scope and limitations.
