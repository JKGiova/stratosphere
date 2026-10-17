# Phase 2 — research workflow

Status: **in progress**, October 17, 2026. The user authorized Phase 2 and selected **BTC/USDT spot hourly candles from Binance's public archive** for the first case. Phase 1 registry and five external content-source adapters remain unfinished; this work does not declare Phase 1 or a release complete.

## Implemented first research path

Pasted strategy text or code → hosted extraction through LangGraph → validated canonical specification and source evidence → explicit plan review → native NautilusTrader backtests → deterministic JSON/Markdown report.

This increment supports **one implementation family**: long-only fast/slow SMA crossover, BTCUSDT spot, one-hour closed candles, fixed BTC quantity, at most one position, no reinforcement/leverage/stops/other filters, and market orders at the next candle's open. The exact-rational crossing implementation uses a previous comparison after SMA warmup; initial alignment alone does not enter. Missing fields produce clarification questions. Unsupported behavior is disclosed rather than dropped. Invalid model JSON, fabricated evidence quotes and numeric parameters absent from their quoted passages block the plan. Human review remains necessary for the model's interpretation of the source.

The canonical `strategy/1` specification distinguishes behavioral identity from the experiment's costs and data. Changing a period or quantity changes the behavioral fingerprint; changing a name does not. These are **local research drafts**, not registered shared strategies or a Supabase registry implementation. Broader conversion of arbitrary external strategies is not verified by this template.

## Try it

Launch the existing client environment:

```sh
.venv/bin/python -m stratosphere
```

Connect the online model in agent chat, then enter `/new`. Paste a complete strategy into the inline research form and review/edit starting capital and cost assumptions. Example:

> Trade BTCUSDT spot on 1-hour closed candles. Compute a fast SMA with period 3 and a slow SMA with period 8, using the hourly close. When flat, buy exactly 0.01 BTC after fast SMA crosses above slow SMA. When long, sell that entire 0.01 BTC after fast SMA crosses below slow SMA. Submit each market order at the next hourly open. No shorting, leverage, reinforcement, stop loss, take profit, other filters or intrabar trading. Hold across sessions and leave end-of-sample exposure open.

This is an engineering test case, not an investment recommendation. The form's initial 10,000 USDT balance, 0.001 fee rate, 2 bps full spread and 1 bps slippage per side are editable simulation proposals. They are shown again in the approval plan and are not claims about a Binance account's fees or liquidity. No actual capital is used. The future stratosphere opening-order commission is not implemented or collected.

Choose **Review plan**, inspect the full rules, evidence and assumptions, then **Approve 2 simulations**. Approval binds the complete plan hash and installed implementation. It authorizes the base run plus one doubled-cost stress run, with no parameter search. No simulation starts before approval. Duplicate approvals and changed plans/implementations cannot silently start another run.

| Command | Implemented behavior |
| --- | --- |
| `/new [text]` | Open the source/cost form in agent chat; optional text prefills it |
| `/approve <job-id> <plan-sha256>` | Approve exactly the reviewed plan and start its simulations |
| `/jobs` | Select the Tests panel, which lists actual local jobs and their states |
| `/show <job-id>` | Inspect a job's plan/questions/report in Focus |
| `/report <job-id>` | Render a completed, integrity-checked report in chat |
| `/pause <job-id>` | Stop the active simulation safely, retaining completed checkpoints |
| `/resume <job-id>` | Resume the unchanged approved plan after fresh online data verification |
| `/cancel [job-id]` | Cancel active research/model work, or a pending/paused local job |

Tab/Shift+Tab still switch panels; Tab inside the research form moves fields. Enter adds a line in its source editor. Ctrl+Enter, where the terminal supports it, or the Review plan button submits. Keys detected in the source are neither saved nor sent. The model key never enters the native worker. Selecting an actual job row opens its details. Completed report metrics can be supplied to conversational chat as measured context; the model does not calculate authoritative results.

Approved active research pauses on TUI quit; completed checkpoints survive and can resume after restart. Extraction without an approved plan is canceled. Jobs are local to this computer/user directory; a running client holds a lock to prevent duplicate execution. Startup recovers interrupted jobs without automatically running them. This does not implement a persistent trading runtime or change the future confirmed keep/close-positions exit flow.

## Data and execution assumptions

The first reproducible profile fixes **January 1–31, 2025**, 744 hourly candles:

- [Binance public archive](https://github.com/binance/binance-public-data) documents monthly ZIP/checksum files and spot microsecond timestamps from January 2025.
- [Pinned BTCUSDT archive](https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2025-01.zip), SHA-256 `e7f2aaa5396a8062a57ab29ce9fab2d76b2c84ec58815d4ab7b3fe16f78c7118`.
- Every new run/resume fetches the archive and its checksum online. Changed archives, unexpected members, wrong timestamp units, missing/duplicate/unordered candles, invalid OHLCV and excessive precision fail explicitly. There is no cached/offline or fixture fallback.
- Raw archive, normalized rows, normalization version and hashes are retained. Provider terms still apply; no raw third-party market dataset is committed to GitHub.

NautilusTrader **1.231.0** executes a packaged trusted strategy in a separate subprocess. Model/user code is never imported or executed. The worker receives validated inputs and a minimal environment without API keys, privileged service credentials, proxy variables or user-site/PYTHONPATH settings. This is process separation for trusted code, **not an operating-system sandbox for arbitrary generated code**. A 120-second timeout per worker and cancellation both kill/wait for the child. Generated-code execution remains disabled pending an actual isolation design.

Closed candles drive signals. Separate synthetic quotes at the following hourly open drive market fills. The synthetic bid/ask applies half the full spread plus per-side slippage, with conservative eight-decimal rounding. Native fills and commissions update a cash account with borrowing disabled. Full fills/synthetic liquidity and candle-close availability are assumptions; depth, impact, true publication latency and historical Binance order filters are not reconstructed. The `SIM` spot instrument uses eight-decimal price/quantity increments and is not a verified broker/live adapter.

The engine's [backtesting concepts](https://nautilustrader.io/docs/latest/concepts/backtesting/) and [fill-price guide](https://nautilustrader.io/docs/latest/concepts/backtesting/fill-prices-and-matching/) inform these distinctions. Current online documentation can describe newer APIs; the actual pinned Python APIs and behavior are checked locally with native conformance tests.

## Reports and retained evidence

The report calculates from native fills and account equity: gross/net P&L and returns, commissions, spread/slippage cost, equity curve, drawdown, filled orders, completed/open trades, turnover, exposure, trade P&L distribution, the same-quantity buy-and-hold benchmark where affordable, and a doubled-cost stress comparison. The baseline is an explicitly calculated benchmark; the strategy execution and account observations come from NautilusTrader.

Chronological 70/30 development/validation segments carry positions and indicators. They are descriptive segments of this fixed case, **not an independent final holdout or an optimization/promotion gate**. No annualized risk ratio is invented. Limitations identify sample concentration, unknown execution conditions and the absence of paper/live verification. A positive result does not promote or activate a strategy.

Each job retains source, extracted evidence, canonical plan/specification, a trusted implementation-source snapshot, approval hash, native requests/results, data archive, report and event history. An artifact manifest checks report/input integrity before display. Files/directories are created with private permissions. Hashes detect changes; they are not an authenticated public performance certificate. Failed/canceled attempts remain. New behavior/configuration requires a newly reviewed plan rather than overwriting old artifacts.

Default storage: `~/Library/Application Support/stratosphere/research` on macOS; `$XDG_STATE_HOME/stratosphere/research` (default `~/.local/state/stratosphere/research`) on Linux. `STRATOSPHERE_RESEARCH_DIR` can choose an explicit local directory. Inspecting retained plans/reports through the app also checks online connectivity. This audit storage does not add an offline research mode, shared registry, catalog publication or authenticated multi-user service.

The normal client environment needs the optional pinned engine. Published-wheel hosts can install with `uv sync --locked --extra engine`. Intel macOS uses the [verified custom-wheel procedure](intel-macos.md); the existing project `.venv` has been updated with that pinned wheel and locked dependencies. No source rebuild was claimed for this update.

## Verification

**213 tests passed locally with zero skips**: 206 core and seven mandatory native-engine tests, using the verified pinned Intel wheel. Lint and formatting pass. A subsequent focused four-test TUI run also passed after the final visual corrections: single-row tables remain visible above a horizontal scrollbar, and plan approval controls scroll into view after layout.

The suite covers source/JSON/parameter/evidence validation, exact behavior identity, full monthly data quality, online-only inspection, exact-plan approval and duplicate-start rejection, private immutable artifacts and report integrity, locking/recovery, pause/resume checkpoint reuse, cancellation and child cleanup, credential-free subprocess environments, form privacy/navigation/resizing, escaped model text in Focus, report/context routing and unchanged chat controls. A deterministic pending-focus test covers canceling onboarding while its removed form is still awaiting cleanup. Independent batch SMA and cash/fee calculations match the native engine; repeated/prefix runs verify causality, costs, account equity and unforced end exposure. Existing upstream Pandas deprecation warnings occur at engine execution (29 observations in this suite); no skip is counted as native verification.

Three complete real-model TUI acceptance cases passed through source review and native base/stress workers. The latest completed in **44.533 seconds** with **167 streamed extraction deltas**, all **744 candles**, **92 filled simulated orders**, **46 completed simulated trades**, retained evidence and an identical independent native replay. Two additional live cases returned clarification questions for missing SMA periods/quantity and unsupported status for RSI/stop-loss behavior; neither started a simulation. All five hosted extraction requests reported usage cost **0 USD**. These are bounded availability/conformance observations, not latency, profitability or model-quality guarantees.

A clean environment installed the built wheel with locked dependencies and the verified native Intel wheel. Using the recorded validated extraction rather than another model request, that installed package fetched fresh online data, launched its own two native subprocesses and exactly matched the live case's base/stress metrics. The package includes all research modules and the stylesheet; distribution checks exclude environment/cache files and the removed maximize launcher. An actual packaged CLI in a PTY preserved window size, rendered inline onboarding, handled five resize events and quit cleanly. The smoke check waits for the rendered interface rather than assuming a two-second cold start.

Source review, real reports/jobs, compact Library and approval controls were visually inspected. Exact-credential scans cover tracked/new files, current evidence and decompressed distributions; keys are not committed. Sanitized evidence and private engineering artifacts are retained under ignored `reports/phase2/`. GitHub CI status is recorded separately after publication.

### GitHub CI status

Implementation commit `d7bf9932ea6c472e7cae57473603aa242febd6a7` is published in the private repository. [Platform run 39037040147](https://github.com/JKGiova/stratosphere/actions/runs/39037040147) and [Intel engine run 39037040058](https://github.com/JKGiova/stratosphere/actions/runs/39037040058) failed before any job steps executed. All five check annotations report the existing account payment/spending-limit restriction; the macOS ARM64 capacity advisory is also present. No fresh cross-platform or Intel-source-build pass is claimed. The final 213-test local suite, real free-model cases, installed-wheel/native replay, visual and packaged-CLI checks passed independently. No account billing setting was changed.

## Remaining Phase 2 work

- Broader validated strategy families/conversion, with conformance cases and explicit capability boundaries.
- Controlled execution of generated implementations only after an actual isolation/resource/network design is verified.
- More historical windows/instruments/providers, publication-time evidence and venue metadata; preserve BTC/USDT as the first selected case rather than a market-scope restriction.
- Richer intervention and durable workflow behavior, protocol configuration and acceptance cases beyond the initial fixed snapshot/template.
- Phase 1's shared registry, ownership/service integration and all five content-source adapters still require implementation and verification. Pasted source text does not fulfill those adapters.

No release, source-adapter completion, profitability, paper/live integration or monetization readiness is claimed.
