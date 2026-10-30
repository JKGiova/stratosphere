# stratosphere development plan

Updated: October 30, 2026. This is the single active development plan.

## Product and decisions

stratosphere is a personal terminal tool in tradingsphere, initially for Linux and macOS.
The source repository is public; credentials, account data and operational artifacts remain private.
Keep English, responsive panels, chat/slash commands, local SQLite, private artifacts and native OS
credential storage. No billing, app login, hosted user database, strategy sharing or release deadline.
NautilusTrader wheel production belongs to the separate `nautilus-trader-macos-intel` repository.
stratosphere consumes its pinned Intel artifact and matching runtime dependencies; Linux and Apple
Silicon retain upstream wheels. No engine build tooling or vendored wheel belongs in this project.

MT5 is the sole active broker data and order connector, including the FTMO account. Alpaca and
Liquid are retired from active commands/setup; their formats, journals, evidence and readers remain
under legacy compatibility. Yahoo/yfinance and NautilusTrader historical research remain available
separately; a spot/ETF result cannot silently become a broker-CFD deployment. No historical approval
transfers authority to MT5. Broker login stays inside the terminal. Connection/installation never
activates trading. Real-account activation requires separate explicit application and terminal
permission. No challenge purchase is authorized.

The requested workflow is orchestrator intake → exact strategy research agent → final report/package
→ one user approval → portfolio capital allocation → strategy trigger/agent investment proposal
→ position risk review (exposure, capital and leverage) → application gates → execution within limits.
The orchestrator never submits orders; models only propose and deterministic application code enforces
limits and owns broker submission. Research runs under finite app limits, without a
human approval prompt. There is no per-order approval within a reviewed MT5 deployment. Existing historical
protocols retain their original readers and authorization semantics.

Canonical behavior and commands: [agent workflow](docs/agent-workflow.md);
[strategy-agent development](docs/strategy-development.md) owns code generation and comparative research.
Mandatory boundaries: [invariants](docs/invariants.md). Other guides: [docs index](docs/README.md).

## Current implementation and evidence

| Area | Implemented | Limits / remaining verification |
| --- | --- | --- |
| Orchestrator and specialists | Typed routing, isolated clarification drafts, public source discovery with optional Agent Reach/OpenCLI/anonymous-browser fallback, local PDF/page extraction and sealed source-grounded strategy briefs; exact registered owners, duplicate reuse and variant lineage | Descriptions may cover any family; unsupported execution semantics remain blocked. Faithful conversion, independent validation and MT5 integration remain unfinished |
| Legacy SMA research | `research-campaign/2` campaigns; at most 12 candidates / 24 native starts, all attempts retained, no consumed-start replay, sealed reports and TUI views | No generic optimizer, independent holdout or global-best claim; real model reliability still needs checks |
| Strategy development | Complete brief → stable strategy agent → validated SMA/EMA/RSI expression program → application-compiled Python → native Yahoo comparisons and a persistent typed-tool loop covering inspection, signal edits, tests, analysis and report preview with checkpoint continuation and application-owned progress stopping; all attempts, code, scopes and descriptive year/regime summaries retained | V2: 4 frozen targets, 32 model calls / 128 reserved native starts / finite active time; first insufficient evaluated revision stops. V1 limits/readers preserved. Long-only fixed units, hourly/daily; no stop orders, shorts or arbitrary Python. Live-model reliability, semantic source conformance and independent holdout remain unverified |
| Final strategy | Exact selected report/definition/account/instrument/implementation and complete paper limits; one human approval and inactive rejection | Historical fixed-quantity evidence does not validate variable paper-policy returns; retirement/upgrades remain unfinished |
| Broker setup | Sole active MT5 role; Alpaca/Liquid commands and automatic credential restoration retired | Historical journals, contracts and readers remain; their approvals cannot activate MT5 |
| MT5 bridge | Local typed EA/client, exact account/session binding and private durable journal; actual Intel macOS FTMO demo reads passed for account/specs/advancing quotes/M1-H1-D1 candles/positions/orders/deals with trading disabled | Linux deployment, disconnect/account-switch/restart, sustained feed freshness, timestamp/session/cost semantics and independent executable attestation remain unverified |
| MT5 execution | Exact operator deployment approval, finite order/volume/cash-risk/margin/spread/equity bounds, mandatory SL, native checks, FOK/IOC entry, owned close/revoke, no uncertain replay; actual one-time demo entry/owned close and broker deal-history reconciliation passed | One account position at a time; diagnostic acceptance does not validate the finished-strategy deployment path. No automatic agent runner, FTMO daily/reset rules or multi-strategy allocation integration yet |
| Gold strategy connection | Application-owned XAUUSD D1 50/200 regime, generated script, stable registry owner, current EA data, exact demo-only one-entry deployment, isolated bounded sizing adapter and deterministic owned exit | One tick per invocation; actual connected bearish/flat observation passed without sizing or orders. No CFD performance validation, strategy-order acceptance, scheduler or portfolio-management loop |
| Historical services | Original research/studies, source/evidence readers, authored ledger/runtime, schemas and journals retained | Historical provider fixtures are not active accounts or transferred approvals |
| Gold data | Actual MT5 export validated for all 21 intervals; H2–D1 reach June 2004 | Short intervals hit 100k terminal limit; full archives/UTC/session/cost semantics remain unverified; this is acquisition, not a CFD backtest |

## Next development gates

1. **Extend connected MT5 verification.** Initial real read-only client/EA checks passed with
   trading disabled. Verify disconnect/account-switch/restart behavior, sustained feed and private
   evidence retention. A separately approved one-time demo connector round-trip passed; next verify
   the normal finished-strategy deployment path and limits. Do not infer sustained readiness from
   one successful diagnostic or acceptance from authored tests.
2. **Connect the agent workflow to MT5.** Source-to-specialist compiled historical research now
   runs separately. Build a faithful CFD research and final-report validator,
   exact registered strategy/report ownership and instrument mapping, timestamp/session/cost
   semantics, signal evidence and MT5-specific portfolio risk/allocation. The headless gold pilot
   connects one explicit operator strategy; its manual deployment approval does not complete the automatic research-to-execution pipeline. Retain old
   research and final-approval contracts without routing them to MT5.
3. **Implement FTMO evaluation rules.** Verify actual Free Trial/two-step Swing profile and phase,
   dated rules, timezone/day-reset equity accounting, commissions/swaps, daily/total loss stops,
   restart/reconciliation and exposure ownership. Do not treat a demo flag as Swing certification.
4. **Finish operational lifecycle.** Add sustained monitoring, inspected ambiguity recovery,
   implementation upgrades, safe stream rewarming, portfolio allocations/multiple owned positions,
   emergency close-all and keep/close exit controls. Current connector exit is an explicit owned
   ticket close; expiry/revocation block entry but retain protective exit policy. Process exit never
   automatically closes positions or continues application monitoring.
5. **Broaden faithful research.** Additional strategy families, broker instruments, session and
   corporate-action semantics, regimes, frozen-selection holdouts and measured dynamic sizing.
   Preserve all failed/rejected attempts and separate data access, model reliability and profitability.

## Verification and release gate

Run focused tests for each change, then necessary integration/compatibility checks. Keep failed logs
and environmental limits. Keep native integration tests in the engine-marked suite, including
orchestrator/TUI paths; engine-free compatibility must not depend on an installed native engine.
The cross-platform application matrix installs verified dependency wheels without compiling them;
external wheel release readiness and application acceptance remain separate checks.
Source-intake verification on October 29, 2026 includes actual local PDF parsing, authored HTTP/model
fixtures and an actual public Wikipedia/Crossref search plus RSI page retrieval. These checks do not
establish live-model extraction reliability or faithful strategy conversion. Specialist development
now has actual generated-code Nautilus fixtures and a separate actual AMZN/TSLA Yahoo comparison
with authored model replies: four cells / eight native starts completed. A real local Ollama check from an authored baseline also completed two native starts; an invalid
revision was rejected. Small-model formalization/refinement reliability remains unresolved. The
general research-to-MT5 agent pipeline remains unconnected; the headless gold pilot is a separate
one-strategy adapter. See the [development guide](docs/strategy-development.md) for exact limits and
evidence.

Distinguish authored fixtures/native simulation, actual model calls,
provider checks and installed-client readiness. No phase or release is complete from a narrower test,
profitable replay or adapter implementation. Require sustained operation, recovery/exit controls,
verified credentials and evidence ownership for every advertised feature.
