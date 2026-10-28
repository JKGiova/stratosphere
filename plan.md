# stratosphere development plan

Updated: October 28, 2026. This is the single active development plan.

## Product and decisions

stratosphere is a private personal terminal tool in tradingsphere, initially for Linux and macOS.
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

The requested workflow is orchestrator bridge → exact strategy research agent → final report/package
→ one user approval → portfolio risk agent → binary entry review → position sizing → application risk
gates → automatic orders within approved limits. Research runs under finite app limits, without a
human approval prompt. There is no per-order approval within a reviewed MT5 deployment. Existing historical
protocols retain their original readers and authorization semantics.

Canonical behavior and commands: [agent workflow](docs/agent-workflow.md).
Mandatory boundaries: [invariants](docs/invariants.md). Other guides: [docs index](docs/README.md).

## Current implementation and evidence

| Area | Implemented | Limits / remaining verification |
| --- | --- | --- |
| Orchestrator and specialists | Typed research routing, exact stable owners, isolated histories, duplicate reuse and variant lineage | Supported family is SMA; broader tools, faithful conversion and regime research remain unfinished |
| Automatic research | New v2 campaigns; at most 12 candidates / 24 native starts, all attempts retained, no consumed-start replay, sealed reports and TUI views | No generic optimizer, independent holdout or global-best claim; real model reliability still needs checks |
| Final strategy | Exact selected report/definition/account/instrument/implementation and complete paper limits; one human approval and inactive rejection | Historical fixed-quantity evidence does not validate variable paper-policy returns; retirement/upgrades remain unfinished |
| Broker setup | Sole active MT5 role; Alpaca/Liquid commands and automatic credential restoration retired | Historical journals, contracts and readers remain; their approvals cannot activate MT5 |
| MT5 bridge | Local typed EA/client, exact account/session binding, account/specification/quote/bar/position/order/deal reads; private durable request journal | Linux deployment, sustained feed freshness, timestamp/session/contract-cost semantics and installed EA verification require checks |
| MT5 execution | Exact operator deployment approval, finite order/volume/cash-risk/margin/spread/equity bounds, mandatory SL, native checks, FOK/IOC entry, owned close/revoke, no uncertain replay | One account position at a time; authored lifecycle tests and compilation are not actual broker order acceptance. No automatic agent runner, FTMO daily/reset rules or multi-strategy allocation integration yet |
| Historical services | Original research/studies, source/evidence readers, authored ledger/runtime, schemas and journals retained | Historical provider fixtures are not active accounts or transferred approvals |
| Gold data | Actual MT5 export validated for all 21 intervals; H2–D1 reach June 2004 | Short intervals hit 100k terminal limit; full archives/UTC/session/cost semantics remain unverified; this is acquisition, not a CFD backtest |

## Next development gates

1. **Verify the connected MT5 bridge.** Attach the compiled EA with trading disabled. Verify exact
   account/session, real read-only data, disconnect/account-switch/restart behavior and private
   evidence retention. Then test broker checks/order/fill/close semantics only under one reviewed
   exact finished-strategy deployment and limits. Do not infer acceptance from authored tests.
2. **Connect the agent workflow to MT5.** Build a faithful CFD research and final-report validator,
   exact registered strategy/report ownership and instrument mapping, timestamp/session/cost
   semantics, signal evidence and MT5-specific portfolio risk/allocation. The new connector's manual
   deployment approval does not complete the automatic research-to-execution pipeline. Retain old
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
Distinguish authored fixtures/native simulation, actual model calls,
provider checks and installed-client readiness. No phase or release is complete from a narrower test,
profitable replay or adapter implementation. Require sustained operation, recovery/exit controls,
verified credentials and evidence ownership for every advertised feature.
