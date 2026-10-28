# stratosphere development plan

Updated: October 28, 2026. This is the single active development plan.

## Product and decisions

stratosphere is a private personal terminal tool in tradingsphere, initially for Linux and macOS.
Keep English, responsive panels, chat/slash commands, local SQLite, private artifacts and native OS
credential storage. No billing, app login, hosted user database, strategy sharing or release deadline.
NautilusTrader wheel production belongs to the separate `nautilus-trader-macos-intel` repository.
stratosphere consumes its pinned Intel artifact and matching runtime dependencies; Linux and Apple
Silicon retain upstream wheels. No engine build tooling or vendored wheel belongs in this project.

Historical research uses Yahoo/yfinance and NautilusTrader. Portfolio roles are **Alpaca paper**,
**Liquid live perpetuals** and **FTMO two-step Swing evaluation through MT5 or a verified equivalent**,
with separate provider/account/mode identities. Alpaca live is deferred, not a current implementation
target. Liquid paper remains historical compatibility only; do not convert its journals or approvals
into live state. The selected roles do not claim adapter availability or authorize activation.
Current execution authorization covers **paper only**. Never purchase data/capacity/challenges or
activate live trading without an explicit request.

The requested workflow is orchestrator bridge → exact strategy research agent → final report/package
→ one user approval → portfolio risk agent → binary entry review → position sizing → application risk
gates → automatic orders within approved limits. Research runs under finite app limits, without a
human approval prompt. There is no per-order approval in the new paper workflow. Existing historical
protocols retain their original readers and authorization semantics.

Canonical behavior and commands: [agent workflow](docs/agent-workflow.md).
Mandatory boundaries: [invariants](docs/invariants.md). Other guides: [docs index](docs/README.md).

## Current implementation and evidence

| Area | Implemented | Limits / remaining verification |
| --- | --- | --- |
| Orchestrator and specialists | Typed research routing, exact stable owners, isolated histories, duplicate reuse and variant lineage | Supported family is SMA; broader tools, faithful conversion and regime research remain unfinished |
| Automatic research | New v2 campaigns; at most 12 candidates / 24 native starts, all attempts retained, no consumed-start replay, sealed reports and TUI views | No generic optimizer, independent holdout or global-best claim; real model reliability still needs checks |
| Final strategy | Exact selected report/definition/account/instrument/implementation and complete paper limits; one human approval and inactive rejection | Historical fixed-quantity evidence does not validate variable paper-policy returns; retirement/upgrades remain unfinished |
| Portfolio setup | Alpaca paper, planned Liquid live perpetuals and planned FTMO evaluation shown separately; retained paper journals and exact account bindings | Only Alpaca paper has an operational workflow; planned identities have no account binding or execution authority |
| Alpaca paper | Fixed paper/data origins, native-stored keys, closed crypto bars, binary/capital/size calls, deterministic gates, reserved IOC orders, read-back, partial/uncertain states and account lease | Authored tests only until real account/feed/order checks; crypto spot only; not sustained or release-ready |
| Liquid | Existing read-only connection/catalog/account diagnostics; historical paper blocker retained | Live perpetual feed, research semantics, permissions and order transport are missing; read access grants no trading authority |
| Historical services | Original research/studies, source/evidence readers, authored ledger/runtime, schemas and journals retained | Preserve ownership and old report bytes; do not present fixture journals as real provider accounts |
| FTMO | Manually launched read-only MT5 diagnostic and local export validator ([guide](docs/mt5.md)); compiled with installed macOS Wine MetaEditor | No execution bridge/activation; diagnostic samples do not verify full history, account/profile, evaluation phase or Swing rules |

## Next development gates

1. **Finish paper lifecycle before operational acceptance.** Add safe explicit stream rewarming,
   approval retirement/implementation upgrades, inspected halt recovery and emergency close-all.
   Finish keep-positions versus close-all exit controls, failed closures and residual-position UI.
   Current stop/quit keeps positions and stops monitoring; gaps halt without backfill.
2. **Verify Alpaca using its real paper account.** Test native credential persistence, permitted feed
   access, exact instrument/precision, real order IDs/partial fills/fees, network ambiguity, competing
   clients, restart and sustained operation. Do not send a paper order before an exact final package
   is approved. Preserve provider evidence and failures. Fixtures are not provider acceptance.
3. **Build Liquid live perpetual support separately.** Verify the actual feed/order capability,
   scoped trading authorization and exact live account. Implement perpetual market semantics,
   funding/margin/liquidation, faithful research and live-specific risk/reconciliation controls.
   Historical spot research, paper approvals and the read-only grant cannot authorize this route.
   Activation needs a separate explicit request after implementation and operational acceptance.
4. **Broaden research and markets.** Implement additional strategy families and conformance, richer
   comparison reports, verified sessions/corporate actions for equities, Forex venue semantics,
   regimes and frozen-selection holdouts. Dynamic sizing needs its own measured simulation evidence.
5. **FTMO bridge.** Start from the read-only MT5 diagnostic; verify the actual connected trial and
   acquire bounded full historical datasets before claiming backtest coverage. MT5 is the preferred
   bridge, with a verified equivalent if required. Verify Linux/macOS deployment, Swing account/phase,
   contract/fee/swap details and dated loss/reset rules. Do not treat evaluation balances as ordinary
   broker paper or personal live funds. Bridge activation and challenge purchases are separate actions.
   Alpaca live remains deferred.

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
