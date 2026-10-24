# stratosphere development plan

Updated: October 24, 2026. This is the single active development plan.

## Product and decisions

stratosphere is a private personal terminal tool in tradingsphere, initially for Linux and macOS.
Keep English, responsive panels, chat/slash commands, local SQLite, private artifacts and native OS
credential storage. No billing, app login, hosted user database, strategy sharing or release deadline.

Historical research uses Yahoo/yfinance and NautilusTrader. Target connections are Alpaca paper/live,
Liquid paper/live and FTMO two-step Swing evaluation, with separate provider/account/mode identities.
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
| Paper portfolios | Separate persisted Alpaca/Liquid paper identities, stable wallet agents and exact account bindings | Liquid execution is blocked on provider capability; no paper/live equivalence |
| Alpaca paper | Fixed paper/data origins, native-stored keys, closed crypto bars, binary/capital/size calls, deterministic gates, reserved IOC orders, read-back, partial/uncertain states and account lease | Authored tests only until real account/feed/order checks; crypto spot only; not sustained or release-ready |
| Liquid | Existing read-only connection/catalog/account diagnostics; explicit paper capability blocker | Current catalog has review links, not isolated automatic paper submission; perpetual research/transport is missing |
| Historical services | Original research/studies, source/evidence readers, authored ledger/runtime, schemas and journals retained | Preserve ownership and old report bytes; do not present fixture journals as real provider accounts |
| Live / FTMO | Scope and identity boundaries only | No live adapter/activation; actual FTMO platform/account and Swing evaluation rules need verification |

## Next development gates

1. **Finish paper lifecycle before operational acceptance.** Add safe explicit stream rewarming,
   approval retirement/implementation upgrades, inspected halt recovery and emergency close-all.
   Finish keep-positions versus close-all exit controls, failed closures and residual-position UI.
   Current stop/quit keeps positions and stops monitoring; gaps halt without backfill.
2. **Verify Alpaca using its real paper account.** Test native credential persistence, permitted feed
   access, exact instrument/precision, real order IDs/partial fills/fees, network ambiguity, competing
   clients, restart and sustained operation. Do not send a paper order before an exact final package
   is approved. Preserve provider evidence and failures. Fixtures are not provider acceptance.
3. **Unblock Liquid paper separately.** Obtain/review an actual paper-only order capability with
   per-request isolation and required provider authorization. Implement perpetual market semantics,
   funding/margin/liquidation and faithful research before assigning spot research to that environment.
   A global paper-mode toggle or a read catalog is insufficient.
4. **Broaden research and markets.** Implement additional strategy families and conformance, richer
   comparison reports, verified sessions/corporate actions for equities, Forex venue semantics,
   regimes and frozen-selection holdouts. Dynamic sizing needs its own measured simulation evidence.
5. **Other environments.** FTMO needs the user's actual platform, two-step Swing account/phase,
   contract/fee/swap details and dated loss/reset rules. Live execution requires separate explicit
   activation and its own safety/operational acceptance after paper verification.

## Verification and release gate

Run focused tests for each change, then necessary integration/compatibility checks. Keep failed logs
and environmental limits. Distinguish authored fixtures/native simulation, actual model calls,
provider checks and installed-client readiness. No phase or release is complete from a narrower test,
profitable replay or adapter implementation. Require sustained operation, recovery/exit controls,
verified credentials and evidence ownership for every advertised feature.
