# NautilusTrader integration

## State

- Revision: 1
- Status: initialized; awaiting a manually assigned module task
- Source baseline: `b3d2ad9`
- Reports inspected: initial reports seeded together by the coordination setup job; no module job has reviewed peer revisions yet.
- Work state: coordination documentation only; module implementation unchanged.

## Interface

**Implemented / current:** Existing native historical simulation through `BacktestEngine` and credential-free development workers. The agreed target also includes continuous strategy execution through Nautilus; feasibility and the general MT5 path remain unverified.

**Consumed / proposed:** Validated compiled strategies and frozen datasets; the MT5 connector would provide the broker data/execution integration. Continuous runtime contracts are proposed, not implemented.

**Starting points:** `src/stratosphere/development/worker.py`, `development/service.py`, `yahoo/engine.py`, `research/engine.py`; [strategy development](../strategy-development.md).

## Completed changes

Located existing simulation/worker entry points and retained v3 finite-run behavior. No native engine/runtime code changed.

## Dependencies and blockers

Strategy contracts define supported behavior. Connector/EA runtime integration requires a shared contract and verification; any cross-module change needs owner review. Nautilus wheel builds stay in the separate repository.

## Verification

Source inspection for this initial handoff only. No new module tests, native run, provider/model
call, live order or EA compilation was performed. Earlier verification is recorded in the focused
guides and must not be represented as verification of a future module change.

## Proposals for owner review

No new cross-module contract is approved by this report. Record concrete proposals here during the
assigned job, naming affected modules and the required owner decision.

## Next task

Suggested for the owner to launch: Verify how the existing simulation integration can support a common backtest/live strategy interface; identify the minimum MT5 adapter requirements without implementing them.
