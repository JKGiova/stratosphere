# MT5 Expert Advisor

## State

- Revision: 1
- Status: initialized; awaiting a manually assigned module task
- Source baseline: `b3d2ad9`
- Reports inspected: initial reports seeded together by the coordination setup job; no module job has reviewed peer revisions yet.
- Work state: coordination documentation only; module implementation unchanged.

## Interface

**Implemented / current:** Terminal-side local bridge operations, data reads, native checks and order submission/confirmation under existing exact deployment limits. This is not yet a general Nautilus execution integration.

**Consumed / proposed:** The Python connector exchanges versioned requests/responses and exact session/deployment bindings with the EA. Changes must preserve native uncertainty handling and exactly owned exits.

**Starting points:** `tools/mt5/StratosphereBridge.mq5`; [MT5 connector](../mt5-bridge.md) and relevant connector/exit invariants.

## Completed changes

Located `OnInit`, `OnTimer`, `OrderCheck` and `OrderSend` in the current bridge source. No MQL5 source changed and no terminal operation ran.

## Dependencies and blockers

Coordinate protocol/data/execution requirements with the connector report. Nautilus-specific needs are not an accepted EA protocol change. Current implementation/EA pins constrain compatibility.

## Verification

Source inspection for this initial handoff only. No new module tests, native run, provider/model
call, live order or EA compilation was performed. Earlier verification is recorded in the focused
guides and must not be represented as verification of a future module change.

## Proposals for owner review

No new cross-module contract is approved by this report. Record concrete proposals here during the
assigned job, naming affected modules and the required owner decision.

## Next task

Suggested for the owner to launch: Document the actual EA request/response and failure semantics needed by the Python connector, separating existing support from proposed extensions.
