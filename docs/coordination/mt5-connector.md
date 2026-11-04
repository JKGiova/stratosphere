# MT5 Python connector

## State

- Revision: 1
- Status: initialized; awaiting a manually assigned module task
- Source baseline: `b3d2ad9`
- Reports inspected: initial reports seeded together by the coordination setup job; no module job has reviewed peer revisions yet.
- Work state: coordination documentation only; module implementation unchanged.

## Interface

**Implemented / current:** `MT5Bridge` local request/read transport, data validation, exact sessions, private durable journals and guarded `MT5Trading` deployment/order/close operations. A general Nautilus-facing adapter remains pending.

**Consumed / proposed:** Existing EA protocol; exact strategy/evidence/account/implementation approval bindings. Nautilus will need a data/execution adapter contract; CLI operator commands already use the connector.

**Starting points:** `src/stratosphere/mt5/bridge.py`, `mt5/trading.py`; [MT5 connector](../mt5-bridge.md).

## Completed changes

Identified current transport/read/journal implementation and its distinction from a future Nautilus adapter. No connector or approval pins changed.

## Dependencies and blockers

EA protocol and Nautilus runtime requirements must agree before cross-module changes. Preserve raw broker timestamp semantics, consumed requests and owned protective exits.

## Verification

Source inspection for this initial handoff only. No new module tests, native run, provider/model
call, live order or EA compilation was performed. Earlier verification is recorded in the focused
guides and must not be represented as verification of a future module change.

## Proposals for owner review

No new cross-module contract is approved by this report. Record concrete proposals here during the
assigned job, naming affected modules and the required owner decision.

## Next task

Suggested for the owner to launch: Map existing EA transport/data/order semantics to the minimum proposed Nautilus adapter interface and list gaps for owner review.
