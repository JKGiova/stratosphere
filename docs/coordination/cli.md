# CLI

## State

- Revision: 1
- Status: initialized; awaiting a manually assigned module task
- Source baseline: `b3d2ad9`
- Reports inspected: initial reports seeded together by the coordination setup job; no module job has reviewed peer revisions yet.
- Work state: coordination documentation only; module implementation unchanged.

## Interface

**Implemented / current:** Package help, MT5 operator command dispatch and compatibility inspection. General strategy/research commands are not implemented.

**Consumed / proposed:** The existing MT5 connector and compatibility inspector. New research commands should call shared strategy/Nautilus operations also available to MCP, without duplicating their rules.

**Starting points:** `src/stratosphere/__main__.py`, `mt5/__main__.py`, `phase0/compatibility.py`; [usage](../usage.md).

## Completed changes

Confirmed the root dispatcher currently exposes `mt5` and `compatibility`. No CLI implementation or installed command changed.

## Dependencies and blockers

Strategy/Nautilus operation contracts and shared structured results need agreement. Separate operator approval/account diagnostics from strategy-facing operations; a command flag is not proof of human authority.

## Verification

Source inspection for this initial handoff only. No new module tests, native run, provider/model
call, live order or EA compilation was performed. Earlier verification is recorded in the focused
guides and must not be represented as verification of a future module change.

## Proposals for owner review

No new cross-module contract is approved by this report. Record concrete proposals here during the
assigned job, naming affected modules and the required owner decision.

## Next task

Suggested for the owner to launch: Propose the minimum CLI command/input/output/error surface over existing engine operations, clearly distinguishing implemented commands from proposed ones.
