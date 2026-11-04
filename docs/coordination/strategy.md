# Strategy contracts and compiler

## State

- Revision: 1
- Status: initialized; awaiting a manually assigned module task
- Source baseline: `b3d2ad9`
- Reports inspected: initial reports seeded together by the coordination setup job; no module job has reviewed peer revisions yet.
- Work state: coordination documentation only; module implementation unchanged.

## Interface

**Implemented / current:** Typed `Definition`/`StrategySpec` contracts, rule-index validation and `compile_strategy`/`load_generated` for supported application-generated strategies.

**Consumed / proposed:** Nautilus integration consumes validated definitions and compiled strategy bytes. CLI/MCP will need the shared validation/result contracts; those public interfaces are not implemented.

**Starting points:** `src/stratosphere/development/contracts.py`, `development/compiler.py`, `phase0/contracts.py`; [strategy development](../strategy-development.md).

## Completed changes

Identified existing typed definitions/compiler as the starting point. No contract/compiler implementation changed.

## Dependencies and blockers

Nautilus integration must confirm the runtime strategy interface. CLI/MCP public request/result shapes remain unagreed; do not invent a second strategy representation.

## Verification

Source inspection for this initial handoff only. No new module tests, native run, provider/model
call, live order or EA compilation was performed. Earlier verification is recorded in the focused
guides and must not be represented as verification of a future module change.

## Proposals for owner review

No new cross-module contract is approved by this report. Record concrete proposals here during the
assigned job, naming affected modules and the required owner decision.

## Next task

Suggested for the owner to launch: Inspect the existing supported strategy contract and propose the smallest public validation/submission interface, including its unsupported cases.
