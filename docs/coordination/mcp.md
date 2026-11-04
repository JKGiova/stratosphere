# MCP server

## State

- Revision: 1
- Status: initialized; awaiting a manually assigned module task
- Source baseline: `b3d2ad9`
- Reports inspected: initial reports seeded together by the coordination setup job; no module job has reviewed peer revisions yet.
- Work state: coordination documentation only; module implementation unchanged.

## Interface

**Implemented / current:** No general external-agent MCP server exists in the application. The agreed target is a typed tool/resource adapter over the same operations as the CLI.

**Consumed / proposed:** Shared strategy/Nautilus operations and structured results; connector data only through an explicitly allowed research-safe surface. Transport/library and tool names remain proposals.

**Starting points:** [usage](../usage.md), [agent responsibilities](../agent-workflow.md), `src/stratosphere/__main__.py`; inspect implementation/dependencies before selecting an MCP library.

## Completed changes

Recorded the currently missing MCP product interface. No server, tool registration, dependency or network listener added.

## Dependencies and blockers

Agree strategy/Nautilus contracts with those modules and align result/error behavior with CLI. Keep credentials, private account diagnostics, human approval and direct broker submission outside strategy tools.

## Verification

Source inspection for this initial handoff only. No new module tests, native run, provider/model
call, live order or EA compilation was performed. Earlier verification is recorded in the focused
guides and must not be represented as verification of a future module change.

## Proposals for owner review

No new cross-module contract is approved by this report. Record concrete proposals here during the
assigned job, naming affected modules and the required owner decision.

## Next task

Suggested for the owner to launch: Propose a minimal MCP tool/resource list and transport choice using verified protocol/library documentation, without adding a server or exposing an endpoint.
