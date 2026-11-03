# External-agent workflow

Agent frameworks own conversations, model selection, source discovery coordination and strategy
proposals. stratosphere owns validated strategy programs, data/backtests, stable identities,
evidence, frozen budgets, human approvals, deterministic risk checks and MT5 submission.
The TUI, command router, internal crew and persistent research tool runner are removed.

## Current engine operations

The [development engine](strategy-development.md) accepts an externally supplied typed definition
and an existing sealed intake, freezes exact target scopes and executes one program with base/stress
costs. It retains failed attempts, compiled bytes, concrete variant owners and selected evidence.
Historical research/study services and one-shot source/report interpretation adapters remain for
compatibility. There is no general external-agent/MCP interface or automatic research-to-MT5 bridge.
[Headless usage](usage.md) owns current command entry points.

Stable registry ownership, duplicate reuse and explicit variant lineage remain application-owned.
Removing conversation code does not delete private historical turns, report artifacts or local
journals. Historical research approvals and v1/v2 formats keep their original meaning. Historical
persistent checkpoints are read-only and cannot resume, replay starts or obtain fresh budgets.

## Target responsibility split

An external orchestrator handles user intent and research coordination. A strategy specialist
supplies a validated definition and interprets its own retained results. Application code checks
source/definition/data ownership and finite research authority. A finished strategy requires one
human approval binding complete evidence, exact account/server/mode, implementation and limits.
Only that deployment permits automatic orders within its limits; connection grants no authority.

Strategy and risk agents may propose trigger reliability, exposure, capital and leverage. The engine
rechecks exact ownership, freshness, account exposure, assignments and available funds before
reservation/submission. Unavailable or invalid entry review denies entry. Protective exits remain
deterministic and independent of an LLM. Account diagnostics and credentials stay outside strategy
model tools. Models cannot approve deployments, run arbitrary acquired code/SQL or submit orders.

## MT5 compatibility

MT5 is the sole active broker connector. [MT5 connector](mt5-bridge.md) owns local transport,
exact deployment approval, no-uncertain-replay and owned exits. Existing
[overnight deployments](mt5-overnight.md) and the retired Gold adapter retain their exact model/risk
implementation pins and protective-exit behavior. They are separate operator-defined adapters,
not the removed general research crew. Removing their shared dependencies requires an explicit
compatibility/external-review design; this cleanup does not change running services or positions.

A Yahoo/spot backtest, source citation, compiler hash or authored fixture cannot establish faithful
CFD behavior, FTMO rules, account ownership, executable attestation or trading authority.
