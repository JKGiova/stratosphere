# Repository instructions

## Scope and source of truth

- Use [plan.md](plan.md) as the single development plan and [docs/README.md](docs/README.md) as the documentation index.
- stratosphere is a personal tool in the tradingsphere suite. Keep both names lowercase.
- Keep the repository public, as authorized by the owner. Do not change visibility or licensing without an explicit request. Keep credentials, account data and operational artifacts private.
- Do not add billing, application login, a hosted user database or automatic strategy sharing.
- MT5 is the sole active data/execution connector, including the FTMO account. Alpaca and Liquid are retired from active routing; retain their historical readers, journals and evidence under legacy compatibility. Keep exact broker server/account/mode identities separate; connecting never activates trading.
- The requested workflow rebuild takes priority: orchestrator bridge → strategy research agents → reviewed final strategy → portfolio risk agent → binary signal review → position sizing. Preserve historical readers and evidence while replacing active routing. Do not resume unrelated interrupted features.

## Development workflow

- Write code, comments, documentation and Git material in English.
- Keep changes and their history on GitHub. Publish only the intended scope; preserve unrelated working-tree changes.
- Keep NautilusTrader wheel builds/releases in `JKGiova/nautilus-trader-macos-intel`. This repository consumes pinned artifacts and validates application behavior; do not vendor wheels or reintroduce engine build tooling here.
- Read relevant code and [behavioral invariants](docs/invariants.md) before changing a subsystem.
- Update the focused guide and plan when behavior or priorities change. Keep one canonical description per topic.
- Document requirements and verified behavior, not chronological conversations or repeated implementation diaries.
- Distinguish implemented code, passing fixture tests, actual model/provider checks and release readiness.
- Use focused tests for the change, then necessary integration checks. Retain failures and explain environmental limits instead of hiding them.
- Do not claim a phase or release complete from a narrower test or foundation.

## Authority and privacy

- Models propose and interpret. Application code owns validation, approvals, budgets and risk gates.
- New agent research runs automatically under finite frozen application budgets. Require one user approval of the finished strategy, exact paper account, implementation and limits; then allow automatic paper orders within those limits. Preserve historical research approval contracts.
- MT5 orders require one exact deployment approval binding the finished strategy, evidence, account/server/mode, implementation/EA and finite limits. Native trading starts disabled. Real-account activation needs separate explicit authorization. Never convert historical Alpaca/Liquid approvals into MT5 authority or claim that connector tests complete the agent runner or verify FTMO rules. A data connection is not trading permission.
- The orchestrator owns conversation, source discovery and strategy intake, never execution. A strategy agent evaluates a strategy trigger and proposes an investment; a position risk agent proposes exposure, capital and leverage within deterministic application limits. Source-grounded briefs are not executable strategies.
- Keep one stable agent per exact strategy, isolated histories, exact duplicate reuse and explicit variant lineage.
- Strategy development uses validated signal programs and application-generated Python only; never run arbitrary model/acquired code. Preserve v1 three-program/four-target/twenty-four-start readers. New v2 tool loops freeze four target scopes and finite model/native/time budgets, stop on the first insufficient evaluated revision, and preserve every attempt and concrete variant owner. Checkpoint continuation must reconcile consumed operations, never replay uncertain native starts or replenish budgets.
- Public research tools use fixed adapters and anonymous sessions only. Missing/technical readers may fall back; access/rate denials, private targets and sensitive output stop. Never import browser login state or expose arbitrary CLI/browser evaluation to strategy models.
- Model output cannot approve plans, run arbitrary SQL/acquired code or submit orders.
- Keep account diagnostics and credential access outside strategy-model tools.
- Persist model and authorized connector credentials only in native OS secure storage, without plaintext or chained fallback.
- Keep secrets out of Git, model contexts, artifacts and worker environments.
- Never broaden provider scopes, purchase data/capacity or activate live trading without authorization.

## Evidence and compatibility

- Preserve versioned schemas, applied migrations, sealed reports, local journals and historical readers.
- Documentation cleanup does not authorize deleting operational evidence or hosted resources.
- Preserve exact candidate/window/cost ownership, all attempted variants and selected-only holdout results.
- Final approval binds the complete strategy, evidence, account, implementation and limits. Application research authorization binds its complete frozen plan and finite budget. No silent retries of consumed native or holdout slots.
- Source acquisition and valid citations do not establish faithful conversion, profitability or execution authority.
- Local hashes and authored fixtures do not establish provider rights, account ownership or executable attestation.
- Detailed subsystem rules in [docs/invariants.md](docs/invariants.md) remain mandatory for affected changes.
