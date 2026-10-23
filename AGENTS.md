# Repository instructions

## Scope and source of truth

- Use [plan.md](plan.md) as the single development plan and [docs/README.md](docs/README.md) as the documentation index.
- stratosphere is a personal tool in the tradingsphere suite. Keep both names lowercase.
- Keep the repository private. Do not change visibility or licensing without an explicit request.
- Do not add billing, application login, a hosted user database or automatic strategy sharing.
- Liquid is the only trading connector. Do not restore Alpaca without a new instruction.
- Stabilize existing workflows before adding features. Do not resume an interrupted feature task merely because its files remain in the working tree.

## Development workflow

- Write code, comments, documentation and Git material in English.
- Keep changes and their history on GitHub. Publish only the intended scope; preserve unrelated working-tree changes.
- Read relevant code and [behavioral invariants](docs/invariants.md) before changing a subsystem.
- Update the focused guide and plan when behavior or priorities change. Keep one canonical description per topic.
- Document requirements and verified behavior, not chronological conversations or repeated implementation diaries.
- Distinguish implemented code, passing fixture tests, actual model/provider checks and release readiness.
- Use focused tests for the change, then necessary integration checks. Retain failures and explain environmental limits instead of hiding them.
- Do not claim a phase or release complete from a narrower test or foundation.

## Authority and privacy

- Models propose and interpret. Application code owns validation, approvals, budgets and risk gates.
- Keep one stable agent per exact strategy, isolated histories, exact duplicate reuse and explicit variant lineage.
- Model output cannot approve plans, run arbitrary SQL/acquired code or submit orders.
- Keep account diagnostics and credential access outside strategy-model tools.
- Persist model and Liquid credentials only in native OS secure storage, without plaintext or chained fallback.
- Keep secrets out of Git, model contexts, artifacts and worker environments.
- Never broaden provider scopes, purchase data/capacity or activate live trading without authorization.

## Evidence and compatibility

- Preserve versioned schemas, applied migrations, sealed reports, local journals and historical readers.
- Documentation cleanup does not authorize deleting operational evidence or hosted resources.
- Preserve exact candidate/window/cost ownership, all attempted variants and selected-only holdout results.
- Approval binds the complete plan, implementation and finite budget. No silent retries of consumed native or holdout slots.
- Source acquisition and valid citations do not establish faithful conversion, profitability or execution authority.
- Local hashes and authored fixtures do not establish provider rights, account ownership or executable attestation.
- Detailed subsystem rules in [docs/invariants.md](docs/invariants.md) remain mandatory for affected changes.
