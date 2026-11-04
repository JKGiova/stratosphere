# Manual module coordination

The owner launches each Codex job. These files carry current module state between jobs; they do
not notify chats, poll for changes or launch work. Scope and accepted decisions belong to
[plan.md](../../plan.md); mandatory behavior belongs to [invariants](../invariants.md).

## Module reports

| Assigned module | Report |
| --- | --- |
| Strategy contracts and compiler | [strategy.md](strategy.md) |
| NautilusTrader integration | [nautilus.md](nautilus.md) |
| MT5 Expert Advisor | [mt5-ea.md](mt5-ea.md) |
| MT5 Python connector | [mt5-connector.md](mt5-connector.md) |
| CLI | [cli.md](cli.md) |
| MCP server | [mcp.md](mcp.md) |

Use the module assigned by the owner, not a previous chat's title or number. If the task does not
identify an owning module, resolve ownership with the owner before modifying files. A coordination
job may update the central plan/index when the owner explicitly assigns that scope.

## Before work

1. Read `AGENTS.md`, the current plan and all six reports. Read relevant focused guides/invariants.
2. Inspect Git status and the assigned source with Graphify; verify report claims against source.
   Record the source commit and report revisions actually inspected. Reports are handoffs, not proof
   that another checkout or pending proposal is implemented.
3. Compare provided/consumed interfaces. If the task needs an unapproved cross-module change,
   record the proposal and its blocker in the assigned report for owner review. Continue work that
   does not depend on that decision. A proposal is never an accepted interface.

## During and after work

Implement only the assigned task within its agreed scope. Follow repository tests, evidence/privacy
rules and Graphify refresh requirements. Inspect overlapping working-tree changes before editing;
these reports provide no file locks or worktree isolation. Do not overwrite another job's work.

Update only the assigned report, keeping the headings below. Increase its revision and replace
stale state instead of appending a diary. Record:

- **State:** status, source baseline, inspected report revisions and whether changes are committed
  or pending. Keep the baseline honest; do not invent the future commit containing the report.
- **Interface:** current provided/consumed behavior, with implemented and proposed forms separated.
- **Completed changes:** the current task's concrete results and source references.
- **Dependencies and blockers:** exact report revisions/contracts needed, changes observed since the
  previous handoff and any unresolved owner decisions.
- **Verification:** actual commands/results and material limits. Link public fixtures/source rather
  than copying private account evidence, credentials or operational logs into these public files.
- **Proposals for owner review:** affected modules, requested contract/scope/plan change and reason.
  Leave other reports, shared plans and cross-module scope unchanged until the owner approves them.
- **Next task:** one useful next task, labeled as a suggestion for the owner to launch.

Use Git for history. Publish the scoped source/report changes as required by repository instructions.
The next manually launched job must use a checkout containing those changes; separate branches or
worktrees do not synchronize automatically. End with what changed, verification, blockers and the
report path. Sending a message or launching another job is not part of this procedure.

## Starting a job

Paste this into a new Codex job and fill in the module and task:

```text
Module: <name from the report index>
Task: <specific planning or implementation task>

Follow AGENTS.md and docs/coordination/README.md. Read plan.md and all six module reports,
then verify the relevant state against source. Work only on the assigned task. Update only this
module's report; record cross-module/plan changes as proposals for my review. Report verification
and blockers. I will manually launch any subsequent job.
```
