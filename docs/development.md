# Development and verification

## Commands

Use the configured Python 3.14 environment and pinned dependencies. Intel engine setup is separate.

```sh
.venv/bin/ruff check src tests
.venv/bin/ruff format --check src tests
.venv/bin/pytest -q
.venv/bin/pytest -q tests/test_strategy_agents.py tests/test_structured_models.py
.venv/bin/pytest -q -m engine
```

Some OAuth tests bind a localhost callback listener. A restricted sandbox may block this before
application behavior runs; verify that case separately with listener permission and record both
outcomes. Do not mask genuine failures as environment issues. Native tests require the pinned engine.

## Bug-fix workflow

1. Record a minimal reproduction, expected/actual behavior, exact revision and environment.
2. Add a meaningful regression case at the owning service boundary; include the TUI path where relevant.
3. Fix the smallest coherent issue. Preserve identity, evidence, approval and credential invariants.
4. Run affected tests and necessary integration/native checks. Check cancellation, ownership and restart.
5. Check the installed package when packaging, startup or persisted readers change.
6. Update the owning guide and `plan.md`, then publish the scoped change to private GitHub.

Avoid tests that simply restate an implementation. Use independent accounting/behavior oracles,
actual process/restart boundaries and retained artifact comparisons for correctness claims.
Fixture tests are not real-provider tests; a green suite is not operational readiness.

## Current verification snapshot

There is no final clean-checkout regression baseline for the interrupted tool-runtime draft yet.
Its checks exposed a sandbox-blocked OAuth listener and a TUI `/data` import collision. The callback
passed separately and the import was repaired locally; later affected checks passed. Review and
verify those changes as a coherent revision before claiming completion.

An installed draft package retained trace/history across two processes. A local 4B model completed
one artificial strategy/jobs lookup; the smaller model repeated calls and was stopped. These checks
do not establish a real provider feed, order transport, sustained service or general model quality.

Documentation cleanup does not validate or publish those application changes. Current failures,
proof outputs and artifacts belong under ignored `reports/`, not repeated prose in every guide.

## Distribution and privacy

Build wheel/source archives with `uv build`. Check packaged code/assets, install independently,
verify persisted evidence reopening and exclude credentials, private state and ignored reports.
Do not loosen pinned engine/lock/source checks to make a test pass. Keep all nonselected/failed
attempts and native/holdout budgets intact.
