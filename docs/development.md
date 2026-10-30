# Development and verification

## Commands

Use the configured Python 3.14 environment and pinned dependencies. The Intel engine is an
external wheel dependency; its build/release tooling lives in its own repository.

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
Mark every test that executes a native simulation with `@pytest.mark.engine`, including TUI and
orchestrator integration tests with authored market data. The `-m 'not engine'` subset must work
without NautilusTrader. The compatibility matrix installs the
external Intel wheel and runs both test groups, without compiling it. Authored candles do not make
a native simulation an engine-free test.

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

The focused creation checks cover exact duplicate reuse, isolated histories, immutable generated
provenance, unresolved/malformed proposals, cancellation, failed analysis, registration rollback,
restart recovery and approval gates. TUI coverage exercises `/create` through its inline review.
The native creation integration compares approved base/stress runs on authored candles against an
independent SMA/accounting oracle. Authored candles establish software behavior, not profitability.

Model transport/schema fixtures include Cheaper Inference; no authenticated Qwen Flash completion
has been established for this provider. The exposed chat credential was not used. The current persistent specialist and optional public-source fallback are documented in
[research tools](research-tools.md); older discarded drafts are not evidence for this implementation.
Fixture checks do not establish real provider feeds, order transport or sustained operation.

## Distribution and privacy

Build wheel/source archives with `uv build`. Check packaged code/assets, install independently,
verify persisted evidence reopening and exclude credentials, private state and ignored reports.
Do not loosen pinned engine/lock/source checks to make a test pass. Keep all nonselected/failed
attempts and native/holdout budgets intact.
