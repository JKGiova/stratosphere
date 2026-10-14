# Phase 0 — sequential verification

Status: in progress, October 14, 2026. Only compatibility and contract proofs are authorized here.

## Gates, in order

| Step | Deliverable | Evidence before moving on |
| --- | --- | --- |
| 1 | Selected stable runtime/dependencies and capability matrix | Pinned install/import results on Linux x86_64/ARM64 and macOS; unsupported wheel boundaries reported explicitly |
| 2 | TUI-library selection and a minimal framework probe | Actual headless keyboard/render/resize checks; no product chat or panels |
| 3 | Versioned canonical strategy/data/experiment contracts | Example documents, schema validation, exact identity and behavioral-variation tests |
| 4 | First accessible historical dataset and NautilusTrader adapter proof | Provenance/checksum, data-quality checks, reproducible real-engine runs and timing/cost/conformance tests |
| 5 | Shared-service access-boundary decision and completion audit | Documented public/private projection, authority boundaries and executable contract checks; every gate linked to evidence |

No Phase 1 TUI application, chief-agent chat, source extraction, catalog service, deployed database, login, broker execution or fee collection is part of this work. A minimal data-acquisition adapter and engine strategy are required to prove Phase 0; they are not a claim of the later research product.

## Step 1 — passed

Python 3.14.7; stable NautilusTrader 1.231.0; LangGraph 1.2.15; Textual 8.2.8; Pydantic 2.14.1; HTTPX 0.28.1. uv.lock records exact transitive resolution. Textual is a candidate until Step 2 passes.

The pinned [PyPI metadata](https://pypi.org/pypi/nautilus_trader/1.231.0/json) publishes Linux glibc 2.35+ x86_64/ARM64 and macOS 26+ ARM64 wheels. It publishes no Intel macOS wheel. The [latest engine documentation](https://nautilustrader.io/docs/latest/getting_started/installation/) describes the 2.x prerelease API, so the proof must use the pinned stable 1.x source/API rather than mixing examples across major versions.

The Intel Mac used locally can verify contracts and framework probes; it cannot install this engine wheel. This is a capability limit to record, not a skipped engine test counted as success. Engine-required CI jobs must install/import the binary and run the behavior gate once implemented.

```sh
uv sync --locked
uv run python -m stratosphere.phase0.compatibility
uv run pytest -m 'not engine'
```

Supported engine hosts add `--extra engine` to sync and run compatibility with `--require-engine`.

The [Step 1 CI run](https://github.com/JKGiova/stratosphere/actions/runs/38581714527) passed on all four runners. Its JSON artifacts confirm actual imports and pinned versions: Linux x86_64 and ARM64 and macOS 26 ARM64 imported NautilusTrader 1.231.0; macOS 26 Intel imported the core stack and explicitly reported the unavailable engine. Eleven tests passed locally. These are installation checks, not yet engine behavior verification.

## Step 2 — passed

Selected Textual 8.2.8 after the probe passed the same platform matrix in [Step 2 CI](https://github.com/JKGiova/stratosphere/actions/runs/38582556733). It provides Python widgets, keyboard events, terminal resizing, async operation and a [headless testing API](https://textual.textualize.io/guide/testing/). The probe checks actual typed text, literal bracket rendering, Enter handling, focus, input layout after shrinking the terminal, continued input and clean Ctrl+Q exit. CI retains an SVG rendering artifact. This verifies framework behavior, not terminal-emulator accessibility or a finished product UI.

```sh
uv run python -m stratosphere.phase0.tui_probe
uv run pytest tests/test_tui_probe.py
```

## Step 3 — passed

See [canonical contracts](phase0-contracts.md), schemas/ and examples/phase0/. Forty-five tests passed locally and in the [Step 3 CI matrix](https://github.com/JKGiova/stratosphere/actions/runs/38583960776). Tests exercise every behavioral group, metadata independence, exact tiny decimal changes, malformed or unresolved specs, duplicate JSON keys, immutable validated objects, explicit digest collisions and example fingerprint/schema consistency. These contracts do not create a registry or compile arbitrary strategies.

## Step 4 — passed

Read-only ECB Data API integration, with the January–March 2025 EURUSD reference snapshot and 63 pinned observations. The committed CSV is attributed ECB data, free from its website; normalization retains exact date/rate values and rejects missing/duplicate/unexpected dates, changed values and unaccepted observation status. Metadata-only CSV differences cannot rewrite a rate. CI must fetch and verify a fresh response rather than count only a mock-provider test.

The pinned native BacktestEngine consumes declared synthetic TradeTick events at each reference's assumed 17:00 UTC availability. The callback executes an intent from the previous observation, then evaluates the newly available rate. The exchange updates before the callback in the pinned [engine source](https://github.com/nautechsystems/nautilus_trader/blob/v1.231.0/nautilus_trader/backtest/engine.pyx). Only the full authored SMA spec and SIM instrument are supported; unrelated rules, spread/slippage models or mismatched protocol bindings fail before execution. No generated/user code is executed.

Tests compare the native event/fill stream with an independent batch oracle, ensure fills occur strictly after signals, match quantity and currency-rounded fees to native account balances, verify zero-cost behavior and identical repeated payloads, and truncate an actual snapshot after the first entry to test causality and retained open exposure. No daily/end-of-run liquidation is inserted. An end-of-snapshot signal without a following observation remains pending. Native order failure or unexpected partial/unfilled execution fails the proof.

Each report records separate run identity, engine/runtime, strategy/data/protocol/instrument digests, implementation and dependency-lock hashes, fill artifacts, balances and explicit limitations. The compatibility protocol has no optimization, holdout, profitability or promotion claim. Data is historically real; liquidity and executable reference-price events are explicitly simulated. Tradable market data, broker rules and actual publication timestamps need later validation.

```sh
uv run python -m stratosphere.phase0.data_proof --fetch
uv sync --locked --extra engine
STRATOSPHERE_REQUIRE_ENGINE=1 uv run pytest -m engine
uv run python -m stratosphere.phase0.engine_proof
```

Engine tests are mandatory on the three supported CI hosts. Intel macOS runs core tests and the online data proof; its unavailable engine is excluded from the compatibility claim.

The [Step 4 CI run](https://github.com/JKGiova/stratosphere/actions/runs/38585064927) passed all four platform jobs. All fetched the online snapshot successfully. The three native-engine artifacts show eight fills with matching timing, fees and account balances, and exactly identical deterministic payloads across Linux x86_64/ARM64 and macOS ARM64 (digest ff3746e22b35f47b8136d1171e23652e837916fa32ea411f2d0e1b5b232b2616). This is correctness/repeatability evidence under the stated simulation, not a profitability assessment. The truncated-snapshot test verifies retained open exposure separately.

## Step 5 — access contracts and audit under verification

Selected hosted HTTPS/JSON application API, with internal PostgreSQL records inaccessible directly from distributed clients. See [access-boundary decision](phase0-access-boundary.md) for authority, RLS/role requirements, public/private projection and transactional registry constraints. Catalog and registration wire schemas are in schemas/. Pure harness tests exercise the intended policies; no real auth, database or service security is claimed. Final CI rechecks all earlier gates with these contracts and stricter input-availability validation.
