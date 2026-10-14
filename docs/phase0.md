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

## Step 1 selections under verification

Python 3.14.7; stable NautilusTrader 1.231.0; LangGraph 1.2.15; Textual 8.2.8; Pydantic 2.14.1; HTTPX 0.28.1. Commit uv.lock for exact transitive resolution. Textual is a candidate until Step 2 passes.

The pinned [PyPI metadata](https://pypi.org/pypi/nautilus_trader/1.231.0/json) publishes Linux glibc 2.35+ x86_64/ARM64 and macOS 26+ ARM64 wheels. It publishes no Intel macOS wheel. The [latest engine documentation](https://nautilustrader.io/docs/latest/getting_started/installation/) describes the 2.x prerelease API, so the proof must use the pinned stable 1.x source/API rather than mixing examples across major versions.

The Intel Mac used locally can verify contracts and framework probes; it cannot install this engine wheel. This is a capability limit to record, not a skipped engine test counted as success. Engine-required CI jobs must install/import the binary and run the behavior gate once implemented.

```sh
uv sync --locked
uv run python -m stratosphere.phase0.compatibility
uv run pytest -m 'not engine'
```

Supported engine hosts add `--extra engine` to sync and run compatibility with `--require-engine`.
