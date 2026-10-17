# Phase 1 — online terminal client, registry and sources

Status: **in progress**, October 17, 2026. Phase 0 remains complete. The user authorized Phase 1 after reviewing the native Intel verification. Work proceeds in sequential, tested increments; this document does not declare the full phase or a release complete.

## Increment 1 — terminal client and conversational model workflow

Implemented:

- Launch the English Textual client with `python -m stratosphere`.
- Session onboarding for NVIDIA NIM: explicit editable model ID and masked API key input. Enter advances from model to key and submits setup; Escape cancels. Reopen with `/model`. Canceling setup blocks chat until configured.
- HTTPX hosted streaming chat through a LangGraph workflow. No tool execution exists in this increment; model text cannot launch operations. Completed turns form alternating user/assistant history. Failed, truncated or canceled turns are excluded from subsequent model context and labeled incomplete on screen.
- `/help`, `/model`, `/strategies`, `/jobs`, `/portfolio`, `/cancel`, `/clear`, `/quit`. These commands are deterministic and never call an LLM. Invalid arguments/commands cause no action. One model response runs at a time; cancellation is required before changing model or clearing history. Changing model starts a fresh model history.
- Chat, strategies, ongoing/completed tests and positions tabs. The latter three explicitly report unavailable integrations rather than invented strategies, tests or zero positions.
- Cancellable streaming, terminal resize/keyboard navigation, and cancellation on exit. No trading runtime or actual position exists in this increment, so the future confirmed keep/close-all exit choice is not operational yet.
- Credentials and conversation remain in process memory; no product persistence or checkpoint store is configured. Ambient LangSmith tracing is explicitly disabled for chat. The exact configured key is blocked in chat input, redacted across streamed chunk boundaries and excluded from configuration repr. Provider HTTP error bodies are not rendered. Terminal control characters are removed and messages render as literal text.

The first provider adapter is NVIDIA NIM, with its fixed HTTPS endpoint. It requests `stream=true`, a 4,096-token response budget, a 15-second connection timeout, a 60-second read-inactivity timeout and a 180-second overall deadline. DeepSeek V4 model IDs request `reasoning_effort=none`. No automatic retry, redirect, provider fallback, model replacement, offline response or model-generated operation is implemented. HTTPS proxies from ambient environment variables are not used by this adapter. A timeout after partial output leaves that turn incomplete.

### Model availability

The requested development model remains `deepseek-ai/deepseek-v4.1-flash`, passed exactly as configured. The [NVIDIA model index](https://docs.api.nvidia.com/nim/reference/llm-apis), checked October 17, lists `deepseek-ai/deepseek-v4-flash`; its [inference reference](https://docs.api.nvidia.com/nim/reference/deepseek-ai-deepseek-v4-flash-infer) documents streaming SSE and `none/high/max` reasoning modes. This discrepancy does not authorize silently substituting the user's selected model. Use the explicit model field or `STRATOSPHERE_MODEL` to select a model available to the user's account. No live authenticated request was made in this increment because no NVIDIA_API_KEY was available to the development process. Protocol tests do not establish actual model availability.

## Try it

The existing project `.venv` can run the client without the optional engine. From the repository root:

```sh
.venv/bin/python -m stratosphere
```

Enter your model ID and key in onboarding. `NVIDIA_API_KEY` and `STRATOSPHERE_MODEL` optionally prefill the fields; confirmation still precedes any request. Do not paste credentials into chat. Settings and history must be entered again after restarting. The key is transmitted only to the selected NVIDIA endpoint for authorization; chat content is sent there for inference and is subject to that provider's handling.

For a fresh core environment, use the project-pinned uv 0.13.0 and Python 3.14.7:

```sh
uv sync --locked
uv run --no-sync python -m stratosphere
uv run --no-sync pytest -m 'not engine'
```

For native Intel regression tests use the [verified custom-wheel environment](intel-macos.md); ordinary `uv sync --extra engine` cannot install an upstream Intel wheel. The isolated environment prepared for this increment can be run with:

```sh
STRATOSPHERE_REQUIRE_ENGINE=1 reports/phase1/test-env/bin/pytest -v
reports/phase1/test-env/bin/python -m stratosphere
```

## Verification

Headless TUI tests exercise onboarding validation/cancel/reopen, keyboard entry, literal chat rendering, panel routing without model calls, resize to 45×12, busy-request rejection, cancellation/retry, history clearing, credential masking and quitting during an active response. Transport-level tests exercise visible deltas before completion, exact request configuration, multi-turn history, reasoning exclusion, split-secret filtering, invalid commands, access/rate-limit/server/redirect failures, non-streaming or malformed responses, truncation, early disconnect, network/timeout errors, and request teardown on cancellation. A tracing test verifies that ambient tracing settings cannot enable chat export.

On macOS 26.5.1 Intel / Python 3.14.7, **108 tests passed with zero skips** (105 core and three native-engine tests). Ruff lint and formatting passed; installed dependency validation and required-engine compatibility inspection passed. The ten existing upstream Timestamp.utcnow deprecation warnings remain visible. No dependency versions or lock contents changed.

Local regression evidence and a terminal screenshot are saved under ignored `reports/phase1/` and `reports/phase1-tui.svg`. These checks use simulated provider responses and the previously verified native Intel wheel. GitHub platform checks remain separate from local verification; the native Intel rebuild can take about 85 minutes.

## Remaining sequential increments

1. Complete selectable provider/configuration support and verify a live authenticated development-model exchange. Build the shared validated operation layer before giving the chief agent execution tools. Keep consequential actions subject to their required approval and capability gates.
2. Implement the controlled registry harness: immutable canonical definitions, exact deduplication, persistent random-ID uniqueness/collision retry, source accumulation, distinct variants and shared/private projections with ownership/transaction tests. Do not expose an unauthenticated public service.
3. Implement all five required source adapters: YouTube, Instagram, X, TradingView and GitHub. Preserve provenance and return explicit inaccessible/unsupported outcomes. Verify accessible examples and provider failure paths for each; none is implemented by this terminal increment.
4. Wire registry/source operations into the same deterministic layer for chat and slash commands, with private/public notices and acceptance tests on Linux/macOS. Add intervention/job behavior as its worker implementation exists.

These are implementation dependencies, not a reduction of Phase 1 scope. Login setup, fee collection and trading execution stay in their later phases. Phase 2 retains the end-to-end strategy implementation/backtest/report workflow. Final first-run onboarding still includes hosted login and optional trading connections once implemented.
