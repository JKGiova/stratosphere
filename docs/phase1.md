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

The development model remains `deepseek-ai/deepseek-v4.1-flash`, passed exactly as configured. Its [NVIDIA model page](https://build.nvidia.com/deepseek-ai/deepseek-v4.1-flash) and the public hosted `/v1/models` catalog confirmed that exact ID on October 17. The generic [NVIDIA model index](https://docs.api.nvidia.com/nim/reference/llm-apis) previously showed V4 Flash rather than V4.1; that discrepancy is resolved by the specific page and catalog. A catalog listing is not proof of inference availability or account permission, and the catalog also returns 200 without authentication.

At initial increment implementation no key was available, so provider protocol tests used simulated responses. The subsequent authorized live attempt below failed. The client keeps explicit model selection; it does not substitute another model. The [V4 Flash inference reference](https://docs.api.nvidia.com/nim/reference/deepseek-ai-deepseek-v4-flash-infer) documents SSE and reasoning modes; successful V4.1 inference and the effect of the reasoning parameter remain unverified.

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

## GitHub verification

[Platform run 39017709666](https://github.com/JKGiova/stratosphere/actions/runs/39017709666) passed for implementation commit `a2cecf6088c63ae8b8bf4ee66fc0ffcffb120bd4`. All four runners (Linux x86_64/ARM64, macOS 26 ARM64/Intel) passed 105 core tests, lint, formatting and the online data proof. The three published-wheel runners additionally passed three mandatory native-engine tests each. Engine tests are deliberately excluded from the ordinary Intel matrix job, not counted as successful there.

The [dedicated fresh Intel source rebuild](https://github.com/JKGiova/stratosphere/actions/runs/39017709685) is still in progress at this update; no result is claimed for it. All 108 local tests passed using the downloaded, previously verified Intel CI wheel. Live authenticated NVIDIA inference remains unverified.

## Live attempt with the user's saved key

October 17, 2026: the user authorized a live test and provided the key through a local, Git-ignored `.env` file with owner-only permissions. The test harness explicitly loads that file into its own process; this does not add automatic dotenv loading or persistent credential storage to the product. The key was not printed or committed.

- The actual headless Textual onboarding → LangGraph → NVIDIA adapter request used the selected V4.1 Flash model with streaming and the existing reasoning setting. It timed out after **60.394 seconds**, with zero visible text deltas and no turn committed to model history. The planned second-turn memory check and subsequent slash-command live check were not reached.
- A direct minimal streaming request to the same model, with 128 output tokens and the reasoning parameter omitted, timed out after **120.154 seconds** before response headers. Extending the read deadline alone did not produce a successful inference.
- A structured text-content request matching the shape in NVIDIA's model example also timed out at **25.127 seconds**. A deliberately invalid credential control returned **401 in 0.222 seconds**. The saved-key requests did not receive an access-denied response, but they also did not complete; neither credential permission nor end-to-end inference is claimed as verified.
- A diagnostic request to the other DeepSeek model listed by the same public catalog returned 404. It did not change the application model or serve as a fallback.

Outcome: **live conversational verification failed**. The UI handled the timeout without executing any operation or adding an incomplete turn to history. The underlying cause remains unconfirmed. No production adapter or timeout change was made solely to hide these failures. Automated protocol/TUI tests and the earlier native-engine/platform verification remain separate evidence, not substitutes for a successful live chat. Sanitized diagnostic reports are retained under ignored reports/phase1/.

## Remaining sequential increments

1. Complete selectable provider/configuration support and verify a live authenticated development-model exchange. Build the shared validated operation layer before giving the chief agent execution tools. Keep consequential actions subject to their required approval and capability gates.
2. Implement the controlled registry harness: immutable canonical definitions, exact deduplication, persistent random-ID uniqueness/collision retry, source accumulation, distinct variants and shared/private projections with ownership/transaction tests. Do not expose an unauthenticated public service.
3. Implement all five required source adapters: YouTube, Instagram, X, TradingView and GitHub. Preserve provenance and return explicit inaccessible/unsupported outcomes. Verify accessible examples and provider failure paths for each; none is implemented by this terminal increment.
4. Wire registry/source operations into the same deterministic layer for chat and slash commands, with private/public notices and acceptance tests on Linux/macOS. Add intervention/job behavior as its worker implementation exists.

These are implementation dependencies, not a reduction of Phase 1 scope. Login setup, fee collection and trading execution stay in their later phases. Phase 2 retains the end-to-end strategy implementation/backtest/report workflow. Final first-run onboarding still includes hosted login and optional trading connections once implemented.
