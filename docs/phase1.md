# Phase 1 — online terminal client, registry and sources

Status: **in progress**, October 17, 2026. Phase 0 remains complete. The user authorized Phase 1 after reviewing the native Intel verification. Work proceeds in sequential, tested increments; this document does not declare the full phase or a release complete.

## Increment 1 — terminal client and conversational model workflow

Implemented:

- Launch the English Textual client with `python -m stratosphere`.
- Session onboarding with an explicit OpenRouter/NVIDIA provider picker, editable model ID and masked API key input. OpenRouter Cohere North Mini Code (free) is the current default. Enter advances from model to key and submits setup; Escape cancels. Reopen with `/model`. Canceling setup blocks chat until configured.
- HTTPX hosted streaming chat through a LangGraph workflow and fixed provider endpoints. No tool execution exists in this increment; model text cannot launch operations. Completed turns form alternating user/assistant history. Failed, truncated or canceled turns are excluded from subsequent model context and labeled incomplete on screen.
- `/help`, `/model`, `/strategies`, `/jobs`, `/portfolio`, `/cancel`, `/clear`, `/quit`. These commands are deterministic and never call an LLM. Invalid arguments/commands cause no action. One model response runs at a time; cancellation is required before changing model or clearing history. Changing model starts a fresh model history.
- A styled four-pane workspace: strategy library, strategy focus, agent chat with inline input, and an activity pane with Tests/Positions/Session tabs. Strategy, test and account surfaces explicitly report unavailable integrations rather than invented records or zero positions.
- Cancellable streaming, terminal resize/keyboard navigation, and cancellation on exit. No trading runtime or actual position exists in this increment, so the future confirmed keep/close-all exit choice is not operational yet.
- Credentials and conversation remain in process memory; no product persistence or checkpoint store is configured. Ambient LangSmith tracing is explicitly disabled for chat. The exact configured key is blocked in chat input, redacted across streamed chunk boundaries and excluded from configuration repr. Provider HTTP error bodies are not rendered. Terminal control characters are removed and messages render as literal text.

Both adapters use fixed HTTPS endpoints. Each requests `stream=true`, a 4,096-token response budget, a 15-second connection timeout, a 60-second read-inactivity timeout and a 180-second overall deadline. DeepSeek V4 model IDs on NIM request `reasoning_effort=none`. OpenRouter uses low reasoning effort with reasoning excluded from visible output. No client automatic retry, redirect, model/provider replacement, offline response or model-generated operation is implemented. OpenRouter may route within the selected free model subject to the zero-price ceiling. HTTPS proxies from ambient environment variables are not used by these adapters. A timeout after partial output leaves that turn incomplete.

### Model availability

The previous development model, retained as an explicit NVIDIA alternative, is `deepseek-ai/deepseek-v4.1-flash`, passed exactly as configured. The current user-authorized development default is Cohere North Mini Code on OpenRouter, selected by the live comparison below. Its [NVIDIA model page](https://build.nvidia.com/deepseek-ai/deepseek-v4.1-flash) and the public hosted `/v1/models` catalog confirmed that exact ID on October 17. The generic [NVIDIA model index](https://docs.api.nvidia.com/nim/reference/llm-apis) previously showed V4 Flash rather than V4.1; that discrepancy is resolved by the specific page and catalog. A catalog listing is not proof of inference availability or account permission, and the catalog also returns 200 without authentication.

At initial increment implementation no key was available, so provider protocol tests used simulated responses. The subsequent authorized NVIDIA live attempt below failed. The client keeps explicit model selection; it does not substitute another model. The [V4 Flash inference reference](https://docs.api.nvidia.com/nim/reference/deepseek-ai-deepseek-v4-flash-infer) documents SSE and reasoning modes; successful V4.1 inference and the effect of the reasoning parameter remain unverified.

## Try it

The existing project `.venv` can run the client without the optional engine. From the repository root:

```sh
.venv/bin/python -m stratosphere
```

Choose OpenRouter in onboarding and enter its API key; the exact Cohere free model ID is prefilled. `OPENROUTER_API_KEY` optionally prefills the OpenRouter key; `NVIDIA_API_KEY` is used only for the NVIDIA selection. `STRATOSPHERE_PROVIDER` and `STRATOSPHERE_MODEL` optionally select initial settings. Confirmation still precedes any request. Do not paste credentials into chat. Settings and history must be entered again after restarting. The app does not automatically load `.env`; the live test harness explicitly loads its selected key. Chat/key data goes to the chosen API endpoint; OpenRouter routes chat to an upstream model provider, as stated in onboarding. Changing provider resets model history and does not carry the other provider's key or previous private conversation into the new request.

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

## Previous OpenRouter Nemotron free selection

October 17, 2026: the user replaced the development/testing model with **`nvidia/nemotron-3-ultra-550b-a55b:free` on OpenRouter**. Both the [specific model page](https://openrouter.ai/nvidia/nemotron-3-ultra-550b-a55b:free) and public `/api/v1/models` catalog list that exact variant, with prompt/completion pricing zero at verification time. A separate OpenRouter API key is required; the NVIDIA key is never selected automatically for that endpoint. Free capacity and rate limits remain provider-controlled.

The client requires OpenRouter model IDs to end in `:free` and sends `provider.max_price` ceilings of zero for prompt and completion tokens. It sends one exact `model`, with no alternative model list or paid-model fallback. The [provider-routing reference](https://openrouter.ai/docs/guides/routing/provider-selection) documents these price ceilings. The request uses `reasoning.effort=low` and `reasoning.exclude=true`, as documented by the [reasoning reference](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens). No new dependency or lock change was required.

The shared stream parser handles OpenRouter keep-alive comments and its final accounting frame, which repeats `finish_reason=stop` with usage information. It still rejects unexpected content after the terminal event, truncation and missing completion. Provider error events within an HTTP 200 SSE stream are mapped to fixed safe messages rather than treated as malformed data or rendering raw provider errors. See the [streaming reference](https://openrouter.ai/docs/api/reference/streaming).

Live tests with the separately supplied OpenRouter key reached HTTP 200 streaming, but the provider emitted an error event with code **503** and message **“Upstream error from Nvidia: Service temporarily overloaded”**. No answer, completed turn, multi-turn verification or cost report was received. After the error-handling correction, the actual TUI correctly showed service unavailable/overloaded and retained no incomplete model history. Live successful inference therefore remains **unverified**. The chosen free model was not substituted, and no paid fallback was attempted. Sanitized reports are retained in ignored reports/phase1/openrouter-*.json.

The OpenRouter extension passed **121 tests locally with zero skips** (118 core plus three native-engine tests) using the verified Intel wheel; lint and formatting passed. Provider protocol, free-model enforcement, no-retry/error handling, price ceilings, repeated usage frames and credential isolation have automated tests. A UI/transport test switches from OpenRouter to NVIDIA and confirms the second provider receives its own key and only the new conversation. These tests are separate from live provider availability.

### OpenRouter extension CI evidence

[Platform run 39019591120](https://github.com/JKGiova/stratosphere/actions/runs/39019591120) passed for implementation commit `6ad421bab6298e8854cc625169076eebbbaa4272`. All four platform jobs passed **118 core tests**, lint, formatting and the online data proof; Linux x86_64/ARM64 and macOS ARM64 additionally passed three mandatory native-engine tests each. The local Intel run passed all 121 tests using the previously verified custom wheel. The [separate fresh Intel source rebuild](https://github.com/JKGiova/stratosphere/actions/runs/39019591125) remains in progress at this update; its result is not claimed. At that snapshot, successful live OpenRouter inference remained unverified because of the observed upstream overload; the later Cohere checks below passed.

## Working free text model selection

October 17, 2026: the user authorized testing several free text-to-text models and choosing one that works. Candidates came from the current [OpenRouter model catalog](https://openrouter.ai/api/v1/models), requiring `:free`, zero prompt/completion prices, text-only input and text output. Each was tested through the actual adapter and LangGraph session with identical request controls, including the zero-price ceiling. Two probes ran concurrently at most; the availability probe had an explicit 35-second deadline, without changing product timeouts.

| Model | Initial streamed text check | Follow-up checks |
| --- | --- | --- |
| `cohere/north-mini-code:free` | Passed, 1.028 s | Memory and exact JSON/arithmetic passed; two full TUI checks passed |
| `liquid/lfm-2.5-2.6b:free` | Passed, 1.227 s | Not run |
| `poolside/laguna-s-2.1:free` | Passed, 3.392 s | Not run |
| `poolside/laguna-xs-2.1:free` | Passed, 0.519 s | Rate-limited on the memory follow-up |
| `xingchen-agi/xing4.0-29b-a4b:free` | No completion within 35 s | Not run |
| `nvidia/nemotron-3.5-lightning:free` | No completion within 35 s | Not run |

The initial prompt required exactly `STRATOSPHERE_OK`. The two fastest successful candidates received a memory prompt followed by a request for only JSON containing the remembered label and the sum of 17 and 25. Cohere returned the exact expected object, `{"label":"CEDAR_734","total":42}`. Poolside XS's follow-up hit a rate limit, so the faster initial response alone was not sufficient for selection.

**Selected default: `cohere/north-mini-code:free`.** Cohere completed seven live requests: the initial probe, two memory/JSON turns, two turns through the actual headless Textual onboarding → LangGraph → OpenRouter flow with an explicit candidate selection, and two more after changing the source default with no model override. The TUI checks confirmed remembered context, the reported model ID, streaming deltas, successful history commits, key masking and a `/portfolio` navigation command without an additional model call. Each request reported usage cost **0 USD**; completed reply times ranged from **0.715 to 3.228 seconds** across these short checks. Those are observations, not latency guarantees or a general capability ranking.

The same 121 local regression tests passed with zero skips after the default change, including the three native Intel engine tests; lint and formatting passed. Zero-price restrictions, the explicit `:free` model requirement and no paid-model fallback remain in place. NVIDIA remains an explicitly selectable alternative. Keys were excluded from tracked files and diagnostic JSON. Sanitized test reports are retained in ignored reports/phase1/free-text-model-tests.json, cohere-candidate-tui.json and openrouter-live-smoke.json.

This verifies current chat availability, basic memory and plain JSON instruction following. It does not verify tool use, enforced JSON-schema generation, strategy correctness or research quality. Rate limits and provider availability still apply. This selection supersedes the Nemotron default while retaining its earlier failure evidence.

### Selected-model platform verification

[Platform run 39021754364](https://github.com/JKGiova/stratosphere/actions/runs/39021754364) passed for selection commit `e54930d7d3a710d69d58ebf672412dc5aef1b82d`. All four platform jobs passed; the existing workflow runs 118 core tests on Linux x86_64/ARM64 and macOS ARM64/Intel, plus three native-engine tests on each published-wheel platform. The separate [fresh Intel engine source-build workflow](https://github.com/JKGiova/stratosphere/actions/runs/39021754488) was triggered too; this update does not claim its result. All 121 local tests passed using the previously verified Intel wheel.

## Four-pane terminal redesign

October 17, 2026: the user requested a complete UI rework in the style of [agentic-trading-workflow](https://github.com/JKGiova/agentic-trading-workflow). The terminal now uses the same navy/cyan visual direction, rounded numbered pane borders, persistent 2×2 workspace, header clock, model/request status strip and F1–F4 navigation. The panels are adapted to stratosphere: strategy library, strategy focus, online agent chat with inline input, and Tests/Positions/Session. Model onboarding shares the theme and retains provider isolation, masked credentials and the privacy/publication notice.

At widths below 80 columns or heights below 24 rows, one selected pane fills the workspace. Drafts and completed chat history survive resize and pane navigation. Streaming completion/cancellation preserve another pane's focus. `/strategies`, `/jobs` and `/portfolio` navigate deterministically; `/` from a non-input pane opens chat without replacing an existing draft. Ctrl+K takes priority over the input widget's text-editing shortcut so it cancels a response from any pane. Request status distinguishes unverified model settings, connecting, responding, completion, cancellation and unavailable service.

No strategy, test or position records are invented. Tables remain empty with explicit unavailable states. The Session tab displays actual model settings, current request state and completed turn count; it does not estimate account balances, token usage or cost. This change does not implement registry access, source adapters, research execution or trading.

Verification: **127 tests passed locally with zero skips** (124 core plus three native Intel engine regressions), with lint and formatting passing. UI tests cover the 80×24, 120×36 and 160×48 grids, 45×12 compact navigation, draft/history retention, pane focus during background completion/cancellation, model setup, literal rendering, credential masking and command routing without inference. Generated screen renders were visually inspected at standard and compact sizes, including onboarding. The built wheel includes the terminal stylesheet and launches successfully from an extracted distribution; both distributions exclude local environment/cache files. Ignored artifacts are in reports/ui/.

The redesigned UI also passed two real requests through onboarding → LangGraph → OpenRouter using the selected Cohere default. It remembered the previous turn, completed both history commits, and routed `/portfolio` without another model request. Responses completed in **1.259 and 2.277 seconds**, each reporting **0 USD**. These short checks verify chat continuity through the redesign, not strategy or research quality. Credentials were excluded from tracked files and saved diagnostic/screen artifacts. GitHub verification is recorded separately after publishing.

### Redesign CI status

Implementation commit `b387cea2c926a1475cc51f80f872326a276e779d` is published in the private repository. [Platform run 39023183524](https://github.com/JKGiova/stratosphere/actions/runs/39023183524) and [Intel engine run 39023183497](https://github.com/JKGiova/stratosphere/actions/runs/39023183497) both failed **before any job steps executed**. All five check annotations report an account payment or spending-limit restriction. GitHub also posted a macOS ARM64 capacity advisory; it did not run the job. No fresh cross-platform or Intel-source-build success is claimed for this commit. Account billing/spending settings must be resolved before rerunning these checks; no billing setting was changed. Local Intel regression, visual inspection, distribution and live-chat evidence above remain valid and separate from blocked CI.

## Remaining sequential increments

1. Extend provider/structured-output support beyond the current verified chat checks. Build the shared validated operation layer before giving the chief agent execution tools. Keep consequential actions subject to their required approval and capability gates.
2. Implement the controlled registry harness: immutable canonical definitions, exact deduplication, persistent random-ID uniqueness/collision retry, source accumulation, distinct variants and shared/private projections with ownership/transaction tests. Do not expose an unauthenticated public service.
3. Implement all five required source adapters: YouTube, Instagram, X, TradingView and GitHub. Preserve provenance and return explicit inaccessible/unsupported outcomes. Verify accessible examples and provider failure paths for each; none is implemented by this terminal increment.
4. Wire registry/source operations into the same deterministic layer for chat and slash commands, with private/public notices and acceptance tests on Linux/macOS. Add intervention/job behavior as its worker implementation exists.

These are implementation dependencies, not a reduction of Phase 1 scope. Login setup, fee collection and trading execution stay in their later phases. Phase 2 retains the end-to-end strategy implementation/backtest/report workflow. Final first-run onboarding still includes hosted login and optional trading connections once implemented.
