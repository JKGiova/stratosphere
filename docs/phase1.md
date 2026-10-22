# Phase 1 — online terminal client, registry and sources

Current-scope override: D57 makes the local SQLite registry the personal store and retires hosted login, automatic sharing and multi-user synchronization. Historical increment/CI records below preserve what was implemented/tested at their dates; their old hosted/commercial remaining-work lists are superseded by the current [roadmap](roadmap.md). This does not mark source/agent scope complete.

Status: **in progress**, October 17, 2026. Phase 0 remains complete. The user authorized Phase 1 after reviewing the native Intel verification. Work proceeds in sequential, tested increments; this document does not declare the full phase or a release complete.

## Increment 1 — terminal client and conversational model workflow

Credential-retention override: D59 adds the [native OS credential store](credentials.md) for selected model settings/keys and the Liquid read-only OAuth grant. Historical memory-only/session-only descriptions below retain their dated increment scope; they do not describe current retention.

Implemented:

- Launch the English Textual client with `python -m stratosphere`.
- Inline agent-chat onboarding with an explicit [12-provider picker](model-providers.md), editable model ID/API base URL and masked API key input. OpenRouter Cohere North Mini Code (free) is the current default. Enter advances from model to key and submits setup; Escape cancels. Reopen with `/model`. Canceling setup blocks chat until configured.
- HTTPX streaming chat through a LangGraph workflow and validated official/local provider endpoints. No tool execution exists in this increment; model text cannot launch operations. Completed turns form alternating user/assistant history. Failed, truncated or canceled turns are excluded from subsequent model context and labeled incomplete on screen.
- `/help`, `/model`, `/strategies`, `/jobs`, `/portfolio`, `/cancel`, `/clear`, `/quit`. These commands are deterministic and never call an LLM. Invalid arguments/commands cause no action. One model response runs at a time; cancellation is required before changing model or clearing history. Changing model starts a fresh model history.
- A responsive workspace with six independent panels: Library, Focus, Chat, Tests, Positions and Session. Six/four/two side-by-side/two stacked/one layouts fit the terminal character grid. A shared navigation strip and Tab/Shift+Tab select every panel; setup uses normal form navigation. Strategy, test and account surfaces explicitly report unavailable integrations rather than invented records or zero positions.
- Cancellable streaming, terminal resize/keyboard navigation, and cancellation on exit. The [Phase 2 increment](phase2.md) adds local research jobs, but no trading runtime or actual position exists, so the future confirmed keep/close-all exit choice is not operational yet.
- Credentials and conversation remain in process memory; no product persistence or checkpoint store is configured. Ambient LangSmith tracing is explicitly disabled for chat. The exact configured key is blocked in chat input, redacted across streamed chunk boundaries and excluded from configuration repr. Provider HTTP error bodies are not rendered. Terminal control characters are removed. Assistant replies render Markdown; user messages and command/status text remain literal. Formatting cannot execute code or open links automatically.

Hosted adapters use official HTTPS endpoints; the explicitly requested local Ollama adapter uses a loopback HTTP endpoint. Provider-specific setup and protocol verification are documented in [model providers](model-providers.md). Each requests `stream=true`, a 4,096-token response budget, a 15-second connection timeout, a 60-second read-inactivity timeout and a 180-second overall deadline. DeepSeek V4 model IDs on NIM request `reasoning_effort=none`. OpenRouter uses low reasoning effort with reasoning excluded from visible output. No client automatic retry, redirect, model/provider replacement, offline response or model-generated operation is implemented. OpenRouter may route within the selected free model subject to the zero-price ceiling. HTTPS proxies from ambient environment variables are not used by these adapters. A timeout after partial output leaves that turn incomplete.

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

Historical implementation; the responsive layout and inline onboarding below supersede its layout and setup-screen behavior.

October 17, 2026: the user requested a complete UI rework in the style of [agentic-trading-workflow](https://github.com/JKGiova/agentic-trading-workflow). The initial redesign introduced the same navy/cyan visual direction, rounded numbered pane borders, persistent 2×2 workspace, header clock, model/request status strip and F1–F4 navigation. The panels were adapted to stratosphere: strategy library, strategy focus, online agent chat with inline input, and Tests/Positions/Session. Model onboarding shares the theme and retains provider isolation, masked credentials and the privacy/publication notice. The subsequent polish pass below records the current keyboard and header behavior.

At widths below 80 columns or heights below 24 rows, one selected pane fills the workspace. Drafts and completed chat history survive resize and pane navigation. Streaming completion/cancellation preserve another pane's focus. `/strategies`, `/jobs` and `/portfolio` navigate deterministically; `/` from a non-input pane opens chat without replacing an existing draft. Ctrl+K takes priority over the input widget's text-editing shortcut so it cancels a response from any pane. Request status distinguishes unverified model settings, connecting, responding, completion, cancellation and unavailable service.

No strategy, test or position records are invented. Tables remain empty with explicit unavailable states. The Session tab displays actual model settings, current request state and completed turn count; it does not estimate account balances, token usage or cost. This change does not implement registry access, source adapters, research execution or trading.

Verification: **127 tests passed locally with zero skips** (124 core plus three native Intel engine regressions), with lint and formatting passing. UI tests cover the 80×24, 120×36 and 160×48 grids, 45×12 compact navigation, draft/history retention, pane focus during background completion/cancellation, model setup, literal rendering, credential masking and command routing without inference. Generated screen renders were visually inspected at standard and compact sizes, including onboarding. The built wheel includes the terminal stylesheet and launches successfully from an extracted distribution; both distributions exclude local environment/cache files. Ignored artifacts are in reports/ui/.

The redesigned UI also passed two real requests through onboarding → LangGraph → OpenRouter using the selected Cohere default. It remembered the previous turn, completed both history commits, and routed `/portfolio` without another model request. Responses completed in **1.259 and 2.277 seconds**, each reporting **0 USD**. These short checks verify chat continuity through the redesign, not strategy or research quality. Credentials were excluded from tracked files and saved diagnostic/screen artifacts. GitHub verification is recorded separately after publishing.

### Redesign CI status

Implementation commit `b387cea2c926a1475cc51f80f872326a276e779d` is published in the private repository. [Platform run 39023183524](https://github.com/JKGiova/stratosphere/actions/runs/39023183524) and [Intel engine run 39023183497](https://github.com/JKGiova/stratosphere/actions/runs/39023183497) both failed **before any job steps executed**. All five check annotations report an account payment or spending-limit restriction. GitHub also posted a macOS ARM64 capacity advisory; it did not run the job. No fresh cross-platform or Intel-source-build success is claimed for this commit. Account billing/spending settings must be resolved before rerunning these checks; no billing setting was changed. Local Intel regression, visual inspection, distribution and live-chat evidence above remain valid and separate from blocked CI.

## Tab navigation and terminal polish

October 17, 2026: the user requested Tab navigation and another polish pass inspired by conversational agent terminals such as Claude Code and Codex. The existing navy/cyan four-pane workspace is retained. Tab cycles Library → Focus → Chat → Tests → Positions → Session and wraps; Shift+Tab reverses it. A single clickable navigation strip stays synchronized with pane focus and slash commands. The redundant activity tab strip is hidden; its three views remain selectable in the shared strip. F1–F4 remain alternate shortcuts. Model-setup forms retain ordinary Tab/Shift+Tab field traversal, including after reopening with `/model`.

The UI uses quieter borders, a left-aligned brand, readable model/request status, literal messages with styled role labels, a simple `›` composer, contextual send/stop hints and a shorter keyboard footer. The header clock is removed. Larger terminals allocate 60% of pane height to chat/activity; dense grids retain equal rows. Compact mode hides the brand header to retain reading space. Drafts and cursor positions survive view switching, and attempts to send while busy or unconfigured preserve the draft for retry. This adds no model tool, research or trading integration.

Verification: **131 tests passed with zero skips** (128 core plus three native Intel engine regressions); lint and formatting passed. Added checks cover forward/backward wrap through all six views at 120×36 and 45×12, draft/cursor retention, modal form traversal, mouse/command/navigation-strip synchronization and switching views while a streamed response completes. Existing security, provider, cancellation/history and engine regressions pass. Standard/compact UI and onboarding renders were visually inspected, and the packaged stylesheet/distribution launch were checked. Artifacts are retained under ignored reports/ui/tab-polish/.

Two new real requests through the polished Textual → LangGraph → OpenRouter flow completed with remembered context, in **1.851 and 0.989 seconds**, reporting **0 USD** each. Both Tab directions and `/portfolio` routing were verified without additional model requests. Free-only routing and provider/key isolation are unchanged; credentials are excluded from tracked files and diagnostic/screen artifacts. These checks verify conversational continuity and navigation, not research or execution capability. CI status is recorded separately after publishing.

### Tab polish CI status

Commit `db665315b574b36e583c43fa7a6fd07977fc9a7d` is published. [Platform run 39024203812](https://github.com/JKGiova/stratosphere/actions/runs/39024203812) and [Intel engine run 39024203832](https://github.com/JKGiova/stratosphere/actions/runs/39024203832) failed before any job steps executed. All five check annotations report the same GitHub account payment/spending-limit restriction as the previous UI commit; the ARM64 macOS capacity advisory is also present. Fresh platform CI and source-build verification remain blocked, separate from the passing 131-test local suite and live/distribution checks. No account billing setting was changed.

## Gloomberb visual rework, including onboarding

October 17, 2026: the user requested a final visual rework following [Gloomberb](https://github.com/gloom-sh/gloomberb), including home/onboarding. The current design uses its default White Phosphor direction, verified against the [theme definitions](https://github.com/gloom-sh/gloomberb/blob/main/src/theme/themes.ts): black/charcoal surfaces, white focus accents, gray selections, thin square dividers and compact full-width pane title bars. Muted body text uses a brighter gray for readability. The [onboarding frame](https://github.com/gloom-sh/gloomberb/blob/main/src/components/onboarding/onboarding-frame.tsx) informed the opaque black welcome screen and centered single-border connection card, adapted using the existing Textual widgets.

First run shows a branded header, welcome heading, provider/model/key form and persistent keyboard hints. `/model` uses the same frame with update wording. Credential masking, the privacy/publication notice before key entry, provider isolation, cancellation and normal Tab form traversal remain intact. The card scrolls and retains input values/focus when resized between 120×36 and 45×12. The workspace welcome follows the same style; dense layouts omit extra brand text to preserve reading space. Tab/Shift+Tab still cycle all six views, and F1–F4 remain alternate shortcuts. All actual strategy/test/account availability states remain explicit; this visual change adds no research or trading integration.

Verification: **133 local tests passed with zero skips** (130 core plus three native Intel engine regressions), including the added onboarding resize/value-retention checks. Lint and formatting passed. Standard/compact workspace and onboarding renders were visually inspected. The built distribution includes its stylesheet and launches the welcome screen and both Tab directions successfully; environment/cache files are excluded. Credential scans passed for tracked files, packages and current UI/live artifacts. Ignored evidence and previews are in reports/ui/gloomberb/.

Two real requests through the new onboarding → LangGraph → OpenRouter flow passed conversation memory and completed history commits, in **1.355 and 1.171 seconds**, each reporting **0 USD**. Slash-command and forward/reverse Tab navigation made no additional model calls. These observations verify chat/navigation continuity, not strategy or execution capability. GitHub CI is recorded separately after publication.

### Gloomberb rework CI status

Commit `3baa4fcbd9b97525f9404626e988b923d1902fee` is published. [Platform run 39024953527](https://github.com/JKGiova/stratosphere/actions/runs/39024953527) and [Intel engine run 39024953491](https://github.com/JKGiova/stratosphere/actions/runs/39024953491) failed before any job steps executed. All five checks report the same GitHub account payment/spending-limit restriction; the ARM64 macOS capacity advisory is also present. No fresh platform or Intel-source-build pass is claimed for this commit. The 133-test local suite, live chat, visual and distribution checks above passed independently. No billing setting was changed.

## Maximized CLI startup

Historical implementation; removed by the subsequent user-requested responsive redesign below. Current startup does not resize the terminal window.

October 17, 2026: the user requested a maximized window on every CLI launch and identified **macOS Terminal** as their terminal app. Before Textual starts, local `Apple_Terminal` sessions invoke a packaged native script with the caller's exact TTY. It selects the containing Terminal window, finds the display with the greatest overlap, converts between Terminal/AppKit coordinates and applies the display's current usable frame. The Dock and menu bar retain their space. Saved Terminal profiles and unrelated windows are not changed.

Other terminals receive the documented [XTWINOPS maximize request](https://invisible-island.net/xterm/ctlseqs/ctlseqs.html). Native failures or a five-second timeout fall back to that advisory request; terminal support/settings govern whether it is honored. Remote SSH/multiplexer sessions do not invoke native desktop automation. Redirected streams and missing/dumb/unknown terminals receive no window request. The CLI explicitly starts Textual with `inline=False`; model setup, Tab navigation and resize handling remain unchanged. The native script uses Apple's [current usable screen rectangle](https://developer.apple.com/documentation/appkit/nsscreen/visibleframe), with no new dependency or saved setting.

Verification: **149 tests passed locally with zero skips** (146 core and three native Intel engine regressions); lint and formatting passed. Sixteen launch tests cover repeat launches/order, redirected streams, unavailable terminals, native process arguments and failures, remote-session guards, and an actual PTY protocol write. macOS-only JXA tests execute the production geometry and TTY-selection helpers against mock windows/displays; they verify a non-front target, missing-TTY rejection, secondary displays and menu/Dock offsets without moving a real desktop window. Those two tests are explicitly skipped on Linux, not counted as native verification there. The complete native script compiles successfully. A real CLI smoke run in a PTY verified the maximize request precedes the alternate screen, onboarding renders, and cancellation/keyboard exit works. That smoke run does not acknowledge graphical window resizing.

The built wheel contains both the Python launcher and native JavaScript asset; source/wheel packages exclude environment/cache files, and credential scans passed. Ignored evidence is in reports/launch/. No live model request was needed for this startup change. Visible resizing and any macOS Automation permission flow remain a manual acceptance check in the user's normal Terminal session; they are not claimed as tested here. GitHub CI status is recorded separately after publication.

### Maximized startup CI status

Commit `2d779178e82a7b5955873a8ca51338e93e8b7f5f` is published. [Platform run 39031949718](https://github.com/JKGiova/stratosphere/actions/runs/39031949718) and [Intel engine run 39031949752](https://github.com/JKGiova/stratosphere/actions/runs/39031949752) failed before any job steps executed. All five checks report the existing GitHub account payment/spending-limit restriction; the macOS ARM64 capacity advisory is also present. Fresh cross-platform and Intel-source-build verification remains blocked. The 149-test local suite, native-script compilation, PTY startup smoke and package checks passed independently. No billing setting was changed.

## Responsive workspace and inline onboarding

October 17, 2026: the user revised the window-size request: remove automatic maximization and adapt to the terminal’s existing character grid. The native Mac script and maximize escape path are removed. Six independently mounted panels now support six (3×2), four (2×2), two horizontal, two vertical and one-panel arrangements. The initial thresholds and selection order are documented in the [terminal interface](terminal-interface.md#workspace-layout-and-navigation). Hidden panels remain reachable through Tab/Shift+Tab, clicks and deterministic slash commands. Resizing retains drafts, cursor position, conversation/model state and the selected panel.

Model onboarding and `/model` are inline forms inside agent chat, with masked session-only keys, provider isolation, privacy notice, validation and normal field navigation. There is no custom initial screen. The brand is static text and no longer opens the built-in header menu. Assistant text renders Markdown while streaming, including headings, emphasis, lists, code and tables; user/command text remains literal. No code execution, automatic link opening, new source adapter, research action or trading capability is added.

Verification: **157 local tests passed with zero skips** (154 core and three native Intel engine regressions), with lint and formatting passing. Tests cover exact layout boundaries, non-overlapping visible panels, every layout transition, all six views in both Tab directions, drafts/cursor/history retention, selected-panel focus during streaming/resize, inline setup validation/cancellation/provider isolation, masked values across every layout and an inert brand. The Markdown test streams headings, emphasis, lists, tables and a split fenced code block, verifies formatted output and literal Rich tags, and retains the original model history. Ten existing upstream Pandas deprecation warnings remain in engine tests.

Generated workspace, inline-onboarding and actual live-response renders were visually inspected. The built wheel includes the responsive layout module and stylesheet; both distributions exclude the removed native launcher and local environment/cache files. A packaged CLI smoke run in an actual PTY preserved the initial 180×48 terminal size, rendered setup inside agent chat, handled five resize events across the layouts, emitted no window-size request and exited cleanly from onboarding. Exact-key scans passed for tracked/new files, current artifacts and distribution contents. Ignored evidence is in reports/ui/responsive/.

One actual inline-onboarding → LangGraph → OpenRouter request using the selected Cohere free model completed a formatted sample in **6.883 seconds**, with **34 streamed text deltas**, one committed conversation turn and reported usage cost **0 USD**. Heading/emphasis/code/table formatting passed, followed by resize/selected-panel preservation and `/jobs` routing without another model request. This is bounded chat/UI verification, not research or trading validation. GitHub CI is recorded separately after publication. Earlier UI/live-model evidence above describes its respective historical revision.

### Responsive redesign CI status

Implementation commit `14fd9d05a413ed77f56dfb53a54814df76b4fd22` is published. [Platform run 39033277280](https://github.com/JKGiova/stratosphere/actions/runs/39033277280) and [Intel engine run 39033277305](https://github.com/JKGiova/stratosphere/actions/runs/39033277305) failed before any job steps executed. All five checks report the existing account payment/spending-limit restriction; the macOS ARM64 capacity advisory is also present. No new cross-platform or Intel-source-build pass is claimed. The 157-test local suite, real free-model formatting check, visual inspection and packaged CLI smoke passed independently. No billing setting was changed.

## Remaining sequential increments

1. Extend provider/structured-output support beyond the current verified chat checks. Build the shared validated operation layer before giving the chief agent execution tools. Keep consequential actions subject to their required approval and capability gates.
2. Implement the controlled registry harness: immutable canonical definitions, exact deduplication, persistent random-ID uniqueness/collision retry, source accumulation, distinct variants and shared/private projections with ownership/transaction tests. Do not expose an unauthenticated public service.
3. Implement all five required source adapters: YouTube, Instagram, X, TradingView and GitHub. Preserve provenance and return explicit inaccessible/unsupported outcomes. Verify accessible examples and provider failure paths for each; none is implemented by this terminal increment.
4. Wire registry/source operations into the same deterministic layer for chat and slash commands, with private/public notices and acceptance tests on Linux/macOS. Integrate shared operations with the local approval/pause/resume/report worker implemented in Phase 2.

These are implementation dependencies, not a reduction of Phase 1 scope. Login setup, fee collection and trading execution stay in their later phases. Phase 2 retains the end-to-end strategy implementation/backtest/report workflow. Final first-run onboarding still includes hosted login and optional trading connections once implemented.

## Subsequent Phase 2 authorization

The user authorized Phase 2 before the remaining Phase 1 registry/source increments were finished, and selected BTC/USDT spot hourly data. The [Phase 2 record](phase2.md) covers the implemented local text/specification/backtest/report path and its explicit limits. Phase 1 remains in progress; pasted text does not implement any of the five content adapters, shared registration or hosted ownership access.

## Direct provider expansion — October 18, 2026

The requested Claude, OpenAI, Grok, Groq, DeepSeek, Gemini, Kimi, Qwen, GLM and Ollama adapters are implemented alongside OpenRouter/NVIDIA. Inline setup now includes a provider-specific API base URL; credentials, addresses and model defaults remain isolated. Claude uses native Messages streaming; compatible APIs share the validated text transport. Local Ollama can omit a key, including in chat/source guards. See [model providers](model-providers.md) for setup, regional scope, official references and limits.

The final local suite passed **380 tests with zero skips**, including the native engine checks; lint, formatting, credential scans, built-wheel onboarding and asset checks passed. Real chat checks passed through the existing free OpenRouter model and an already installed local Ollama model. New hosted-provider authentication and model quality remain unverified. This does not implement source adapters, shared registration or unrestricted model tools; Phase 1 remains in progress.

GitHub verification for this provider increment is recorded in [model providers](model-providers.md#github-verification): implementation `98235d0` is published, but the platform and Intel jobs were blocked before execution by the existing account billing restriction.

## Strategy-agent foundation — October 18, 2026

The user subsequently authorized Phases 3–5 and a per-strategy agent architecture. The first foundation adds stable local identities, exact duplicate/source/submission handling, separate specialist histories, validated natural-language routing, and Reports in place of Session. The new Supabase project has a private schema with tested ownership and immutable identity/report controls. This is a controlled development foundation: hosted application-service synchronization, registry publication/authentication and external source integrations remain incomplete. See [strategy agents](strategy-agents.md) and the [full acceptance ledger](phase3-5.md).
