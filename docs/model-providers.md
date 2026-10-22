# Model providers

Status: implemented adapters with protocol and workflow tests, October 18, 2026. Authenticated live access to the new hosted providers remains unverified until their own credentials are available. Local Ollama chat was verified against a running server as recorded below. This adds provider choice to the existing chat and validated research extraction; it does not add agent execution tools or complete Phase 1.

Start `python -m stratosphere`, then choose a provider in onboarding or `/model`. Enter its model ID, API base URL and API key. Model IDs are sent exactly as entered. The app does not discover models, install them, change provider automatically or retry with another model. A configured session is labeled unverified until a request completes.

| Provider ID | API key environment variable | Editable starting model | Default API base URL |
| --- | --- | --- | --- |
| `claude` | `ANTHROPIC_API_KEY` | `claude-sonnet-5-5` | `https://api.anthropic.com/v1` |
| `openai` | `OPENAI_API_KEY` | `gpt-4.1-mini` | `https://api.openai.com/v1` |
| `grok` | `XAI_API_KEY` | `grok-4.7` | `https://api.x.ai/v1` |
| `groq` | `GROQ_API_KEY` | `llama-3.3-70b-versatile` | `https://api.groq.com/openai/v1` |
| `deepseek` | `DEEPSEEK_API_KEY` | `deepseek-flash` | `https://api.deepseek.com/v1` |
| `gemini` | `GEMINI_API_KEY` | `gemini-3.8-flash` | `https://generativelanguage.googleapis.com/v1beta/openai` |
| `kimi` | `MOONSHOT_API_KEY` | `kimi-k3` | `https://api.moonshot.ai/v1` |
| `qwen` | `DASHSCOPE_API_KEY` | `qwen3.7-plus` | `https://dashscope.aliyuncs.com/compatible-mode/v1` |
| `glm` | `ZAI_API_KEY` | `glm-5.3` | `https://api.z.ai/api/paas/v4` |
| `ollama` | `OLLAMA_API_KEY` (optional locally) | `gpt-oss:20b` | `http://localhost:11434/v1` |
| `openrouter` | `OPENROUTER_API_KEY` | `cohere/north-mini-code:free` | `https://openrouter.ai/api/v1` |
| `nvidia` | `NVIDIA_API_KEY` | `deepseek-ai/deepseek-v4.1-flash` | `https://integrate.api.nvidia.com/v1` |

OpenRouter remains the default, restricted to explicit `:free` models and zero prompt/completion price ceilings. Other hosted APIs use the user's provider account and may charge for requests. Model availability depends on account access and provider changes; the example IDs above are not a claim that all accounts can access them.

Environment variables prefill only the matching provider's fields. `STRATOSPHERE_PROVIDER` selects initial onboarding, `STRATOSPHERE_MODEL` overrides its initial model, and `<PROVIDER_ID_IN_UPPERCASE>_BASE_URL` prefills that provider's address (for example `QWEN_BASE_URL` or `OLLAMA_BASE_URL`). Otherwise enter values directly in the masked onboarding form. The client does not read `.env` automatically. Under D59, the selected settings/key are remembered in the [native OS credential store](credentials.md) and restored on startup; general chat remains session-only and strategy histories/research evidence stay private locally. `/model-forget` removes saved model access. Switching providers discards unsubmitted credentials and addresses, and never forwards another provider's key. Applying model settings starts a fresh chat history.

## Addresses and regional accounts

Cloud requests use fixed official HTTPS destinations. Claude uses `/messages`; other adapters append `/chat/completions` to the displayed base URL. Enter the base URL without those final paths. Redirects and ambient HTTP proxies are disabled. Arbitrary gateways and account subscription/OAuth credentials are outside this implementation.

For Qwen, use the address matching your API key's region. The default is the documented legacy mainland China address. International/US legacy addresses `https://dashscope-intl.aliyuncs.com/compatible-mode/v1` and `https://dashscope-us.aliyuncs.com/compatible-mode/v1` are accepted. Current workspace URLs are recommended: `https://<workspace-id>.<region>.maas.aliyuncs.com/compatible-mode/v1`, with regions `ap-southeast-1`, `us-east-1`, `cn-hongkong` or `ap-northeast-1`. Copy the actual workspace address from Model Studio. No regional failover occurs. GLM currently targets the Z.ai general API, and Kimi the Moonshot international API; their China-specific and coding-subscription endpoints are not configured.

For Ollama, run the server separately and install the model you enter, for example:

```sh
ollama serve
ollama pull gpt-oss:20b
python -m stratosphere
```

Choose `ollama` and leave the local key empty. Local addresses may use `localhost`, `127.0.0.1` or `[::1]` with an explicit port and `/v1` path. Ollama's direct cloud address `https://ollama.com/v1` is also accepted, and requires an Ollama API key and a model available there. Plain HTTP to remote hosts is rejected. Selecting local inference is the explicit exception introduced by this request to the earlier hosted-model-only plan: research data acquisition, approved runs/resume and app report inspection still require their existing online checks. There is no cached model fallback or general offline research mode.

## Streaming and verification boundaries

Claude uses native Messages authentication, a top-level system prompt and validated content-block/message events. Thinking is excluded from visible text/history; tools, refusals, incomplete blocks and truncation cannot become successful turns. The other new providers use their official OpenAI-compatible text endpoints. OpenAI and Groq use `max_completion_tokens`; OpenAI sets `store=false`. Kimi K3 requests low reasoning effort. Ollama requests `reasoning_effort=none` and uses JSON-schema constrained responses for structured workflow operations; other model reasoning settings use provider defaults. All adapters retain the 4,096-token cap (including reasoning where the provider counts it), 15-second connect timeout, 60-second inactivity timeout and 180-second total deadline. Large reasoning responses may exceed these bounds and return an explicit failure.

The shared transport redacts the configured key even across streamed deltas, strips terminal controls and hides provider error bodies. Missing keys are rejected for hosted destinations; keyless local Ollama neither sends an empty bearer header nor treats every chat/source as containing a credential. Completed text enters history only after a valid finish event and terminal marker. Cancellation closes the response without committing the turn. Model output never executes commands. Research still parses the same evidence-checked schema and requires exact-plan approval before simulations.

Tests cover all requested providers' endpoint/auth/body routing, text streaming, cancellation, incomplete/error handling, onboarding credential isolation and the existing research extraction contract. They are simulated protocol tests; they do not establish equal model quality, live account access, or provider tool equivalence. A real free OpenRouter request completed in 1.109 seconds. A real local Ollama request using the already installed `hf.co/XHToken/Spark-X2.5-1.7B-GGUF:Q4_K_M` completed in 10.942 seconds, with one committed chat turn and no key. That installed model was selected explicitly for this check; the editable default remains `gpt-oss:20b`. This verifies the local Ollama transport and chat path, not strategy extraction quality for that model. The built wheel/sdist passed asset and exact-credential scans; isolated built-wheel onboarding passed at 45×12 and 110×40.

## Final local validation

### Structured workflow schema delivery

A local metadata inspection on 21 October 2026 found Ollama **0.40.6** and the same two installed models. Qwopus3.5 4B advertises `thinking.values=[true]`; Spark-X2.5 1.7B exposes no thinking metadata. The adapter's existing `reasoning_effort=none` is therefore a request, not proof that either model disables reasoning. [Ollama's controls](https://docs.ollama.com/capabilities/thinking) and [compatibility reference](https://docs.ollama.com/api/openai-compatibility) make behavior model-dependent. This inspection generated no text, changed no model/settings and downloaded nothing; it does not supersede the retained timeout or interpretation-quality findings below.

Structured requests previously passed a schema to the transport, but only Ollama received it through a native format parameter. Other adapters silently omitted it, so workflows without a complete inline example could ask for an output shape the model never saw.

The `request-json-schema-text/1` delivery contract now appends the exact, deterministically serialized application schema to a copied system message for all 12 configured adapters, or creates a system message when absent. Claude's existing conversion folds it into the native top-level system field. Ollama retains its additional native schema constraint; no new native-format capability is assumed for other providers. Ordinary chat is unchanged, and the transport never mutates caller messages or history. Instructions remain prompt guidance, not proof of compliant output: client parsing, reference checks, deterministic operations and approval gates still apply. Invalid output is neither repaired nor retried automatically; OpenRouter's existing free-model and zero-price limits remain unchanged.

New source/account review provenance includes this delivery contract. Budget/risk review aborts if it changes between stages. Historical persisted formats and readers remain compatible; old report hashes are not replaced with a digest of the new prompts. An actual prior paper report and two prior source reports reopened without changing artifact bytes or hashes, using an explicit fixture online gate.

Wire tests verify exact schema delivery and unchanged ordinary chat across all 12 adapters. These are HTTP fixtures, not authenticated live-provider capability checks. Local Spark completed two strategy-owned source reviews with six valid source references and isolated/restored contexts in 37.623 seconds; response inspection still found inaccurate interpretations, including an unsupported crossing claim against a period-only citation and nonsensical terminology. Interpretation quality remains unverified.

The unchanged 4B account workflow returned a valid two-instance budget but timed out during risk generation after 180 seconds; its partial narrative included unsupported accounting claims. After the delivery/prompt clarification, Spark timed out before completing its budget. Neither check completed a reliable budget/risk review. After the user resolved the initial payload/destination approval block, exactly two sealed artificial-fixture requests ran on `cohere/north-mini-code:free` with streaming, low reasoning, a 4,096-token ceiling and zero-price guards. The budget passed validation, but the risk stream returned no visible text and a non-success terminal condition classified as truncation; no completed review resulted (81.952 seconds total). The diagnostic did not retain the raw terminal reason or usage, so the root cause remains unknown. No further calls or limit changes followed. The [approved fixture record](phase5-paper-reviews.md#approved-openrouter-fixture-follow-up) separates that live failure from 230 passing local boundary checks. No model was downloaded, no paid capacity was enabled and no application provider was replaced automatically. Final regression/package evidence is recorded in the [acceptance ledger](phase3-5.md).

The final suite passed **380 tests with zero skips**, including seven tests with the pinned native NautilusTrader engine. The 29 upstream Pandas deprecation warnings remain visible. Ruff lint/formatting and `git diff --check` passed. No dependency or lockfile changes were needed. The rebuilt wheel/sdist and isolated package onboarding checks passed after the final credential validation changes. Both package archives and publishable files passed exact scans for configured local credentials. Screenshots confirmed focused address/key fields remain accessible in the existing scrollable inline form.

New hosted APIs have protocol/workflow coverage only. Live checks passed for the existing free OpenRouter route and the new local Ollama route, as detailed above. Cross-platform GitHub execution is recorded separately after publication; local tests do not imply a new CI pass or release approval.

## Official API references

- [Anthropic Messages](https://platform.claude.com/docs/en/api/messages/create) and [streaming](https://platform.claude.com/docs/en/build-with-claude/streaming).
- [OpenAI Chat Completions](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create) and [GPT-4.1 mini](https://developers.openai.com/api/docs/models/gpt-4.1-mini).
- [xAI Chat Completions](https://docs.x.ai/developers/rest-api-reference/inference/chat-completions). This stateless text integration does not implement xAI's newer Responses features.
- [Groq compatibility](https://console.groq.com/docs/openai) and [text streaming](https://console.groq.com/docs/text-chat).
- [DeepSeek API](https://api-docs.deepseek.com/).
- [Gemini OpenAI compatibility](https://ai.google.dev/gemini-api/docs/openai).
- [Kimi quickstart](https://platform.kimi.ai/docs/overview).
- [Qwen OpenAI compatibility and workspace addresses](https://www.alibabacloud.com/help/en/model-studio/compatibility-of-openai-with-dashscope).
- [Z.ai quickstart](https://docs.z.ai/guides/overview/quick-start).
- [Ollama OpenAI compatibility](https://docs.ollama.com/api/openai-compatibility).

## GitHub verification

Implementation commit `98235d0c22ed4e36f32e78286bc73fed3686fcab` is published to the private repository. [Platform run 39111042797](https://github.com/JKGiova/stratosphere/actions/runs/39111042797) and [Intel run 39111042821](https://github.com/JKGiova/stratosphere/actions/runs/39111042821) failed before execution. GitHub reports that recent account payments failed or the spending limit must be increased. No new cross-platform CI pass is claimed, and no billing setting was changed. The 380-test local suite, package checks and live OpenRouter/Ollama checks passed independently.

## Complete report evidence and local Ollama context

[Measured report analyses](report-analysis.md) send the full frozen candidate/cell evidence set. They do not silently drop candidates or raw-value summaries to fit a model. Configure a large enough local context using a separate Ollama model definition and select its name with `/model`; [Ollama documents this setting through `num_ctx`](https://docs.ollama.com/api/openai-compatibility#setting-the-local-context-size), rather than an OpenAI-compatible per-request field. A recognized local HTTP 400 context-overflow response now yields a fixed actionable message without exposing provider text or retrying. Local Ollama prefill uses the existing 180-second overall request budget as its read timeout; other providers retain a 60-second read timeout and the overall 180-second cap is unchanged. No server setting/model alias is created by the TUI itself.
