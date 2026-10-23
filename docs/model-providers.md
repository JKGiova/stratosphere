# Model providers

## Selection

Configure a provider, exact model ID and allowed API base URL in onboarding or `/model`.
The implemented adapters are OpenRouter, NVIDIA NIM, Claude, OpenAI, Grok, Groq, DeepSeek,
Gemini, Kimi, Qwen, GLM, Ollama and Cheaper Inference. Their configured endpoints/defaults are defined in
[`models/settings.py`](../src/stratosphere/models/settings.py); a default ID is not a live-access guarantee.

OpenRouter is restricted to explicit `:free` models and zero prompt/completion price ceilings.
Other hosted providers may charge the user's account. The app does not discover/install models,
automatically switch providers, relax price ceilings or retry with a fallback model.

Environment variables prefill only the corresponding provider's fields. `STRATOSPHERE_PROVIDER`
and `STRATOSPHERE_MODEL` select initial values; provider base URL overrides must still pass the
allowlist. The client does not load `.env` automatically. Saved configuration uses the
[native credential store](credentials.md), not a project environment file.

## Transports

Claude uses native Messages streaming. Other adapters use supported OpenAI-compatible text
endpoints. Cloud addresses are restricted to the reviewed official HTTPS destinations; redirects
and ambient proxies are disabled. Credentials are scoped to the selected host/provider.

Local Ollama requires an already running server and installed model. A local key can be empty.
Local HTTP is limited to loopback addresses and explicit ports. This inference choice does not
remove research/reporting online gates. Use a model with enough configured context; the TUI does
not silently create aliases, change server settings or drop evidence to fit.

## Cheaper Inference

Select **Cheaper Inference** in `/model`; its preset is `qwen3.7-flash`, with the reviewed base URL
`https://api.cheaperinference.com/v1`. `CHEAPER_INFERENCE_API_KEY` can prefill only this provider;
masked setup saves it through native secure storage. Existing provider settings are not changed.
Cheaper Inference is a gateway and may send prompts to its serving model provider. This is separate
from the direct Alibaba Qwen adapter and may charge the user's provider account.

The [official API documentation](https://api.cheaperinference.com/docs) specifies Bearer-authenticated,
OpenAI-compatible chat streaming. The adapter's transport/schema behavior is fixture-tested;
availability of this exact model and authenticated completion still require a secure replacement
key and an actual provider check. No account funding or fallback is automatic.

## Structured workflows

Every adapter receives the exact application JSON Schema as explicit system-prompt data.
Ollama additionally uses its native schema constraint. Do not infer native tool/format support
for other providers. Caller messages and ordinary chat remain unchanged.

Completed output must pass transport finish checks, strict JSON parsing, duplicate-key rejection,
typed validation and evidence/ownership checks. Schema compliance and valid citations do not prove
semantic correctness. No model output is an approval or execution mandate.

Timeouts, context overflow, malformed/truncated output and cancellation fail explicitly without
committing an incomplete chat turn. Keys are filtered even across streamed deltas; terminal controls
and raw provider error bodies are not exposed. Model reasoning/latency/quality remain provider-dependent.

## Verification

All 13 adapters have simulated protocol/schema checks. This is not authenticated live verification
of every provider/model. Actual local and free-provider checks cover specific dated examples only.
The small installed local model can repeat calls or produce invalid output. One 4B artificial lookup
succeeded; reliable source/report/account interpretation remains a separate acceptance requirement.
