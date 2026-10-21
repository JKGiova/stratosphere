"""Protocol, credential routing and workflow coverage for direct model providers."""

import asyncio
import json

import httpx
import pytest
from pydantic import SecretStr, ValidationError
from textual.widgets import Input, Select

from stratosphere.chat import ChatSession
from stratosphere.models import create_provider
from stratosphere.models.claude import ClaudeChat
from stratosphere.models.compatible import CompatibleChat
from stratosphere.models.settings import BASE_URLS, DEFAULT_MODELS, KEY_ENVIRONMENTS, ModelSettings
from stratosphere.models.streaming import ModelError
from stratosphere.tui import StratosphereApp

DIRECT = [p for p in DEFAULT_MODELS if p not in {"openrouter", "nvidia", "claude"}]
KEY = "isolated-provider-secret"


def settings(provider, **kwargs):
    return ModelSettings(
        provider=provider,
        model=DEFAULT_MODELS[provider],
        api_key=SecretStr("" if provider == "ollama" else KEY),
        **kwargs,
    )


def frame(content="", finish=None, **delta):
    return (
        "data: "
        + json.dumps(
            {
                "choices": [
                    {
                        "delta": {"content": content, **delta},
                        "finish_reason": finish,
                    }
                ]
            }
        )
        + "\n\n"
    )


def native(kind, **data):
    return f"event: {kind}\ndata: " + json.dumps({"type": kind, **data}) + "\n\n"


def claude_frames(text="Hello"):
    return [
        native("message_start", message={"role": "assistant"}),
        native("content_block_start", index=0, content_block={"type": "text", "text": ""}),
        native("content_block_delta", index=0, delta={"type": "text_delta", "text": text}),
        native("content_block_stop", index=0),
        native("message_delta", delta={"stop_reason": "end_turn"}),
        native("message_stop"),
    ]


class Wire(httpx.AsyncByteStream):
    def __init__(self, parts):
        self.parts = parts
        self.closed = False

    async def __aiter__(self):
        for part in self.parts:
            # Deliberately split frames at arbitrary byte boundaries.
            data = part.encode()
            yield data[:7]
            yield data[7:]

    async def aclose(self):
        self.closed = True


def client(provider, parts, *, status=200, requests=None, **kwargs):
    wire = Wire(parts)

    def handler(request):
        if requests is not None:
            requests.append(request)
        return httpx.Response(status, headers={"content-type": "text/event-stream"}, stream=wire)

    config = settings(provider, **kwargs)
    adapter = ClaudeChat if provider == "claude" else CompatibleChat
    return adapter(config, transport=httpx.MockTransport(handler)), wire


@pytest.mark.parametrize("provider", DIRECT)
async def test_direct_provider_stream_and_exact_request(provider):
    requests = []
    adapter, wire = client(
        provider,
        [
            ": heartbeat\n\n",
            frame(reasoning_content="private thinking"),
            frame("Hello "),
            frame("world"),
            frame(finish="stop"),
            'data: {"choices":[],"usage":{}}\n\n',
            "data: [DONE]\n\n",
        ],
        requests=requests,
    )
    session = ChatSession(adapter)
    stream = session.respond("Hi")
    assert await anext(stream) == "Hello "
    assert "".join([part async for part in stream]) == "world"
    assert len(session.history) == 2 and wire.closed
    request = requests[0]
    assert str(request.url) == BASE_URLS[provider] + "/chat/completions"
    if provider == "ollama":
        assert "authorization" not in request.headers
    else:
        assert request.headers["authorization"] == f"Bearer {KEY}"
    body = json.loads(request.content)
    assert body["model"] == DEFAULT_MODELS[provider] and body["stream"] is True
    assert body["messages"][-1] == {"role": "user", "content": "Hi"}
    assert "tools" not in body and "provider" not in body
    limit = "max_completion_tokens" if provider in {"openai", "groq"} else "max_tokens"
    assert body[limit] == 4096
    if provider == "openai":
        assert body["store"] is False and "max_tokens" not in body
    if provider == "kimi":
        assert body["reasoning_effort"] == "low"
    if provider == "ollama":
        assert body["reasoning_effort"] == "none"


@pytest.mark.parametrize("provider", list(DEFAULT_MODELS))
def test_factory_and_missing_key(provider):
    assert create_provider(settings(provider)).settings.provider == provider
    if provider != "ollama":
        with pytest.raises(ValidationError):
            ModelSettings(provider=provider, model=DEFAULT_MODELS[provider])


@pytest.mark.parametrize("provider", DIRECT + ["claude"])
@pytest.mark.parametrize("status", [401, 429, 503, 307])
async def test_no_fallback_redirect_or_history_on_provider_failure(provider, status):
    requests = []
    adapter, wire = client(provider, [KEY], status=status, requests=requests)
    session = ChatSession(adapter)
    with pytest.raises(ModelError) as error:
        _ = [part async for part in session.respond("Hi")]
    assert KEY not in str(error.value)
    assert len(requests) == 1 and session.history == [] and wire.closed


@pytest.mark.parametrize("provider", DIRECT)
@pytest.mark.parametrize("ending", ["", "data: [DONE]\n\n", frame(finish="length")])
async def test_compatible_incomplete_stream_does_not_commit(provider, ending):
    adapter, wire = client(provider, [frame("partial"), ending])
    session = ChatSession(adapter)
    with pytest.raises(ModelError):
        _ = [part async for part in session.respond("Hi")]
    assert session.history == [] and wire.closed


async def test_native_claude_system_auth_thinking_redaction_and_multi_turn():
    requests = []
    thinking = [
        native("content_block_start", index=0, content_block={"type": "thinking"}),
        native("content_block_delta", index=0, delta={"type": "thinking_delta", "thinking": KEY}),
        native("content_block_delta", index=0, delta={"type": "signature_delta", "signature": KEY}),
        native("content_block_stop", index=0),
    ]
    parts = claude_frames()
    # Text moves to the second content block after hidden reasoning.
    parts[1:1] = [native("ping"), *thinking]
    parts = [
        part.replace('"index": 0', '"index": 1') if i >= 6 else part for i, part in enumerate(parts)
    ]
    # Split the configured credential across text events.
    parts[7:8] = [
        native(
            "content_block_delta", index=1, delta={"type": "text_delta", "text": "Hi " + KEY[:8]}
        ),
        native(
            "content_block_delta",
            index=1,
            delta={"type": "text_delta", "text": KEY[8:] + "\u001b[31m"},
        ),
    ]
    adapter, wire = client("claude", parts, requests=requests)
    session = ChatSession(adapter)
    reply = "".join([p async for p in session.respond("First")])
    assert reply == "Hi [redacted][31m" and wire.closed
    request = requests[0]
    assert str(request.url) == "https://api.anthropic.com/v1/messages"
    assert request.headers["x-api-key"] == KEY
    assert request.headers["anthropic-version"] == "2023-06-01"
    assert "authorization" not in request.headers
    body = json.loads(request.content)
    assert isinstance(body["system"], str) and body["system"]
    assert body["messages"] == [{"role": "user", "content": "First"}]
    assert body["stream"] is True and body["max_tokens"] == 4096
    # A new request retains only user-visible text and preserves system separation.
    _ = [p async for p in session.respond("Second")]
    second = json.loads(requests[1].content)
    assert [m["role"] for m in second["messages"]] == ["user", "assistant", "user"]
    assert KEY not in second["messages"][1]["content"]


@pytest.mark.parametrize(
    "mutation",
    [
        "no-start",
        "no-block-stop",
        "no-stop",
        "no-reason",
        "wrong-index",
        "bad-json",
        "truncated",
        "tool",
        "refusal",
        "duplicate-start",
        "empty",
        "error",
        "late-text",
    ],
)
async def test_native_claude_rejects_invalid_or_incomplete_messages(mutation):
    parts = claude_frames()
    if mutation == "no-start":
        parts.pop(0)
    elif mutation == "no-block-stop":
        parts.pop(3)
    elif mutation == "no-stop":
        parts.pop()
    elif mutation == "no-reason":
        parts.pop(4)
    elif mutation == "wrong-index":
        parts[2] = parts[2].replace('"index": 0', '"index": 3')
    elif mutation == "bad-json":
        parts[2] = "data: {broken\n\n"
    elif mutation == "truncated":
        parts[4] = native("message_delta", delta={"stop_reason": "max_tokens"})
    elif mutation == "tool":
        parts[1] = native("content_block_start", index=0, content_block={"type": "tool_use"})
    elif mutation == "refusal":
        parts[4] = native("message_delta", delta={"stop_reason": "refusal"})
    elif mutation == "duplicate-start":
        parts.insert(1, parts[0])
    elif mutation == "empty":
        parts = claude_frames("   ")
    elif mutation == "error":
        parts[3] = native("error", error={"type": "overloaded_error", "message": KEY})
    elif mutation == "late-text":
        parts.insert(5, parts[2])
    adapter, wire = client("claude", parts)
    session = ChatSession(adapter)
    with pytest.raises(ModelError) as error:
        _ = [p async for p in session.respond("Hi")]
    assert KEY not in str(error.value) and session.history == [] and wire.closed


@pytest.mark.parametrize("provider", DIRECT + ["claude"])
async def test_cancel_closes_new_provider_stream(provider):
    entered = asyncio.Event()

    class Waiting(Wire):
        async def __aiter__(self):
            entered.set()
            await asyncio.Event().wait()
            yield b""

    wire = Waiting([])
    adapter = create_provider(settings(provider))
    adapter.transport = httpx.MockTransport(
        lambda _: httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=wire,
        )
    )
    session = ChatSession(adapter)

    async def consume():
        return [p async for p in session.respond("Hi")]

    task = asyncio.create_task(consume())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert wire.closed and not session.history


@pytest.mark.parametrize(
    "provider,url",
    [
        ("qwen", "https://workspace-123.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1"),
        ("qwen", "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"),
        ("ollama", "http://127.0.0.1:11435/v1"),
        ("ollama", "http://[::1]:11434/v1"),
    ],
)
def test_explicit_supported_destinations(provider, url):
    assert settings(provider, base_url=url).endpoint == url + "/chat/completions"


@pytest.mark.parametrize(
    "provider,url",
    [
        ("openai", "https://attacker.example/v1"),
        (
            "qwen",
            "https://workspace.ap-southeast-1.maas.aliyuncs.com.attacker.example/compatible-mode/v1",
        ),
        ("qwen", "http://dashscope.aliyuncs.com/compatible-mode/v1"),
        ("ollama", "http://remote.example:11434/v1"),
        ("ollama", "http://localhost:11434/v1?key=secret"),
        ("ollama", "http://user:password@localhost:11434/v1"),
        ("ollama", "http://localhost:99999/v1"),
        ("openai", "https://api.openai.com/v1#secret"),
    ],
)
def test_unapproved_destinations_rejected(provider, url):
    with pytest.raises(ValidationError):
        settings(provider, base_url=url)


def test_ollama_cloud_requires_its_own_key():
    with pytest.raises(ValidationError):
        settings("ollama", base_url="https://ollama.com/v1")
    config = ModelSettings(
        provider="ollama",
        model="gpt-oss:120b-cloud",
        api_key=SecretStr(KEY),
        base_url="https://ollama.com/v1",
    )
    assert create_provider(config).request_headers()["Authorization"] == f"Bearer {KEY}"


@pytest.mark.parametrize("provider", DIRECT + ["claude"])
async def test_onboarding_provider_isolation_and_submission(provider, monkeypatch, tmp_path):
    monkeypatch.setenv("STRATOSPHERE_RESEARCH_DIR", str(tmp_path))
    monkeypatch.setenv("STRATOSPHERE_PROVIDER", "openrouter")
    monkeypatch.delenv("STRATOSPHERE_MODEL", raising=False)
    monkeypatch.setenv(KEY_ENVIRONMENTS[provider], "" if provider == "ollama" else KEY)
    app = StratosphereApp()
    async with app.run_test(size=(110, 40)) as pilot:
        app.query_one("#api-key", Input).value = "other-provider-key"
        app.query_one("#base-url", Input).value = "https://other.example/v1"
        app.query_one("#provider", Select).value = provider
        await pilot.pause()
        assert app.query_one("#api-key", Input).value == ("" if provider == "ollama" else KEY)
        assert app.query_one("#base-url", Input).value == BASE_URLS[provider]
        assert app.query_one("#model-id", Input).value == DEFAULT_MODELS[provider]
        app.setup.apply_settings()
        await pilot.pause()
        assert app.settings.provider == provider and app.setup is None
        # Slash commands must remain usable with an empty local credential.
        app.query_one("#prompt", Input).value = "/help"
        app.query_one("#prompt", Input).focus()
        await pilot.press("enter")
        await pilot.pause()
        assert app.query_one("#prompt", Input).value == ""
        assert app.session.history == []


@pytest.mark.parametrize("provider", DIRECT + ["claude"])
async def test_adapter_yields_text_before_terminal_event(provider):
    parts = (
        claude_frames()
        if provider == "claude"
        else [
            frame("Hello"),
            frame(finish="stop"),
            "data: [DONE]\n\n",
        ]
    )
    adapter, wire = client(provider, parts)
    stream = adapter.stream([{"role": "user", "content": "Hi"}])
    assert await anext(stream) == "Hello"
    assert not wire.closed
    _ = [part async for part in stream]
    assert wire.closed


@pytest.mark.parametrize("provider", DIRECT + ["claude"])
async def test_same_validated_research_contract_across_providers(
    provider,
    research_draft,
    research_source,
):
    from stratosphere.research.extraction import extract_strategy

    text = research_draft.model_dump_json()
    parts = (
        claude_frames(text)
        if provider == "claude"
        else [
            frame(text),
            frame(finish="stop"),
            "data: [DONE]\n\n",
        ]
    )
    requests = []
    adapter, wire = client(provider, parts, requests=requests)
    assert await extract_strategy(adapter, research_source) == research_draft
    assert len(requests) == 1 and wire.closed
    body = json.loads(requests[0].content)
    assert body["messages"][-1]["content"] == research_source


@pytest.mark.parametrize("key", ["bad\nkey", "bad\rkey", "bad key", "badékey"])
@pytest.mark.parametrize("provider", ["claude", "openai", "ollama"])
def test_invalid_header_credentials_rejected_before_network(provider, key):
    with pytest.raises(ValidationError):
        ModelSettings(provider=provider, model=DEFAULT_MODELS[provider], api_key=SecretStr(key))


@pytest.mark.parametrize(
    "provider_name,body,context_error",
    [
        (
            "ollama",
            {
                "error": {
                    "message": "request (11281 tokens) exceeds the available context size "
                    "(4096 tokens), try increasing it"
                }
            },
            True,
        ),
        (
            "ollama",
            {
                "error": "request (11281 tokens) exceeds the available context size "
                "(4096 tokens), try increasing it"
            },
            True,
        ),
        ("ollama", {"error": {"message": KEY}}, False),
        (
            "ollama",
            {
                "error": {
                    "message": "request (1 tokens) exceeds the available context size "
                    "(4096 tokens), try increasing it"
                }
            },
            False,
        ),
        ("ollama", {"error": {"message": "X" * 20000}}, False),
        ("ollama", ["unsupported error format"], False),
        (
            "openai",
            {
                "error": {
                    "message": "request (11281 tokens) exceeds the available context size "
                    "(4096 tokens), try increasing it"
                }
            },
            False,
        ),
    ],
)
async def test_local_ollama_context_error_is_actionable_without_echo_or_retry(
    provider_name, body, context_error
):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(400, json=body)

    provider = CompatibleChat(settings(provider_name), transport=httpx.MockTransport(handler))
    with pytest.raises(ModelError) as caught:
        _ = [part async for part in provider.stream([{"role": "user", "content": "Authored test"}])]
    message = str(caught.value)
    assert ("larger num_ctx" in message) == context_error
    assert KEY not in message and "11281" not in message and "4096" not in message
    assert len(requests) == 1


@pytest.mark.parametrize("provider_name,expected_read", [("ollama", 180), ("openai", 60)])
async def test_local_prefill_uses_existing_total_budget_without_shorter_read_timeout(
    provider_name, expected_read
):
    requests = []
    provider, _ = client(
        provider_name, [frame("Hello"), frame(finish="stop"), "data: [DONE]\n\n"], requests=requests
    )
    assert (
        "".join([part async for part in provider.stream([{"role": "user", "content": "Hello"}])])
        == "Hello"
    )
    assert requests[0].extensions["timeout"]["read"] == expected_read
    assert requests[0].extensions["timeout"]["connect"] == 15
