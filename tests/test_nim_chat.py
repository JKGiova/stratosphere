import asyncio
import json

import httpx
import pytest
from pydantic import SecretStr

from stratosphere.chat import ChatSession
from stratosphere.commands import route_command
from stratosphere.models.nim import ModelError, ModelSettings, NimChat

KEY = "unit-test-secret"
SETTINGS = ModelSettings(model="deepseek-ai/deepseek-v4.1-flash", api_key=SecretStr(KEY))


def event(content="", *, finish=None, **delta):
    return (
        "data: "
        + json.dumps(
            {"choices": [{"delta": {"content": content, **delta}, "finish_reason": finish}]}
        )
        + "\n\n"
    )


class Body(httpx.AsyncByteStream):
    def __init__(self, parts):
        self.parts = parts
        self.closed = False

    async def __aiter__(self):
        for part in self.parts:
            if isinstance(part, Exception):
                raise part
            yield part.encode()

    async def aclose(self):
        self.closed = True


def provider(parts, *, status=200, requests=None):
    body = Body(parts)

    def handler(request):
        if requests is not None:
            requests.append(request)
        return httpx.Response(status, headers={"content-type": "text/event-stream"}, stream=body)

    return NimChat(SETTINGS, transport=httpx.MockTransport(handler)), body


@pytest.mark.asyncio
async def test_stream_visible_before_completion_and_exact_request():
    requests = []
    client, body = provider(
        [
            ": heartbeat\n\n",
            event(reasoning_content="hidden"),
            event("Hello "),
            event("world"),
            event(finish="stop"),
            "data: [DONE]\n\n",
        ],
        requests=requests,
    )
    stream = client.stream([{"role": "user", "content": "Hi"}])
    assert await anext(stream) == "Hello "
    assert not body.closed
    assert [part async for part in stream] == ["world"]
    assert body.closed
    request = requests[0]
    assert str(request.url) == "https://integrate.api.nvidia.com/v1/chat/completions"
    assert request.headers["authorization"] == f"Bearer {KEY}"
    payload = json.loads(request.content)
    assert payload["stream"] is True
    assert payload["reasoning_effort"] == "none"
    assert payload["model"] == SETTINGS.model
    assert "tools" not in payload


@pytest.mark.asyncio
async def test_secret_spanning_deltas_and_terminal_controls_are_filtered():
    client, _ = provider(
        [
            event("Echo: unit-"),
            event("test-secret\x1b\x07"),
            event(finish="stop"),
            "data: [DONE]\n\n",
        ]
    )
    chunks = [chunk async for chunk in client.stream([])]
    assert "".join(chunks) == "Echo: [redacted]"
    assert all(KEY not in chunk for chunk in chunks)
    assert KEY not in repr(SETTINGS)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, expected",
    [(401, "denied"), (403, "denied"), (429, "rate limit"), (500, "failed"), (307, "failed")],
)
async def test_errors_do_not_echo_provider_body_or_retry(status, expected):
    requests = []
    client, body = provider([KEY], status=status, requests=requests)
    with pytest.raises(ModelError, match=expected) as error:
        _ = [part async for part in client.stream([])]
    assert KEY not in str(error.value)
    assert len(requests) == 1
    assert body.closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parts, expected",
    [
        ([event("Partial")], "disconnected"),
        ([event("Partial"), "data: [DONE]\n\n"], "incomplete"),
        ([event(finish="stop"), "data: [DONE]\n\n"], "incomplete"),
        (["data: not-json\n\n"], "invalid"),
        (['data: {"error":"unit-test-secret"}\n\n'], "request failed"),
        ([event("Partial", finish="length")], "truncated"),
        ([event(tool_calls=[{"name": "trade"}])], "invalid"),
        ([httpx.ReadTimeout(KEY)], "timed out"),
        ([httpx.ConnectError(KEY)], "Cannot reach"),
    ],
)
async def test_incomplete_and_failed_streams(parts, expected):
    client, body = provider(parts)
    with pytest.raises(ModelError, match=expected) as error:
        _ = [part async for part in client.stream([])]
    assert KEY not in str(error.value)
    assert body.closed


@pytest.mark.asyncio
async def test_history_commits_only_completed_turns_and_commands_never_call_model():
    requests = []
    client, _ = provider(
        [event("hello"), event(finish="stop"), "data: [DONE]\n\n"], requests=requests
    )
    session = ChatSession(client)
    assert route_command("/portfolio").action == "portfolio"
    assert requests == []
    assert "".join([part async for part in session.respond("Hi")]) == "hello"
    assert session.history == [
        {"role": "user", "content": "Hi"},
        {"role": "assistant", "content": "hello"},
    ]
    session.provider, _ = provider([event("broken"), httpx.ReadTimeout(KEY)])
    with pytest.raises(ModelError):
        _ = [part async for part in session.respond("failed question")]
    assert len(session.history) == 2
    assert session.busy is False


@pytest.mark.asyncio
async def test_cancellation_closes_stream_and_discards_turn():
    started = asyncio.Event()
    closed = asyncio.Event()

    class HangingBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield event("partial").encode()
            started.set()
            await asyncio.Event().wait()

        async def aclose(self):
            closed.set()

    client = NimChat(
        SETTINGS,
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                stream=HangingBody(),
            )
        ),
    )
    session = ChatSession(client)

    async def consume():
        return [part async for part in session.respond("question")]

    task = asyncio.create_task(consume())
    await asyncio.wait_for(started.wait(), timeout=3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()
    assert session.history == []
    assert session.busy is False


@pytest.mark.parametrize("text", ["/jobs extra", "/quit now", "/unknown", "/jobs; rm -rf /", "/"])
def test_invalid_commands_have_no_effect(text):
    assert route_command(text).action == "message"


@pytest.mark.asyncio
async def test_non_streaming_response_is_rejected_without_displaying_body():
    client = NimChat(
        SETTINGS,
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json={"error": KEY},
            )
        ),
    )
    with pytest.raises(ModelError, match="streaming") as error:
        _ = [part async for part in client.stream([])]
    assert KEY not in str(error.value)


@pytest.mark.asyncio
async def test_ambient_tracing_disabled_for_chat(monkeypatch):
    from langsmith.utils import tracing_is_enabled

    monkeypatch.setenv("LANGSMITH_TRACING", "true")
    monkeypatch.setenv("LANGCHAIN_TRACING_V2", "true")

    class PrivateProvider:
        async def stream(self, messages):
            assert tracing_is_enabled() is False
            yield "private reply"

    session = ChatSession(PrivateProvider())
    assert [part async for part in session.respond("private input")] == ["private reply"]


@pytest.mark.asyncio
async def test_second_turn_has_only_successful_history():
    requests = []

    def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Body(
                [
                    event("reply"),
                    event(finish="stop"),
                    "data: [DONE]\n\n",
                ]
            ),
        )

    session = ChatSession(NimChat(SETTINGS, transport=httpx.MockTransport(handler)))
    _ = [part async for part in session.respond("first")]
    _ = [part async for part in session.respond("second")]
    assert [message["role"] for message in requests[-1]["messages"]] == [
        "system",
        "user",
        "assistant",
        "user",
    ]
    assert requests[-1]["messages"][-1]["content"] == "second"
