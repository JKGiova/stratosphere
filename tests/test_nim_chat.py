import asyncio
import json

import httpx
import pytest
from pydantic import SecretStr

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
async def test_cancellation_closes_stream():
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

    async def consume():
        return [part async for part in client.stream([{"role": "user", "content": "question"}])]

    task = asyncio.create_task(consume())
    await asyncio.wait_for(started.wait(), timeout=3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()


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
