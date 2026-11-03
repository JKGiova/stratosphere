import json

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from stratosphere.models import create_provider
from stratosphere.models.nim import NimChat
from stratosphere.models.openrouter import OpenRouterChat
from stratosphere.models.settings import DEFAULT_MODELS, ModelSettings
from stratosphere.models.streaming import ModelError

MODEL = DEFAULT_MODELS["openrouter"]
SETTINGS = ModelSettings(provider="openrouter", model=MODEL, api_key=SecretStr("or-test-key"))


def frame(content="", finish=None, **extra):
    return (
        "data: "
        + json.dumps(
            {
                "choices": [{"delta": {"content": content}, "finish_reason": finish}],
                **extra,
            }
        )
        + "\n\n"
    )


class Stream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield (": OPENROUTER PROCESSING\n\n" + frame("OK") + frame(finish="stop")).encode()
        yield (frame(finish="stop", usage={"cost": 0}) + "data: [DONE]\n\n").encode()


@pytest.mark.asyncio
async def test_exact_free_model_request_and_openrouter_usage_frame():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream())

    provider = OpenRouterChat(SETTINGS, transport=httpx.MockTransport(handler))
    assert [part async for part in provider.stream([{"role": "user", "content": "Hi"}])] == ["OK"]
    assert len(requests) == 1
    request = requests[0]
    assert str(request.url) == "https://openrouter.ai/api/v1/chat/completions"
    assert request.headers["authorization"] == "Bearer or-test-key"
    body = json.loads(request.content)
    assert body["model"] == MODEL
    assert body["provider"] == {"max_price": {"prompt": 0, "completion": 0}}
    assert body["reasoning"] == {"effort": "low", "exclude": True}
    assert body["stream"] is True
    assert "models" not in body and "tools" not in body and "reasoning_effort" not in body


@pytest.mark.parametrize("status", [401, 402, 404, 429, 503])
@pytest.mark.asyncio
async def test_provider_failure_never_falls_back(status):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, json={"error": "or-test-key"})

    provider = OpenRouterChat(SETTINGS, transport=httpx.MockTransport(handler))
    with pytest.raises(ModelError) as error:
        _ = [part async for part in provider.stream([{"role": "user", "content": "Hi"}])]
    assert "or-test-key" not in str(error.value)
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "last",
    [
        frame("unexpected", "stop", usage={"cost": 0}),
        frame(finish="stop"),
        'data: {"error":{"message":"or-test-key"}}\n\n',
    ],
)
async def test_invalid_post_terminal_data_is_not_treated_as_accounting(last):
    body = frame("OK") + frame(finish="stop") + last + "data: [DONE]\n\n"
    provider = OpenRouterChat(
        SETTINGS,
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=body,
            )
        ),
    )
    with pytest.raises(ModelError):
        _ = [part async for part in provider.stream([])]


@pytest.mark.asyncio
async def test_overload_inside_http_200_stream_is_reported_safely_without_retry():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=('data: {"choices":[],"error":{"code":503,"message":"or-test-key"}}\n\n'),
        )

    provider = OpenRouterChat(SETTINGS, transport=httpx.MockTransport(handler))
    with pytest.raises(ModelError, match="overloaded") as error:
        _ = [part async for part in provider.stream([{"role": "user", "content": "Hi"}])]
    assert "or-test-key" not in str(error.value)
    assert len(requests) == 1


def test_provider_credentials_and_free_only_selection_are_explicit():
    assert isinstance(create_provider(SETTINGS), OpenRouterChat)
    with pytest.raises(ValueError, match="do not match"):
        NimChat(SETTINGS)
    with pytest.raises(ValueError, match="do not match"):
        OpenRouterChat(ModelSettings(model="some-model", api_key=SecretStr("nim-test-key")))
    with pytest.raises(ValidationError):
        ModelSettings(
            provider="openrouter",
            model=MODEL.removesuffix(":free"),
            api_key=SecretStr("or-test-key"),
        )
