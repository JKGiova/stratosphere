import json

import httpx
import pytest
from pydantic import SecretStr, ValidationError
from textual.widgets import Input, Select

from stratosphere.chat import ChatSession
from stratosphere.models import create_provider
from stratosphere.models.nim import NimChat
from stratosphere.models.openrouter import OpenRouterChat
from stratosphere.models.settings import DEFAULT_MODELS, ModelSettings
from stratosphere.models.streaming import ModelError
from stratosphere.tui import StratosphereApp

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

    session = ChatSession(OpenRouterChat(SETTINGS, transport=httpx.MockTransport(handler)))
    assert [part async for part in session.respond("Hi")] == ["OK"]
    assert len(session.history) == 2
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
async def test_provider_failure_never_falls_back_or_commits_history(status):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, json={"error": "or-test-key"})

    session = ChatSession(OpenRouterChat(SETTINGS, transport=httpx.MockTransport(handler)))
    with pytest.raises(ModelError) as error:
        _ = [part async for part in session.respond("Hi")]
    assert "or-test-key" not in str(error.value)
    assert session.history == [] and len(requests) == 1


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

    session = ChatSession(OpenRouterChat(SETTINGS, transport=httpx.MockTransport(handler)))
    with pytest.raises(ModelError, match="overloaded") as error:
        _ = [part async for part in session.respond("Hi")]
    assert "or-test-key" not in str(error.value)
    assert session.history == [] and len(requests) == 1


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


@pytest.mark.asyncio
async def test_picker_never_carries_a_key_to_the_other_provider(monkeypatch):
    monkeypatch.setenv("NVIDIA_API_KEY", "nim-private-test")
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.delenv("STRATOSPHERE_PROVIDER", raising=False)
    monkeypatch.delenv("STRATOSPHERE_MODEL", raising=False)
    app = StratosphereApp()
    async with app.run_test() as pilot:
        screen = app.screen
        assert screen.query_one("#provider", Select).value == "openrouter"
        assert screen.query_one("#api-key", Input).value == ""
        assert screen.query_one("#model-id", Input).value == MODEL
        screen.query_one("#provider", Select).value = "nvidia"
        await pilot.pause()
        assert screen.query_one("#api-key", Input).value == "nim-private-test"
        assert screen.query_one("#model-id", Input).value == DEFAULT_MODELS["nvidia"]
        screen.query_one("#provider", Select).value = "openrouter"
        await pilot.pause()
        assert screen.query_one("#api-key", Input).value == ""
        assert "nim-private-test" not in app.export_screenshot()


@pytest.mark.asyncio
async def test_switching_provider_does_not_forward_previous_chat_or_credential(monkeypatch):
    monkeypatch.setenv("NVIDIA_API_KEY", "nim-test-key")
    monkeypatch.setenv("OPENROUTER_API_KEY", "or-test-key")
    monkeypatch.delenv("STRATOSPHERE_MODEL", raising=False)
    monkeypatch.delenv("STRATOSPHERE_PROVIDER", raising=False)
    requests = []

    class SwitchingStream(httpx.AsyncByteStream):
        def __init__(self, content):
            self.content = content

        async def __aiter__(self):
            yield frame(self.content).encode()
            yield frame(finish="stop", usage={"cost": 0}).encode()
            yield b"data: [DONE]\n\n"

    def handler(request):
        requests.append(request)
        content = json.dumps({"action": "chat"}) if len(requests) % 2 else "OK"
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=SwitchingStream(content),
        )

    def factory(settings):
        kind = OpenRouterChat if settings.provider == "openrouter" else NimChat
        return kind(settings, transport=httpx.MockTransport(handler))

    app = StratosphereApp(provider_factory=factory)
    async with app.run_test() as pilot:
        app.screen.query_one("#api-key", Input).focus()
        await pilot.press("enter")
        await pilot.pause()
        field = app.query_one("#prompt", Input)
        field.value = "private previous provider conversation"
        field.focus()
        await pilot.press("enter")
        await pilot.pause()
        await app.response_worker.wait()
        field.value = "/model"
        field.focus()
        await pilot.press("enter")
        await pilot.pause()
        app.screen.query_one("#provider", Select).value = "nvidia"
        await pilot.pause()
        app.screen.query_one("#api-key", Input).focus()
        await pilot.press("enter")
        await pilot.pause()
        field.value = "new question"
        field.focus()
        await pilot.press("enter")
        await pilot.pause()
        await app.response_worker.wait()
    assert len(requests) == 4
    assert requests[0].headers["authorization"] == "Bearer or-test-key"
    assert requests[1].headers["authorization"] == "Bearer or-test-key"
    assert all(r.headers["authorization"] == "Bearer nim-test-key" for r in requests[2:])
    assert all(r.url.host == "integrate.api.nvidia.com" for r in requests[2:])
    assert all(b"private previous provider conversation" not in r.content for r in requests[2:])
    route = json.loads(json.loads(requests[2].content)["messages"][-1]["content"])
    assert route == {
        "request": "new question",
        "active_strategy": None,
        "pending_research": None,
        "pending_intake": None,
        "catalog": [],
    }
    assert [m["content"] for m in json.loads(requests[3].content)["messages"]][1:] == [
        "new question",
    ]
