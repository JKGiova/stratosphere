import json

import httpx
import pytest

from stratosphere.agents.orchestrator import Route
from stratosphere.models.compatible import CompatibleChat
from stratosphere.models.settings import ModelSettings
from stratosphere.models.streaming import ModelError
from stratosphere.models.structured import structured_response


async def test_ollama_router_uses_native_json_schema_and_no_reasoning():
    requests = []

    def handler(request):
        requests.append(request)
        frames = [
            {"choices": [{"delta": {"content": '{"action":"analyze"}'}, "finish_reason": None}]},
            {"choices": [{"delta": {}, "finish_reason": "stop"}]},
        ]
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=(
                "".join("data: " + json.dumps(frame) + "\n\n" for frame in frames)
                + "data: [DONE]\n\n"
            ),
        )

    provider = CompatibleChat(
        ModelSettings(provider="ollama", model="installed-model"),
        transport=httpx.MockTransport(handler),
    )
    assert (
        await structured_response(provider, [{"role": "user", "content": "Analyze"}], Route)
    ).action == "analyze"
    payload = json.loads(requests[0].content)
    assert payload["reasoning_effort"] == "none"
    assert payload["response_format"]["type"] == "json_schema"
    assert payload["response_format"]["json_schema"]["schema"] == Route.model_json_schema()
    assert "Authorization" not in requests[0].headers


@pytest.mark.parametrize(
    "reply",
    [
        '{"action":"analyze","action":"new"}',
        '```json\n{"action":"analyze"}\n```',
        '{"action":"approve"}',
        '{"action":"analyze","sql":"select secret"}',
    ],
)
async def test_duplicate_or_unapproved_workflow_actions_are_rejected(reply):
    class Provider:
        async def stream(self, messages):
            yield reply

    with pytest.raises(ModelError):
        await structured_response(Provider(), [], Route)
