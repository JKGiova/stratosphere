"""OpenRouter free-model adapter with a zero prompt/completion price ceiling."""

from stratosphere.models.streaming import ChatCompletions


class OpenRouterChat(ChatCompletions):
    provider = "openrouter"
    endpoint = "https://openrouter.ai/api/v1/chat/completions"

    def request_body(self, messages: list[dict[str, str]]) -> dict:
        body = super().request_body(messages)
        body["provider"] = {"max_price": {"prompt": 0, "completion": 0}}
        body["reasoning"] = {"effort": "low", "exclude": True}
        return body
