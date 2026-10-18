"""Direct official OpenAI-compatible APIs; no routing or provider substitution."""

from stratosphere.models.settings import ModelSettings
from stratosphere.models.streaming import ChatCompletions


class CompatibleChat(ChatCompletions):
    def __init__(self, settings: ModelSettings, **kwargs):
        self.provider = settings.provider
        super().__init__(settings, **kwargs)

    def request_body(self, messages: list[dict[str, str]]) -> dict:
        body = super().request_body(messages)
        if self.provider in {"openai", "groq"}:
            body["max_completion_tokens"] = body.pop("max_tokens")
        if self.provider == "openai":
            body["store"] = False
        if self.provider == "kimi" and self.settings.model.startswith("kimi-k3"):
            body["reasoning_effort"] = "low"
        return body
