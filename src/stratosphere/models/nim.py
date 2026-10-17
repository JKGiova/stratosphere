"""NVIDIA NIM adapter retained as an explicit alternative."""

from stratosphere.models.settings import ModelSettings as ModelSettings
from stratosphere.models.streaming import ChatCompletions
from stratosphere.models.streaming import ModelError as ModelError

DEFAULT_MODEL = "deepseek-ai/deepseek-v4.1-flash"
ENDPOINT = "https://integrate.api.nvidia.com/v1/chat/completions"


class NimChat(ChatCompletions):
    provider = "nvidia"
    endpoint = ENDPOINT

    def request_body(self, messages: list[dict[str, str]]) -> dict:
        body = super().request_body(messages)
        if self.settings.model.startswith("deepseek-ai/deepseek-v4"):
            body["reasoning_effort"] = "none"
        return body
