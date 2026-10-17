"""Online model adapters."""

from stratosphere.models.nim import NimChat
from stratosphere.models.openrouter import OpenRouterChat
from stratosphere.models.settings import ModelSettings
from stratosphere.models.streaming import ChatCompletions


def create_provider(settings: ModelSettings) -> ChatCompletions:
    return {"nvidia": NimChat, "openrouter": OpenRouterChat}[settings.provider](settings)
