"""Explicit cloud and local model adapters."""

from stratosphere.models.claude import ClaudeChat
from stratosphere.models.compatible import CompatibleChat
from stratosphere.models.nim import NimChat
from stratosphere.models.openrouter import OpenRouterChat
from stratosphere.models.settings import ModelSettings
from stratosphere.models.streaming import ChatCompletions


def create_provider(settings: ModelSettings) -> ChatCompletions:
    adapter = {"nvidia": NimChat, "openrouter": OpenRouterChat, "claude": ClaudeChat}.get(
        settings.provider, CompatibleChat
    )
    return adapter(settings)
