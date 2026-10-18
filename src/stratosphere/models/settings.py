"""Explicit provider selection, official API destinations and isolated credentials."""

import re
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

Provider = Literal[
    "nvidia",
    "openrouter",
    "claude",
    "openai",
    "grok",
    "groq",
    "deepseek",
    "gemini",
    "kimi",
    "qwen",
    "glm",
    "ollama",
]
DEFAULT_PROVIDER = "openrouter"
# Defaults are editable examples, not automatic model discovery or fallback choices.
DEFAULT_MODELS = {
    "openrouter": "cohere/north-mini-code:free",
    "nvidia": "deepseek-ai/deepseek-v4.1-flash",
    "claude": "claude-sonnet-5-5",
    "openai": "gpt-4.1-mini",
    "grok": "grok-4.7",
    "groq": "llama-3.3-70b-versatile",
    "deepseek": "deepseek-flash",
    "gemini": "gemini-3.8-flash",
    "kimi": "kimi-k3",
    "qwen": "qwen3.7-plus",
    "glm": "glm-5.3",
    "ollama": "gpt-oss:20b",
}
PROVIDER_LABELS = {
    "openrouter": "OpenRouter (free)",
    "nvidia": "NVIDIA NIM",
    "claude": "Claude (Anthropic)",
    "openai": "OpenAI",
    "grok": "Grok (xAI)",
    "groq": "Groq",
    "deepseek": "DeepSeek",
    "gemini": "Gemini (Google)",
    "kimi": "Kimi (Moonshot)",
    "qwen": "Qwen (Alibaba Cloud)",
    "glm": "GLM (Z.ai)",
    "ollama": "Ollama",
}
KEY_ENVIRONMENTS = {
    "openrouter": "OPENROUTER_API_KEY",
    "nvidia": "NVIDIA_API_KEY",
    "claude": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "grok": "XAI_API_KEY",
    "groq": "GROQ_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
    "gemini": "GEMINI_API_KEY",
    "kimi": "MOONSHOT_API_KEY",
    "qwen": "DASHSCOPE_API_KEY",
    "glm": "ZAI_API_KEY",
    "ollama": "OLLAMA_API_KEY",
}
BASE_URLS = {
    "openrouter": "https://openrouter.ai/api/v1",
    "nvidia": "https://integrate.api.nvidia.com/v1",
    "claude": "https://api.anthropic.com/v1",
    "openai": "https://api.openai.com/v1",
    "grok": "https://api.x.ai/v1",
    "groq": "https://api.groq.com/openai/v1",
    "deepseek": "https://api.deepseek.com/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai",
    "kimi": "https://api.moonshot.ai/v1",
    "qwen": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    "glm": "https://api.z.ai/api/paas/v4",
    "ollama": "http://localhost:11434/v1",
}


def valid_base_url(provider: str, value: str) -> bool:
    """Accept official provider addresses; plain HTTP is limited to local Ollama."""
    try:
        if any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value):
            return False
        url = urlsplit(value)
        if url.username or url.password or url.query or url.fragment:
            return False
        if provider == "ollama":
            return (
                url.scheme == "http"
                and url.hostname in {"localhost", "127.0.0.1", "::1"}
                and url.path == "/v1"
                and url.port is not None
            ) or value == "https://ollama.com/v1"
        if value == BASE_URLS[provider]:
            return True
        if provider == "qwen":
            return (
                url.scheme == "https"
                and url.port in {None, 443}
                and (
                    url.hostname in {"dashscope-intl.aliyuncs.com", "dashscope-us.aliyuncs.com"}
                    or re.fullmatch(
                        r"[a-zA-Z0-9-]+\.(ap-southeast-1|us-east-1|cn-hongkong|ap-northeast-1)"
                        r"\.maas\.aliyuncs\.com",
                        url.hostname or "",
                    )
                    is not None
                )
                and url.path == "/compatible-mode/v1"
            )
        return False
    except ValueError:
        return False


class ModelSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    # Preserve old programmatic NVIDIA configurations; the TUI selects OpenRouter explicitly.
    provider: Provider = "nvidia"
    model: str = Field(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9_./:-]+$")
    api_key: SecretStr = Field(default=SecretStr(""), repr=False)
    base_url: str | None = Field(default=None, max_length=500)

    @property
    def resolved_base_url(self) -> str:
        return (self.base_url or BASE_URLS[self.provider]).rstrip("/")

    @property
    def endpoint(self) -> str:
        suffix = "/messages" if self.provider == "claude" else "/chat/completions"
        return self.resolved_base_url + suffix

    @model_validator(mode="after")
    def validate_selection(self):
        key = self.api_key.get_secret_value()
        if not key.strip() and (
            self.provider != "ollama" or self.resolved_base_url.startswith("https")
        ):
            raise ValueError("A model API key is required.")
        if not key.isascii() or any(ord(c) < 33 or ord(c) == 127 for c in key):
            raise ValueError("Invalid API key.")
        if self.provider == "openrouter" and not self.model.endswith(":free"):
            raise ValueError("This OpenRouter setup requires an explicit :free model ID.")
        if not valid_base_url(self.provider, self.resolved_base_url):
            raise ValueError("Use an official provider base URL, or a local Ollama /v1 address.")
        return self
