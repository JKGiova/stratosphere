"""Explicit provider selection and separate credential sources."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

DEFAULT_PROVIDER = "openrouter"
DEFAULT_MODELS = {
    "openrouter": "cohere/north-mini-code:free",
    "nvidia": "deepseek-ai/deepseek-v4.1-flash",
}
PROVIDER_LABELS = {"openrouter": "OpenRouter", "nvidia": "NVIDIA NIM"}
KEY_ENVIRONMENTS = {"openrouter": "OPENROUTER_API_KEY", "nvidia": "NVIDIA_API_KEY"}


class ModelSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    # Preserve existing programmatic NVIDIA configurations; the TUI explicitly
    # selects its new OpenRouter default rather than reusing an old credential.
    provider: Literal["nvidia", "openrouter"] = "nvidia"
    model: str = Field(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9_./:-]+$")
    api_key: SecretStr = Field(repr=False)

    @model_validator(mode="after")
    def validate_selection(self):
        if not self.api_key.get_secret_value().strip():
            raise ValueError("A model API key is required.")
        if self.provider == "openrouter" and not self.model.endswith(":free"):
            raise ValueError("This OpenRouter setup requires an explicit :free model ID.")
        return self
