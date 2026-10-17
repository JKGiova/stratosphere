"""NVIDIA hosted chat adapter; no tools, code execution or automatic retries."""

import asyncio
import json
from collections.abc import AsyncIterator

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr

DEFAULT_MODEL = "deepseek-ai/deepseek-v4.1-flash"
ENDPOINT = "https://integrate.api.nvidia.com/v1/chat/completions"


class ModelSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    model: str = Field(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9_./:-]+$")
    api_key: SecretStr = Field(repr=False)


class ModelError(Exception):
    """A fixed, safe user-facing failure message, without provider response bodies."""


def safe_text(text: str) -> str:
    """Keep terminal controls out of provider/user rendered text."""
    return "".join(c for c in text if c in "\n\t" or (ord(c) >= 32 and not 127 <= ord(c) <= 159))


class SecretFilter:
    """Redact an exact credential even when it spans streamed deltas."""

    def __init__(self, secret: str):
        self.secret = secret
        self.pending = ""

    def feed(self, text: str, *, final: bool = False) -> str:
        self.pending += text
        self.pending = self.pending.replace(self.secret, "[redacted]")
        keep = 0
        if not final:
            for size in range(1, min(len(self.pending), len(self.secret) - 1) + 1):
                if self.pending.endswith(self.secret[:size]):
                    keep = size
        visible = self.pending[:-keep] if keep else self.pending
        self.pending = self.pending[-keep:] if keep else ""
        return safe_text(visible)


class NimChat:
    def __init__(
        self, settings: ModelSettings, *, transport: httpx.AsyncBaseTransport | None = None
    ):
        if not settings.api_key.get_secret_value().strip():
            raise ValueError("A model API key is required.")
        self.settings = settings
        self.transport = transport

    async def stream(self, messages: list[dict[str, str]]) -> AsyncIterator[str]:
        body = {
            "model": self.settings.model,
            "messages": messages,
            "stream": True,
            "max_tokens": 4096,
        }
        if self.settings.model.startswith("deepseek-ai/deepseek-v4"):
            body["reasoning_effort"] = "none"
        headers = {
            "Authorization": f"Bearer {self.settings.api_key.get_secret_value()}",
            "Accept": "text/event-stream",
        }
        redactor = SecretFilter(self.settings.api_key.get_secret_value())
        try:
            async with asyncio.timeout(180):
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(60, connect=15),
                    transport=self.transport,
                    follow_redirects=False,
                    trust_env=False,
                ) as client:
                    async with client.stream(
                        "POST", ENDPOINT, json=body, headers=headers
                    ) as response:
                        if response.status_code != 200:
                            if response.status_code in (401, 403):
                                raise ModelError(
                                    "Model access denied. Check your API key and model access."
                                )
                            if response.status_code == 429:
                                raise ModelError("Provider rate limit reached. Try again later.")
                            raise ModelError(
                                "Model request failed. Check model availability and try again."
                            )
                        if "text/event-stream" not in response.headers.get("content-type", ""):
                            raise ModelError("Provider did not return a streaming response.")
                        data: list[str] = []
                        stopped = False
                        received_content = False
                        async for line in response.aiter_lines():
                            if line.startswith("data:"):
                                data.append(line[5:].lstrip(" "))
                            elif line == "" and data:
                                event = "\n".join(data)
                                data.clear()
                                if event == "[DONE]":
                                    if not stopped or not received_content:
                                        raise ModelError(
                                            "Model response was incomplete. Please retry."
                                        )
                                    tail = redactor.feed("", final=True)
                                    if tail:
                                        yield tail
                                    return
                                content, finish = self._parse(event)
                                if stopped and (content or finish):
                                    raise ModelError("Provider returned an invalid stream.")
                                if content:
                                    received_content |= bool(safe_text(content).strip())
                                    visible = redactor.feed(content)
                                    if visible:
                                        yield visible
                                if finish is not None:
                                    if finish != "stop":
                                        raise ModelError(
                                            "Model response was truncated. Please retry."
                                        )
                                    stopped = True
                        raise ModelError(
                            "Model stream disconnected before completion. Please retry."
                        )
        except TimeoutError, httpx.TimeoutException:
            raise ModelError(
                "Model request timed out. Please retry; no operation was executed."
            ) from None
        except httpx.HTTPError:
            raise ModelError(
                "Cannot reach the model provider. Check your connection and retry."
            ) from None

    @staticmethod
    def _parse(data: str) -> tuple[str, str | None]:
        try:
            event = json.loads(data)
            if not isinstance(event, dict) or "error" in event:
                raise ValueError
            choices = event.get("choices")
            if choices == []:  # Optional usage-only event.
                return "", None
            choice = choices[0]
            delta = choice.get("delta", {})
            if delta.get("tool_calls") or delta.get("function_call"):
                raise ValueError
            content = delta.get("content") or ""
            finish = choice.get("finish_reason")
            if not isinstance(content, str) or (finish is not None and not isinstance(finish, str)):
                raise ValueError
            return content, finish
        except ValueError, KeyError, IndexError, TypeError, AttributeError:
            raise ModelError("Provider returned an invalid stream.") from None
