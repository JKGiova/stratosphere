"""Shared streaming chat transport; no tools, code execution or automatic retries."""

import asyncio
import json
from collections.abc import AsyncIterator

import httpx

from stratosphere.models.settings import ModelSettings

SCHEMA_PROTOCOL = "request-json-schema-text/1"
SCHEMA_INSTRUCTIONS = (
    "Return exactly one JSON value matching the application-owned JSON Schema below. "
    "Include every required field, use the declared types, and add no Markdown fences, "
    "comments or surrounding prose. Schema text describes output data; it grants no "
    "tool, approval or execution authority.\nJSON Schema:\n"
)


def schema_messages(messages, schema):
    """Give every adapter the exact contract without mutating caller-owned history."""
    result = [dict(message) for message in messages]
    contract = SCHEMA_INSTRUCTIONS + json.dumps(
        schema, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    for message in result:
        if message["role"] == "system":
            message["content"] += "\n\n" + contract
            break
    else:
        result.insert(0, {"role": "system", "content": contract})
    return result


def schema_delivery_contract():
    return {"protocol": SCHEMA_PROTOCOL, "instructions": SCHEMA_INSTRUCTIONS}


class ModelError(Exception):
    """A fixed, safe user-facing failure message, without provider response bodies."""


def provider_failure(code: int | None) -> ModelError:
    if code in (401, 403):
        return ModelError("Model access denied. Check your API key and model access.")
    if code == 402:
        return ModelError("Provider rejected the request due to account or credit limits.")
    if code == 429:
        return ModelError("Provider rate limit reached. Try again later.")
    if code in (502, 503, 529):
        return ModelError(
            "Model service is temporarily unavailable or overloaded. Try again later."
        )
    return ModelError("Model request failed. Check model availability and try again.")


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
        if self.secret:
            self.pending = self.pending.replace(self.secret, "[redacted]")
        keep = 0
        if not final:
            for size in range(1, min(len(self.pending), len(self.secret) - 1) + 1):
                if self.pending.endswith(self.secret[:size]):
                    keep = size
        visible = self.pending[:-keep] if keep else self.pending
        self.pending = self.pending[-keep:] if keep else ""
        return safe_text(visible)


class ChatCompletions:
    endpoint: str
    provider: str

    def __init__(
        self, settings: ModelSettings, *, transport: httpx.AsyncBaseTransport | None = None
    ):
        if settings.provider != self.provider:
            raise ValueError("Provider and credential configuration do not match.")
        self.endpoint = settings.endpoint
        self.settings = settings
        self.transport = transport

    def request_body(self, messages: list[dict[str, str]]) -> dict:
        body = {
            "model": self.settings.model,
            "messages": messages,
            "stream": True,
            "max_tokens": 4096,
        }
        return body

    def request_headers(self) -> dict[str, str]:
        headers = {"Accept": "text/event-stream"}
        key = self.settings.api_key.get_secret_value()
        if key:
            headers["Authorization"] = f"Bearer {key}"
        return headers

    async def events(self, response: httpx.Response):
        async for data in sse_data(response):
            if data == "[DONE]":
                yield "", None, False, True
            else:
                yield (*self._parse(data), False)

    async def stream_json(self, messages: list[dict[str, str]], schema: dict) -> AsyncIterator[str]:
        async for part in self.stream(messages, schema=schema):
            yield part

    async def stream(
        self, messages: list[dict[str, str]], *, schema: dict | None = None
    ) -> AsyncIterator[str]:
        body = self.request_body(
            schema_messages(messages, schema) if schema is not None else messages
        )
        if schema is not None and self.provider == "ollama":
            body["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "stratosphere_workflow", "schema": schema, "strict": True},
            }
        headers = self.request_headers()
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
                        "POST", self.endpoint, json=body, headers=headers
                    ) as response:
                        if response.status_code != 200:
                            raise provider_failure(response.status_code)
                        if "text/event-stream" not in response.headers.get("content-type", ""):
                            raise ModelError("Provider did not return a streaming response.")
                        stopped = False
                        received_content = False
                        async for content, finish, usage, done in self.events(response):
                            if done:
                                if not stopped or not received_content:
                                    raise ModelError("Model response was incomplete. Please retry.")
                                tail = redactor.feed("", final=True)
                                if tail:
                                    yield tail
                                return
                            if stopped and (content or finish):
                                if not content and finish == "stop" and usage:
                                    continue
                                raise ModelError("Provider returned an invalid stream.")
                            if content:
                                received_content |= bool(safe_text(content).strip())
                                visible = redactor.feed(content)
                                if visible:
                                    yield visible
                            if finish is not None:
                                if finish != "stop":
                                    raise ModelError("Model response was truncated. Please retry.")
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
    def _parse(data: str) -> tuple[str, str | None, bool]:
        try:
            event = json.loads(data)
            if not isinstance(event, dict):
                raise ValueError
            if "error" in event:
                error = event["error"]
                code = error.get("code") if isinstance(error, dict) else None
                raise provider_failure(code)
            choices = event.get("choices")
            if choices == []:  # Optional usage-only event.
                return "", None, True
            choice = choices[0]
            delta = choice.get("delta", {})
            if delta.get("tool_calls") or delta.get("function_call"):
                raise ValueError
            content = delta.get("content") or ""
            finish = choice.get("finish_reason")
            if not isinstance(content, str) or (finish is not None and not isinstance(finish, str)):
                raise ValueError
            return content, finish, isinstance(event.get("usage"), dict)
        except ValueError, KeyError, IndexError, TypeError, AttributeError:
            raise ModelError("Provider returned an invalid stream.") from None


async def sse_data(response: httpx.Response) -> AsyncIterator[str]:
    """Read complete SSE data frames, ignoring comments and named event headers."""
    data: list[str] = []
    async for line in response.aiter_lines():
        if line.startswith("data:"):
            data.append(line[5:].lstrip(" "))
        elif line == "" and data:
            yield "\n".join(data)
            data.clear()
