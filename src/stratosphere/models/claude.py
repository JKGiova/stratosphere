"""Anthropic Messages API with native streaming and explicit completion checks."""

import json

import httpx

from stratosphere.models.streaming import ChatCompletions, ModelError, provider_failure, sse_data


class ClaudeChat(ChatCompletions):
    provider = "claude"

    def request_headers(self) -> dict[str, str]:
        return {
            "Accept": "text/event-stream",
            "x-api-key": self.settings.api_key.get_secret_value(),
            "anthropic-version": "2023-06-01",
        }

    def request_body(self, messages: list[dict[str, str]]) -> dict:
        return {
            "model": self.settings.model,
            "system": "\n\n".join(m["content"] for m in messages if m["role"] == "system"),
            "messages": [m for m in messages if m["role"] != "system"],
            "max_tokens": 4096,
            "stream": True,
        }

    async def events(self, response: httpx.Response):
        started = False
        finished = False
        active: tuple[int, str] | None = None
        next_index = 0
        async for data in sse_data(response):
            try:
                event = json.loads(data)
                kind = event["type"]
                if not isinstance(kind, str):
                    raise ValueError
                if kind == "error":
                    code = {"overloaded_error": 529, "rate_limit_error": 429}.get(
                        event.get("error", {}).get("type")
                    )
                    raise provider_failure(code)
                if kind == "ping":
                    continue
                if kind == "message_start":
                    if started or event["message"]["role"] != "assistant":
                        raise ValueError
                    started = True
                elif kind == "content_block_start":
                    if not started or finished or active or event["index"] != next_index:
                        raise ValueError
                    block = event["content_block"]
                    block_type = block["type"]
                    if block_type not in {"text", "thinking", "redacted_thinking"}:
                        raise ValueError
                    active = (event["index"], block_type)
                    if block_type == "text":
                        text = block.get("text", "")
                        if not isinstance(text, str):
                            raise ValueError
                        yield text, None, False, False
                elif kind == "content_block_delta":
                    if active is None or active[0] != event["index"] or finished:
                        raise ValueError
                    delta = event["delta"]
                    if delta["type"] == "text_delta":
                        if active[1] != "text" or not isinstance(delta["text"], str):
                            raise ValueError
                        yield delta["text"], None, False, False
                    elif delta["type"] not in {"thinking_delta", "signature_delta"}:
                        raise ValueError
                    elif active[1] != "thinking":
                        raise ValueError
                elif kind == "content_block_stop":
                    if active is None or active[0] != event["index"]:
                        raise ValueError
                    active = None
                    next_index += 1
                elif kind == "message_delta":
                    if not started or active:
                        raise ValueError
                    reason = event["delta"].get("stop_reason")
                    if reason is not None:
                        if finished:
                            raise ValueError
                        finished = True
                        yield "", "stop" if reason == "end_turn" else reason, False, False
                elif kind == "message_stop":
                    if not started or not finished or active:
                        raise ValueError
                    yield "", None, False, True
                    return
                # Unknown top-level event types are allowed by Anthropic's versioning policy.
            except ValueError, KeyError, IndexError, TypeError, AttributeError:
                raise ModelError("Provider returned an invalid stream.") from None
