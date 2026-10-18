"""Strict structured workflow output with optional provider-native JSON constraints."""

import json

from pydantic import ValidationError

from stratosphere.models.streaming import ModelError


async def structured_response(provider, messages: list[dict[str, str]], contract):
    stream = (
        provider.stream_json(messages, contract.model_json_schema())
        if hasattr(provider, "stream_json")
        else provider.stream(messages)
    )
    text = "".join([part async for part in stream])

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate key")
            result[key] = value
        return result

    try:
        return contract.model_validate(json.loads(text, object_pairs_hook=unique))
    except ValueError, ValidationError, TypeError:
        raise ModelError(
            "The model returned invalid structured workflow output. No operation ran."
        ) from None
