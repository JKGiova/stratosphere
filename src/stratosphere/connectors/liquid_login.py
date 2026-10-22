"""Exact native-keyring payload for a fixed-resource, read-only Liquid grant."""

import math
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator


class SavedReadGrant(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    version: Literal["liquid-read-login/1"] = "liquid-read-login/1"
    issuer: Literal["https://coinvest.liquid.trade"] = "https://coinvest.liquid.trade"
    resource: Literal["https://coinvest.liquid.trade/mcp"] = "https://coinvest.liquid.trade/mcp"
    scope: Literal["read"] = "read"
    client_id: str = Field(min_length=1, max_length=8192)
    access_token: SecretStr = Field(repr=False)
    refresh_token: SecretStr | None = Field(default=None, repr=False)
    issued_at: float
    expires_at: float

    @field_validator("client_id", "access_token", "refresh_token")
    @classmethod
    def valid_opaque(cls, value):
        if value is None:
            return value
        text = value.get_secret_value() if isinstance(value, SecretStr) else value
        if not 0 < len(text) <= 8192 or not all(33 <= ord(c) <= 126 for c in text):
            raise ValueError("Invalid stored grant")
        return value

    @field_validator("issued_at", "expires_at", mode="before")
    @classmethod
    def valid_time(cls, value):
        if type(value) not in {int, float} or not math.isfinite(value) or value <= 0:
            raise ValueError("Invalid stored grant time")
        return value

    @model_validator(mode="after")
    def bounded(self):
        if not 0 < self.expires_at - self.issued_at < 2**31:
            raise ValueError("Invalid stored grant lifetime")
        return self

    def payload(self):
        value = self.model_dump(mode="json")
        value["access_token"] = self.access_token.get_secret_value()
        value["refresh_token"] = (
            self.refresh_token.get_secret_value() if self.refresh_token is not None else None
        )
        return value
