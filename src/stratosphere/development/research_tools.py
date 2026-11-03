"""Deterministic indicator edits for validating historical development receipts."""

from stratosphere.phase0.contracts import StrategySpec
from stratosphere.research.store import ResearchError


def tune_indicator(spec, indicator_id, period):
    payload = spec.model_dump(mode="json")
    matches = [i for i in payload["indicators"] if i["id"] == indicator_id]
    if len(matches) != 1:
        raise ResearchError("Select one existing indicator ID")
    matches[0]["period"] = period
    payload["data_requirements"]["warmup_bars"] = max(
        payload["data_requirements"]["warmup_bars"],
        max(i["period"] + 1 for i in payload["indicators"]),
    )
    return StrategySpec.model_validate(payload)
