"""Historical v2 action schema; retained solely to validate sealed receipts."""

from typing import Literal

from pydantic import Field, model_validator

from stratosphere.development.contracts import validate_program
from stratosphere.phase0.contracts import Contract, StrategySpec


class Action(Contract):
    tool: Literal[
        "inspect_brief",
        "research_capabilities",
        "inspect_program",
        "inspect_data",
        "tune_indicator",
        "analyze_results",
        "analyze_conditions",
        "build_report",
        "search_sources",
        "read_source",
        "propose_program",
        "test_program",
        "backtest",
        "compare_results",
        "finish",
    ]
    reason: str = Field(min_length=1, max_length=1500)
    query: str | None = Field(default=None, min_length=1, max_length=180)
    source_index: int | None = Field(default=None, ge=0, strict=True)
    specification: StrategySpec | None = None
    indicator_id: str | None = Field(default=None, min_length=1, max_length=64)
    period: int | None = Field(default=None, strict=True, ge=1, le=519)

    @model_validator(mode="after")
    def valid(self):
        if (self.tool == "propose_program") != (self.specification is not None):
            raise ValueError("Only propose_program accepts a specification")
        if (self.tool == "search_sources") != (self.query is not None):
            raise ValueError("Only search_sources accepts a public query")
        if (self.tool == "read_source") != (self.source_index is not None):
            raise ValueError("Only read_source accepts a retained search-result index")
        if (self.tool == "tune_indicator") != (
            self.indicator_id is not None and self.period is not None
        ):
            raise ValueError("Only tune_indicator accepts one existing indicator ID and a period")
        if self.tool != "tune_indicator" and (
            self.indicator_id is not None or self.period is not None
        ):
            raise ValueError("Unexpected indicator edit arguments")
        if self.specification is not None:
            validate_program(self.specification)
        return self
