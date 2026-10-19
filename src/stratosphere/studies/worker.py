"""Native study cells validate the entire pinned archive before selecting a window."""

from pathlib import Path
from typing import Literal

from pydantic import Field

from stratosphere.phase0.contracts import Contract
from stratosphere.research.engine import run_simulation
from stratosphere.studies.contracts import (
    RegistryStudyProtocol,
    StudyProtocol,
    study_implementation_digest,
)
from stratosphere.studies.data import normalize_month


class CellRequest(Contract):
    kind: Literal["bounded-study-cell/1"] = "bounded-study-cell/1"
    protocol: StudyProtocol | RegistryStudyProtocol
    candidate_index: int = Field(strict=True, ge=0)
    window: Literal["development", "validation", "holdout"]
    selected_index: int | None = Field(default=None, strict=True, ge=0)


def cell_bars(request: CellRequest, directory: Path):
    protocol = request.protocol
    plan = protocol.simulation(protocol.candidates[request.candidate_index])
    development = normalize_month(
        (directory / "development.zip").read_bytes(), protocol.development
    )
    warmup = plan.draft.slow_period + 1
    if request.window == "development":
        bars = development.bars[: protocol.split]
        start = bars[0].open_ns
    elif request.window == "validation":
        bars = development.bars[max(0, protocol.split - warmup) :]
        start = development.bars[protocol.split].open_ns
    else:
        if request.candidate_index != request.selected_index:
            raise ValueError("Only the frozen selection can access the final holdout")
        holdout = normalize_month((directory / "holdout.zip").read_bytes(), protocol.holdout)
        bars = development.bars[-warmup:] + holdout.bars
        start = holdout.bars[0].open_ns
    return plan, bars, start


def execute_cell(payload: dict, multiplier: int, directory: Path) -> dict:
    request = CellRequest.model_validate(payload)
    if request.protocol.implementation_sha256 != study_implementation_digest():
        raise ValueError("Study implementation changed after approval")
    if request.candidate_index >= len(request.protocol.candidates):
        raise ValueError("Candidate is outside the reviewed protocol")
    plan, bars, start = cell_bars(request, directory)
    return {
        "protocol_sha256": request.protocol.digest,
        "candidate_index": request.candidate_index,
        "window": request.window,
        "cost_multiplier": multiplier,
        "result": run_simulation(plan, bars, multiplier=multiplier, trade_start_ns=start),
    }
