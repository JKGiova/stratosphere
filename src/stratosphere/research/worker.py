"""Private subprocess entry point executing only the packaged, validated implementation."""

import argparse
import json
import os
from importlib.metadata import PackageNotFoundError
from pathlib import Path

from stratosphere.research.contracts import MarketBar, ResearchPlan, implementation_digest
from stratosphere.research.data import BAR_COUNT, FIRST_OPEN_NS, bars_digest
from stratosphere.research.engine import run_simulation


def execute(request: dict, multiplier: int) -> dict:
    plan = ResearchPlan.model_validate(request["plan"])
    if plan.implementation_sha256 != implementation_digest():
        raise ValueError("Implementation changed after approval")
    bars = tuple(MarketBar.model_validate(row) for row in request["bars"])
    if len(bars) != BAR_COUNT or bars[0].open_ns != FIRST_OPEN_NS:
        raise ValueError("Unapproved dataset window")
    if bars_digest(bars) != request["normalized_sha256"]:
        raise ValueError("Input content integrity mismatch")
    result = run_simulation(plan, bars, multiplier=multiplier)
    return {
        "plan_sha256": plan.digest,
        "normalized_sha256": request["normalized_sha256"],
        "result": result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--multiplier", type=int, choices=[1, 2], required=True)
    args = parser.parse_args()
    try:
        payload = execute(json.loads(args.request.read_text()), args.multiplier)
        status = 0
    except PackageNotFoundError, ModuleNotFoundError:
        payload, status = {"error": "engine_unavailable"}, 1
    except Exception:
        payload, status = {"error": "simulation_failed"}, 1
    # Exclusive creation prevents a rerun from overwriting an existing attempt or checkpoint.
    descriptor = os.open(args.result, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as file:
        json.dump(payload, file, sort_keys=True)
        file.flush()
        os.fsync(file.fileno())
    raise SystemExit(status)


if __name__ == "__main__":
    main()
