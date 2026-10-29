"""Execute only packaged data acquisition or byte-verified application-compiled strategies."""

import argparse
import hashlib
import json
import os
from pathlib import Path


def simulate(payload, directory, multiplier):
    from stratosphere.development.compiler import load_generated
    from stratosphere.development.contracts import CellPlan, PriceSnapshot, implementation_digest
    from stratosphere.yahoo.engine import run_price_simulation

    plan = CellPlan.model_validate(payload["plan"])
    snapshot = PriceSnapshot.model_validate(payload["snapshot"])
    code = directory / "strategy.py"
    if (
        plan.implementation_sha256 != implementation_digest()
        or snapshot.digest != plan.snapshot_sha256
        or snapshot.request != plan.request
        or hashlib.sha256(code.read_bytes()).hexdigest() != plan.code_sha256
    ):
        raise ValueError("Frozen code, input or implementation changed")
    if len(snapshot.bars) <= plan.specification.data_requirements.warmup_bars:
        raise ValueError("Insufficient history for warmup and measured evaluation")
    if plan.specification.missing_data_policy.action == "abort" and snapshot.gaps:
        raise ValueError("Observed gaps violate the strategy's abort policy")
    values = [i.input for i in plan.specification.indicators]

    def requires_volume(node):
        return (node.op == "field" and node.value == "volume") or any(
            requires_volume(a) for a in node.args
        )

    if (
        "volume" in values
        or any(
            requires_volume(r.condition)
            for r in (*plan.specification.entry_rules, *plan.specification.exit_rules)
        )
        or any(requires_volume(c) for c in plan.specification.market_conditions)
    ) and any(b.volume is None for b in snapshot.bars):
        raise ValueError("Strategy needs observed volume; missing values cannot become zero")
    strategy = load_generated(code, plan.specification)
    return run_price_simulation(plan, snapshot, strategy, multiplier=multiplier)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["fetch", "simulate"])
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--multiplier", type=int, choices=[1, 2], default=1)
    args = parser.parse_args()
    status = 0
    try:
        payload = json.loads(args.input.read_text())
        if args.action == "fetch":
            from stratosphere.development.contracts import Market
            from stratosphere.yahoo.data import acquire

            result = acquire(Market.model_validate(payload), args.cache)
        else:
            result = simulate(payload, args.input.parent, args.multiplier)
    except Exception:
        result, status = (
            {"error": "Development data/native operation failed; no automatic replay"},
            1,
        )
    fd = os.open(args.output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(result, stream, allow_nan=False, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    raise SystemExit(status)


if __name__ == "__main__":
    main()
