"""Fixed historical acquisition/native execution entry points in isolated processes."""

import argparse
import json
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["fetch", "simulate"])
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--multiplier", type=int, choices=[1, 2], default=1)
    parser.add_argument("--cache", type=Path, required=True)
    args = parser.parse_args()
    status = 0
    try:
        from stratosphere.yahoo.contracts import YahooPlan, YahooRequest, YahooSnapshot

        payload = json.loads(args.input.read_text())
        if args.action == "fetch":
            from stratosphere.yahoo.data import acquire

            result = acquire(YahooRequest.model_validate(payload), args.cache)
        else:
            from stratosphere.yahoo.engine import run_simulation

            result = run_simulation(
                YahooPlan.model_validate(payload["plan"]),
                YahooSnapshot.model_validate(payload["snapshot"]),
                multiplier=args.multiplier,
            )
    except Exception:
        # Provider exceptions can include cookie/crumb values or unexpected response bodies.
        result, status = {"error": "Yahoo historical operation failed; no automatic retry."}, 1
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as file:
        json.dump(result, file, allow_nan=False, sort_keys=True)
        file.flush()
        os.fsync(file.fileno())
    raise SystemExit(status)


if __name__ == "__main__":
    main()
