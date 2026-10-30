"""Read-only strategy inspection, bounded evidence analysis and deterministic signal edits."""

from stratosphere.development.compiler import compile_strategy
from stratosphere.phase0.contracts import StrategySpec
from stratosphere.research.store import ResearchError

METRICS = (
    "initial_equity",
    "final_equity",
    "net_pnl",
    "net_return_pct",
    "gross_return_pct",
    "gross_pnl_before_modeled_costs",
    "fees",
    "spread_and_slippage_cost",
    "max_drawdown_pct",
    "filled_orders",
    "completed_trades",
    "open_positions",
    "turnover_quote",
    "turnover_initial_equity",
    "exposure_pct",
    "trade_distribution",
)


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


def inspect_program(spec):
    return {
        "specification": spec.model_dump(mode="json"),
        "application_generated_python": compile_strategy(spec).decode(),
        "meaning": "Application compiler output, not arbitrary Python or an MT5 EA",
    }


def inspect_data(base, plan):
    targets = []
    for pin in plan["datasets"]:
        market = plan["definition"]["markets"][pin["index"]]
        item = {"market": market, "state": pin["state"]}
        if pin["state"] == "ready":
            snap = base.snapshot(pin)
            item |= {
                "bars": len(snap.bars),
                "observed_gaps": len(snap.gaps),
                "currency": snap.currency,
                "exchange": snap.exchange,
                "timezone": snap.timezone,
                "first_open_ns": snap.bars[0].open_ns,
                "last_close_ns": snap.bars[-1].close_ns,
                "snapshot_sha256": snap.digest,
            }
        else:
            item["error"] = pin.get("error", "No usable frozen dataset")
        targets.append(item)
    return {
        "targets": targets,
        "meaning": "Frozen historical inputs; no live/account feed or holdout",
    }


def analyze_results(experiments, *, conditions=False):
    results = []
    for report in experiments:
        for row in report["results"]:
            item = {
                "strategy_id": row["strategy_id"],
                "market": row["market"],
                "state": row["state"],
            }
            if conditions:
                item["conditions"] = row.get("conditions", {})
            else:
                item["metrics"] = {
                    name: {k: values[k] for k in METRICS if k in values}
                    for name, values in row.get("metrics", {}).items()
                }
            results.append(item)
    return {
        "results": results[-8:],
        "omitted_earlier_rows": max(0, len(results) - 8),
        "meaning": "Own measured evidence only; descriptive analysis, no holdout",
    }


def report_preview(state, experiments):
    return {
        "best_experiment_id": state["best_experiment"],
        "evaluated_programs": len(experiments),
        "summary": analyze_results(experiments),
        "execution": "blocked",
        "source_conformance": "not_verified",
        "holdout": "none",
        "meaning": "Report preview; application finish seals the final report, no approval",
    }
