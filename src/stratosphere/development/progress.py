"""Frozen application-owned continuation criteria, independent of model claims."""

from decimal import Decimal

POLICY = {
    "score": "minimum_across_targets_stress_return_minus_drawdown",
    "minimum_absolute_improvement_percentage_points": "0.10",
    "minimum_relative_improvement": "0.05",
    "maximum_target_score_regression_percentage_points": "0.10",
    "minimum_completed_stress_trades": 5,
    "maximum_stress_drawdown_pct": "20",
    "holdout": "none",
}


def measurement(rows):
    if not rows or any(r["state"] != "completed" for r in rows):
        return None
    scores = []
    for row in rows:
        metrics = row["metrics"]["stress"]
        scores.append(Decimal(metrics["net_return_pct"]) - Decimal(metrics["max_drawdown_pct"]))
    return {"score": str(min(scores)), "target_scores": [str(s) for s in scores]}


def evaluate(best_rows, candidate_rows):
    best, candidate = measurement(best_rows), measurement(candidate_rows)
    if candidate is None:
        return {"continue": False, "reason": "technical_failure", "measurement": None}
    if best is None:
        return {"continue": True, "reason": "baseline", "measurement": candidate}
    threshold = max(
        Decimal(POLICY["minimum_absolute_improvement_percentage_points"]),
        abs(Decimal(best["score"])) * Decimal(POLICY["minimum_relative_improvement"]),
    )
    regression = any(
        Decimal(new)
        < Decimal(old) - Decimal(POLICY["maximum_target_score_regression_percentage_points"])
        for old, new in zip(best["target_scores"], candidate["target_scores"], strict=True)
    )
    sufficient = all(
        r["metrics"]["stress"]["completed_trades"] >= POLICY["minimum_completed_stress_trades"]
        and Decimal(r["metrics"]["stress"]["max_drawdown_pct"])
        <= Decimal(POLICY["maximum_stress_drawdown_pct"])
        for r in candidate_rows
    )
    improves = Decimal(candidate["score"]) - Decimal(best["score"]) >= threshold
    return {
        "continue": improves and sufficient and not regression,
        "reason": "significant_progress"
        if improves and sufficient and not regression
        else "no_significant_progress",
        "measurement": candidate,
        "required_improvement": str(threshold),
    }
