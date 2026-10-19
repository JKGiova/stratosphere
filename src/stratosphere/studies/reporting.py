"""Measured study comparisons, explicit holdout status and complete attempt denominators."""

from decimal import ROUND_HALF_UP, Decimal

from stratosphere.phase0.contracts import fingerprint
from stratosphere.research.contracts import SimulationPlan
from stratosphere.research.engine import execution_quote
from stratosphere.research.metrics import amount


def baselines(protocol, bars):
    config = protocol.config
    capital = Decimal(config.initial_balance)
    quantity = Decimal(protocol.parameters.quantity)
    result = {"cash": {"net_pnl": "0", "final_equity": amount(capital)}}
    for multiplier in (1, 2):
        _, ask = execution_quote(
            bars[0].open,
            SimulationPlan(draft=protocol.parameters, config=protocol.config),
            multiplier,
        )
        cost = ask * quantity
        fee = (cost * Decimal(config.fee_rate) * multiplier).quantize(
            Decimal("0.00000001"), rounding=ROUND_HALF_UP
        )
        result[f"buy_hold_{multiplier}"] = (
            {
                "net_pnl": amount(quantity * Decimal(bars[-1].close) - cost - fee),
                "final_equity": amount(capital - cost - fee + quantity * Decimal(bars[-1].close)),
                "quantity": str(quantity),
                "end_position": "left open and marked at final close",
            }
            if cost + fee <= capital
            else {"unavailable": "Insufficient starting cash for quantity"}
        )
    return result


def build_study_report(
    job_id, protocol, attempts, selection, holdout, development, holdout_snapshot
):
    complete = bool(holdout) and all(cell["status"] == "completed" for cell in holdout.values())
    baseline = {
        "development": baselines(protocol, development.bars[: protocol.split]),
        "validation": baselines(protocol, development.bars[protocol.split :]),
    }
    if holdout_snapshot:
        baseline["holdout"] = baselines(protocol, holdout_snapshot.bars)
    return {
        "schema_version": "study-report/2"
        if protocol.schema_version == "bounded-study/2"
        else "study-report/1",
        "name": protocol.name + " · bounded study",
        "job_id": job_id,
        "plan_sha256": protocol.digest,
        "strategy_sha256": fingerprint(protocol.specification),
        "implementation_sha256": protocol.implementation_sha256,
        "parent_strategy_id": protocol.parent_strategy_id,
        "hypothesis": protocol.hypothesis,
        "protocol": protocol.model_dump(mode="json"),
        "selection": selection,
        "attempted_candidates": len(attempts),
        "invalid_candidates": sum(a["status"] == "invalid" for a in attempts),
        "ineligible_candidates": sum(a["status"] == "ineligible" for a in attempts),
        "native_attempts": sum(
            "response" in c or c["status"] in {"failed", "canceled", "interrupted"}
            for a in attempts
            for c in a["cells"].values()
        )
        + len(holdout),
        "attempts": [
            {
                **attempt,
                "selection_status": "selected"
                if attempt["index"] == selection["selected_index"]
                else "not_selected_on_validation"
                if attempt["status"] == "eligible"
                else "invalid_or_incomplete",
            }
            for attempt in attempts
        ],
        "holdout": holdout,
        "holdout_status": "completed_once"
        if complete
        else "incomplete"
        if holdout
        else "not_evaluated",
        "metrics": holdout["1"]["metrics"] if complete else {},
        "metrics_strategy_id": attempts[selection["selected_index"]]["strategy_id"]
        if complete
        else None,
        "metrics_role": "selected variant final holdout" if complete else "no completed holdout",
        "doubled_cost_stress": holdout["2"]["metrics"] if complete else {},
        "baselines": baseline,
        "development_normalized_sha256": development.normalized_sha256,
        "holdout_normalized_sha256": holdout_snapshot.normalized_sha256
        if holdout_snapshot
        else None,
        "assumptions": list(protocol.assumptions),
        "limitations": [
            "Two months and one SMA family are not broad validation or a profitability guarantee.",
            "Multiple candidates create selection bias; all attempts remain visible.",
            "The final holdout cannot be reused to optimize another study in this workspace.",
            "Local tracking cannot detect external or earlier human knowledge of public prices.",
            "Warm indicators and flat account resets differ from an uninterrupted deployment.",
            "Synthetic fills cannot verify real liquidity, fees, latency or account access.",
            "No paper/live evidence or automatic promotion is implied.",
            "Binance Vision commercial data rights require a separate written license.",
        ],
    }


def study_markdown(report):
    protocol, selection = report["protocol"], report["selection"]
    lines = [
        f"# {report['name']}",
        "",
        "Research only · BTCUSDT spot · hourly SMA",
        "",
        f"Hypothesis: {report['hypothesis']}",
        "",
        f"Development/validation: {protocol['development']['month']}. "
        f"Final holdout: {protocol['holdout']['month']}.",
        f"Candidates: {report['attempted_candidates']} total; "
        f"{report['invalid_candidates']} invalid; "
        f"{report['ineligible_candidates']} incomplete/failed. "
        f"Native attempts: {report['native_attempts']}.",
        f"Frozen selection: {selection['selected_index']}. "
        f"Holdout: **{report['holdout_status']}**.",
        "",
        "Selection uses validation at doubled costs, lower drawdown, then listed order.",
        "",
        "| Candidate | State | Validation base P&L | Validation doubled-cost P&L |",
        "| --- | --- | --- | --- |",
    ]
    for attempt in report["attempts"]:
        params, cells = attempt["parameters"], attempt["cells"]
        values = [
            cells.get(f"validation_{m}", {}).get("metrics", {}).get("net_pnl", "unavailable")
            for m in (1, 2)
        ]
        lines.append(
            f"| {attempt['index']}: {params['fast']}/{params['slow']} | "
            f"{attempt['status']} | {values[0]} | {values[1]} |"
        )
    if report["metrics"]:
        lines += [
            "",
            "## Selected candidate: final holdout",
            "",
            f"Net P&L: {report['metrics']['net_pnl']} USDT. "
            f"Doubled costs: {report['doubled_cost_stress']['net_pnl']} USDT.",
            f"Completed trades: {report['metrics']['completed_trades']}. "
            f"Maximum drawdown: {report['metrics']['max_drawdown_pct']}%.",
        ]
    lines += [
        "",
        "## Limitations",
        "",
        *[f"- {line}" for line in report["limitations"]],
        "",
        "## Immutable references",
        "",
        f"Protocol: `{report['plan_sha256']}`",
        "",
        f"Implementation: `{report['implementation_sha256']}`",
        "",
        "Sealed JSON retains every candidate, window/cost result, equity, fills, "
        "distributions and baselines.",
    ]
    return "\n".join(lines)


def protocol_markdown(job_id, protocol):
    candidates = ", ".join(f"{c.fast}/{c.slow}" for c in protocol.candidates)
    config = protocol.config
    return "\n".join(
        [
            "# Review bounded study",
            "",
            f"Job `{job_id}` · parent strategy `{protocol.parent_strategy_id}`",
            *(
                [
                    "Anchor: registered definition `"
                    + protocol.registry_anchor.strategy_sha256
                    + "`. "
                    "No parent research plan or source conversion is claimed.",
                    "Synthetic commissions: exact price × quantity × reviewed fee, "
                    "rounded half up to eight decimals before native money construction.",
                ]
                if protocol.schema_version == "bounded-study/2"
                else []
            ),
            "",
            f"Hypothesis: {protocol.hypothesis}",
            f"SMA candidates (listed tie order): {candidates}",
            "",
            f"Development/validation: {protocol.development.month} · "
            f"{protocol.development.count} hourly observations.",
            f"Separate final holdout: {protocol.holdout.month} · "
            f"{protocol.holdout.count} observations.",
            "Only checksums have been acquired at proposal time; holdout bars remain unopened.",
            "Data entitlement: commercial rights unverified. Binance Vision commercial use "
            "requires a separate written license.",
            "Published terms: https://github.com/binance/binance-public-data/blob/master/TERMS_AND_CONDITIONS.md",
            "",
            f"Fixed quantity: {protocol.parameters.quantity} BTC. "
            f"Starting cash per window: {config.initial_balance} USDT.",
            f"Fee {config.fee_rate}; spread {config.spread_bps} bps; "
            f"slippage {config.slippage_bps} bps.",
            f"Approved budget: {protocol.max_simulations} native starts; "
            f"{protocol.required_simulations} planned slots.",
            "Each slot runs at most once. Retained failed/interrupted cells disqualify "
            "that candidate; no silent retries.",
            "",
            "## Protocol",
            "",
            *[f"- {a}" for a in protocol.assumptions],
            "",
            f"Development archive: `{protocol.development.sha256}`",
            f"Holdout archive: `{protocol.holdout.sha256}`",
            "",
            f"Approve once: `/approve {job_id} {protocol.digest}`",
            f"Cancel: `/cancel {job_id}`",
        ]
    )
