"""Deterministic reports from actual native fills and account equity, never model arithmetic."""

from decimal import ROUND_HALF_UP, Decimal

from stratosphere.phase0.contracts import fingerprint
from stratosphere.research.contracts import ARCHIVE_URL, MarketBar, ResearchPlan
from stratosphere.research.engine import execution_quote


def amount(value):
    return str(Decimal(value).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP))


def summarize(result: dict, initial: str) -> dict:
    fills, curve = result["fills"], result["equity_curve"]
    capital = Decimal(initial)
    fees = sum((Decimal(fill["commission"]) for fill in fills), Decimal(0))
    impact = sum(
        (
            abs(Decimal(fill["price"]) - Decimal(fill["reference_open"]))
            * Decimal(fill["quantity"])
            for fill in fills
        ),
        Decimal(0),
    )
    peak, max_drawdown = capital, Decimal(0)
    for point in curve:
        equity = Decimal(point["equity"])
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, (peak - equity) / peak)
    final = Decimal(curve[-1]["equity"])
    entry = None
    trades = []
    for fill in fills:
        if fill["side"] == "BUY":
            if entry:
                raise ValueError("Unexpected reinforced position")
            entry = fill
        else:
            if entry is None:
                raise ValueError("A closing fill lacks its entry")
            quantity = Decimal(fill["quantity"])
            pnl = (Decimal(fill["price"]) - Decimal(entry["price"])) * quantity
            net = pnl - Decimal(entry["commission"]) - Decimal(fill["commission"])
            trades.append(
                {
                    "entry_ns": entry["fill_ns"],
                    "exit_ns": fill["fill_ns"],
                    "quantity": str(quantity),
                    "net_pnl": amount(net),
                }
            )
            entry = None
    boundary = len(curve) * 7 // 10
    previous = capital
    segments = []
    for label, rows in [("development", curve[:boundary]), ("validation", curve[boundary:])]:
        end = Decimal(rows[-1]["equity"])
        segments.append(
            {
                "name": label,
                "first_ns": rows[0]["timestamp_ns"],
                "last_ns": rows[-1]["timestamp_ns"],
                "observations": len(rows),
                "net_equity_change": amount(end - previous),
                "convention": "continuous account and indicators; not a separate holdout",
            }
        )
        previous = end
    turnover = sum((Decimal(f["price"]) * Decimal(f["quantity"]) for f in fills), Decimal(0))
    wins = sum(Decimal(trade["net_pnl"]) > 0 for trade in trades)
    losses = sum(Decimal(trade["net_pnl"]) < 0 for trade in trades)
    return {
        "initial_equity": amount(capital),
        "final_equity": amount(final),
        "net_pnl": amount(final - capital),
        "net_return_pct": amount((final / capital - 1) * 100),
        "gross_pnl_before_modeled_costs": amount(final - capital + fees + impact),
        "gross_return_pct": amount((final - capital + fees + impact) / capital * 100),
        "fees": amount(fees),
        "spread_and_slippage_cost": amount(impact),
        "max_drawdown_pct": amount(max_drawdown * 100),
        "filled_orders": len(fills),
        "completed_trades": len(trades),
        "open_positions": result["open_positions"],
        "turnover_quote": amount(turnover),
        "turnover_initial_equity": amount(turnover / capital),
        "exposure_pct": amount(
            sum(Decimal(p["base"]) > 0 for p in curve) / Decimal(len(curve)) * 100
        ),
        "trade_distribution": {
            "wins": wins,
            "losses": losses,
            "flat": len(trades) - wins - losses,
            "min_net_pnl": min((t["net_pnl"] for t in trades), key=Decimal, default=None),
            "max_net_pnl": max((t["net_pnl"] for t in trades), key=Decimal, default=None),
        },
        "trades": trades,
        "segments": segments,
    }


def build_report(
    plan: ResearchPlan,
    bars: tuple[MarketBar, ...],
    normalized_sha256: str,
    base: dict,
    stress: dict,
) -> dict:
    metrics = summarize(base, plan.config.initial_balance)
    stressed = summarize(stress, plan.config.initial_balance)
    _, first_ask = execution_quote(bars[0].open, plan)
    quantity = Decimal(plan.draft.quantity)
    capital = Decimal(plan.config.initial_balance)
    cost = first_ask * quantity
    fee = (cost * Decimal(plan.config.fee_rate)).quantize(
        Decimal("0.00000001"), rounding=ROUND_HALF_UP
    )
    baseline = None
    if cost + fee <= capital:
        baseline = {
            "name": "buy and hold the same fixed BTC quantity from the first open",
            "net_pnl": amount(quantity * Decimal(bars[-1].close) - cost - fee),
            "final_equity": amount(capital - cost - fee + quantity * Decimal(bars[-1].close)),
            "end_position": "left open and marked at final close",
        }
    return {
        "schema_version": "research-report/1",
        "plan_sha256": plan.digest,
        "strategy_sha256": fingerprint(plan.specification),
        "source_sha256": plan.source_sha256,
        "implementation_sha256": plan.implementation_sha256,
        "archive_sha256": plan.archive_sha256,
        "normalized_sha256": normalized_sha256,
        "source_url": ARCHIVE_URL,
        "engine_version": base["engine_version"],
        "runtime_versions": base.get("runtime_versions", {}),
        "name": plan.draft.name,
        "symbol": "BTCUSDT",
        "timeframe": "1 hour",
        "first_ns": bars[0].open_ns,
        "last_ns": bars[-1].close_ns,
        "observations": len(bars),
        "metrics": metrics,
        "doubled_cost_stress": stressed,
        "baseline": baseline,
        "baseline_unavailable": None
        if baseline
        else "Insufficient starting cash for the benchmark quantity",
        "equity_curve": base["equity_curve"],
        "fills": base["fills"],
        "signals": base["signals"],
        "pending_signal": base["pending_signal"],
        "assumptions": list(plan.assumptions),
        "evidence": [item.model_dump() for item in plan.draft.evidence],
        "limitations": [
            "One month and one implementation; no optimization or broad-market validation.",
            "OHLCV cannot verify historical spread, depth, queue priority or publication latency.",
            "Sparse/concentrated trades and marked open positions can dominate the result.",
            "No independent holdout, annualized risk ratio, paper, broker or live verification.",
            "No profitability requirement and no automatic promotion or activation.",
        ],
    }


def report_markdown(report: dict) -> str:
    if report.get("schema_version") in {"study-report/1", "study-report/2"}:
        from stratosphere.studies.reporting import study_markdown

        return study_markdown(report)
    metric = report["metrics"]
    lines = [
        f"# {report['name']}",
        "",
        "Research only · BTCUSDT spot · 1-hour candles · January 2025",
        "",
        "| Measure | Value |",
        "| --- | --- |",
    ]
    for label, key in [
        ("Net P&L (USDT)", "net_pnl"),
        ("Net return (%)", "net_return_pct"),
        ("Gross P&L before modeled costs", "gross_pnl_before_modeled_costs"),
        ("Fees (USDT)", "fees"),
        ("Spread/slippage (USDT)", "spread_and_slippage_cost"),
        ("Max drawdown (%)", "max_drawdown_pct"),
        ("Completed trades", "completed_trades"),
        ("Open positions at end", "open_positions"),
        ("Exposure (%)", "exposure_pct"),
    ]:
        lines.append(f"| {label} | {metric[key]} |")
    lines += [
        "",
        f"Doubled-cost net P&L: {report['doubled_cost_stress']['net_pnl']} USDT.",
        "",
        "## Chronological segments",
        "",
    ]
    lines += [
        f"- {part['name'].title()}: {part['observations']} bars; "
        f"equity change {part['net_equity_change']} USDT."
        for part in metric["segments"]
    ]
    lines += [
        "",
        "Positions and indicators carry across the split; this is not a separate final holdout.",
        "",
        "## Limitations",
        "",
    ]
    lines += [f"- {line}" for line in report["limitations"]]
    lines += [
        "",
        "## Immutable references",
        "",
        f"Plan: `{report['plan_sha256']}`",
        f"Strategy: `{report['strategy_sha256']}`",
        f"Archive: `{report['archive_sha256']}`",
        f"Implementation: `{report['implementation_sha256']}`",
        "",
        "The JSON report retains the equity curve, fills, trades, evidence, costs and assumptions.",
    ]
    return "\n".join(lines)
