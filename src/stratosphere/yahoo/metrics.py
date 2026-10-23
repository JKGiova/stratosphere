"""Quote-rounded Yahoo accounting; completed trade P&L comes from native cash deltas."""

from decimal import ROUND_HALF_UP, Decimal

from stratosphere.research.metrics import amount, summarize


def summarize_yahoo(result, initial):
    metrics = summarize(result, initial)
    fills = result["fills"]
    quantum = Decimal(1).scaleb(-result["cash_precision"])
    impact = sum(
        (
            abs(
                Decimal(fill["execution_notional"])
                - (Decimal(fill["reference_open"]) * Decimal(fill["quantity"])).quantize(
                    quantum, rounding=ROUND_HALF_UP
                )
            )
            for fill in fills
        ),
        Decimal(0),
    )
    turnover = sum((Decimal(fill["execution_notional"]) for fill in fills), Decimal(0))
    capital = Decimal(initial)
    gross = Decimal(metrics["net_pnl"]) + Decimal(metrics["fees"]) + impact
    metrics.update(
        spread_and_slippage_cost=amount(impact),
        turnover_quote=amount(turnover),
        turnover_initial_equity=amount(turnover / capital),
        gross_pnl_before_modeled_costs=amount(gross),
        gross_return_pct=amount(gross / capital * 100),
    )
    trades, entry = [], None
    for fill in fills:
        if fill["side"] == "BUY":
            entry = fill
        else:
            trades.append(
                {
                    "entry_ns": entry["fill_ns"],
                    "exit_ns": fill["fill_ns"],
                    "quantity": fill["quantity"],
                    "net_pnl": amount(
                        Decimal(entry["quote_cash_delta"]) + Decimal(fill["quote_cash_delta"])
                    ),
                }
            )
            entry = None
    pnls = [Decimal(t["net_pnl"]) for t in trades]
    metrics.update(
        trades=trades,
        completed_trades=len(trades),
        trade_distribution={
            "wins": sum(pnl > 0 for pnl in pnls),
            "losses": sum(pnl < 0 for pnl in pnls),
            "flat": sum(pnl == 0 for pnl in pnls),
            "min_net_pnl": amount(min(pnls)) if pnls else None,
            "max_net_pnl": amount(max(pnls)) if pnls else None,
        },
    )
    return metrics
