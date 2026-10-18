import importlib.util
import os
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal
from fractions import Fraction

import pytest

from stratosphere.research.contracts import ResearchPlan
from stratosphere.research.engine import run_simulation
from stratosphere.research.metrics import build_report


@pytest.fixture
def native_plan(research_plan):
    if importlib.util.find_spec("nautilus_trader") is None:
        if os.getenv("STRATOSPHERE_REQUIRE_ENGINE") == "1":
            pytest.fail("Pinned native engine is required by this verification gate")
        pytest.skip("Native engine unavailable; this is not native verification")
    return research_plan


def oracle(plan, bars, multiplier=1, *, trade_start_ns=None):
    """Independent batch SMA and cash ledger; does not reuse production signal or quote code."""
    closes = [Fraction(bar.close) for bar in bars]
    fast, slow = plan.draft.fast_period, plan.draft.slow_period
    differences = {
        i: sum(closes[i - fast + 1 : i + 1]) / fast - sum(closes[i - slow + 1 : i + 1]) / slow
        for i in range(slow - 1, len(bars))
    }
    cash, quantity = Decimal(plan.config.initial_balance), Decimal(plan.draft.quantity)
    base = Decimal(0)
    pending = None
    fills, signals, curve = [], [], []
    quantum = Decimal("0.00000001")
    impact = (
        (Decimal(plan.config.spread_bps) / 2 + Decimal(plan.config.slippage_bps))
        * multiplier
        / 10000
    )
    for i, bar in enumerate(bars):
        if pending:
            side, signal_ns = pending
            opening = Decimal(bar.open)
            px = (opening * (1 + impact if side == "BUY" else 1 - impact)).quantize(
                quantum, rounding=ROUND_CEILING if side == "BUY" else ROUND_FLOOR
            )
            fee = (px * quantity * Decimal(plan.config.fee_rate) * multiplier).quantize(
                quantum, rounding=ROUND_HALF_UP
            )
            notional = (px * quantity).quantize(quantum, rounding=ROUND_HALF_UP)
            cash += -notional - fee if side == "BUY" else notional - fee
            base += quantity if side == "BUY" else -quantity
            fills.append(
                {
                    "side": side,
                    "signal_ns": signal_ns,
                    "fill_ns": bar.open_ns,
                    "price": f"{px:.8f}",
                    "commission": str(fee),
                }
            )
            pending = None
        measured = trade_start_ns is None or bar.open_ns >= trade_start_ns
        if i - 1 in differences and measured:
            previous, current = differences[i - 1], differences[i]
            if not base and previous <= 0 < current:
                pending = "BUY", bar.close_ns
            elif base and previous >= 0 > current:
                pending = "SELL", bar.close_ns
            if pending:
                signals.append({"side": pending[0], "signal_ns": pending[1]})
        if measured:
            curve.append(cash + base * Decimal(bar.close))
    return signals, fills, curve, base, pending


@pytest.mark.engine
def test_native_results_match_independent_rules_next_open_costs_and_cash(native_plan, market_bars):
    result = run_simulation(native_plan, market_bars)
    signals, fills, curve, held, pending = oracle(native_plan, market_bars)
    assert result["signals"] == signals
    assert len(result["fills"]) == len(fills) > 2
    for actual, expected in zip(result["fills"], fills, strict=True):
        for key, value in expected.items():
            assert actual[key] == value
        assert actual["fill_ns"] > actual["signal_ns"]
        assert actual["commission_currency"] == "USDT"
    assert [Decimal(point["equity"]) for point in result["equity_curve"]] == curve
    assert Decimal(result["balances"]["BTC"]) == held
    assert result["pending_signal"] == pending
    assert run_simulation(native_plan, market_bars) == result


@pytest.mark.engine
def test_warmup_causality_and_open_end_positions_are_preserved(native_plan, market_bars):
    full = run_simulation(native_plan, market_bars)
    first_buy = next(fill for fill in full["fills"] if fill["side"] == "BUY")
    boundary = (
        next(i for i, bar in enumerate(market_bars) if bar.open_ns == first_buy["fill_ns"]) + 1
    )
    prefix = run_simulation(native_plan, market_bars[:boundary])
    assert prefix["fills"] == [first_buy]
    assert prefix["open_positions"] == 1
    assert all(
        signal["signal_ns"] >= market_bars[native_plan.draft.slow_period].close_ns
        for signal in full["signals"]
    )
    for length in [20, 60, 100]:
        partial = run_simulation(native_plan, market_bars[:length])
        assert partial["equity_curve"] == full["equity_curve"][:length]
        assert partial["fills"] == [
            f for f in full["fills"] if f["fill_ns"] <= market_bars[length - 1].close_ns
        ]


@pytest.mark.engine
def test_reports_reconcile_costs_segments_distribution_and_frozen_stress(
    native_plan, market_bars, research_snapshot
):
    base = run_simulation(native_plan, market_bars)
    stress = run_simulation(native_plan, market_bars, multiplier=2)
    report = build_report(
        native_plan, market_bars, research_snapshot.normalized_sha256, base, stress
    )
    metrics = report["metrics"]
    assert Decimal(metrics["net_pnl"]) == Decimal(metrics["final_equity"]) - Decimal(
        metrics["initial_equity"]
    )
    assert sum(Decimal(p["net_equity_change"]) for p in metrics["segments"]) == Decimal(
        metrics["net_pnl"]
    )
    assert metrics["completed_trades"] == sum(
        metrics["trade_distribution"][key] for key in ("wins", "losses", "flat")
    )
    assert Decimal(metrics["gross_pnl_before_modeled_costs"]) == (
        Decimal(metrics["net_pnl"])
        + Decimal(metrics["fees"])
        + Decimal(metrics["spread_and_slippage_cost"])
    )
    assert stress["signals"] == base["signals"]
    assert Decimal(report["doubled_cost_stress"]["net_pnl"]) < Decimal(metrics["net_pnl"])
    assert Decimal(metrics["max_drawdown_pct"]) >= 0
    assert report["equity_curve"] == base["equity_curve"]
    assert report["source_sha256"] == native_plan.source_sha256


@pytest.mark.engine
def test_insufficient_capital_fails_without_a_successful_native_report(native_plan, market_bars):
    payload = native_plan.model_dump(mode="json")
    payload["config"]["initial_balance"] = "0.01"
    with pytest.raises(RuntimeError):
        run_simulation(ResearchPlan.model_validate(payload), market_bars)
