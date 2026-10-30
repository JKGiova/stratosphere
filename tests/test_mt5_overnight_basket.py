"""Independent global-calendar/FX oracles and authored multi-agent account coordination."""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from test_mt5_bridge import connection
from test_mt5_overnight import Advisors, epoch

from stratosphere.mt5.bridge import BridgeError, MT5Bridge
from stratosphere.mt5.overnight import OvernightAgent
from stratosphere.mt5.overnight_basket import Basket, OvernightBasket
from stratosphere.mt5.overnight_markets import intersection
from stratosphere.mt5.overnight_strategy import entry_bounds, windows
from stratosphere.mt5.trading import Deployment, MT5Trading
from stratosphere.registry.workspace import WorkspaceDatabase


def catalog(symbols):
    return {"count": str(len(symbols)), **{f"s.{i}.symbol": s for i, s in enumerate(symbols, 1)}}


def test_catalog_exact_intersection_and_non_equivalent_indices():
    value = catalog(
        ["XAUUSD", "US100.cash", "US500.cash", "GER40.cash", "HK50.cash", "CHINA50.cash"]
    )
    result = intersection(value)
    assert {r["symbol"] for r in result["matched"]} == {"US500.cash", "GER40.cash", "HK50.cash"}
    assert any("Nasdaq Composite" in s for s in result["not_equivalent_or_unmapped"])
    MT5Bridge.validate_data("symbols", value, {})
    with pytest.raises(BridgeError):
        intersection(catalog(["GER40.cash", "GER40.cash"]))
    with pytest.raises(ValueError):
        MT5Bridge.validate_data("symbols", {"count": "5001"}, {})


def test_independent_global_calendar_oracles():
    start, end = epoch("2026-10-30T00:00"), epoch("2026-10-31T00:00")
    for symbol, entry, exit_at in (
        ("AUS200.cash", "2026-10-30T04:55", "2026-11-01T23:00"),
        ("JP225.cash", "2026-10-30T06:25", "2026-11-02T00:00"),
        ("HK50.cash", "2026-10-30T07:55", "2026-11-02T01:30"),
        ("FRA40.cash", "2026-10-30T16:25", "2026-11-02T08:00"),
        ("GER40.cash", "2026-10-30T16:25", "2026-11-02T08:00"),
        ("UK100.cash", "2026-10-30T16:25", "2026-11-02T08:00"),
        ("N25.cash", "2026-10-30T16:25", "2026-11-02T08:00"),
        ("US500.cash", "2026-10-30T19:55", "2026-11-02T14:30"),
    ):
        w = windows(start, end, symbol)[0]
        assert w.entry_start == epoch(entry)
        assert w.exit_at == epoch(exit_at)
    assert windows(epoch("2026-11-03T00:00"), epoch("2026-11-04T00:00"), "JP225.cash") == ()
    assert windows(epoch("2026-07-03T00:00"), epoch("2026-07-04T00:00"), "US500.cash") == ()
    assert windows(epoch("2026-07-03T00:00"), epoch("2026-07-04T00:00"), "UK100.cash")


@asynccontextmanager
async def basket_fixture(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        clock = [int(time.time())]
        provider = Advisors()
        original = bridge.read

        async def read(op, **args):
            symbol = args.get("symbol")
            if op == "account":
                return peer.session | {
                    "connected": "1",
                    "currency": "USD",
                    "equity": "100000",
                    "balance": "100000",
                    "free_margin": "100000",
                    "margin": "0",
                    "trading_input": "1",
                    "terminal_trade_allowed": "1",
                    "utc_epoch": str(clock[0]),
                }
            if op == "symbol":
                return {
                    "symbol": symbol,
                    "tick_size": "0.01",
                    "tick_value": "0.01",
                    "contract_size": "1",
                    "volume_min": "0.01",
                    "volume_max": "1000",
                    "volume_step": "0.01",
                    "profit_currency": "EUR" if symbol == "FRA40.cash" else "USD",
                    "margin_currency": "EUR" if symbol == "FRA40.cash" else "USD",
                }
            if op == "quote":
                return {
                    "bid": "1.09" if symbol == "EURUSD" else "25000",
                    "ask": "1.10" if symbol == "EURUSD" else "25001",
                    "age_ms": "0",
                    "server_time_ms": str(clock[0] * 1000),
                }
            result = await original(op, **args)
            if op == "positions" and int(result["count"]):
                result["p.1.type"] = "0"
            return result

        bridge.read = read

        async def factory():
            return provider

        database = WorkspaceDatabase(tmp_path / "workspace.sqlite3")
        limits = {
            "max_volume": "0.10",
            "max_order_risk": "100",
            "max_order_margin": "500",
            "max_spread": "2",
            "min_equity": "99500",
            "cost_buffer": "5",
            "max_orders": 1,
            "expires": clock[0] + 7 * 86400,
        }
        agents = [
            OvernightAgent(
                bridge, database, symbol, provider_factory=factory, clock=lambda: clock[0]
            )
            for symbol in ("FRA40.cash", "US500.cash")
        ]
        deployments = [
            Deployment.model_validate((await agent.draft(limits))["deployment"]) for agent in agents
        ]
        value = Basket(
            implementation_sha256=bridge.implementation_digest,
            catalog_sha256=bridge.retain_evidence(
                json.dumps(catalog([a.symbol for a in agents])).encode()
            ),
            created_utc=clock[0],
            deployments=tuple(deployments),
        )
        runner = OvernightBasket(
            bridge, database, value, provider_factory=factory, clock=lambda: clock[0]
        )
        yield runner, peer, provider, clock


async def test_fx_notional_uses_current_broker_currency_conversion(tmp_path):
    async with basket_fixture(tmp_path) as (runner, _, _, _):
        agent = runner.agents["FRA40.cash"]
        snapshot = await agent.capture(quote=True)
        bounds = entry_bounds(
            agent.symbol,
            snapshot["quote"],
            snapshot["symbol"],
            snapshot["account"],
            runner.deployments[agent.symbol],
        )
        assert bounds["notional_per_lot"] == "27501.10"
        assert snapshot["quote"]["conversion_to_usd"] == "1.10"


async def test_no_basket_authority_from_individual_files_or_wrong_digest(tmp_path):
    async with basket_fixture(tmp_path) as (runner, peer, provider, _):
        with pytest.raises(BridgeError, match="exact reviewed"):
            await runner.approve("0" * 64)
        with pytest.raises(BridgeError, match="user-approved overnight basket"):
            await runner.serve()
        assert peer.sent == 0 and not provider.calls


async def test_basket_routes_owned_position_and_skips_overlap_without_agent_fault(tmp_path):
    async with basket_fixture(tmp_path) as (runner, peer, provider, clock):
        await runner.approve(runner.basket.digest)
        report = runner.reports["FRA40.cash"]
        clock[0] = report.windows[0].entry_start
        result = await runner.cycle()
        assert result["action"] == "submitted_and_reconciled" and result["symbol"] == "FRA40.cash"
        uswindow = next(w for w in runner.reports["US500.cash"].windows if w.entry_start > clock[0])
        clock[0] = uswindow.entry_start
        result = await runner.cycle()
        assert result["action"] == "hold_owned_position"
        assert result["blocked_entry_symbols"] == ["US500.cash"]
        assert peer.sent == 1 and len(provider.calls) == 2
        clock[0] = report.windows[0].exit_at
        result = await runner.cycle()
        assert result["action"] == "closed_owned_position"
        assert peer.sent == 2 and len(provider.calls) == 2


async def test_basket_expiry_and_stale_health_shutdown(tmp_path):
    async with basket_fixture(tmp_path) as (runner, _, _, clock):
        await runner.approve(runner.basket.digest)
        clock[0] = runner.basket.deployments[0].expires + 1
        assert (await runner.cycle())["action"] == "expired_flat"
        stop = asyncio.Event()
        stop.set()
        await runner.serve(stop=stop)
        assert json.loads((runner.folder / "health.json").read_bytes())["state"] == "stopped"


async def test_catalog_pin_and_complete_universe_are_required(tmp_path):
    async with basket_fixture(tmp_path) as (runner, _, _, _):
        incomplete = runner.basket.model_copy(update={"deployments": runner.basket.deployments[:1]})
        with pytest.raises(BridgeError, match="all and only"):
            OvernightBasket(runner.bridge, runner.database, incomplete)
        path = (
            runner.bridge.state_directory() / "evidence" / (runner.basket.catalog_sha256 + ".bin")
        )
        path.write_bytes(b"changed catalog")
        with pytest.raises(BridgeError, match="catalog missing or changed"):
            OvernightBasket(runner.bridge, runner.database, runner.basket)


async def test_partial_native_approval_is_revoked_and_never_recreated(tmp_path, monkeypatch):
    async with basket_fixture(tmp_path) as (runner, _, _, _):
        original = MT5Trading.approve
        calls = []

        async def fail_after_commit(self, deployment, exact_digest, **kwargs):
            await original(self, deployment, exact_digest, **kwargs)
            calls.append(exact_digest)
            if len(calls) == 2:
                raise OSError("Authored failure after native policy publication")

        monkeypatch.setattr(MT5Trading, "approve", fail_after_commit)
        with pytest.raises(OSError):
            await runner.approve(runner.basket.digest)
        assert not (runner.folder / "approved.json").exists()
        with runner.bridge.journal() as db:
            assert [r[0] for r in db.execute("SELECT state FROM approvals")] == [
                "revoked",
                "revoked",
            ]
        with pytest.raises(BridgeError, match="approval history"):
            await runner.approve(runner.basket.digest)


def test_published_basket_schema():
    path = Path(__file__).parents[1] / "schemas/mt5-overnight-basket-v1.json"
    assert json.loads(path.read_bytes()) == Basket.model_json_schema()
