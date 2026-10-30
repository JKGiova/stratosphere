"""Calendar oracles and operational-agent safety tests with an authored filesystem EA."""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest
from test_mt5_bridge import connection

from stratosphere.development.contracts import validate_program
from stratosphere.mt5.bridge import BridgeError, UncertainOrder
from stratosphere.mt5.overnight import OvernightAgent, OvernightSizing, Review
from stratosphere.mt5.overnight_strategy import document, signal, windows
from stratosphere.mt5.trading import Deployment
from stratosphere.phase0.contracts import Expression, fingerprint
from stratosphere.registry.workspace import WorkspaceDatabase


def epoch(value):
    return int(datetime.fromisoformat(value).replace(tzinfo=UTC).timestamp())


def test_schedule_oracles_weekend_dst_holiday_and_early_close():
    result = windows(epoch("2026-10-30T18:00"), epoch("2026-11-04T00:00"))
    assert result[0].entry_start == epoch("2026-10-30T19:55")
    assert result[0].entry_end == epoch("2026-10-30T19:59:30")
    assert result[0].exit_at == epoch("2026-11-02T14:30")  # US DST changed over weekend
    assert result[1].entry_start == epoch("2026-11-02T20:55")
    result = windows(epoch("2026-11-25T17:00"), epoch("2026-11-28T00:00"))
    assert [w.session for w in result] == ["2026-11-25", "2026-11-27"]
    assert result[0].exit_at == epoch("2026-11-27T14:30")
    assert result[1].entry_start == epoch("2026-11-27T17:55")  # 13:00 NY early close


def test_profile_not_gold_or_spot_and_no_generic_backtest_conversion():
    a, b = document("US100.cash"), document("US500.cash")
    assert a.formal_spec.instrument_constraints.asset_classes == ("index_cfd",)
    assert a.formal_spec.indicators == ()
    assert a.formal_spec.entry_rules[0].condition.op == "session"
    assert fingerprint(a.formal_spec) != fingerprint(b.formal_spec)
    with pytest.raises(ValueError):
        validate_program(a.formal_spec)
    with pytest.raises(ValueError):
        Expression(op="session", value="arbitrary-calendar-code")
    with pytest.raises(BridgeError):
        document("XAUUSD")


class Advisors:
    def __init__(self, *, volume="0.01", reliable=True, skip=False, mutate=None, fail=False):
        self.calls = []
        self.volume, self.reliable, self.skip = volume, reliable, skip
        self.mutate, self.fail = mutate, fail

    async def stream(self, messages):
        self.calls.append(messages)
        if self.fail:
            raise RuntimeError("Authored model failure")
        context = json.loads(messages[-1]["content"])
        if "strategy_id" in context:
            yield json.dumps(
                {
                    "strategy_id": context["strategy_id"],
                    "signal_sha256": context["signal_sha256"],
                    "context_sha256": context["context_sha256"],
                    "verdict": "reliable" if self.reliable else "not_reliable",
                    "evidence": ["signal"],
                    "explanation": "Authored signal verdict",
                }
            )
        else:
            if self.mutate:
                self.mutate()
            yield json.dumps(
                {
                    "schema_version": "mt5-overnight-sizing-proposal/1",
                    "context_sha256": context["context_sha256"],
                    "decision": "skip" if self.skip else "enter",
                    "volume": "0" if self.skip else self.volume,
                    "explanation": "Authored numeric risk proposal",
                }
            )


@asynccontextmanager
async def operational(tmp_path, *, provider=None):
    async with connection(tmp_path) as (bridge, peer):
        now = int(time.time())
        calendar_windows = windows(now, now + 7 * 86400)
        clock = [now]
        state = {"quote_age": "0", "spread": "1", "utc_drift": 0, "mode": "demo"}
        provider = provider or Advisors()
        original = bridge.read

        async def read(op, **args):
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
                    "mode": state["mode"],
                    "utc_epoch": str(int(clock[0]) + state["utc_drift"]),
                }
            if op == "symbol":
                return {
                    "symbol": "US100.cash",
                    "tick_size": "0.01",
                    "tick_value": "0.01",
                    "contract_size": "1",
                    "volume_min": "0.01",
                    "volume_max": "1000",
                    "volume_step": "0.01",
                    "profit_currency": "USD",
                    "margin_currency": "USD",
                }
            if op == "quote":
                from decimal import Decimal

                return {
                    "bid": "25000",
                    "ask": str(Decimal("25000") + Decimal(state["spread"])),
                    "age_ms": state["quote_age"],
                    "server_time_ms": str(int(clock[0]) * 1000),
                }
            result = await original(op, **args)
            if op == "positions" and int(result["count"]):
                result["p.1.type"] = "0"
            return result

        bridge.read = read

        async def factory():
            return provider

        database = WorkspaceDatabase(tmp_path / "workspace.sqlite3")
        agent = OvernightAgent(
            bridge, database, "US100.cash", provider_factory=factory, clock=lambda: clock[0]
        )
        limits = {
            "max_volume": "0.10",
            "max_order_risk": "100",
            "max_order_margin": "500",
            "max_spread": "2",
            "min_equity": "99500",
            "cost_buffer": "5",
            "max_orders": 5,
            "expires": now + 7 * 86400,
        }
        draft = await agent.draft(limits)
        deployment = Deployment.model_validate(draft["deployment"])
        yield agent, peer, provider, clock, calendar_windows, state, deployment


async def approve(agent, deployment):
    await agent.trading.approve(deployment, deployment.digest)


async def test_status_draft_do_not_activate_or_call_models_and_reuse_agent(tmp_path):
    async with operational(tmp_path) as (agent, peer, model, *_):
        result = await agent.status()
        assert result["positions"] == 0 and not model.calls and peer.sent == 0
        again = OvernightAgent(agent.bridge, agent.database, "US100.cash")
        assert again.record["public_id"] == agent.record["public_id"]
        assert again.record["agent_id"] == agent.record["agent_id"]
        assert (agent.runtime / "overnight.py").is_file()
        with agent.bridge.journal() as db:
            assert db.execute("SELECT COUNT(*) FROM approvals").fetchone()[0] == 0


async def test_unapproved_agent_cannot_run_or_infer(tmp_path):
    async with operational(tmp_path) as (agent, peer, model, clock, ws, _, deployment):
        clock[0] = ws[0].entry_start
        with pytest.raises(BridgeError, match="user-approved"):
            await agent.serve(deployment.digest)
        assert peer.sent == 0 and not model.calls


async def test_approved_entry_native_fill_reconcile_next_open_exit_without_model(tmp_path):
    async with operational(tmp_path) as (agent, peer, model, clock, ws, _, deployment):
        await approve(agent, deployment)
        assert (await agent.tick(deployment.digest))["action"] == "waiting_for_entry_window"
        clock[0] = ws[0].entry_start
        result = await agent.tick(deployment.digest)
        assert result["action"] == "submitted_and_reconciled"
        assert peer.sent == 1 and len(model.calls) == 2
        assert (await agent.tick(deployment.digest))["action"] == "hold_owned_position"
        assert len(model.calls) == 2
        await agent.trading.revoke(deployment.digest)
        clock[0] = ws[0].exit_at
        result = await agent.tick(deployment.digest)
        assert result["action"] == "closed_owned_position"
        assert peer.sent == 2 and len(model.calls) == 2
        context = json.dumps(model.calls)
        for private in ("12345", "FTMO-Demo", "login", "api_key", "balance", "free_margin"):
            assert private not in context


async def test_restart_consumed_trigger_does_not_reenter_after_stop_loss(tmp_path):
    async with operational(tmp_path) as (agent, peer, model, clock, ws, _, deployment):
        await approve(agent, deployment)
        clock[0] = ws[0].entry_start
        await agent.tick(deployment.digest)
        peer.positions = {"count": "0"}  # authored broker SL exit
        restarted = OvernightAgent(
            agent.bridge,
            agent.database,
            "US100.cash",
            provider_factory=agent.provider_factory,
            clock=agent.clock,
        )
        assert (await restarted.tick(deployment.digest))["action"] == "trigger_already_consumed"
        assert peer.sent == 1 and len(model.calls) == 2


@pytest.mark.parametrize("change", ["late", "quote", "clock", "mode", "exposure", "size", "stop"])
async def test_refreshed_gates_deny_entry(tmp_path, change):
    async with operational(tmp_path) as (agent, peer, model, clock, ws, state, deployment):
        await approve(agent, deployment)
        clock[0] = ws[0].entry_start

        def mutate():
            if change == "late":
                clock[0] = ws[0].entry_end
            elif change == "quote":
                state["quote_age"] = "6000"
            elif change == "clock":
                state["utc_drift"] = 60
            elif change == "mode":
                state["mode"] = "real"
            elif change == "exposure":
                peer.positions = {"count": "1", "p.1.symbol": "external"}
            elif change == "stop":
                agent.stop_event = asyncio.Event()
                agent.stop_event.set()

        model.mutate = mutate
        if change == "size":
            model.volume = "0.105"
        with pytest.raises(BridgeError):
            await agent.tick(deployment.digest)
        assert peer.sent == 0 and (agent.folder / "halt.json").is_file()


@pytest.mark.parametrize(
    "reliable,skip,calls,action",
    [(False, False, 1, "strategy_agent_rejected"), (True, True, 2, "risk_agent_skipped")],
)
async def test_agents_can_reject_with_no_retry(tmp_path, reliable, skip, calls, action):
    async with operational(tmp_path, provider=Advisors(reliable=reliable, skip=skip)) as values:
        agent, peer, model, clock, ws, _, deployment = values
        await approve(agent, deployment)
        clock[0] = ws[0].entry_start
        assert (await agent.tick(deployment.digest))["action"] == action
        assert (await agent.tick(deployment.digest))["action"] == "trigger_already_consumed"
        assert peer.sent == 0 and len(model.calls) == calls


async def test_uncertain_submission_never_replayed_across_restart(tmp_path):
    async with operational(tmp_path) as (agent, peer, model, clock, ws, _, deployment):
        await approve(agent, deployment)
        clock[0] = ws[0].entry_start
        peer.drop = True
        with pytest.raises(UncertainOrder):
            await agent.tick(deployment.digest)
        restarted = OvernightAgent(
            agent.bridge,
            agent.database,
            "US100.cash",
            provider_factory=agent.provider_factory,
            clock=agent.clock,
        )
        assert (await restarted.tick(deployment.digest))["action"] == "halted_flat"
        assert peer.sent == 1 and len(model.calls) == 2


async def test_failure_consumes_review_and_latches_halt(tmp_path):
    async with operational(tmp_path, provider=Advisors(fail=True)) as values:
        agent, peer, model, clock, ws, _, deployment = values
        await approve(agent, deployment)
        clock[0] = ws[0].entry_start
        with pytest.raises(RuntimeError):
            await agent.tick(deployment.digest)
        assert (await agent.tick(deployment.digest))["action"] == "halted_flat"
        assert len(model.calls) == 1 and peer.sent == 0


async def test_closed_market_preparation_defers_feasibility_without_relaxing_entry(tmp_path):
    async with operational(tmp_path) as (agent, peer, model, clock, ws, state, deployment):
        state["quote_age"] = "60000"
        limits = deployment.model_dump(
            include={
                "max_volume",
                "max_order_risk",
                "max_order_margin",
                "max_spread",
                "min_equity",
                "cost_buffer",
                "max_orders",
                "expires",
            }
        )
        prepared = await agent.draft(limits)
        assert prepared["bounds"]["status"] == "deferred_until_fresh_trigger"
        reviewed = Deployment.model_validate(prepared["deployment"])
        await approve(agent, reviewed)
        clock[0] = ws[0].entry_start
        with pytest.raises(BridgeError, match="stale"):
            await agent.tick(reviewed.digest)
        assert not model.calls and peer.sent == 0


async def test_native_request_expiry_is_inside_entry_window(tmp_path):
    from stratosphere.mt5.bridge import decode

    async with operational(tmp_path) as (agent, _, _, clock, ws, _, deployment):
        await approve(agent, deployment)
        clock[0] = ws[0].entry_start
        await agent.tick(deployment.digest)
        with agent.bridge.journal() as db:
            payload = db.execute("SELECT payload FROM requests WHERE op='submit'").fetchone()[0]
        assert int(decode(payload)["expires"]) < ws[0].entry_end


async def test_owned_exit_after_expiry_with_no_model_provider(tmp_path):
    async with operational(tmp_path) as (agent, peer, model, clock, ws, _, deployment):
        await approve(agent, deployment)
        clock[0] = ws[0].entry_start
        await agent.tick(deployment.digest)

        async def unavailable():
            raise AssertionError("An exit must not load model credentials")

        agent.provider_factory = unavailable
        clock[0] = deployment.expires + 1
        assert (await agent.tick(deployment.digest))["action"] == "closed_owned_position"
        assert peer.sent == 2 and len(model.calls) == 2


async def test_service_owner_lock_and_bounded_shutdown(tmp_path):
    async with operational(tmp_path) as (agent, _, model, _, _, _, deployment):
        await approve(agent, deployment)
        stop = asyncio.Event()
        task = asyncio.create_task(agent.serve(deployment.digest, interval=5, stop=stop))
        try:
            async with asyncio.timeout(3):
                while not (agent.runtime / "health.json").is_file():
                    await asyncio.sleep(0.01)
            with pytest.raises(BridgeError, match="Another overnight"):
                await agent.tick(deployment.digest)
            assert not model.calls
        finally:
            stop.set()
            await task
        assert json.loads((agent.runtime / "health.json").read_bytes())["state"] == "stopped"


async def test_retirement_hides_catalog_but_preserves_identity_agent_and_exit(tmp_path):
    async with operational(tmp_path) as (agent, peer, model, clock, ws, _, deployment):
        await approve(agent, deployment)
        clock[0] = ws[0].entry_start
        await agent.tick(deployment.digest)
        before = agent.database.strategy(agent.record["public_id"])
        agent.database.retire(before["public_id"], "Operator replaced this exact strategy")
        assert agent.database.catalog() == []
        assert agent.database.catalog(include_retired=True) == [before]
        assert agent.database.strategy(before["public_id"]) == before
        agent.halt("Authored halt does not disable protective exit")
        clock[0] = ws[0].exit_at
        assert (await agent.tick(deployment.digest))["action"] == "closed_owned_position"
        assert peer.sent == 2 and len(model.calls) == 2


async def test_pins_block_forged_review_or_strategy(tmp_path):
    async with operational(tmp_path) as (agent, _, _, _, _, _, deployment):
        wrong = deployment.model_copy(update={"symbol": "US500.cash"})
        with pytest.raises(BridgeError, match="exact frozen"):
            agent.review(wrong)


def test_published_overnight_schemas():
    root = Path(__file__).parents[1] / "schemas"
    for name, cls in (
        ("mt5-overnight-review-v1", Review),
        ("mt5-overnight-sizing-v1", OvernightSizing),
    ):
        assert json.loads((root / f"{name}.json").read_bytes()) == cls.model_json_schema()


def test_signal_identity_stable_per_session_different_per_instrument():
    w = windows(epoch("2026-10-30T18:00"), epoch("2026-10-31T00:00"))[0]
    assert signal("US100.cash", w) == signal("US100.cash", w)
    assert signal("US100.cash", w)["signal_sha256"] != signal("US500.cash", w)["signal_sha256"]
