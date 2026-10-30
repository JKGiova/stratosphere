"""Gold signal/size oracles and the existing file EA gateway, using authored demo data only."""

import json
import time
from contextlib import asynccontextmanager
from decimal import Decimal
from fractions import Fraction

import pytest
from test_mt5_bridge import connection

from stratosphere.development.contracts import validate_program
from stratosphere.mt5.bridge import BridgeError
from stratosphere.mt5.gold import GoldConnection
from stratosphere.mt5.gold_strategy import (
    BAR_COUNT,
    compiled_script,
    document,
    entry_bounds,
    evaluate,
)
from stratosphere.registry.workspace import WorkspaceDatabase


def candles(*, rising=True):
    now = int(time.time())
    last = (now // 86400 - 1) * 86400
    result = {"count": str(BAR_COUNT)}
    for i in range(1, BAR_COUNT + 1):
        close = (1000 + i) if rising else (2000 - i)
        for key, value in {
            "time": last - (BAR_COUNT - i) * 86400,
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "tick_volume": 100,
            "spread": 10,
        }.items():
            result[f"b.{i}.{key}"] = str(value)
    return result


def quote():
    return {
        "bid": "1300",
        "ask": "1300.1",
        "age_ms": "0",
        "server_time_ms": str(int(time.time()) * 1000),
    }


class Sizer:
    def __init__(self, volume="0.01", *, skip=False, mutate=None):
        self.volume, self.skip, self.mutate = volume, skip, mutate
        self.calls = []

    async def stream(self, messages):
        self.calls.append(messages)
        context = json.loads(messages[-1]["content"])
        if self.mutate:
            self.mutate()
        yield json.dumps(
            {
                "context_sha256": context["context_sha256"],
                "decision": "skip" if self.skip else "enter",
                "volume": "0" if self.skip else self.volume,
                "explanation": "Authored bounded risk proposal",
            }
        )


@asynccontextmanager
async def pilot(tmp_path, *, rising=True, provider=None):
    async with connection(tmp_path) as (bridge, peer):
        state = {"rates": candles(rising=rising), "quote": quote(), "mode": "demo"}
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
                }
            if op == "symbol":
                return {
                    "symbol": "XAUUSD",
                    "tick_size": "0.01",
                    "tick_value": "1",
                    "contract_size": "100",
                    "volume_min": "0.01",
                    "volume_max": "100",
                    "volume_step": "0.01",
                    "profit_currency": "USD",
                    "margin_currency": "USD",
                }
            if op in {"rates", "quote"}:
                return state[op]
            return await original(op, **args)

        bridge.read = read
        provider = provider or Sizer()

        async def factory():
            return provider

        agent = GoldConnection(
            bridge, WorkspaceDatabase(tmp_path / "library.sqlite3"), provider_factory=factory
        )
        yield agent, peer, provider, state


def limits(**changes):
    return {
        "max_volume": "0.05",
        "max_order_risk": "100",
        "max_order_margin": "500",
        "max_spread": "1",
        "min_equity": "99000",
        "cost_buffer": "2",
        "max_orders": 1,
        "expires": int(time.time()) + 600,
    } | changes


async def approve(agent):
    from stratosphere.mt5.trading import Deployment

    draft = await agent.draft(limits())
    value = Deployment.model_validate(draft["deployment"])
    await agent.trading.approve(value, value.digest)
    return value


def test_daily_signal_matches_independent_means_and_has_explicit_cfd_identity():
    rates = candles()
    result = evaluate(rates, quote())
    closes = [Fraction(rates[f"b.{i}.close"]) for i in range(1, BAR_COUNT + 1)]
    assert Decimal(result["sma50"]) == Decimal(sum(closes[-50:]).numerator) / 50
    assert Decimal(result["sma200"]) == Decimal(sum(closes[-200:]).numerator) / 200
    assert result["desired_position"] == "long" and result["new_golden_cross"] is False
    assert result["mean_true_range_14"] == "2"
    assert evaluate(candles(rising=False), quote())["desired_position"] == "flat"
    spec = document().formal_spec
    assert spec.instrument_constraints.asset_classes == ("commodity_cfd",)
    assert spec.instrument_constraints.fixed_symbols == ("MT5:XAUUSD",)
    with pytest.raises(ValueError, match="Unsupported"):
        validate_program(spec)  # Never silently routed through historical Yahoo/spot execution.


@pytest.mark.parametrize("mutation", ["short", "stale", "future", "unordered", "forming", "gap"])
def test_unusable_windows_and_quotes_block_signal(mutation):
    rates, tick = candles(), quote()
    if mutation == "short":
        rates["count"] = "259"
    elif mutation == "stale":
        tick["age_ms"] = "5001"
    elif mutation == "future":
        tick["age_ms"] = "-2001"
    elif mutation == "unordered":
        rates["b.2.time"] = rates["b.1.time"]
    elif mutation == "forming":
        tick["server_time_ms"] = str(int(rates["b.260.time"]) * 1000)
    else:
        rates["b.1.time"] = str(int(rates["b.2.time"]) - 6 * 86400)
    with pytest.raises((BridgeError, ValueError)):
        evaluate(rates, tick)


async def test_bearish_flat_status_and_tick_do_not_ask_model_or_activate(tmp_path):
    async with pilot(tmp_path, rising=False) as (agent, peer, provider, state):
        status = await agent.status()
        assert status["signal"]["desired_position"] == "flat"
        assert (agent.folder / "gold_golden.py").read_bytes() == compiled_script()
        assert (await agent.tick("not-an-approval"))["action"] == "stay_flat"
        assert provider.calls == [] and peer.sent == 0
        again = GoldConnection(agent.bridge, agent.database)
        assert again.record["public_id"] == agent.record["public_id"]
        assert again.record["agent_id"] == agent.record["agent_id"]


async def test_bullish_regime_needs_exact_approval_before_sizing(tmp_path):
    async with pilot(tmp_path) as (agent, peer, provider, state):
        with pytest.raises(BridgeError, match="No current user-approved"):
            await agent.tick("a" * 64)
        assert not provider.calls and peer.sent == 0


async def test_approved_entry_goes_through_sizer_native_check_and_existing_gateway(tmp_path):
    async with pilot(tmp_path) as (agent, peer, provider, state):
        deployment = await approve(agent)
        result = await agent.tick(deployment.digest)
        assert result["action"] == "submitted_and_reconciled"
        assert peer.sent == 1 and len(provider.calls) == 1
        content = provider.calls[0][-1]["content"]
        assert all(
            value not in content for value in ("FTMO-Demo", '"login"', '"equity"', '"free_margin"')
        )
        with agent.bridge.journal() as db:
            operations = [r[0] for r in db.execute("SELECT op FROM requests")]
        assert "check" in operations and "submit" in operations
        # A broker stop/explicit external closure cannot replenish this one-entry deployment.
        peer.positions = {}
        with pytest.raises(BridgeError, match="entry budget consumed"):
            await agent.tick(deployment.digest)
        assert len(provider.calls) == 1 and peer.sent == 1


@pytest.mark.parametrize("volume", ["50", "0.011", "0.001"])
async def test_agent_cannot_exceed_limits_or_change_broker_step(tmp_path, volume):
    async with pilot(tmp_path, provider=Sizer(volume)) as (agent, peer, provider, state):
        deployment = await approve(agent)
        with pytest.raises(BridgeError, match="invalid or excessive"):
            await agent.tick(deployment.digest)
        assert peer.sent == 0
        with pytest.raises(BridgeError, match="slot already consumed"):
            await agent.tick(deployment.digest)
        assert len(provider.calls) == 1


async def test_skip_never_submits_and_uncertain_advisory_cannot_replay(tmp_path):
    async with pilot(tmp_path, provider=Sizer(skip=True)) as (agent, peer, provider, state):
        deployment = await approve(agent)
        assert (await agent.tick(deployment.digest))["action"] == "agent_skipped"
        assert peer.sent == 0
        with pytest.raises(BridgeError, match="slot already consumed"):
            await agent.tick(deployment.digest)


async def test_changed_signal_after_inference_blocks_entry(tmp_path):
    async with pilot(tmp_path) as (agent, peer, provider, state):
        provider.mutate = lambda: state.update(rates=candles(rising=False))
        deployment = await approve(agent)
        with pytest.raises(BridgeError, match="signal/session/exposure changed"):
            await agent.tick(deployment.digest)
        assert peer.sent == 0


async def test_owned_bearish_exit_is_deterministic_and_has_no_second_model_call(tmp_path):
    async with pilot(tmp_path) as (agent, peer, provider, state):
        deployment = await approve(agent)
        await agent.tick(deployment.digest)
        peer.positions["p.1.type"] = "0"
        state["rates"] = candles(rising=False)
        await agent.trading.revoke(deployment.digest)
        result = await agent.tick(deployment.digest)
        assert result["action"] == "closed_owned_position"
        assert peer.sent == 2 and len(provider.calls) == 1
        assert peer.positions == {"count": "0"}


async def test_external_position_is_never_closed(tmp_path):
    async with pilot(tmp_path, rising=False) as (agent, peer, provider, state):
        deployment = await approve(agent)
        peer.positions = {
            "count": "1",
            "p.1.symbol": "XAUUSD",
            "p.1.type": "0",
            "p.1.ticket": "999",
            "p.1.magic": "77",
            "p.1.volume": "0.01",
        }
        with pytest.raises(BridgeError, match="no exact owned entry"):
            await agent.tick(deployment.digest)
        assert peer.sent == 0 and provider.calls == []


@pytest.mark.parametrize(
    "changes", [{"max_orders": 2}, {"expires": 1}, {"expires": "tomorrow"}, {"symbol": "EURUSD"}]
)
async def test_draft_requires_finite_exact_pilot_scope(tmp_path, changes):
    async with pilot(tmp_path) as (agent, peer, provider, state):
        with pytest.raises(BridgeError):
            await agent.draft(limits(**changes))
        assert not provider.calls and peer.sent == 0


async def test_native_rejection_blocks_send_without_consuming_a_second_advisory(tmp_path):
    async with pilot(tmp_path) as (agent, peer, provider, state):
        deployment = await approve(agent)
        original = agent.bridge._request

        async def rejected(op, *args, **kwargs):
            if op == "check":
                raise BridgeError("Authored native OrderCheck rejection")
            return await original(op, *args, **kwargs)

        agent.bridge._request = rejected
        with pytest.raises(BridgeError, match="OrderCheck rejection"):
            await agent.tick(deployment.digest)
        assert peer.sent == 0 and len(provider.calls) == 1


async def test_initial_stop_and_max_lots_follow_independent_accounting_bounds(tmp_path):
    async with pilot(tmp_path) as (agent, peer, provider, state):
        deployment = await approve(agent)
        observation = await agent.capture()
        bound = entry_bounds(
            observation["signal"],
            observation["quote"],
            observation["symbol"],
            observation["account"],
            deployment,
        )
        assert Decimal(bound["stop_loss"]) == Decimal("1296")
        assert Decimal(bound["risk_per_lot_estimate"]) == Decimal("410")
        assert Decimal(bound["max_lots"]) == Decimal("0.05")
        assert Decimal(bound["max_notional"]) == Decimal("10000")


async def test_uncertain_native_submit_never_replays_after_restart(tmp_path):
    from stratosphere.mt5.bridge import UncertainOrder

    async with pilot(tmp_path) as (agent, peer, provider, state):
        deployment = await approve(agent)
        peer.drop = True
        with pytest.raises(UncertainOrder):
            await agent.tick(deployment.digest)
        assert peer.sent == 1
        restarted = GoldConnection(
            agent.bridge, agent.database, provider_factory=agent.provider_factory
        )
        with pytest.raises(BridgeError, match="outcome unresolved"):
            await restarted.tick(deployment.digest)
        assert peer.sent == 1 and len(provider.calls) == 1


async def test_cancelled_advisory_stays_consumed_and_never_sends(tmp_path):
    import asyncio

    entered = asyncio.Event()

    class Waiting(Sizer):
        async def stream(self, messages):
            self.calls.append(messages)
            entered.set()
            await asyncio.Event().wait()
            yield ""

    async with pilot(tmp_path, provider=Waiting()) as (agent, peer, provider, state):
        deployment = await approve(agent)
        task = asyncio.create_task(agent.tick(deployment.digest))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(BridgeError, match="slot already consumed"):
            await agent.tick(deployment.digest)
        assert peer.sent == 0 and len(provider.calls) == 1


async def test_real_mode_and_wrong_connection_review_cannot_enter(tmp_path):
    from stratosphere.mt5.gold_strategy import PROFILE, fingerprint

    async with pilot(tmp_path) as (agent, peer, provider, state):
        deployment = await approve(agent)
        observation = await agent.capture()
        agent.trading.verify = lambda *args, **kwargs: None
        with pytest.raises(BridgeError, match="exact gold strategy"):
            agent.verify_deployment(deployment.model_copy(update={"mode": "real"}), observation)
        wrong = {
            "schema_version": "mt5-gold-connection-review/1",
            "strategy_id": "9" * 24,
            "profile": PROFILE,
            "implementation_sha256": agent.bridge.implementation_digest,
            "specification_sha256": fingerprint(document().formal_spec),
        }
        wrong_pin = agent.bridge.retain_evidence(json.dumps(wrong).encode())
        with pytest.raises(BridgeError, match="matching connection review"):
            agent.verify_deployment(
                deployment.model_copy(update={"report_sha256": wrong_pin}), observation
            )
        assert peer.sent == 0 and not provider.calls


def test_gold_sizing_schema_is_published_and_legacy_documents_keep_identity():
    from pathlib import Path

    from stratosphere.mt5.gold import SizingProposal
    from stratosphere.phase0.contracts import StrategyDocument, fingerprint

    schema = Path(__file__).parents[1] / "schemas/mt5-gold-sizing-v1.json"
    assert json.loads(schema.read_bytes()) == SizingProposal.model_json_schema()
    # Original authored identity remains byte-for-byte compatible after additive CFD vocabulary.
    example = Path(__file__).parents[1] / "examples/phase0/strategy.json"
    if not example.is_file():
        example = Path(__file__).parents[1] / "examples/strategy.json"
    value = StrategyDocument.model_validate_json(example.read_bytes())
    assert (
        json.loads(value.model_dump_json())["formal_spec"]
        == json.loads(example.read_bytes())["formal_spec"]
    )
    assert fingerprint(value.formal_spec) != fingerprint(document().formal_spec)
