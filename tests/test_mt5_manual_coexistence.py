"""Manual exposure, native races and isolated owned exits through the real file journal."""

import asyncio
import hashlib
import json
import time
from pathlib import Path

import pytest
from test_mt5_bridge import connection, deployment, order
from test_mt5_overnight_basket import basket_fixture

from stratosphere.mt5.bridge import BridgeError, EntryBlocked, MT5Bridge, UncertainOrder, decode
from stratosphere.mt5.exposure import rows, snapshot
from stratosphere.mt5.overnight_basket import (
    CoexistingBasket,
    OvernightBasket,
    basket_reader,
    draft,
)
from stratosphere.mt5.trading import CoexistingDeployment, MT5Trading


def manual(symbol="SOLUSD", ticket="900", *, magic="0"):
    return {"ticket": ticket, "symbol": symbol, "magic": magic, "volume": "3.01", "type": "0"}


async def test_manual_positions_and_orders_survive_agent_entry_hold_and_owned_exit(tmp_path):
    async with basket_fixture(tmp_path, manual_coexistence=True) as (runner, peer, model, clock):
        original = manual()
        peer.positions = snapshot([original])
        peer.orders = snapshot([manual("XAUUSD", "901")])
        await runner.approve(runner.basket.digest)
        window = runner.reports["FRA40.cash"].windows[0]
        clock[0] = window.entry_start
        assert (await runner.cycle())["action"] == "submitted_and_reconciled"
        assert rows(peer.positions)[0] == original
        assert len(rows(peer.positions)) == 2 and peer.sent == 1
        # A manual trade on the same symbol AFTER entry must never be closed with the agent.
        extra = manual("FRA40.cash", "902")
        peer.positions = snapshot(rows(peer.positions) + [extra])
        assert (await runner.cycle())["action"] == "hold_owned_position"
        clock[0] = runner.basket.deployments[0].expires + 1
        assert (await runner.cycle())["action"] == "closed_owned_position"
        assert rows(peer.positions) == [original, extra]
        assert len(model.calls) == 2 and peer.sent == 2
        assert rows(peer.orders) == [manual("XAUUSD", "901")]
        assert not (runner.folder / "halt.json").exists()


@pytest.mark.parametrize("pending", [False, True])
async def test_external_same_symbol_defers_without_consuming_model_or_halting(tmp_path, pending):
    async with basket_fixture(tmp_path, manual_coexistence=True) as (runner, peer, model, clock):
        await runner.approve(runner.basket.digest)
        setattr(peer, "orders" if pending else "positions", snapshot([manual("FRA40.cash")]))
        window = runner.reports["FRA40.cash"].windows[0]
        clock[0] = window.entry_start
        assert (await runner.cycle())["action"] == "external_symbol_occupied"
        assert not model.calls and peer.sent == 0 and not (runner.folder / "halt.json").exists()
        setattr(peer, "orders" if pending else "positions", {"count": "0"})
        assert (await runner.cycle())["action"] == "submitted_and_reconciled"


@pytest.mark.parametrize(
    "free,equity", [("0", "100000"), ("-100", "100000"), ("9999", "100000"), ("100000", "99000")]
)
async def test_current_margin_and_equity_shortage_keeps_service_alive(tmp_path, free, equity):
    async with basket_fixture(tmp_path, manual_coexistence=True) as (runner, peer, model, clock):
        await runner.approve(runner.basket.digest)
        peer.account_updates.update(free_margin=free, equity=equity)
        clock[0] = runner.reports["FRA40.cash"].windows[0].entry_start
        result = await runner.cycle()
        assert result["action"] == "entry_deferred"
        assert not model.calls and peer.sent == 0
        assert not (runner.folder / "halt.json").exists()
        # Account recovery inside the SAME unconsumed window may proceed normally.
        peer.account_updates.update(free_margin="100000", equity="100000")
        assert (await runner.cycle())["action"] == "submitted_and_reconciled"


async def test_manual_margin_change_during_model_wait_does_not_replay_trigger(tmp_path):
    async with basket_fixture(tmp_path, manual_coexistence=True) as (runner, peer, model, clock):
        await runner.approve(runner.basket.digest)
        clock[0] = runner.reports["FRA40.cash"].windows[0].entry_start
        model.mutate = lambda: peer.account_updates.update(free_margin="9000")
        assert (await runner.cycle())["action"] == "entry_deferred"
        assert len(model.calls) == 2 and peer.sent == 0
        assert not runner.agents["FRA40.cash"].halted(runner.deployments["FRA40.cash"])
        peer.account_updates.update(free_margin="100000")
        assert (await runner.cycle())["action"] == "waiting_for_entry_window"
        assert len(model.calls) == 2


async def test_manual_same_symbol_change_during_model_wait_is_deferred(tmp_path):
    async with basket_fixture(tmp_path, manual_coexistence=True) as (runner, peer, model, clock):
        await runner.approve(runner.basket.digest)
        clock[0] = runner.reports["FRA40.cash"].windows[0].entry_start
        model.mutate = lambda: setattr(peer, "positions", snapshot([manual("FRA40.cash")]))
        assert (await runner.cycle())["action"] == "entry_deferred"
        assert peer.sent == 0 and rows(peer.positions) == [manual("FRA40.cash")]


async def test_second_agent_stays_blocked_only_by_owned_slot_not_manual_exposure(tmp_path):
    async with basket_fixture(tmp_path, manual_coexistence=True) as (runner, peer, model, clock):
        await runner.approve(runner.basket.digest)
        peer.positions = snapshot([manual()])
        clock[0] = runner.reports["FRA40.cash"].windows[0].entry_start
        await runner.cycle()
        clock[0] = next(
            w.entry_start for w in runner.reports["US500.cash"].windows if w.entry_start > clock[0]
        )
        result = await runner.cycle()
        assert result["action"] == "hold_owned_position"
        assert result["blocked_entry_symbols"] == ["US500.cash"]
        assert peer.sent == 1 and len(model.calls) == 2


async def test_unrelated_nonzero_magic_is_not_adopted_and_manual_close_is_forbidden(tmp_path):
    async with basket_fixture(tmp_path, manual_coexistence=True) as (runner, peer, _, clock):
        await runner.approve(runner.basket.digest)
        peer.positions = snapshot([manual("FRA40.cash", magic="777")])
        with pytest.raises(BridgeError, match="externally owned"):
            await MT5Trading(runner.bridge).close(runner.deployments["FRA40.cash"].digest, "900")
        assert peer.sent == 0
        # This case inspects ownership outside entry windows, regardless of the wall clock.
        clock[0] = min(w.entry_start for r in runner.reports.values() for w in r.windows) - 1
        assert (await runner.cycle())["action"] == "waiting_for_entry_window"


@pytest.mark.parametrize("mode", ["0", "1", "unknown"])
async def test_netting_or_unverified_account_cannot_approve_coexistence(tmp_path, mode):
    async with connection(tmp_path) as (bridge, peer):
        final = CoexistingDeployment.model_validate(
            deployment(bridge).model_dump(exclude={"schema_version", "limitations"})
        )
        peer.account_updates["margin_mode"] = mode
        with pytest.raises(BridgeError, match="hedging"):
            await MT5Trading(bridge).approve(final, final.digest)
        assert peer.sent == 0


async def test_uncertain_native_entry_with_manual_exposure_still_halts_no_retry(tmp_path):
    async with basket_fixture(tmp_path, manual_coexistence=True) as (runner, peer, model, clock):
        await runner.approve(runner.basket.digest)
        peer.positions = snapshot([manual()])
        peer.drop = True
        clock[0] = runner.reports["FRA40.cash"].windows[0].entry_start
        with pytest.raises(UncertainOrder):
            await runner.cycle()
        with pytest.raises(BridgeError, match="Unresolved native"):
            await runner.cycle()
        assert peer.sent == 1 and len(model.calls) == 2


async def test_native_known_margin_rejection_is_consumed_without_becoming_uncertain(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        final = CoexistingDeployment.model_validate(
            deployment(bridge).model_dump(exclude={"schema_version", "limitations"})
        )
        await MT5Trading(bridge).approve(final, final.digest)
        peer.drop = True
        # Retain a real, pinned wire receipt for a definite pre-submit native rejection.
        from stratosphere.mt5.bridge import atomic_write, encode

        async def native_reject():
            while True:
                for p in (peer.root / "requests").glob("*.tsv"):
                    request = decode(p.read_bytes())
                    if request["op"] != "submit":
                        continue
                    result = {
                        k: request[k]
                        for k in ("schema", "session", "login", "server", "mode", "id", "op")
                    }
                    result.update(status="error", entry_blocked="1", error="Insufficient margin")
                    atomic_write(peer.root / "responses" / p.name, encode(result))
                    return
                await asyncio.sleep(0.005)

        task = asyncio.create_task(native_reject())
        with pytest.raises(EntryBlocked):
            await MT5Trading(bridge).order(final.digest, order())
        await task
        with bridge.journal() as db:
            assert (
                db.execute("SELECT state FROM requests WHERE op='submit'").fetchone()[0]
                == "observed"
            )
        with pytest.raises(BridgeError, match="replayed"):
            await MT5Trading(bridge).order(final.digest, order())


def test_versioned_contracts_preserve_v1_readers_and_publish_new_scope():
    root = Path(__file__).parents[1] / "schemas"
    for name, cls in (
        ("mt5-deployment-v2", CoexistingDeployment),
        ("mt5-overnight-basket-v2", CoexistingBasket),
    ):
        assert json.loads((root / (name + ".json")).read_bytes()) == cls.model_json_schema()


@pytest.mark.parametrize("used", [0, 1])
async def test_reviewed_replacement_preserves_halts_and_remaining_budgets(
    tmp_path, monkeypatch, used
):
    async with basket_fixture(tmp_path, max_orders=2) as (old, peer, model, clock):
        await old.approve(old.basket.digest)
        if used:
            model.reliable = False
            clock[0] = old.reports["FRA40.cash"].windows[0].entry_start
            assert (await old.cycle())["action"] == "strategy_agent_rejected"
            clock[0] = int(time.time())
        old.halt("Historical external exposure failure")
        for d in old.basket.deployments:
            assert (old.agents[d.symbol].folder / "halt.json").is_file()
        # Model an application upgrade; real deployments use independently frozen source trees.
        old_digest = old.bridge.implementation_digest
        monkeypatch.setattr(
            MT5Bridge,
            "implementation_digest",
            property(lambda self: hashlib.sha256((old_digest + "upgrade").encode()).hexdigest()),
        )
        peer.positions = snapshot([manual()])
        prepared = await draft(
            old.bridge,
            old.database,
            {},
            provider_factory=lambda: factory(model),
            previous=old.basket,
        )
        basket = basket_reader.validate_python(prepared["basket"])
        assert basket.previous_usage["FRA40.cash"] == used
        assert (
            next(d.max_orders for d in basket.deployments if d.symbol == "FRA40.cash") == 2 - used
        )
        assert all(d.expires == old.basket.deployments[0].expires for d in basket.deployments)
        new = OvernightBasket(
            old.bridge,
            old.database,
            basket,
            provider_factory=lambda: factory(model),
            clock=lambda: clock[0],
        )
        with pytest.raises(BridgeError, match="approval required"):
            await new.recover(basket.digest)
        await new.approve(basket.digest)
        with pytest.raises(BridgeError, match="recovery has not completed"):
            await new.serve()
        await new.recover(basket.digest)
        with new.bridge.journal() as db:
            assert all(
                db.execute("SELECT state FROM approvals WHERE digest=?", (d.digest,)).fetchone()[0]
                == "revoked"
                for d in old.basket.deployments
            )
        assert (old.folder / "halt.json").is_file()
        for d in basket.deployments:
            assert not new.agents[d.symbol].halted(d)
        clock[0] = min(r.windows[0].entry_start for r in new.reports.values()) - 60
        assert (await new.cycle())["action"] == "waiting_for_entry_window"
        new.halt("A distinct new fault")
        assert all(new.agents[d.symbol].halted(d) for d in basket.deployments)
        assert peer.sent == 0


async def test_manual_different_symbol_arriving_during_model_wait_does_not_halt(tmp_path):
    async with basket_fixture(tmp_path, manual_coexistence=True) as (runner, peer, model, clock):
        await runner.approve(runner.basket.digest)
        clock[0] = runner.reports["FRA40.cash"].windows[0].entry_start
        model.mutate = lambda: setattr(peer, "positions", snapshot([manual()]))
        assert (await runner.cycle())["action"] == "submitted_and_reconciled"
        assert rows(peer.positions)[0] == manual() and peer.sent == 1


async def factory(model):
    return model
