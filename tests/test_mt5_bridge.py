"""Authored EA peer tests; these are not broker-order acceptance or FTMO certification."""

import asyncio
import time
from contextlib import asynccontextmanager

import pytest

from stratosphere.mt5.bridge import (
    SCHEMA,
    BridgeError,
    MT5Bridge,
    UncertainOrder,
    atomic_write,
    decode,
    encode,
)
from stratosphere.mt5.trading import Deployment, MT5Trading, Order


class Peer:
    def __init__(self, root):
        self.root = root
        for name in ("requests", "responses", "approvals"):
            (root / name).mkdir()
        self.session = dict(
            schema=SCHEMA,
            session="123-456",
            login="12345",
            server="FTMO-Demo",
            mode="demo",
            ea_sha256="a" * 64,
        )
        atomic_write(root / "session.tsv", encode(self.session))
        self.seen = set()
        self.drop = False
        self.mismatch = False
        self.retcode = "10009"
        self.positions = {}
        self.sent = 0

    async def serve(self):
        while True:
            for path in (self.root / "requests").glob("*.tsv"):
                if path.name in self.seen:
                    continue
                self.seen.add(path.name)
                request = decode(path.read_bytes())
                result = {
                    key: request[key]
                    for key in ("schema", "session", "login", "server", "mode", "id", "op")
                }
                result.update(status="ok")
                if self.mismatch:
                    result["login"] = "999"
                op = request["op"]
                if op == "account":
                    result.update(
                        connected="1",
                        currency="USD",
                        equity="100000",
                        balance="100000",
                        free_margin="100000",
                        margin="0",
                        ea_sha256="a" * 64,
                    )
                elif op in {"positions", "orders"}:
                    result.update(self.positions if op == "positions" else {"count": "0"})
                    result.setdefault("count", "0")
                elif op == "quote":
                    result.update(bid="4000", ask="4000.1", server_time_ms="100000", age_ms="0")
                elif op == "rates":
                    result.update(
                        count="1",
                        **{
                            "b.1.time": "100",
                            "b.1.open": "10",
                            "b.1.high": "12",
                            "b.1.low": "9",
                            "b.1.close": "11",
                            "b.1.tick_volume": "1",
                            "b.1.spread": "2",
                        },
                    )
                elif op == "check":
                    result["check_retcode"] = "0"
                elif op in {"submit", "close"}:
                    self.sent += 1
                    if self.drop:
                        continue
                    result.update(
                        retcode=self.retcode,
                        filled_volume=request.get("volume", "0.01"),
                        order="123",
                        deal="124",
                        fill_price="4000",
                    )
                    self.positions = (
                        {
                            "count": "1",
                            "p.1.ticket": "123",
                            "p.1.magic": request["magic"],
                            "p.1.symbol": request["symbol"],
                            "p.1.volume": request["volume"],
                        }
                        if op == "submit"
                        else {"count": "0"}
                    )
                atomic_write(self.root / "responses" / path.name, encode(result))
            await asyncio.sleep(0.005)


@asynccontextmanager
async def connection(tmp_path):
    peer = Peer(tmp_path)
    bridge = MT5Bridge(tmp_path, timeout=0.2, state_root=tmp_path / "state")
    task = asyncio.create_task(peer.serve())
    try:
        yield bridge, peer
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def deployment(bridge, **changes):
    fields = dict(
        strategy_id="1" * 24,
        strategy_sha256=bridge.retain_evidence(b"authored complete strategy fixture"),
        report_sha256=bridge.retain_evidence(b"authored report fixture; not provider evidence"),
        implementation_sha256=bridge.implementation_digest,
        ea_sha256="a" * 64,
        login="12345",
        server="FTMO-Demo",
        mode="demo",
        currency="USD",
        symbol="XAUUSD",
        max_volume="0.01",
        max_order_risk="25",
        max_order_margin="100",
        max_spread="1",
        min_equity="99500",
        cost_buffer="2",
        max_orders=5,
        expires=int(time.time()) + 600,
    )
    fields.update(changes)
    return Deployment(**fields)


def order(**changes):
    fields = dict(signal_sha256="d" * 64, side="buy", volume="0.01", stop_loss="3990")
    fields.update(changes)
    return Order(**fields)


async def test_actual_file_transport_reads_and_retains_pinned_receipts(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        assert (await bridge.account())["currency"] == "USD"
        assert (await bridge.read("quote", symbol="XAUUSD"))["bid"] == "4000"
        assert (await bridge.read("rates", symbol="XAUUSD", frame="PERIOD_H1", count=1))[
            "count"
        ] == "1"
        with bridge.journal() as db:
            assert (
                db.execute("SELECT count(*) FROM requests WHERE state='observed'").fetchone()[0]
                == 3
            )
        assert peer.sent == 0


async def test_changed_account_and_mismatched_response_are_rejected(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        await bridge.account()
        peer.mismatch = True
        with pytest.raises(BridgeError, match="mismatch"):
            await bridge.account()
        peer.session["login"] = "98765"
        atomic_write(tmp_path / "session.tsv", encode(peer.session))
        with pytest.raises(BridgeError, match="account changed"):
            await bridge.account()


async def test_read_interface_and_protocol_pins_cannot_submit_or_be_overridden(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        with pytest.raises(BridgeError):
            await bridge.read("submit")
        with pytest.raises(BridgeError):
            await bridge._request("account", {"login": "99999"})
        for frame, count in (("PERIOD_CURRENT", 10), ("PERIOD_H1", 100001)):
            with pytest.raises(BridgeError):
                await bridge.read("rates", symbol="XAUUSD", frame=frame, count=count)
        assert peer.sent == 0


async def test_one_exact_approval_check_submit_and_owned_close(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        trading = MT5Trading(bridge)
        final = deployment(bridge)
        with pytest.raises(BridgeError, match="user-approved"):
            await trading.order(final.digest, order())
        with pytest.raises(BridgeError, match="exact"):
            await trading.approve(final, "0" * 64)
        await trading.approve(final, final.digest)
        await trading.order(final.digest, order(), check_only=True)
        assert peer.sent == 0
        await trading.order(final.digest, order())
        assert peer.sent == 1
        with pytest.raises(BridgeError, match="replayed"):
            await trading.order(final.digest, order())
        await trading.revoke(final.digest)
        with pytest.raises(BridgeError, match="user-approved"):
            await trading.order(final.digest, order(signal_sha256="e" * 64))
        await trading.close(final.digest, "123")
        assert peer.sent == 2 and peer.positions == {"count": "0"}


async def test_uncertain_submission_halts_across_restart_and_never_replays(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        trading = MT5Trading(bridge)
        final = deployment(bridge)
        await trading.approve(final, final.digest)
        peer.drop = True
        with pytest.raises(UncertainOrder):
            await trading.order(final.digest, order())
        reopened = MT5Trading(MT5Bridge(tmp_path, timeout=0.2, state_root=tmp_path / "state"))
        with pytest.raises(BridgeError, match="unresolved"):
            await reopened.order(final.digest, order(signal_sha256="e" * 64))
        assert peer.sent == 1
        with bridge.journal() as db:
            assert (
                db.execute("SELECT state FROM requests WHERE op='submit'").fetchone()[0]
                == "uncertain"
            )


async def test_broker_acknowledgment_is_not_a_confirmed_fill(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        trading = MT5Trading(bridge)
        final = deployment(bridge)
        await trading.approve(final, final.digest)
        peer.retcode = "10008"
        with pytest.raises(UncertainOrder, match="reconciliation"):
            await trading.order(final.digest, order())


async def test_approval_expiry_changed_code_account_and_real_activation_are_gated(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        trading = MT5Trading(bridge)
        for changes in (
            {"expires": 1},
            {"implementation_sha256": "0" * 64},
            {"login": "99999"},
            {"ea_sha256": "f" * 64},
        ):
            final = deployment(bridge, **changes)
            with pytest.raises(BridgeError):
                await trading.approve(final, final.digest)
        real = deployment(bridge, mode="real")
        with pytest.raises(BridgeError, match="allow-real"):
            await trading.approve(real, real.digest)
        assert peer.sent == 0


def test_wire_and_data_reject_duplicates_corrupt_bars_and_symlink_paths(tmp_path):
    with pytest.raises(BridgeError):
        decode(b"a\tb\na\tc\n")
    with pytest.raises(BridgeError):
        encode({"symbol": "XAUUSD\nsubmit"})
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(BridgeError, match="symbolic"):
        MT5Bridge(link)
    with pytest.raises(ValueError):
        MT5Bridge.validate_data("quote", {"ask": "NaN", "bid": "1"}, {})
    with pytest.raises(ValueError):
        MT5Bridge.validate_data("rates", {"count": "100001"}, {"count": 100000})


async def test_account_lease_excludes_competing_clients(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        first, second = (
            MT5Trading(bridge),
            MT5Trading(MT5Bridge(tmp_path, state_root=tmp_path / "state")),
        )
        with first.lease(), pytest.raises(BridgeError, match="lease"):
            with second.lease():
                pytest.fail("Competing trading client acquired account lease")


async def test_same_account_in_another_terminal_shares_lease_and_approval_state(tmp_path):
    first_root, second_root = tmp_path / "first", tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    Peer(first_root)
    Peer(second_root)
    first = MT5Bridge(first_root, state_root=tmp_path / "canonical")
    second = MT5Bridge(second_root, state_root=tmp_path / "canonical")
    assert first.state_directory() == second.state_directory()
    with MT5Trading(first).lease(), pytest.raises(BridgeError, match="lease"):
        with MT5Trading(second).lease():
            pytest.fail("Another terminal bypassed the account lease")
    with first.journal() as db:
        db.execute(
            "INSERT INTO requests VALUES ('test','submit','account',?,'uncertain',NULL)",
            (b"original intent",),
        )
    with second.journal() as db:
        assert db.execute("SELECT state FROM requests WHERE id='test'").fetchone()[0] == "uncertain"


@pytest.mark.parametrize("damage", ["missing_table", "version", "empty"])
async def test_damaged_journal_cannot_clear_uncertainty_or_recreate_authority(tmp_path, damage):
    async with connection(tmp_path) as (bridge, peer):
        await bridge.account()
        path = bridge.state_directory() / "client.sqlite3"
        if damage == "empty":
            path.write_bytes(b"")
        else:
            with bridge.journal() as db:
                db.execute(
                    "DROP TABLE requests" if damage == "missing_table" else "PRAGMA user_version=0"
                )
        before = path.read_bytes()
        with pytest.raises(BridgeError, match="journal"):
            bridge.journal()
        assert path.read_bytes() == before
        assert peer.sent == 0


async def test_changed_retained_evidence_cannot_approve_or_submit(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        final = deployment(bridge)
        path = bridge.state_directory() / "evidence" / (final.report_sha256 + ".bin")
        path.write_bytes(b"changed report")
        with pytest.raises(BridgeError, match="evidence"):
            await MT5Trading(bridge).approve(final, final.digest)
        assert peer.sent == 0


async def test_uncertain_close_cannot_be_reissued_with_a_new_request_id(tmp_path):
    async with connection(tmp_path) as (bridge, peer):
        trading = MT5Trading(bridge)
        final = deployment(bridge)
        await trading.approve(final, final.digest)
        await trading.order(final.digest, order())
        peer.drop = True
        with pytest.raises(UncertainOrder):
            await trading.close(final.digest, "123")
        with pytest.raises(BridgeError, match="Close outcome unresolved"):
            await trading.close(final.digest, "123")
        assert peer.sent == 2  # One entry and one uncertain close; no second close dispatch.
