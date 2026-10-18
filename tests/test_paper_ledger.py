"""Durable authored-paper economics, not a Liquid integration."""

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from decimal import ROUND_HALF_UP, Decimal

import pytest
from pydantic import ValidationError

from stratosphere.paper.ledger import Account, Fill, Instance, Intent, PaperError, PaperLedger
from stratosphere.registry.workspace import WorkspaceDatabase


@pytest.fixture
def paper(tmp_path, research_plan):
    registry = WorkspaceDatabase(tmp_path / "workspace.sqlite3")
    strategy = registry.register(research_plan.specification, "Paper fixture", {"text": "fixture"})
    account = Account(account_id="local-paper-" + "a" * 32, starting_cash="1000")
    ledger = PaperLedger(tmp_path / "paper" / "account.sqlite3", account)
    instance = Instance(
        instance_id="b" * 32,
        strategy_id=strategy["public_id"],
        strategy_sha256=strategy["spec_sha256"],
        implementation_sha256="c" * 64,
        account_id=account.account_id,
        capital="1000",
        max_order_notional="1000",
        max_base_quantity="10",
        max_quote_age_ns=20,
    )
    ledger.add_instance(instance, registry)
    return ledger, account, instance, registry


def intent(instance, number=1, **changes):
    data = dict(
        intent_id=f"{number:032x}",
        instance_id=instance.instance_id,
        side="BUY",
        quantity="7",
        price_bound="100",
        fee_allowance="0",
        created_ns=100,
        expires_ns=120,
        quote_ns=99,
        signal_sha256="d" * 64,
        proposal_sha256="e" * 64,
    )
    return Intent(**(data | changes))


def fill(order, quantity="4", price="100", fee="0", event="fill-1"):
    return Fill(
        event_id=hashlib.sha256(event.encode()).hexdigest(),
        intent_id=order.intent_id,
        quantity=quantity,
        price=price,
        notional=format(
            (Decimal(quantity) * Decimal(price)).quantize(
                Decimal("0.00000001"), rounding=ROUND_HALF_UP
            ),
            "f",
        ),
        fee=fee,
    )


def test_restart_pins_idempotency_and_one_delivery_claim(paper):
    ledger, account, instance, registry = paper
    order = intent(instance)
    assert ledger.reserve(order, now_ns=100) is None
    before = ledger.snapshot()
    assert ledger.reserve(order, now_ns=110) is None and ledger.snapshot() == before
    assert before["reserved_cash"] == "700"
    reopened = PaperLedger(ledger.path, account)
    assert reopened.snapshot() == before
    reopened.add_instance(instance, registry)
    with pytest.raises(PaperError, match="cannot change"):
        reopened.add_instance(instance.model_copy(update={"capital": "999"}), registry)
    with pytest.raises(PaperError, match="idempotency"):
        reopened.reserve(intent(instance, quantity="6"), now_ns=100)
    reopened.begin_delivery(order.intent_id, now_ns=100)
    with pytest.raises(PaperError, match="already claimed"):
        PaperLedger(ledger.path, account).begin_delivery(order.intent_id, now_ns=101)
    assert reopened.reserve(intent(instance, 2), now_ns=101) == "reconciliation_required"
    assert ledger.path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "patch,now,reason",
    [
        ({}, 120, "stale_or_expired"),
        ({}, 98, "stale_or_expired"),
        ({"quote_ns": 70}, 100, "stale_or_expired"),
        ({"quantity": "11"}, 100, "instance_size_limit"),
        ({"price_bound": "150"}, 100, "instance_size_limit"),
        ({"quantity": "10", "fee_allowance": "1"}, 100, "insufficient_unreserved_cash"),
        ({"side": "SELL"}, 100, "insufficient_attributed_position"),
    ],
)
def test_denied_intents_are_retained_and_cannot_revive_on_retry(paper, patch, now, reason):
    ledger, _, instance, _ = paper
    order = intent(instance, **patch)
    assert ledger.reserve(order, now_ns=now) == reason
    state = ledger.snapshot()
    assert state["reserved_cash"] == "0" and not state["orders"]
    assert state["decisions"][order.intent_id]["reason"] == reason
    assert ledger.reserve(order, now_ns=100) == reason


def test_partial_cancel_reservations_attributed_exposure_and_entry_block(paper):
    ledger, account, instance, _ = paper
    order = intent(instance)
    ledger.reserve(order, now_ns=100)
    ledger.begin_delivery(order.intent_id, now_ns=100)
    assert ledger.observe_fill(fill(order))
    state = ledger.snapshot()
    assert state["cash"] == "600" and state["reserved_cash"] == "300"
    assert state["instances"][instance.instance_id]["base"] == "4"
    ledger.cancel_request(order.intent_id)
    assert ledger.snapshot()["reserved_cash"] == "300"
    assert ledger.settle(order.intent_id, "canceled", confirmed_filled="4")
    assert ledger.snapshot()["reserved_cash"] == "0"
    assert ledger.reserve(intent(instance, 2), now_ns=100) == "reinforcement_not_supported"
    ledger.block_entries()
    exit_order = intent(instance, 3, side="SELL", quantity="4")
    assert ledger.reserve(exit_order, now_ns=100) is None
    ledger.begin_delivery(exit_order.intent_id, now_ns=100)
    assert ledger.observe_fill(fill(exit_order, "4", "101", event="exit"))
    state = ledger.snapshot()
    assert state["cash"] == "1004" and state["instances"][instance.instance_id]["base"] == "0"
    assert PaperLedger(ledger.path, account).snapshot() == state


def test_fee_cost_and_fill_deduplication_are_exact(paper):
    ledger, _, instance, _ = paper
    order = intent(instance, quantity="0.7", price_bound="101", fee_allowance="0.03")
    ledger.reserve(order, now_ns=100)
    ledger.begin_delivery(order.intent_id, now_ns=100)
    observation = fill(order, "0.4", "100.12345678", "0.01")
    assert ledger.observe_fill(observation) and ledger.observe_fill(observation)
    state = ledger.snapshot()
    assert state["cash"] == "959.94061729" and state["reserved_cash"] == "30.32"
    assert len(state["fills"]) == 1
    assert not ledger.observe_fill(fill(order, "0.5", event="fill-1"))
    state = ledger.snapshot()
    assert state["cash"] == "959.94061729" and state["blocks"] == ["conflicting_fill_event"]


@pytest.mark.parametrize(
    "quantity,price,fee", [("8", "100", "0"), ("4", "101", "0"), ("4", "100", "1")]
)
def test_breaches_preserve_observed_economics_and_block_actions(paper, quantity, price, fee):
    ledger, _, instance, _ = paper
    order = intent(instance)
    ledger.reserve(order, now_ns=100)
    ledger.begin_delivery(order.intent_id, now_ns=100)
    assert not ledger.observe_fill(fill(order, quantity, price, fee))
    state = ledger.snapshot()
    assert state["instances"][instance.instance_id]["base"] == quantity
    assert state["blocks"] == ["observed_execution_breach"]
    assert ledger.reserve(intent(instance, 2), now_ns=100) == "reconciliation_required"


def test_ambiguous_reconciliation_requires_missing_fills_and_cancel_preserves_it(paper):
    ledger, _, instance, _ = paper
    order = intent(instance)
    ledger.reserve(order, now_ns=100)
    ledger.begin_delivery(order.intent_id, now_ns=100)
    ledger.mark_ambiguous(order.intent_id)
    ledger.cancel_request(order.intent_id)
    assert ledger.snapshot()["orders"][order.intent_id]["status"] == "ambiguous"
    assert not ledger.settle(order.intent_id, "canceled", confirmed_filled="4")
    assert ledger.snapshot()["reserved_cash"] == "700"
    assert ledger.observe_fill(fill(order))
    assert ledger.snapshot()["orders"][order.intent_id]["status"] == "ambiguous"
    assert ledger.settle(order.intent_id, "canceled", confirmed_filled="4")
    assert ledger.snapshot()["reserved_cash"] == "0"


@pytest.mark.parametrize("now,control", [(120, None), (100, "block_entries"), (100, "cancel")])
def test_delivery_rechecks_expiry_entry_block_and_pending_cancel(paper, now, control):
    ledger, _, instance, _ = paper
    order = intent(instance)
    ledger.reserve(order, now_ns=100)
    if control == "block_entries":
        ledger.block_entries()
    if control == "cancel":
        ledger.cancel_request(order.intent_id)
    with pytest.raises(PaperError):
        ledger.begin_delivery(order.intent_id, now_ns=now)
    assert ledger.snapshot()["reserved_cash"] == "700"


def test_concurrent_clients_cannot_reserve_same_capital(paper):
    ledger, account, instance, _ = paper
    other = PaperLedger(ledger.path, account)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda pair: pair[0].reserve(intent(instance, pair[1]), now_ns=100),
                [(ledger, 1), (other, 2)],
            )
        )
    assert results.count(None) == 1 and results.count("instance_has_pending_order") == 1
    assert ledger.snapshot()["reserved_cash"] == "700"


def test_allocation_and_strategy_pins_cannot_be_replaced(paper):
    ledger, account, instance, registry = paper
    with pytest.raises(PaperError, match="allocation"):
        ledger.add_instance(instance.model_copy(update={"instance_id": "f" * 32}), registry)
    with pytest.raises(PaperError, match="registered strategy"):
        ledger.add_instance(instance.model_copy(update={"strategy_sha256": "f" * 64}), registry)
    with pytest.raises(PaperError, match="re-funded"):
        PaperLedger(ledger.path, account.model_copy(update={"starting_cash": "2000"}))


def test_journal_mutation_is_blocked_and_corruption_is_retained(paper):
    ledger, account, instance, _ = paper
    ledger.reserve(intent(instance), now_ns=100)
    with sqlite3.connect(ledger.path) as db:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("DELETE FROM events")
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("UPDATE events SET payload='{}'")
        db.execute("DROP TRIGGER retain_update")
        db.execute("UPDATE events SET payload=? WHERE seq=1", (json.dumps({"kind": "damaged"}),))
    with pytest.raises(PaperError, match="integrity"):
        PaperLedger(ledger.path, account)


def test_account_is_authored_only_and_amounts_cannot_be_floats():
    with pytest.raises(ValidationError):
        Account(account_id="liquid-account", starting_cash="1000")
    with pytest.raises(ValidationError):
        Account(account_id="local-paper-" + "a" * 32, starting_cash=1000.0)


def test_observed_currency_rounding_cannot_expand_an_order_budget(paper):
    ledger, _, instance, _ = paper
    order = intent(instance, quantity="1", price_bound="0.00000001")
    ledger.reserve(order, now_ns=100)
    ledger.begin_delivery(order.intent_id, now_ns=100)
    # Native Money rounds this half-quantum fill up; remaining quantity needs
    # another quantum. Retain the actual debit and freeze rather than expand.
    observation = fill(order, quantity="0.5", price="0.00000001")
    assert not ledger.observe_fill(observation)
    state = ledger.snapshot()
    assert state["cash"] == "999.99999999" and state["reserved_cash"] == "0.00000001"
    assert state["blocks"] == ["observed_execution_breach"]


def test_conflicting_terminal_observation_preserves_uncertainty(paper):
    ledger, _, instance, _ = paper
    order = intent(instance)
    ledger.reserve(order, now_ns=100)
    ledger.begin_delivery(order.intent_id, now_ns=100)
    assert ledger.settle(order.intent_id, "canceled", confirmed_filled="0")
    assert not ledger.settle(order.intent_id, "expired", confirmed_filled="0")
    assert ledger.snapshot()["orders"][order.intent_id]["status"] == "ambiguous"
    assert ledger.reserve(intent(instance, 2), now_ns=100) == "reconciliation_required"


def test_other_strategy_cannot_close_an_attributed_position(paper, tmp_path, research_plan):
    from stratosphere.phase0.contracts import StrategySpec

    ledger, account, original, registry = paper
    other_spec = StrategySpec.model_validate(
        research_plan.specification.model_dump()
        | {"position_sizing": {"mode": "fixed_base", "amount": "0.02"}}
    )
    other_strategy = registry.register(other_spec, "Other strategy", {"text": "other fixture"})
    other_account = account.model_copy(update={"account_id": "local-paper-" + "f" * 32})
    shared = PaperLedger(tmp_path / "shared" / "paper.sqlite3", other_account)
    first = Instance.model_validate(
        original.model_dump() | {"account_id": other_account.account_id, "capital": "500"}
    )
    second = Instance.model_validate(
        first.model_dump()
        | {
            "instance_id": "c" * 32,
            "strategy_id": other_strategy["public_id"],
            "strategy_sha256": other_strategy["spec_sha256"],
        }
    )
    shared.add_instance(first, registry)
    shared.add_instance(second, registry)
    opening = intent(first, quantity="1")
    shared.reserve(opening, now_ns=100)
    shared.begin_delivery(opening.intent_id, now_ns=100)
    assert shared.observe_fill(fill(opening, "1"))
    assert (
        shared.reserve(intent(second, 2, side="SELL", quantity="1"), now_ns=100)
        == "insufficient_attributed_position"
    )
    assert shared.snapshot()["instances"][first.instance_id]["base"] == "1"
    assert shared.snapshot()["instances"][second.instance_id]["base"] == "0"
    assert shared.snapshot()["cash"] == "900"


@pytest.mark.parametrize("status", ["expired", "rejected"])
def test_confirmed_zero_fill_terminal_state_releases_reservation(paper, status):
    ledger, account, instance, _ = paper
    order = intent(instance)
    ledger.reserve(order, now_ns=100)
    ledger.begin_delivery(order.intent_id, now_ns=100)
    assert ledger.settle(order.intent_id, status, confirmed_filled="0")
    assert PaperLedger(ledger.path, account).snapshot()["reserved_cash"] == "0"


def test_late_fill_is_retained_after_cancellation_and_blocks_new_actions(paper):
    ledger, _, instance, _ = paper
    order = intent(instance)
    ledger.reserve(order, now_ns=100)
    ledger.begin_delivery(order.intent_id, now_ns=100)
    ledger.settle(order.intent_id, "canceled", confirmed_filled="0")
    assert not ledger.observe_fill(fill(order))
    state = ledger.snapshot()
    assert state["cash"] == "600" and state["instances"][instance.instance_id]["base"] == "4"
    assert state["blocks"] == ["observed_execution_breach"]


def test_unvalidated_model_copy_cannot_write_unsupported_account(paper, tmp_path):
    _, account, _, _ = paper
    path = tmp_path / "bad" / "paper.sqlite3"
    with pytest.raises(ValidationError):
        PaperLedger(path, account.model_copy(update={"market": "BTCUSDT.LIQUID"}))
    assert not path.exists()


def test_symlink_journal_cannot_modify_another_file(paper, tmp_path):
    _, account, _, _ = paper
    target = tmp_path / "sentinel"
    target.write_text("preserve")
    link = tmp_path / "linked.sqlite3"
    link.symlink_to(target)
    with pytest.raises(PaperError, match="symbolic"):
        PaperLedger(link, account)
    assert target.read_text() == "preserve"
