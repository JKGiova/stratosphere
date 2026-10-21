"""Read-only account projections retain exact economics and all diagnostic rows."""

import asyncio
import json
import sqlite3
from datetime import UTC, datetime
from decimal import localcontext

import pytest
from test_paper_decisions import activate, evaluate
from test_paper_decisions import decision as decision
from test_paper_runtime import stream as stream

from stratosphere.paper.inspection import JournalInspector, JournalView
from stratosphere.paper.ledger import Fill, Intent, PaperError

AT = datetime(2026, 10, 21, tzinfo=UTC)


async def online():
    pass


@pytest.fixture
def inspector(decision):
    return JournalInspector(decision[0].ledger.path, online_check=online, clock=lambda: AT)


async def test_snapshot_does_not_create_or_modify_account_and_has_no_mark(
    inspector, decision, tmp_path
):
    missing = tmp_path / "absent" / "account.sqlite3"
    reader = JournalInspector(missing, online_check=online)
    with pytest.raises(PaperError, match="no account was created"):
        await reader.capture()
    assert not missing.parent.exists()
    ledger = decision[0].ledger
    before = ledger.path.read_bytes()
    view = await inspector.capture()
    assert ledger.path.read_bytes() == before
    assert view.summary["settled_cash"] == "1000" and view.summary["held_base"] == "0"
    assert view.summary["reserved_cash"] == "0" and not view.orders and not view.fills
    assert view.positions[0]["cash"] == "500" and view.positions[0]["base"] == "0"
    assert not view.execution_eligible and "unavailable" in view.summary["valuation"]
    assert view.captured_at == AT
    assert JournalView.model_validate_json(view.model_dump_json()) == view
    assert "provider account, feed and execution are unverified" in view.markdown("positions")


async def test_partial_fills_cancellation_ambiguity_terminal_and_late_economics(
    inspector, decision
):
    service, _, _, _, now = activate(decision)
    intent = Intent.model_validate(evaluate(decision)["intent"])
    service.begin_delivery(intent.intent_id, now_ns=now)
    ledger = service.ledger
    fill = Fill(
        event_id="a" * 64,
        intent_id=intent.intent_id,
        quantity="0.004",
        price="101",
        notional="0.40399999",
        fee="0.0004",
    )
    assert ledger.observe_fill(fill)
    ledger.cancel_request(intent.intent_id)
    ledger.mark_ambiguous(intent.intent_id)
    before = ledger.path.read_bytes()
    with localcontext() as context:
        context.prec = 6
        view = await inspector.capture()
    assert ledger.path.read_bytes() == before
    assert view.summary["settled_cash"] == "999.59560001"
    assert view.summary["held_base"] == "0.004"
    assert view.positions[0]["cash"] == "499.59560001"
    order = view.orders[0]
    assert order["status"] == "ambiguous" and order["cancellation_requested"] is True
    assert order["remaining_quantity"] == "0.006" and order["reserved_cash"] == "0.60661"
    assert order["filled_notional"] == "0.40399999" and order["fees"] == "0.0004"
    assert view.summary["reconciliation_required"] and view.fills == (fill.model_dump(),)
    assert ledger.settle(intent.intent_id, "canceled", confirmed_filled="0.004")
    terminal = await inspector.capture()
    assert terminal.orders[0]["status"] == "canceled"
    assert terminal.summary["reserved_cash"] == "0" and terminal.summary["held_base"] == "0.004"
    late = fill.model_copy(
        update={"event_id": "b" * 64, "quantity": "0.001", "notional": "0.101", "fee": "0.0001"}
    )
    assert not ledger.observe_fill(late)
    final = await inspector.capture()
    assert final.summary["settled_cash"] == "999.49450001" and final.summary["held_base"] == "0.005"
    assert final.fills == (fill.model_dump(), late.model_dump())
    assert final.summary["reconciliation_required"]
    assert any(row.get("reason") == "observed_execution_breach" for row in final.health)


async def test_all_fill_rows_and_denied_decisions_are_accessible_without_truncation(
    inspector, decision
):
    ledger = decision[0].ledger
    _, _, signal, _, now = decision
    template = Intent(
        intent_id="1" * 32,
        instance_id=decision[1].streams[0].instance_id,
        side="BUY",
        quantity="0.01",
        price_bound="101",
        fee_allowance="0.00101",
        created_ns=now,
        expires_ns=now + 100,
        quote_ns=now,
        signal_sha256="d" * 64,
        proposal_sha256="e" * 64,
    )
    assert ledger.reserve(template, now_ns=now) is None
    ledger.begin_delivery(template.intent_id, now_ns=now)
    for index in range(25):
        fill = Fill(
            event_id=f"{index:064x}",
            intent_id=template.intent_id,
            quantity="0.0004",
            price="101",
            notional="0.0404",
            fee="0.00001",
        )
        assert ledger.observe_fill(fill)
    for index in range(25):
        denied = template.model_copy(update={"intent_id": f"{index + 100:032x}", "quantity": "2"})
        assert ledger.reserve(denied, now_ns=now) is not None
    before = ledger.path.read_bytes()
    view = await inspector.capture()
    first, pages = view.page("fills", 1)
    second, _ = view.page("fills", 2)
    assert pages == 2 and len(first) == 20 and len(second) == 5
    assert [row["event_id"] for row in first + second] == [f"{index:064x}" for index in range(25)]
    health = tuple(
        row for page in range(1, view.page("health")[1] + 1) for row in view.page("health", page)[0]
    )
    assert (
        health == view.health
        and len([row for row in health if row["kind"] == "denied_decision"]) == 25
    )
    assert "Page 2/2 · 25 total rows" in view.markdown("fills", 2)
    assert ledger.path.read_bytes() == before


async def test_snapshot_has_retained_stream_plan_limits_and_quotes_without_revaluation(
    inspector, decision
):
    activate(decision)
    ledger = decision[0].ledger
    ledger.block_entries()
    view = await inspector.capture()
    state = ledger.snapshot()
    plan = next(row for row in view.health if row["kind"] == "decision_plan")
    assert plan["plan"] == state["signal_plan"]["plan"]
    assert plan["peak_equity"] == state["signal_plan"]["peak_equity"]
    assert plan["risk_latches"] == state["signal_plan"]["risk_latches"]
    stream = next(row for row in view.health if row["kind"] == "closed_bar_stream")
    assert stream["accepted_count"] == 12 and stream["approved"]
    assert "last_authored_quote" in [row["kind"] for row in view.health]
    assert view.summary["quote_status_at_capture"] == "authored_stale_or_invalid"
    assert view.summary["entries_blocked"] and "unavailable" in view.summary["valuation"]


@pytest.mark.parametrize("number", [0, -1, True, 1.5, 2, 10**30])
async def test_invalid_pages_are_rejected(inspector, number):
    view = await inspector.capture()
    with pytest.raises(PaperError, match="Select a page"):
        view.page("positions", number)
    with pytest.raises(PaperError, match="Select positions"):
        view.page("unknown", 1)


@pytest.mark.parametrize("version", [1, 2, 3, 4])
async def test_versions_are_read_without_migration(inspector, version):
    with sqlite3.connect(inspector.journal) as database:
        database.execute(f"PRAGMA user_version={version}")
    before = inspector.journal.read_bytes()
    if version == 4:
        with pytest.raises(PaperError, match="Unsupported"):
            await inspector.capture()
    else:
        assert (await inspector.capture()).account.evidence == "authored_fixture_only"
    assert inspector.journal.read_bytes() == before


async def test_corrupt_journal_symlinks_and_offline_are_rejected(inspector, tmp_path):
    link = tmp_path / "alias"
    link.symlink_to(inspector.journal.parent, target_is_directory=True)
    inspector.journal = link / inspector.journal.name
    with pytest.raises(PaperError, match="No readable"):
        await inspector.capture()
    inspector.journal = inspector.journal.resolve()
    with sqlite3.connect(inspector.journal) as database:
        database.execute("DROP TRIGGER retain_update")
        database.execute("UPDATE events SET digest=? WHERE seq=1", ("f" * 64,))
    before = inspector.journal.read_bytes()
    with pytest.raises(PaperError, match="integrity"):
        await inspector.capture()
    assert inspector.journal.read_bytes() == before

    async def offline():
        raise PaperError("Offline inspection is unavailable")

    inspector.online_check = offline
    with pytest.raises(PaperError, match="Offline"):
        await inspector.capture()


@pytest.mark.parametrize("encoding", ["raw", "json_unicode"])
async def test_known_credentials_are_rejected_before_display(inspector, decision, encoding):
    secret = "inspection-secret-DO-NOT-DISPLAY"
    value = secret if encoding == "raw" else "".join(f"\\u{ord(char):04x}" for char in secret)
    ledger = decision[0].ledger
    with ledger._transaction() as database:
        _, chain = ledger._load(database)
        ledger._append(database, chain, {"kind": "block", "reason": value})
    inspector.secret_check = lambda data: secret.encode() in data
    before = ledger.path.read_bytes()
    with pytest.raises(PaperError, match="Credential-bearing") as error:
        await inspector.capture()
    assert secret not in str(error.value) and ledger.path.read_bytes() == before


async def test_restart_captures_exact_state_with_no_duplicate_or_mutation(inspector, decision):
    activate(decision)
    evaluate(decision)
    original = await inspector.capture()
    restarted = JournalInspector(inspector.journal, online_check=online, clock=lambda: AT)
    assert await restarted.capture() == original
    assert not asyncio.current_task().cancelling()
    assert json.loads(original.model_dump_json())["orders"][0]["status"] == "reserved"
