"""Two private advisory agents, retained facts and no account mutation."""

import asyncio
import json
from datetime import UTC, datetime

import httpx
import pytest
from pydantic import SecretStr
from test_paper_decisions import activate, evaluate
from test_paper_decisions import decision as decision
from test_paper_runtime import stream as stream

from stratosphere.models.openrouter import OpenRouterChat
from stratosphere.models.settings import ModelSettings
from stratosphere.models.streaming import ModelError
from stratosphere.paper.ledger import PaperError
from stratosphere.paper.reviews import (
    PaperReviewService,
    agent_id,
    read_book,
    review_markdown,
    reviewer_digest,
)
from stratosphere.research.store import ResearchError, ResearchStore

AT = datetime(2026, 10, 19, tzinfo=UTC)


async def online():
    pass


class Provider:
    def __init__(self):
        self.calls = []
        self.mutate = None
        self.budget_change = None
        self.risk_change = None

    async def stream_json(self, messages, schema):
        self.calls.append(messages)
        body = json.loads(messages[1]["content"])
        if schema["title"] == "BudgetProposal":
            result = {
                "allocations": [
                    {
                        "instance_index": r["instance_index"],
                        "capital": r["capital_cap"],
                        "fact_indices": [0],
                    }
                    for r in body["rows"]
                ],
                "rationale": "Retain authored caps pending verification.",
                "questions": ["Which provider permissions are verified?"],
            }
            if self.budget_change:
                self.budget_change(result)
        else:
            rows = body["basis"]["rows"]
            result = {
                "findings": [
                    {
                        "instance_indices": [r["instance_index"] for r in rows],
                        "category": "missing_verification",
                        "rationale": "These authored observations do not verify executable quotes.",
                        "fact_indices": [0],
                    }
                ],
                "questions": [],
            }
            if self.risk_change:
                self.risk_change(result)
        if self.mutate:
            self.mutate(len(self.calls))
        yield json.dumps(result)


@pytest.fixture
def reviews(decision, tmp_path):
    service = decision[0]
    return PaperReviewService(
        service.ledger.path,
        ResearchStore(tmp_path / "paper-reviews"),
        online_check=online,
        clock=lambda: AT,
    ), Provider()


def test_read_only_snapshot_does_not_create_or_migrate_an_account(tmp_path, decision):
    missing = tmp_path / "missing" / "account.sqlite3"
    with pytest.raises(PaperError, match="No readable"):
        read_book(missing, at=AT)
    assert not missing.parent.exists()
    ledger = decision[0].ledger
    before = ledger.path.read_bytes()
    basis = read_book(ledger.path, at=AT)
    assert basis.account.starting_cash == "1000" and len(basis.rows) == 1
    assert basis.rows[0].capital_cap == "500" and not basis.rows[0].allocation_locked
    assert basis.quote_status == "unavailable" and not basis.execution_eligible
    assert ledger.path.read_bytes() == before
    assert [f.fact_index for f in basis.facts] == list(range(len(basis.facts)))


async def test_two_role_reports_are_bound_inspectable_and_leave_account_unchanged(
    reviews, decision
):
    service, provider = reviews
    ledger = decision[0].ledger
    before = ledger.path.read_bytes()
    value = await service.review(provider, "ollama:authored-test")
    assert ledger.path.read_bytes() == before and len(provider.calls) == 2
    assert value.budget_agent_id != value.risk_agent_id
    assert value.budget_agent_id == agent_id(value.basis.account.account_id, "budget")
    assert value.reviewer_sha256 == reviewer_digest()
    assert not value.state_changed_during_review
    assert service.store.state(value.id) == "completed" and service.retained(value.id) == value
    assert await service.open(value.id) == value and service.records() == [value]
    assert json.loads(provider.calls[1][1]["content"])[
        "budget_proposal"
    ] == value.budget.model_dump(mode="json")
    assert "advisory" in review_markdown(value).lower() and "unverified" in review_markdown(value)
    assert not ledger.snapshot()["orders"] and not ledger.snapshot()["signal_plan"]
    assert (service.store.directory(value.id) / "review.json").stat().st_mode & 0o777 == 0o600


async def test_retained_positions_lock_budget_and_old_quotes_are_explicitly_stale(
    reviews, decision
):
    ready = activate(decision)
    evaluate(ready)
    service, provider = reviews
    value = await service.review(provider, "fixture")
    assert value.basis.rows[0].allocation_locked and value.budget.allocations[0].capital == "500"
    assert value.basis.quote_status == "authored_stale_or_invalid"
    assert value.basis.risk_metrics["reserved_cash"] == "1.01101"
    assert value.basis.execution_eligible is False


def test_capture_before_retained_quote_receipt_cannot_label_it_recent(decision):
    service, _, _, quote, now = activate(decision)
    event = (now // 10**9 + 3) * 10**9
    later = quote.model_copy(
        update={"quote_id": "d" * 64, "event_ns": event, "available_ns": event + 1500000000}
    )
    assert service.observe_quote(later, received_ns=later.available_ns) is None
    early = read_book(service.ledger.path, at=datetime.fromtimestamp(event // 10**9 + 1, UTC))
    assert early.quote_status == "authored_stale_or_invalid"
    received = read_book(service.ledger.path, at=datetime.fromtimestamp(event // 10**9 + 2, UTC))
    assert received.quote_status == "authored_declared_recent"
    assert not received.execution_eligible


@pytest.mark.parametrize(
    "bad", ["unknown", "duplicate", "missing", "over_cap", "negative", "unknown_fact"]
)
async def test_invalid_budget_is_retained_as_failed_without_risk_call(reviews, bad):
    service, provider = reviews

    def change(value):
        row = value["allocations"][0]
        if bad == "unknown":
            row["instance_index"] = 999
        elif bad == "duplicate":
            value["allocations"].append(dict(row))
        elif bad == "missing":
            value["allocations"] = []
        elif bad == "over_cap":
            row["capital"] = "501"
        elif bad == "negative":
            row["capital"] = "-1"
        elif bad == "unknown_fact":
            row["fact_indices"] = [999]

    provider.budget_change = change
    with pytest.raises(ModelError):
        await service.review(provider, "fixture")
    (identity,) = service.store.jobs()
    assert service.store.state(identity) == "failed" and len(provider.calls) == 1
    assert not (service.store.directory(identity) / "review.json").exists()
    assert not service.records()


async def test_locked_allocation_cannot_be_changed_by_a_model(reviews, decision):
    activate(decision)
    evaluate(decision)
    service, provider = reviews
    provider.budget_change = lambda p: p["allocations"][0].update(capital="1")
    with pytest.raises(ModelError, match="locked"):
        await service.review(provider, "fixture")
    assert len(provider.calls) == 1


@pytest.mark.parametrize(
    "bad", ["unknown_instance", "unknown_fact", "omitted_instance", "approval", "bad_question"]
)
async def test_risk_references_and_no_approval_contract_are_enforced(reviews, bad):
    service, provider = reviews

    def change(value):
        if bad == "unknown_instance":
            value["findings"][0]["instance_indices"] = [999]
        elif bad == "unknown_fact":
            value["findings"][0]["fact_indices"] = [999]
        elif bad == "omitted_instance":
            value["findings"] = []
        elif bad == "approval":
            value["approved"] = True
        else:
            value["questions"] = ["not a question"]

    provider.risk_change = change
    with pytest.raises(ModelError):
        await service.review(provider, "fixture")
    (identity,) = service.store.jobs()
    assert (
        service.store.state(identity) == "failed"
        and (service.store.directory(identity) / "budget.json").is_file()
    )
    assert not service.records()


async def test_changed_journal_is_a_dated_snapshot_not_an_approval(reviews, decision):
    service, provider = reviews
    provider.mutate = lambda count: decision[0].ledger.block_entries() if count == 1 else None
    value = await service.review(provider, "fixture")
    assert value.state_changed_during_review
    assert value.basis.journal_sha256 != value.journal_at_finish_sha256
    assert value.execution == "advisory_only_no_approval_or_mutation"
    assert (
        json.loads(provider.calls[1][1]["content"])["basis"]["journal_sha256"]
        == value.basis.journal_sha256
    )


async def test_cancellation_preserves_attempt_and_budget_without_a_completed_report(reviews):
    service, provider = reviews
    started = asyncio.Event()
    hold = asyncio.Event()
    original = provider.stream_json

    async def pause(messages, schema):
        if schema["title"] == "RiskProposal":
            started.set()
            await hold.wait()
        async for chunk in original(messages, schema):
            yield chunk

    provider.stream_json = pause
    task = asyncio.create_task(service.review(provider, "fixture"))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    (identity,) = service.store.jobs()
    assert service.store.state(identity) == "canceled" and not service.records()
    assert (service.store.directory(identity) / "budget.json").is_file()


@pytest.mark.parametrize("risk_content", ["empty", "complete_json"])
async def test_openrouter_nonstop_risk_preserves_budget_without_retry_or_publication(
    reviews, risk_content
):
    service, fixture = reviews
    before = service.journal.read_bytes()
    requests = []

    async def handler(request):
        body = json.loads(request.content)
        requests.append(body)
        assert len(requests) <= 2
        assert body["stream"] is True and body["max_tokens"] == 4096
        assert body["provider"]["max_price"] == {"prompt": 0, "completion": 0}
        budget = len(requests) == 1
        title = "BudgetProposal" if budget else "RiskProposal"
        assert title in body["messages"][0]["content"]
        parts = [part async for part in fixture.stream_json(body["messages"], {"title": title})]
        content = "" if not budget and risk_content == "empty" else "".join(parts)
        event = {
            "choices": [
                {
                    "delta": {"content": content},
                    "finish_reason": "stop" if budget else "length",
                }
            ]
        }
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content="data: " + json.dumps(event) + "\n\ndata: [DONE]\n\n",
        )

    provider = OpenRouterChat(
        ModelSettings(
            provider="openrouter",
            model="cohere/north-mini-code:free",
            api_key=SecretStr("artificial-openrouter-key"),
        ),
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(ModelError, match="truncated"):
        await service.review(provider, "openrouter:fixture")
    assert len(requests) == 2 and service.journal.read_bytes() == before
    (identity,) = service.store.jobs()
    directory = service.store.directory(identity)
    assert service.store.state(identity) == "failed"
    assert (directory / "budget.json").is_file()
    assert not (directory / "review.json").exists()
    assert not (directory / "manifest.json").exists()
    assert service.records() == []
    with pytest.raises(ResearchError):
        service.retained(identity)
    assert json.loads(requests[1]["messages"][1]["content"])["basis"] == json.loads(
        requests[0]["messages"][1]["content"]
    )


async def test_secrets_in_raw_or_escaped_model_prose_are_never_retained(reviews):
    service, provider = reviews
    secret = 'key-with-"quotes\\and-slash'
    forms = (secret, json.dumps(secret)[1:-1])
    service.secret_check = lambda data: any(s.encode() in data for s in forms)
    provider.budget_change = lambda value: value.update(rationale=forms[1])
    with pytest.raises(ResearchError, match="Credential"):
        await service.review(provider, "fixture")
    for identity in service.store.jobs():
        assert not (service.store.directory(identity) / "budget.json").exists()
        assert service.store.state(identity) == "failed"


async def test_offline_or_missing_model_makes_no_review_artifacts(reviews):
    service, provider = reviews

    async def offline():
        raise ResearchError("offline")

    service.online_check = offline
    with pytest.raises(ResearchError, match="offline"):
        await service.review(provider, "fixture")
    assert not service.store.root.exists() and not provider.calls


async def test_report_corruption_and_symlink_are_not_listed_or_opened(reviews, tmp_path):
    service, provider = reviews
    value = await service.review(provider, "fixture")
    path = service.store.directory(value.id) / "review.json"
    original = path.read_bytes()
    path.write_text("{}")
    with pytest.raises(ResearchError, match="integrity"):
        await service.open(value.id)
    assert not service.records() and "damaged" in service.attempts()[0]["state"]
    path.write_bytes(original)
    manifest = service.store.directory(value.id) / "manifest.json"
    target = tmp_path / "outside.json"
    manifest.rename(target)
    manifest.symlink_to(target)
    with pytest.raises(ResearchError, match="symbolic"):
        await service.open(value.id)


def test_naive_capture_clock_and_symlink_journal_are_rejected(decision, tmp_path):
    with pytest.raises(ResearchError, match="timezone"):
        read_book(decision[0].ledger.path, at=datetime(2026, 1, 1))
    path = tmp_path / "alias.sqlite3"
    path.symlink_to(decision[0].ledger.path)
    with pytest.raises(PaperError, match="No readable"):
        read_book(path, at=AT)


async def test_distinct_accounts_have_distinct_roles_and_no_previous_review_context(
    reviews, decision, tmp_path
):
    from stratosphere.paper.ledger import Account, Instance, PaperLedger

    service, provider = reviews
    first = await service.review(provider, "fixture")
    account = Account(account_id="local-paper-" + "c" * 32, starting_cash="2000")
    other = PaperLedger(tmp_path / "other" / "account.sqlite3", account)
    state = decision[0].ledger.snapshot()
    original = Instance.model_validate(next(iter(state["instances"].values()))["mandate"])
    other.add_instance(
        original.model_copy(update={"instance_id": "d" * 32, "account_id": account.account_id}),
        decision[0].registry,
    )
    second_service = PaperReviewService(
        other.path, service.store, online_check=online, clock=lambda: AT
    )
    provider.calls.clear()
    provider.budget_change = lambda v: v.update(rationale="Second account review")
    second = await second_service.review(provider, "fixture")
    assert (
        len(
            {
                first.budget_agent_id,
                first.risk_agent_id,
                second.budget_agent_id,
                second.risk_agent_id,
            }
        )
        == 4
    )
    contexts = json.dumps(provider.calls)
    assert first.basis.account.account_id not in contexts
    assert first.budget_agent_id not in contexts and first.budget.rationale not in contexts
    assert second.basis.account.account_id in contexts
    # Retained proposals are historical; they are not appended to any role's next prompt.
    provider.calls.clear()
    await service.review(provider, "fixture")
    assert "Second account review" not in provider.calls[0][1]["content"]


async def test_damaged_journal_is_rejected_before_any_model_or_artifact(reviews):
    import sqlite3

    service, provider = reviews
    with sqlite3.connect(service.journal) as db:
        db.execute("DROP TRIGGER retain_update")
        db.execute("UPDATE events SET digest=? WHERE seq=2", ("f" * 64,))
    before = service.journal.read_bytes()
    with pytest.raises(PaperError, match="integrity"):
        await service.review(provider, "fixture")
    assert not service.store.root.exists() and not provider.calls
    assert service.journal.read_bytes() == before


async def test_model_cannot_route_unrelated_chat_to_private_account_snapshot(
    decision, tmp_path, research_plan
):
    from stratosphere.agents.orchestrator import Orchestrator
    from stratosphere.research.service import ResearchService

    class Router:
        async def stream_json(self, messages, schema):
            yield json.dumps({"action": "paper_review"})

    database = decision[0].registry
    bridge = Orchestrator(
        Router(),
        database,
        ResearchService(ResearchStore(tmp_path / "research"), online_check=online),
    )
    with pytest.raises(ModelError, match="invalid route"):
        _ = [part async for part in bridge.respond("Analyze the registered strategy")]
    assert bridge.last_action is None
    assert not database.history(database.catalog()[0]["public_id"])


async def test_matching_file_hashes_do_not_bypass_proposal_validation(reviews):
    import hashlib

    service, provider = reviews
    value = await service.review(provider, "fixture")
    directory = service.store.directory(value.id)
    report = directory / "review.json"
    payload = json.loads(report.read_text())
    payload["budget"]["allocations"][0]["capital"] = "501"
    report.write_text(json.dumps(payload))
    manifest = directory / "manifest.json"
    hashes = json.loads(manifest.read_text())
    hashes[report.name] = hashlib.sha256(report.read_bytes()).hexdigest()
    manifest.write_text(json.dumps(hashes))
    with pytest.raises(ResearchError, match="integrity"):
        service.retained(value.id)
    assert not service.records() and "damaged" in service.attempts()[0]["state"]


async def test_symlink_report_root_is_rejected_before_disclosure(reviews, tmp_path):
    service, provider = reviews
    target = tmp_path / "other-reports"
    target.mkdir()
    service.store.root.symlink_to(target)
    with pytest.raises(ResearchError, match="symbolic"):
        await service.review(provider, "fixture")
    assert not provider.calls and not list(target.iterdir())


async def test_loaded_prompt_contract_is_pinned_across_model_await(reviews, monkeypatch):
    from stratosphere.paper import reviews as module

    service, provider = reviews
    original_prompt, digest = module.RISK_PROMPT, reviewer_digest()
    provider.mutate = lambda count: monkeypatch.setattr(module, "RISK_PROMPT", "Changed prompt")
    value = await service.review(provider, "fixture")
    assert value.reviewer_sha256 == digest
    assert provider.calls[1][0]["content"] == original_prompt


async def test_changed_schema_delivery_between_roles_cannot_publish_a_mixed_contract(
    reviews, monkeypatch
):
    from stratosphere.models import streaming

    service, provider = reviews
    provider.mutate = lambda count: monkeypatch.setattr(
        streaming, "SCHEMA_INSTRUCTIONS", "Changed delivery contract"
    )
    with pytest.raises(ResearchError, match="Schema delivery changed"):
        await service.review(provider, "fixture")
    (identity,) = service.store.jobs()
    assert len(provider.calls) == 1 and service.store.state(identity) == "failed"
    assert (service.store.directory(identity) / "budget.json").exists()
    assert not (service.store.directory(identity) / "review.json").exists()
