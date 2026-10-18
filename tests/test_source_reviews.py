"""Specialist-owned source interpretations retain evidence, never execution authority."""

import asyncio
import json
import sqlite3

import httpx
import pytest
from pydantic import ValidationError

from stratosphere.agents.orchestrator import Orchestrator
from stratosphere.models.streaming import ModelError
from stratosphere.phase0.contracts import StrategySpec
from stratosphere.registry.workspace import RegistryError, WorkspaceDatabase
from stratosphere.research.service import ResearchService
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.sources.contracts import SourceRequest, SuppliedRequest
from stratosphere.sources.review import SourceReviewService, review_markdown
from stratosphere.sources.service import SourceService
from stratosphere.sources.store import SourceStore

TEXT = "SMA 3/8.\r\nThis source is unverified.\r\nSMA 3/8.\r\n"


async def online():
    pass


class ReviewProvider:
    def __init__(self, response=None, route=None):
        self.response, self.route = response, route
        self.requests = []

    async def stream_json(self, messages, schema):
        self.requests.append((messages, schema))
        if "action" in schema.get("properties", {}):
            payload = self.route
        else:
            payload = self.response or {
                "findings": [
                    {
                        "category": "rule_candidate",
                        "interpretation": "The source mentions SMA periods; "
                        "entry/exit equivalence is unverified.",
                        "evidence_index": 2,
                    }
                ],
                "questions": ["Which entry, exit and position sizing rules apply?"],
            }
        yield json.dumps(payload)

    async def stream(self, messages):
        self.requests.append((messages, None))
        yield "I can analyze only my own retained evidence; no test was executed."


@pytest.fixture
async def reviews(tmp_path, research_plan):
    db = WorkspaceDatabase(tmp_path / "workspace.sqlite3")
    first = db.register(research_plan.specification, "First fixture", {"text": "first"})
    other_spec = StrategySpec.model_validate(
        research_plan.specification.model_dump()
        | {"position_sizing": {"mode": "fixed_base", "amount": "0.02"}}
    )
    second = db.register(other_spec, "Second fixture", {"text": "second"})
    sources = SourceService(
        SourceStore(tmp_path / "sources", known_secrets=("hidden-fixture-key",)),
        online_check=online,
    )
    snapshot = await sources.supply(
        SuppliedRequest(label="Authored rules"), TEXT.encode(), kind="code"
    )
    service = SourceReviewService(sources, db)
    research = ResearchService(ResearchStore(tmp_path / "research"), online_check=online)
    return db, first, second, sources, snapshot, service, research


async def test_review_quotes_references_ownership_and_restart(reviews):
    db, first, second, sources, snapshot, service, _ = reviews
    provider = ReviewProvider()
    value = await service.review(first["public_id"], snapshot.id, provider, "fixture-model")
    assert value.execution == "blocked" and value.behavioral_conformance == "not_verified"
    assert value.model_used and value.findings[0].start == TEXT.rfind("SMA 3/8.")
    text = review_markdown(value, snapshot)
    assert "source lines 3–3" in text and "Interpretations are unverified" in text
    assert (
        len(db.source_reviews(first["public_id"])) == 1
        and db.source_reviews(second["public_id"]) == []
    )
    assert len(db.catalog()) == 2 and not db.reports() and not db.jobs(first["public_id"])
    reopened = SourceReviewService(sources, WorkspaceDatabase(db.path))
    assert await reopened.open(value.id, strategy_id=first["public_id"]) == value
    with pytest.raises(RegistryError):
        await reopened.open(value.id, strategy_id=second["public_id"])
    with pytest.raises(RegistryError):
        db.record_source_review(second["public_id"], value.model_dump(mode="json"))
    with sqlite3.connect(db.path) as sql:
        with pytest.raises(sqlite3.IntegrityError):
            sql.execute("DELETE FROM source_reviews")
        with pytest.raises(sqlite3.IntegrityError):
            sql.execute("UPDATE source_reviews SET payload='{}'")
    prompt = json.loads(provider.requests[0][0][1]["content"])
    assert prompt["strategy"]["public_id"] == first["public_id"]
    assert second["public_id"] not in json.dumps(provider.requests)


@pytest.mark.parametrize(
    "patch",
    [
        {"quote": "invented text"},
        {"quote": " "},
        {"evidence_index": 99},
        {"evidence_index": True},
        {"evidence_index": -1},
        {"evidence_index": "0"},
        {"evidence_index": 1.5},
        {"occurrence": 0},
        {"interpretation": ""},
        {"action": "approve"},
        {"execution": "ready"},
    ],
)
async def test_invalid_model_quotes_and_authority_are_retained_as_failed_attempts(reviews, patch):
    db, first, _, _, snapshot, service, _ = reviews
    finding = {
        "category": "rule_candidate",
        "interpretation": "tentative",
        "evidence_index": 0,
    } | patch
    provider = ReviewProvider({"findings": [finding], "questions": []})
    with pytest.raises(ModelError):
        await service.review(first["public_id"], snapshot.id, provider, "fixture")
    assert not db.source_reviews()
    assert service.store.state(service.store.jobs()[0]) == "failed"
    assert not (service.store.directory(service.store.jobs()[0]) / "review.json").exists()


@pytest.mark.parametrize(
    "url,status",
    [
        ("https://youtu.be/dQw4w9WgXcQ", "metadata_only"),
        ("https://www.instagram.com/p/fixture/", "source_unavailable"),
    ],
)
async def test_metadata_and_inaccessible_sources_do_not_invoke_a_model(reviews, url, status):
    db, first, _, sources, _, service, _ = reviews
    sources.transport = httpx.MockTransport(
        lambda r: httpx.Response(200, json={"title": "SMA 3/8."})
    )
    snapshot = await sources.acquire(SourceRequest(url=url))
    provider = ReviewProvider()
    value = await service.review(first["public_id"], snapshot.id, provider, "fixture")
    assert value.status == status and not value.model_used and not value.findings
    assert value.questions and not provider.requests and len(db.source_reviews()) == 1


async def test_cancel_keeps_attempt_and_operation_without_partial_history(reviews):
    db, first, _, sources, snapshot, service, research = reviews
    started = asyncio.Event()

    class Waiting(ReviewProvider):
        async def stream_json(self, messages, schema):
            started.set()
            await asyncio.Future()
            yield ""

    orchestrator = Orchestrator(Waiting(), db, research, sources=sources)
    agent = orchestrator.agent(first["public_id"])
    task = asyncio.create_task(agent.review_source(snapshot.id, "Review this source."))
    try:
        await asyncio.wait_for(started.wait(), 5)
    except TimeoutError:
        if task.done():
            await task  # Surface a pre-provider failure instead of waiting indefinitely.
        else:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        raise
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not db.history(first["public_id"]) and not db.source_reviews()
    assert service.store.state(service.store.jobs()[0]) == "canceled"
    with db.connect() as sql:
        assert sql.execute("SELECT status FROM operations").fetchone()[0] == "canceled"


async def test_changed_source_during_model_request_cannot_publish(reviews):
    db, first, _, sources, snapshot, service, _ = reviews

    class Tampering(ReviewProvider):
        async def stream_json(self, messages, schema):
            raw = sources.store.directory(snapshot.id) / (
                "blob_" + snapshot.raw_materials[0].sha256 + ".bin"
            )
            raw.write_text("changed")
            async for delta in super().stream_json(messages, schema):
                yield delta

    with pytest.raises(ResearchError):
        await service.review(first["public_id"], snapshot.id, Tampering(), "fixture")
    assert not db.source_reviews() and service.store.state(service.store.jobs()[0]) == "failed"


@pytest.mark.parametrize("name", ["review.json", "manifest.json", "request.json"])
async def test_review_file_corruption_blocks_inspection(reviews, name):
    _, first, _, _, snapshot, service, _ = reviews
    value = await service.review(first["public_id"], snapshot.id, ReviewProvider(), "fixture")
    path = service.store.directory(value.id) / name
    content = json.loads(path.read_text())
    content["unexpected"] = "tamper"
    path.write_text(json.dumps(content))
    with pytest.raises((ResearchError, ValidationError, ValueError)):
        await service.open(value.id)


async def test_source_reviews_are_private_completed_agent_context(reviews):
    db, first, second, sources, snapshot, _, research = reviews
    provider = ReviewProvider()
    orchestrator = Orchestrator(provider, db, research, sources=sources)
    first_agent = orchestrator.agent(first["public_id"])
    second_agent = orchestrator.agent(second["public_id"])
    value = await first_agent.review_source(snapshot.id, "Review my source.")
    assert len(db.history(first["public_id"])) == 2 and db.history(second["public_id"]) == []
    _ = "".join([p async for p in second_agent.respond("Analyze my own evidence.")])
    assert value.id not in json.dumps(provider.requests[-1])
    _ = "".join([p async for p in first_agent.respond("Analyze my own evidence.")])
    assert value.id in json.dumps(provider.requests[-1])
    assert "not_verified" in json.dumps(provider.requests[-1])
    restored = Orchestrator(provider, WorkspaceDatabase(db.path), research, sources=sources)
    assert restored.agent(first["public_id"]).session.history == db.history(first["public_id"])


@pytest.mark.parametrize("reference", ["source_id", "url", "markdown_url"])
async def test_natural_review_uses_the_same_specialist_operation(reviews, reference):
    db, first, _, sources, snapshot, _, research = reviews
    url = "https://www.instagram.com/p/fixture/"
    source = snapshot.id if reference == "source_id" else url
    request = "Review " + (f"[this]({url})" if reference == "markdown_url" else source)
    provider = ReviewProvider(
        route={"action": "source_review", "source": source, "strategy_id": None, "target": None}
    )
    orchestrator = Orchestrator(provider, db, research, sources=sources)
    orchestrator.select(first["public_id"])
    text = "".join([p async for p in orchestrator.respond(request)])
    assert (
        "Execution remains blocked" in text
        and orchestrator.responding_strategy == first["public_id"]
    )
    assert orchestrator.last_action["action"] == "source_review" and len(db.source_reviews()) == 1
    assert not db.jobs(first["public_id"]) and len(db.catalog()) == 2


@pytest.mark.parametrize(
    "source", ["f" * 32, "https://www.instagram.com/p/other/", "https://github.com/owner/repo"]
)
async def test_router_cannot_invent_a_snapshot_or_rewrite_a_source_url(reviews, source):
    db, first, _, sources, snapshot, _, research = reviews
    provider = ReviewProvider(
        route={"action": "source_review", "source": source, "strategy_id": None, "target": None}
    )
    orchestrator = Orchestrator(provider, db, research, sources=sources)
    orchestrator.select(first["public_id"])
    with pytest.raises(ModelError):
        _ = "".join(
            [
                p
                async for p in orchestrator.respond(
                    "Check the evidence in "
                    + snapshot.id
                    + " https://github.com/owner/repo/blob/main/strategy.py"
                )
            ]
        )
    assert not db.source_reviews() and not orchestrator.source_reviews.store.jobs()


async def test_plain_url_intake_works_before_any_strategy_and_never_registers(reviews):
    db, _, _, sources, _, _, research = reviews
    # Separate empty registry: no routing or interpretation call is needed for an exact URL.
    empty = WorkspaceDatabase(db.path.parent / "empty.sqlite3")
    provider = ReviewProvider()
    orchestrator = Orchestrator(provider, empty, research, sources=sources)
    reply = "".join([p async for p in orchestrator.respond("https://www.instagram.com/p/fixture/")])
    assert "requires_credentials" in reply and not provider.requests and not empty.catalog()
    assert orchestrator.last_action["action"] == "source"


async def test_credential_bearing_model_interpretation_is_not_saved(reviews):
    db, first, _, _, snapshot, service, _ = reviews
    provider = ReviewProvider(
        {
            "findings": [
                {
                    "category": "assumption",
                    "interpretation": "hidden-fixture-key",
                    "evidence_index": 0,
                }
            ],
            "questions": [],
        }
    )
    with pytest.raises(ResearchError, match="Credential"):
        await service.review(first["public_id"], snapshot.id, provider, "fixture")
    assert not db.source_reviews()
    for path in service.store.root.rglob("*"):
        if path.is_file():
            assert b"hidden-fixture-key" not in path.read_bytes()


async def test_source_review_open_is_online_and_symlink_safe(reviews, tmp_path):
    _, first, _, sources, snapshot, service, _ = reviews
    value = await service.review(first["public_id"], snapshot.id, ReviewProvider(), "fixture")

    async def offline():
        raise httpx.ConnectError("offline")

    sources.online_check = offline
    with pytest.raises(ResearchError, match="online"):
        await service.open(value.id)
    sources.online_check = online
    path = service.store.directory(value.id) / "review.json"
    moved = tmp_path / "review-copy.json"
    path.rename(moved)
    path.symlink_to(moved)
    with pytest.raises(ResearchError, match="integrity"):
        await service.open(value.id)


@pytest.mark.parametrize(
    "prefix",
    [
        "Review source",
        "Please review snapshot",
        "Can you inspect source",
        "Analyze retained source",
    ],
)
async def test_explicit_source_review_never_routes_to_applicability(reviews, prefix):
    db, first, _, sources, snapshot, _, research = reviews
    provider = ReviewProvider(route={"action": "assess"})
    bridge = Orchestrator(provider, db, research, sources=sources)
    bridge.select(first["public_id"])
    reply = "".join([p async for p in bridge.respond(prefix + " " + snapshot.id)])
    assert bridge.last_action["action"] == "source_review" and "Source review" in reply
    assert len(provider.requests) == 1 and not db.assessments()


@pytest.mark.parametrize(
    "format_string", ["Review [source]({url})", "Inspect source `{url}`", "Analyze source <{url}>"]
)
async def test_source_url_wrappers_do_not_rewrite_or_create_duplicate_references(
    reviews, format_string
):
    db, first, _, sources, _, _, research = reviews
    bridge = Orchestrator(ReviewProvider(), db, research, sources=sources)
    bridge.select(first["public_id"])
    url = "https://github.com/owner/repo/blob/main/" + "a" * 32 + ".py"
    route = await bridge.route(format_string.format(url=url))
    assert route.action == "source_review" and route.source == url and route.strategy_id is None


async def test_source_review_targets_must_be_explicit_and_unambiguous(reviews):
    db, first, second, sources, snapshot, _, research = reviews
    bridge = Orchestrator(ReviewProvider(), db, research, sources=sources)
    bridge.select(first["public_id"])
    route = await bridge.route("Review source " + snapshot.id + " for " + second["public_id"])
    assert route.strategy_id == second["public_id"]
    with pytest.raises(ModelError, match="one exact strategy"):
        await bridge.route(
            "Review source " + snapshot.id + " for " + first["public_id"] + " and Second fixture"
        )
    with pytest.raises(ModelError, match="one exact strategy"):
        await bridge.route(
            "Review source "
            + snapshot.id
            + " for "
            + first["public_id"]
            + " and "
            + second["public_id"]
        )
    with pytest.raises(ModelError, match="one source"):
        await bridge.route("Review sources " + snapshot.id + " and " + "f" * 32)


@pytest.mark.parametrize(
    "text,calls",
    [
        ("Do not review source", 0),
        ("Please don't inspect source", 0),
        ("Never acquire source", 0),
        ("Review research job", 1),
    ],
)
async def test_source_parser_does_not_override_negation_or_other_id_namespaces(
    reviews, text, calls
):
    db, first, _, sources, snapshot, _, research = reviews
    provider = ReviewProvider(
        route={"action": "source_review", "source": snapshot.id}
        if calls == 0
        else {"action": "chat"}
    )
    bridge = Orchestrator(provider, db, research, sources=sources)
    bridge.select(first["public_id"])
    route = await bridge.route(text + " " + snapshot.id)
    assert route.action == "chat" and len(provider.requests) == calls


async def test_mixed_case_https_source_is_acquired_as_a_url(reviews):
    db, _, _, sources, _, _, research = reviews
    provider = ReviewProvider()
    bridge = Orchestrator(provider, db, research, sources=sources)
    reply = "".join([p async for p in bridge.respond("HTTPS://WWW.INSTAGRAM.COM/p/fixture/")])
    assert "requires_credentials" in reply and not provider.requests


async def test_empty_model_output_and_questions_only_do_not_publish_a_grounded_review(reviews):
    db, first, _, _, snapshot, service, _ = reviews
    for questions in ([], ["What rules apply?"]):
        provider = ReviewProvider({"findings": [], "questions": questions})
        with pytest.raises(ModelError):
            await service.review(first["public_id"], snapshot.id, provider, "fixture")
    assert not db.source_reviews() and all(
        service.store.state(i) == "failed" for i in service.store.jobs()
    )


async def test_blank_code_is_retained_as_unreadable_without_a_model_call(reviews):
    from stratosphere.sources.contracts import SourcePiece

    db, first, _, sources, _, service, _ = reviews
    request = SuppliedRequest(label="Authored whitespace code fixture")
    identity = sources.store.create(request)
    raw = sources.store.raw(identity, b" \r\n", "text/plain", "authored blank code")
    snapshot = sources._finish(
        identity,
        request,
        dict(
            access_state="acquired",
            pieces=(
                SourcePiece(kind="code", text=" \r\n", locator="line 1", first_line=1, last_line=1),
            ),
            raw_materials=(raw,),
            limitations=("Authored fixture, not a provider-read proof.",),
        ),
    )
    provider = ReviewProvider()
    value = await service.review(first["public_id"], snapshot.id, provider, "fixture")
    assert value.status == "no_readable_material" and not value.model_used and not value.findings
    assert not provider.requests and len(db.source_reviews()) == 1


async def test_review_identity_binds_loaded_prompt_before_the_provider_request(
    reviews, monkeypatch
):
    import stratosphere.sources.review as module

    _, first, _, _, snapshot, service, _ = reviews
    initial = module.reviewer_digest()

    class ChangedPrompt(ReviewProvider):
        async def stream_json(self, messages, schema):
            assert messages[0]["content"] == module.PROMPT
            monkeypatch.setattr(module, "PROMPT", "Updated for a later request")
            async for part in super().stream_json(messages, schema):
                yield part

    value = await service.review(first["public_id"], snapshot.id, ChangedPrompt(), "fixture")
    assert value.reviewer_sha256 == initial and value.reviewer_sha256 != module.reviewer_digest()


async def test_a_valid_quote_with_an_empty_question_cannot_publish(reviews):
    db, first, _, _, snapshot, service, _ = reviews
    provider = ReviewProvider(
        {
            "findings": [
                {
                    "category": "rule_candidate",
                    "interpretation": "Tentative observation",
                    "evidence_index": 0,
                }
            ],
            "questions": [""],
        }
    )
    with pytest.raises(ModelError):
        await service.review(first["public_id"], snapshot.id, provider, "fixture")
    assert not db.source_reviews() and service.store.state(service.store.jobs()[0]) == "failed"
