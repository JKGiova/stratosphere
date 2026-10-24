"""One reviewed campaign, exact strategy owners, finite starts and retained failures."""

import asyncio
import hashlib
import json
import uuid
from pathlib import Path

from stratosphere.phase0.contracts import fingerprint
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.workflow.contracts import (
    AutomaticCampaignPlan,
    CampaignPlan,
    CandidatePin,
    ResearchProposal,
)
from stratosphere.yahoo.contracts import YahooSnapshot, implementation_digest
from stratosphere.yahoo.service import plan_markdown as yahoo_plan_markdown


def payload_digest(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()


def workflow_digest():
    return hashlib.sha256(
        b"".join(
            p.name.encode() + b"\0" + p.read_bytes()
            for p in sorted(Path(__file__).parent.glob("*.py"))
        )
    ).hexdigest()


class ResearchCampaigns:
    def __init__(self, root, yahoo, database, *, secret_check=lambda data: False):
        self.store = ResearchStore(root)
        self.yahoo, self.database, self.secret_check = yahoo, database, secret_check

    def clean(self, value):
        if self.secret_check(json.dumps(value, ensure_ascii=False).encode()):
            raise ResearchError("Credential-bearing workflow data was rejected")

    def recover(self):
        for identity in self.store.jobs():
            try:
                with self.store.lock(identity):
                    if self.store.state(identity) in {"preparing", "running"}:
                        self.store.event(identity, "interrupted", "No automatic replay")
            except ResearchError:
                continue

    async def prepare(self, brief, proposal: ResearchProposal, *, parent=None, automatic=False):
        if proposal.status != "ready":
            raise ResearchError("Resolve proposal questions before preparing a campaign")
        self.clean({"brief": brief, "proposal": proposal.model_dump(mode="json")})
        identity = uuid.uuid7().hex
        self.store.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.store.directory(identity).mkdir(mode=0o700)
        self.store.write(identity, "brief.json", {"brief": brief})
        self.store.write(identity, "proposal.json", proposal.model_dump(mode="json"))
        self.store.event(identity, "preparing")
        pins, blocked = [], []
        try:
            with self.store.lock(identity):
                for index, request in enumerate(proposal.candidates):
                    record = self.database.register(
                        request.specification(),
                        proposal.name,
                        {
                            "kind": "workflow-candidate/1",
                            "campaign": identity,
                            "candidate_index": index,
                            "hypothesis": proposal.hypothesis,
                        },
                        parent=parent,
                        comparison=True,
                    )
                    parent = parent or record["public_id"]
                    self.store.write(
                        identity,
                        f"candidate_{'abcdefghijklmnopqrstuvwxyz'[index]}.json",
                        {
                            "strategy_id": record["public_id"],
                            "request": request.model_dump(mode="json"),
                        },
                    )
                    try:
                        job, plan = await self.yahoo.prepare(request)
                        if plan.strategy_id != record["public_id"]:
                            raise ResearchError("Research owner identity changed")
                        pins.append(
                            CandidatePin(
                                index=index,
                                job_id=job,
                                strategy_id=plan.strategy_id,
                                plan_sha256=plan.digest,
                                implementation_sha256=plan.implementation_sha256,
                            )
                        )
                    except ResearchError as error:
                        blocked.append(
                            {
                                "index": index,
                                "strategy_id": record["public_id"],
                                "request": request.model_dump(mode="json"),
                                "error": str(error),
                            }
                        )
                self.store.write(identity, "blocked.json", {"attempts": blocked})
                if not pins:
                    self.store.event(identity, "blocked", "No validated candidate dataset")
                    raise ResearchError("All candidate datasets were blocked; evidence retained")
                plan = (AutomaticCampaignPlan if automatic else CampaignPlan)(
                    campaign_id=identity,
                    proposal_sha256=fingerprint(proposal),
                    implementation_sha256=workflow_digest(),
                    candidates=tuple(pins),
                    blocked_attempts_sha256=payload_digest(blocked),
                    native_starts=2 * len(pins),
                )
                self.store.write(identity, "plan.json", plan.model_dump(mode="json"))
                self.store.event(identity, "policy_ready" if automatic else "awaiting_approval")
                return identity, plan
        except asyncio.CancelledError:
            self.store.event(identity, "canceled", "Preparation evidence retained")
            raise

    def plan(self, identity):
        directory = self.store.directory(identity)
        if directory.is_symlink() or any(p.is_symlink() for p in directory.iterdir()):
            raise ResearchError("Workflow evidence cannot contain redirected paths")
        value = self.store.read(identity, "plan.json")
        contract = (
            AutomaticCampaignPlan
            if value.get("schema_version") == "agent-research-plan/2"
            else CampaignPlan
        )
        plan = contract.model_validate(value)
        proposal = ResearchProposal.model_validate(self.store.read(identity, "proposal.json"))
        if plan.campaign_id != identity or plan.proposal_sha256 != fingerprint(proposal):
            raise ResearchError("Campaign proposal changed")
        blocked = self.store.read(identity, "blocked.json")["attempts"]
        if plan.blocked_attempts_sha256 != payload_digest(blocked):
            raise ResearchError("Blocked candidate evidence changed")
        indices = [c.index for c in plan.candidates] + [b["index"] for b in blocked]
        if sorted(indices) != list(range(len(proposal.candidates))):
            raise ResearchError("Campaign does not account for every attempted candidate")
        for pin in plan.candidates:
            candidate = self.yahoo.plan(pin.job_id)
            if (
                candidate.digest != pin.plan_sha256
                or candidate.strategy_id != pin.strategy_id
                or candidate.implementation_sha256 != pin.implementation_sha256
                or candidate.request != proposal.candidates[pin.index]
            ):
                raise ResearchError("Candidate plan changed")
        return plan

    def approve(self, identity, digest):
        with self.store.lock(identity):
            plan = self.plan(identity)
            if isinstance(plan, AutomaticCampaignPlan):
                raise ResearchError(
                    "New research uses application policy; approve only the finished strategy"
                )
            if self.store.state(identity) != "awaiting_approval" or plan.digest != digest:
                raise ResearchError("Approve the exact pending campaign once")
            if plan.implementation_sha256 != workflow_digest() or any(
                c.implementation_sha256 != implementation_digest() for c in plan.candidates
            ):
                raise ResearchError("Research implementation changed; prepare a new campaign")
            if any(
                self.yahoo.store.state(c.job_id) != "awaiting_approval" for c in plan.candidates
            ):
                raise ResearchError("A candidate was independently approved or consumed")
            self.store.write(identity, "approval.json", {"plan_sha256": digest})
            self.store.event(identity, "approved")

    def authorize_research(self, identity):
        """Application policy only: finite frozen research, with no order authority."""
        with self.store.lock(identity):
            plan = self.plan(identity)
            if not isinstance(plan, AutomaticCampaignPlan):
                raise ResearchError("Historical campaigns keep their original approval contract")
            if self.store.state(identity) != "policy_ready":
                raise ResearchError("Research policy cannot replay an authorized campaign")
            if plan.implementation_sha256 != workflow_digest() or any(
                c.implementation_sha256 != implementation_digest() for c in plan.candidates
            ):
                raise ResearchError("Research implementation changed")
            if any(
                self.yahoo.store.state(c.job_id) != "awaiting_approval" for c in plan.candidates
            ):
                raise ResearchError("A candidate was independently approved or consumed")
            self.store.write(
                identity,
                "approval.json",
                {
                    "schema_version": "research-policy-authorization/1",
                    "authority": plan.authorization,
                    "plan_sha256": plan.digest,
                    "native_starts": plan.native_starts,
                    "human_approval": False,
                    "order_authority": False,
                },
            )
            self.store.event(identity, "approved", "Application research policy; no human approval")

    async def run(self, identity):
        with self.store.lock(identity):
            plan = self.plan(identity)
            if (
                self.store.state(identity) != "approved"
                or self.store.read(identity, "approval.json")["plan_sha256"] != plan.digest
            ):
                raise ResearchError("The exact campaign needs human approval before execution")
            if isinstance(plan, AutomaticCampaignPlan):
                expected = {
                    "schema_version": "research-policy-authorization/1",
                    "authority": plan.authorization,
                    "plan_sha256": plan.digest,
                    "native_starts": plan.native_starts,
                    "human_approval": False,
                    "order_authority": False,
                }
                if self.store.read(identity, "approval.json") != expected:
                    raise ResearchError("Application research authorization changed")
            if plan.implementation_sha256 != workflow_digest() or any(
                c.implementation_sha256 != implementation_digest() for c in plan.candidates
            ):
                raise ResearchError("Research implementation changed")
            await self.yahoo.require_online()
            self.store.event(identity, "running")
            results = []
            try:
                for pin in plan.candidates:
                    try:
                        if isinstance(plan, AutomaticCampaignPlan):
                            self.yahoo.authorize_campaign(
                                pin.job_id,
                                pin.plan_sha256,
                                campaign_id=identity,
                                campaign_sha256=plan.digest,
                            )
                        else:
                            # Compatibility: this derives from the historical human decision.
                            self.yahoo.approve(pin.job_id, pin.plan_sha256)
                        report = await self.yahoo.run(pin.job_id)
                        results.append(
                            {
                                "job_id": pin.job_id,
                                "strategy_id": pin.strategy_id,
                                "state": "completed",
                                "report_sha256": payload_digest(report),
                                "report": report,
                            }
                        )
                    except ResearchError as error:
                        results.append(
                            {
                                "job_id": pin.job_id,
                                "strategy_id": pin.strategy_id,
                                "state": "failed",
                                "error": str(error),
                            }
                        )
                report = {
                    "schema_version": "agent-research-report/1",
                    "campaign_id": identity,
                    "plan_sha256": plan.digest,
                    "results": results,
                    "blocked": self.store.read(identity, "blocked.json")["attempts"],
                    "deployment_status": "research_only",
                    "limitations": [
                        "All attempted candidates retained; no winner selected.",
                        "No independent holdout or trading eligibility proof.",
                        "Portfolio assignment needs exact final-report approval.",
                    ],
                }
                self.store.write(identity, "report.json", report)
                self.store.write(identity, "report.md", report_markdown(report))
                self.store.write(identity, "report_seal.json", {"sha256": payload_digest(report)})
                self.store.event(identity, "completed")
                return self.report(identity)
            except asyncio.CancelledError:
                self.store.write(identity, "partial.json", {"results": results})
                self.store.event(identity, "canceled", "No automatic candidate replay")
                raise
            except Exception:
                self.store.event(identity, "failed", "No automatic candidate replay")
                raise

    def report(self, identity):
        plan = self.plan(identity)
        if self.store.state(identity) != "completed":
            raise ResearchError("No completed workflow report")
        report = self.store.read(identity, "report.json")
        if (
            report["plan_sha256"] != plan.digest
            or payload_digest(report) != self.store.read(identity, "report_seal.json")["sha256"]
        ):
            raise ResearchError("Workflow report changed")
        if len(report["results"]) != len(plan.candidates):
            raise ResearchError("Workflow report lost attempted candidates")
        for pin, result in zip(plan.candidates, report["results"], strict=True):
            if result["job_id"] != pin.job_id or result["strategy_id"] != pin.strategy_id:
                raise ResearchError("Workflow report ownership changed")
            if result["state"] == "completed":
                actual = self.yahoo.report(pin.job_id)
                if payload_digest(actual) != result["report_sha256"] or actual != result["report"]:
                    raise ResearchError("Candidate result changed")
        return report


def plan_markdown(identity, plan, yahoo):
    lines = [
        "# Strategy-agent research plan",
        "",
        f"Campaign `{identity}`",
        "",
        f"Exactly **{plan.native_starts} native starts**, including base and stress costs.",
        "All candidates and blocked attempts remain visible; no automatic optimization or holdout.",
        "",
    ]
    for pin in plan.candidates:
        request = yahoo.plan(pin.job_id).request
        lines.append(
            f"- Strategy `{pin.strategy_id}` · {request.symbol} {request.interval} · "
            f"SMA {request.fast}/{request.slow} · {request.start}–{request.end} exclusive · "
            f"{request.quantity} units · costs `{request.config.model_dump_json()}` · "
            f"plan `{pin.plan_sha256}`"
        )
        snapshot = YahooSnapshot.model_validate(yahoo.store.read(pin.job_id, "snapshot.json"))
        lines += ["", yahoo_plan_markdown(pin.job_id, yahoo.plan(pin.job_id), snapshot)]
    if isinstance(plan, AutomaticCampaignPlan):
        return "\n".join(lines) + (
            "\n\nApplication research policy: at most 12 candidates / 24 native starts. "
            "No research approval is requested. Only the finished strategy and its paper limits "
            "can receive user approval; this campaign grants no execution authority."
        )
    lines += [
        "",
        f"Workflow implementation `{plan.implementation_sha256}`.",
        "Approve the full campaign below. Candidate commands above are inspection references; "
        "independently approving a candidate invalidates campaign approval.",
    ]
    return "\n".join(lines) + f"\n\nApprove: `/flow-approve {identity} {plan.digest}`"


def report_markdown(report):
    lines = [
        "# Strategy-agent research report",
        "",
        "Research only; no deployment or live execution.",
        "",
        "| Exact strategy | Symbol | Interval | Base return | Stress return | State |",
        "| --- | --- | --- | ---: | ---: | --- |",
    ]
    for item in report["results"]:
        if item["state"] == "completed":
            r = item["report"]
            lines.append(
                f"| {item['strategy_id']} | {r['request']['symbol']} | "
                f"{r['request']['interval']} | {r['metrics']['base']['net_return_pct']}% | "
                f"{r['metrics']['stress']['net_return_pct']}% | completed |"
            )
        else:
            lines.append(f"| {item['strategy_id']} | — | — | — | — | failed |")
    lines += ["", "## Blocked attempts", ""]
    lines += [
        f"- {r['request']['symbol']} {r['request']['interval']}: {r['error']}"
        for r in report["blocked"]
    ]
    lines += ["", "## Failed simulation attempts", ""]
    lines += [
        f"- Strategy `{r['strategy_id']}` · job `{r['job_id']}`: {r['error']}"
        for r in report["results"]
        if r["state"] != "completed"
    ]
    return "\n".join(lines)
