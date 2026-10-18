"""Exact-identity evidence lookup and dated applicability, without profitability ranking."""

from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal

from stratosphere.catalog.contracts import (
    ApplicabilityAssessment,
    AssessmentTarget,
    EvidenceReference,
    MarketContext,
)
from stratosphere.phase0.contracts import StrategySpec
from stratosphere.registry.workspace import WorkspaceDatabase
from stratosphere.research.store import ResearchError


async def unavailable_context(target: AssessmentTarget) -> MarketContext:
    return MarketContext(
        target=target,
        origin="unavailable",
        missing_reason="No verified current market-context provider is connected.",
    )


class AssessmentService:
    def __init__(
        self,
        database: WorkspaceDatabase,
        research,
        studies,
        *,
        context_provider=unavailable_context,
        clock=lambda: datetime.now(UTC),
    ):
        self.database, self.research, self.studies = database, research, studies
        self.context_provider, self.clock = context_provider, clock

    def _evidence(self, strategy, target) -> tuple[EvidenceReference, ...]:
        references = []
        # All records here belong to this private development workspace. A future hosted
        # service must supply only owner-authorized/publishable evidence projections.
        for retained in self.database.reports():
            stored = retained["report"]
            relevant = stored.get("strategy_sha256") == strategy["spec_sha256"]
            if retained["kind"] == "study":
                relevant = relevant or any(
                    a.get("strategy_sha256") == strategy["spec_sha256"]
                    for a in stored.get("attempts", [])
                )
            if not relevant:
                continue
            service = self.studies if retained["kind"] == "study" else self.research
            report = service.report(retained["job_id"])
            if report != stored:
                raise ResearchError("Retained evidence differs from its sealed report")
            if report.get("schema_version") == "research-report/1":
                if (target.asset_class, target.symbol, target.timeframe_seconds) != (
                    "crypto_spot",
                    report.get("symbol"),
                    3600,
                ):
                    continue
                references.append(
                    EvidenceReference(
                        job_id=retained["job_id"],
                        artifact_sha256=retained["id"],
                        plan_sha256=report["plan_sha256"],
                        strategy_id=strategy["public_id"],
                        strategy_sha256=strategy["spec_sha256"],
                        kind="single_research",
                        attempted_variants=1,
                        candidate_status="completed",
                        final_holdout="not_evaluated",
                        development_month="2025-01",
                        metrics={
                            "continuous_base": report["metrics"],
                            "continuous_doubled_costs": report["doubled_cost_stress"],
                        },
                        limitations=tuple(report["limitations"]),
                    )
                )
            elif report.get("schema_version") == "study-report/1":
                if (target.asset_class, target.symbol, target.timeframe_seconds) != (
                    "crypto_spot",
                    "BTCUSDT",
                    3600,
                ):
                    continue
                for candidate in report["attempts"]:
                    if (
                        candidate.get("strategy_sha256") != strategy["spec_sha256"]
                        or candidate.get("strategy_id") != strategy["public_id"]
                    ):
                        continue
                    metrics = {
                        key: cell["metrics"]
                        for key, cell in candidate["cells"].items()
                        if cell["status"] == "completed"
                    }
                    selected = report["selection"]["selected_index"] == candidate["index"]
                    holdout = "not_evaluated"
                    if selected:
                        holdout = "incomplete"
                        # Never attribute the selected variant's holdout to its parent or siblings.
                        if (
                            report["holdout_status"] == "completed_once"
                            and report.get("metrics_strategy_id") == strategy["public_id"]
                        ):
                            holdout = "completed_for_exact_variant"
                        metrics.update(
                            {
                                f"holdout_{m}": cell["metrics"]
                                for m, cell in report["holdout"].items()
                                if cell["status"] == "completed"
                            }
                        )
                    references.append(
                        EvidenceReference(
                            job_id=retained["job_id"],
                            artifact_sha256=retained["id"],
                            plan_sha256=report["plan_sha256"],
                            strategy_id=strategy["public_id"],
                            strategy_sha256=strategy["spec_sha256"],
                            kind="study_variant",
                            attempted_variants=report["attempted_candidates"],
                            candidate_status=candidate["status"],
                            final_holdout=holdout,
                            development_month=report["protocol"]["development"]["month"],
                            holdout_month=report["protocol"]["holdout"]["month"],
                            metrics=metrics,
                            limitations=tuple(report["limitations"]),
                        )
                    )
        return tuple(references)

    async def assess(self, public_id: str, target: AssessmentTarget) -> dict:
        await self.research.require_online()
        strategy = self.database.strategy(public_id)
        spec = StrategySpec.model_validate(strategy["specification"])
        try:
            context = MarketContext.model_validate(await self.context_provider(target))
        except Exception:
            context = MarketContext(
                target=target,
                origin="unavailable",
                missing_reason="The configured context provider failed or returned invalid data; "
                "no substitute observation was used.",
            )
        at = self.clock()
        if at.tzinfo is None or at.utcoffset() is None:
            raise ResearchError("The assessment clock must supply an aware timestamp")
        at = at.astimezone(UTC)
        condition = context.condition(at, spec.missing_data_policy.max_staleness_seconds)
        if (
            context.target.asset_class,
            context.target.symbol,
            context.target.timeframe_seconds,
        ) != (
            target.asset_class,
            target.symbol,
            target.timeframe_seconds,
        ):
            condition = "mismatched_target"
        compatibility = {
            "asset_class": target.asset_class in spec.instrument_constraints.asset_classes,
            "symbol": spec.instrument_constraints.selection == "deployment_choice"
            or target.symbol in spec.instrument_constraints.fixed_symbols,
            "timeframe": target.timeframe_seconds in spec.timeframes,
            "holding_preference": target.allow_overnight
            or spec.execution_requirements.session_end == "close",
        }
        matched = all(compatibility.values())
        supported_data = (target.asset_class, target.symbol, target.timeframe_seconds) == (
            "crypto_spot",
            "BTCUSDT",
            3600,
        ) and spec.data_requirements.input == "ohlcv"
        capability = (
            "historical_archive_with_unverified_commercial_rights"
            if supported_data
            else "not_verified"
        )
        evidence = self._evidence(strategy, target)
        missing = [
            "No approved empirical sample/robustness/promotion thresholds are configured.",
            "No verified paper/live execution evidence or account capability is available.",
            "Artifact integrity does not establish market representativeness "
            "or commercial data rights.",
        ]
        if not supported_data:
            missing.append(
                "No verified tradable data adapter for the requested market/input/interval."
            )
        else:
            missing.append("Binance Vision commercial data entitlement has not been verified.")
        if condition != "current_snapshot":
            missing.append("Current market context is " + condition.replace("_", " ") + ".")
        else:
            missing.append("A dated current snapshot alone does not establish regime suitability.")
        if not evidence:
            missing.append(
                "No sealed native result applies to this exact strategy and requested market."
            )
        if not any(e.final_holdout == "completed_for_exact_variant" for e in evidence):
            missing.append("No completed final holdout applies to this exact strategy and market.")
        if target.max_drawdown_pct is not None:
            breached = [
                (e.job_id, name)
                for e in evidence
                for name, metrics in e.metrics.items()
                if Decimal(metrics["max_drawdown_pct"]) > Decimal(target.max_drawdown_pct)
            ]
            if breached:
                missing.append(
                    "Retained simulated drawdown exceeds the requested preference in "
                    + str(len(breached))
                    + " measured window(s)."
                )
        jobs = self.database.jobs(public_id)
        assessment = ApplicabilityAssessment(
            strategy_id=public_id,
            strategy_sha256=strategy["spec_sha256"],
            name=strategy["name"],
            assessed_at=at,
            target=target,
            compatibility=compatibility,
            data_capability=capability,
            implementation_evidence="retained_native_results" if evidence else "not_verified",
            current_context=context,
            current_context_status=condition,
            evidence=evidence,
            retained_jobs=dict(Counter(job["state"] for job in jobs)),
            attempted_variants=sum(e.attempted_variants for e in evidence),
            conclusion="research_candidate_requires_verification"
            if matched
            else "incompatible_rules",
            missing_verification=tuple(missing),
            next_steps=(
                "Resolve rule/target mismatches; performance does not transfer between markets.",
                "Verify provider/data-use rights, input quality and instrument semantics.",
                "Define empirical criteria, review an applicable protocol and retain all attempts.",
                "Obtain a dated verified current snapshot and test applicability explicitly.",
                "Verify paper behavior and deterministic controls before any separate activation.",
            ),
        )
        payload = assessment.model_dump(mode="json")
        digest = self.database.record_assessment(public_id, payload)
        return {"id": digest, "assessment": payload, "assessed_at": payload["assessed_at"]}

    async def open(self, digest):
        await self.research.require_online()
        record = self.database.assessment(digest)
        ApplicabilityAssessment.model_validate(record["assessment"])
        return record


def assessment_markdown(record):
    assessment = record["assessment"]
    target = assessment["target"]
    lines = [
        "# Applicability · " + assessment["name"],
        "",
        f"Strategy `{assessment['strategy_id']}`",
        "",
        f"Assessed at **{assessment['assessed_at']}** · historical assessment snapshot",
        "",
        f"Target: {target['asset_class']} · {target['symbol']} · "
        f"{target['timeframe_seconds']} seconds.",
        f"Overnight holding allowed: {'yes' if target['allow_overnight'] else 'no'}. "
        f"Drawdown preference: {target['max_drawdown_pct'] or 'not provided'}"
        + ("%." if target["max_drawdown_pct"] is not None else "."),
        "",
        "Conclusion: **" + assessment["conclusion"].replace("_", " ") + "**.",
        "",
        "## Compatibility",
        "",
        *[
            f"- {key.replace('_', ' ')}: {'matches' if value else 'does not match'}"
            for key, value in assessment["compatibility"].items()
        ],
        "",
        "Data capability: " + assessment["data_capability"].replace("_", " ") + ".",
        "Implementation evidence: " + assessment["implementation_evidence"].replace("_", " ") + ".",
        "",
        "## Retained empirical evidence",
        "",
        f"Applicable result references: {len(assessment['evidence'])}. "
        f"Candidate evaluations in those studies (sum): {assessment['attempted_variants']}.",
    ]
    for evidence in assessment["evidence"]:
        lines += [
            f"- Job `{evidence['job_id']}` · {evidence['candidate_status']} · "
            f"holdout: {evidence['final_holdout'].replace('_', ' ')} · "
            f"study denominator: {evidence['attempted_variants']}."
        ]
        if evidence["metrics"]:
            lines += [
                "",
                "| Window / modeled costs | Net P&L (USDT) | Drawdown (%) | Trades |",
                "| --- | --- | --- | --- |",
            ]
            for key, metric in evidence["metrics"].items():
                label = key.replace("_1", " · base").replace("_2", " · doubled")
                label = label.replace("_", " ")
                lines.append(
                    f"| {label} | {metric['net_pnl']} | "
                    f"{metric['max_drawdown_pct']} | {metric['completed_trades']} |"
                )
            lines += [
                "",
                "Retained simulation metrics; market representativeness "
                "and data rights are not reverified by this assessment.",
                "",
            ]
    lines += [
        "",
        "## Current context at assessment time",
        "",
        assessment["current_context_status"].replace("_", " ") + ".",
        assessment["current_context"].get("missing_reason") or "",
        f"Observation time: {assessment['current_context']['as_of'] or 'unavailable'}. "
        f"Availability time: {assessment['current_context']['available_at'] or 'unavailable'}.",
        *[f"- {observation}" for observation in assessment["current_context"]["observations"]],
        *[f"- Source: {source}" for source in assessment["current_context"]["source_refs"]],
        "A retained assessment is dated evidence, not a permanent suitability label.",
        "",
        "## Missing verification",
        "",
        *[f"- {item}" for item in assessment["missing_verification"]],
        "",
        "## Next research steps",
        "",
        *[f"- {item}" for item in assessment["next_steps"]],
        "",
        f"Assessment: `{record['id']}`",
        "",
        "No profitability ranking, validation badge, promotion or trading activation.",
    ]
    return "\n".join(lines)


def recommendations_markdown(records, target):
    compatible = [r for r in records if r["assessment"]["conclusion"] != "incompatible_rules"]
    lines = [
        "# Local research candidates",
        "",
        f"Requested: {target.asset_class} · {target.symbol} · {target.timeframe_seconds} seconds.",
        "",
        f"Compatible rule sets: {len(compatible)} of {len(records)} assessed.",
        "",
        "Candidates are in stable identity order, without a profitability score. "
        "This private development catalog is not a verified shared-user leaderboard.",
        "",
    ]
    for record in records:
        value = record["assessment"]
        lines += [
            f"- **{value['name']}** · `{value['strategy_id']}` · "
            f"{value['conclusion'].replace('_', ' ')}. "
            f"Evidence references: {len(value['evidence'])}; candidate evaluations: "
            f"{value['attempted_variants']}; context: "
            f"{value['current_context_status'].replace('_', ' ')}. "
            f"Assessment `{record['id']}` at {value['assessed_at']}."
        ]
    lines += [
        "",
        "Inspect a dated record with /assessment <assessment-id>. "
        "Missing verification remains blocking; this does not activate trading.",
    ]
    return "\n".join(lines)
