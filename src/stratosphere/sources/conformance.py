"""Dated source/rule mapping assessment, explicitly separate from an executable plan."""

import hashlib
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from stratosphere.phase0.contracts import Contract, Digest, fingerprint
from stratosphere.research.contracts import Extraction
from stratosphere.research.store import ResearchError, ResearchStore
from stratosphere.sources.pine import (
    ConversionBlocked,
    PineChartContext,
    SourceRuleReference,
    converter_digest,
    map_pine_rules,
)

NUMERICAL_GAP = (
    "Pine float comparisons round operands to nine fractional digits; the packaged "
    "SMA kernel uses exact rational arithmetic. Numerical/first-cross conformance is "
    "not verified; rule mapping does not authorize a converted simulation."
)


class SourceConformance(Contract):
    schema_version: Literal["source-conformance/1"] = "source-conformance/1"
    id: str = Field(pattern=r"^[a-f0-9]{32}$")
    assessed_at: AwareDatetime
    source_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    source_snapshot_sha256: Digest
    converter_sha256: Digest
    chart_context: PineChartContext | None
    classification: Literal[
        "partial_source", "missing_context", "unsupported", "rules_mapped_conformance_blocked"
    ]
    mapped_rules: Extraction | None = None
    rule_references: tuple[SourceRuleReference, ...] = ()
    blockers: tuple[str, ...] = Field(min_length=1)
    execution: Literal["blocked"] = "blocked"
    actual_pine_runtime_verified: Literal[False] = False
    arithmetic_equivalence_verified: Literal[False] = False

    @model_validator(mode="after")
    def utc(self):
        object.__setattr__(self, "assessed_at", self.assessed_at.astimezone(UTC))
        if self.classification == "rules_mapped_conformance_blocked":
            if self.mapped_rules is None or not self.rule_references or self.chart_context is None:
                raise ValueError("Mapped rules require source/context references")
        elif self.mapped_rules is not None or self.rule_references:
            raise ValueError("Unsupported/unavailable source cannot have a rule mapping")
        return self

    @property
    def digest(self):
        return fingerprint(self)


def assessment_digest():
    return hashlib.sha256(Path(__file__).read_bytes() + converter_digest().encode()).hexdigest()


class ConformanceService:
    def __init__(self, sources, store: ResearchStore, *, clock=lambda: datetime.now(UTC)):
        self.sources, self.store, self.clock = sources, store, clock

    async def assess(self, source_id, context=None):
        snapshot = await self.sources.open(source_id)
        fields = {}
        if snapshot.access_state != "acquired":
            classification = "partial_source"
            blockers = (
                "Acquire or supply complete source code; partial metadata/media cannot establish "
                "behavior.",
            )
        elif context is None:
            classification = "missing_context"
            blockers = (
                "Declare the original asset class, symbol, interval, regular chart type and "
                "source-default properties; these are not inferred from code or titles.",
            )
        else:
            context = PineChartContext.model_validate(context)
            try:
                _, draft, refs = map_pine_rules(snapshot, context)
                classification = "rules_mapped_conformance_blocked"
                fields = dict(mapped_rules=draft, rule_references=refs)
                blockers = (
                    NUMERICAL_GAP,
                    "An actual Pine runtime comparison has not been performed. Original chart "
                    "data/properties are user-declared, not provider-verified.",
                )
            except ConversionBlocked as error:
                classification, blockers = "unsupported", (str(error),)
        identity = uuid.uuid7().hex
        value = SourceConformance(
            id=identity,
            assessed_at=self.clock(),
            source_id=source_id,
            source_snapshot_sha256=snapshot.digest,
            converter_sha256=assessment_digest(),
            chart_context=context,
            classification=classification,
            blockers=blockers,
            **fields,
        )
        if self.sources.store.has_secret(value.model_dump_json().encode()):
            raise ResearchError("Credential-bearing mapping was rejected before persistence")
        self.store.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.store.directory(identity).mkdir(mode=0o700)
        self.store.write(identity, "assessment.json", value.model_dump(mode="json"))
        self.store.write(identity, "manifest.json", {"sha256": value.digest})
        self.store.event(identity, "blocked", classification)
        return value

    async def open(self, identity):
        await self.sources.require_online()
        directory = self.store.directory(identity)
        if directory.is_symlink() or any(
            (directory / name).is_symlink() for name in ("assessment.json", "manifest.json")
        ):
            raise ResearchError("Source conformance integrity check failed")
        value = SourceConformance.model_validate(self.store.read(identity, "assessment.json"))
        if value.id != identity or value.digest != self.store.read(identity, "manifest.json").get(
            "sha256"
        ):
            raise ResearchError("Source conformance integrity check failed")
        snapshot = self.sources.store.snapshot(value.source_id)
        if snapshot.digest != value.source_snapshot_sha256:
            raise ResearchError("The source differs from this conformance assessment")
        return value


def conformance_markdown(value):
    lines = [
        f"# Source conformance {value.id}",
        "**Execution blocked** · " + value.classification.replace("_", " "),
        "As of " + value.assessed_at.isoformat(),
        f"Source `{value.source_id}` · snapshot `{value.source_snapshot_sha256}`",
        "No executable plan, strategy registration, approval or simulation was created.",
        "## Blockers",
        *("- " + text for text in value.blockers),
    ]
    if value.mapped_rules:
        draft = value.mapped_rules
        lines += [
            "## Recognized rule shape only",
            f"SMA {draft.fast_period}/{draft.slow_period}; fixed {draft.quantity} BTC; "
            "long-only cross entry/exit.",
            "These values describe a rule mapping, not verified Pine/Nautilus equivalence.",
        ]
    for ref in value.rule_references:
        origin = (
            f"source lines {ref.first_line}-{ref.last_line}"
            if ref.origin == "source_piece"
            else "user-declared chart context"
        )
        lines += [
            f"## {ref.field} · {origin}",
            "\n".join("    " + line for line in ref.quote.splitlines()),
        ]
    return "\n\n".join(lines)
