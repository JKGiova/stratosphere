# Agent workflow

## Current structure

The orchestrator recognizes explicit operations or asks the configured model for a validated route.
It delegates strategy work to one stable agent identity per exact registered definition. All
specialists share implementation/provider code while keeping separate local histories and evidence.
They are sessions, not separately trained models or continuously running processes.

Ordinary chat uses a one-response LangGraph path. Dedicated application workflows cover analysis,
source reviews, bounded study proposals, dated assessments, report inspection and measured-report
interpretation. Budget/risk reviews use separate explicit account contexts. Approval, registration,
account diagnostics and native execution remain application-owned operations.

## Explicit strategy creation

Use `/create <brief>` or start chat with “Create a strategy …”. This generates an independent
BTC/USDT hourly spot, long-only SMA crossover candidate. The model proposes complete periods,
fixed BTC quantity, a descriptive name and a falsifiable hypothesis. Unsupported or ambiguous
proposals stop with questions; malformed output cannot register a strategy. External URLs/code
belong in source intake, and existing strategy variants belong in `/study`.

Application code validates and compiles the proposal using the supported implementation. It
retains the original brief, model reference, proposal, schema/prompt hashes and generated origin.
Rendered declarations are explicitly model-generated, not external quotes or faithful-conversion
proof. Registration atomically binds the exact strategy, its stable agent and a pending research
job. Exact duplicates reuse the existing strategy/agent and retain the additional provenance.
Different independent definitions receive separate agents; this is not a variant-lineage shortcut.

The owning agent immediately critiques the untested candidate with its own history/evidence.
Creation uses at most one proposal call and one initial analysis call, each capped at 180 seconds,
without automatic retries or fallback. Failed/canceled attempts remain under `research/creations/`;
complete plans remain inspectable through `/jobs` and `/show`, including after restart. A failed
analysis leaves the plan available for review without committing an incomplete agent turn.

The handoff displays the exact January 2025 archive, capital, costs, implementation and two-run
base/stress plan. Human approval of its digest remains mandatory before native execution. Approval
and `/resume` use the existing owning-agent research path. There is no automatic optimization,
holdout use, execution or profitability claim. Models cannot approve their own proposals.

## Deferred general runtime

The generic `/work` model/tool loop was removed from the working tree at the user's request.
It is not available. The narrow creation workflow does not adopt DeepSeek Harness or restore that
runtime. Autonomous repeated experiments and broader strategy families remain separate work.
