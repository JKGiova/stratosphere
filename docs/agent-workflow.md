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

## Interrupted tool-runtime work

Recent interrupted development produced a local bounded model → tool → observation → model draft
and tests. It is not part of the published application baseline. Stabilize it before treating it
as complete or layering another framework on top. DeepSeek Harness has not been adopted.

The draft entry points are `/work <task>` and `Work on this strategy: <task>`. They bind the selected
strategy before model waits. Proposed capabilities cover own definition/jobs/provenance, sealed
report facts, source/report reviews, dated assessments and study/test review handoffs.

Default draft limits are eight total model calls, six tool calls and 180 seconds, including nested
model stages. Context/output limits fail explicitly without truncation. Repeated identical calls
are rejected. Run headers/events retain outcomes and safe observations; failed/interrupted work
must never silently replay. `/agent-tools`, `/agent-runs` and `/agent-run <id>` inspect this draft.

## Stabilization acceptance

- Complete success, failure, cancellation, budget exhaustion and restart paths with actual UI controls.
- Preserve owner identity/history across Library navigation, model waits and forms.
- Use exact typed capabilities over existing validated services; reject invented actions/arguments.
- Stop for human study/test approval. A paused approved job offers explicit resume, not reapproval.
- Keep source acquisition limited to explicitly authorized material and preserve original access limits.
- Keep raw simulation tables, credentials, Liquid catalogs and account evidence outside strategy prompts.
- Count nested calls against the same budget and deadline; never retry ambiguous actions automatically.
- Retain inspectable failures without partial chat turns or changed sealed evidence.
- Verify real model task completion, not only valid JSON or valid evidence references.

One installed 4B local model completed an artificial lookup task; the smaller model repeated calls
and was stopped. This is limited behavior evidence, not broad workflow/model reliability.
