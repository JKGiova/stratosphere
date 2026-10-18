# stratosphere plan

This directory contains the initial plan for **stratosphere**, the component of the **tradingsphere** suite dedicated to agent-driven quantitative strategies.

**Status:** Phase 0 complete, October 14, 2026. [Compatibility and contract proofs](phase0.md) are verified within their stated limits; [Phase 1](phase1.md) is in progress with its first terminal/model-chat increment; [Phase 2](phase2.md) has begun with the reviewed BTC/USDT hourly SMA research workflow; Phases 3–5 are in progress with strategy agents, the private database foundation and bounded local SMA studies; Phases 6–7 remain unimplemented. The [decision log](decisions.md) separates product requirements, selected implementation decisions and open proposals. Detailed physical schema, production components and operational criteria remain implementation work.

## Reading order

1. [Product vision and plan](product-plan.md): what the platform should enable.
2. [Architecture and agents](architecture.md): how to separate research, testing, management, and execution.
3. [Sources and strategy lifecycle](strategy-lifecycle.md): how to turn content into versioned strategies.
4. [Data and validation](testing-and-validation.md): how to produce comparable evidence.
5. [Portfolio and execution](portfolio-and-execution.md): how to move from signals to live positions.
6. [Database model](database-model.md): tables, fields, identity, provenance, and evidence.
7. [Terminal interface](terminal-interface.md): chat, slash commands, and workflow intervention.
8. [Community catalog](community-catalog.md): automatic sharing and evidence-based recommendations.
9. [Decision log](decisions.md): confirmed decisions versus proposals and open items.
10. [Authentication and connectors](authentication-and-connectors.md): application login versus trading authorization.
11. [Roadmap](roadmap.md): implementation phases and completion criteria.
12. [Conversation log](conversation-log.md): full historical chat in its original language, maintained as this conversation continues.
13. [Phase 0 verification](phase0.md), [canonical contracts](phase0-contracts.md) and [access boundary](phase0-access-boundary.md): selected decisions, example protocol, commands, measured tests and limits.

## Maintaining the plan

- Update the status when a feature has been implemented and verified.
- Record technical decisions, their rationale, and consequences in the roadmap; introduce dedicated decision records when needed.
- Keep requirements, proposals, and measured results separate.
- Link future tasks and pull requests to the relevant phase and completion criterion.
- Keep only examples without credentials or personal account data in the repository.
- Write all development material and plan documentation in English and use the lowercase names stratosphere and tradingsphere. The user-authorized conversation log preserves original language and historical spellings.
- Append new user-visible exchanges to conversation-log.md during subsequent responses in the same planning conversation, as specified in AGENTS.md.

## Roadmap refinement

See [planning questions](planning-questions.md) for the numbered questionnaire covering decisions not yet settled. The first answer batch is recorded in decisions.md (D20–D33). Unanswered questions remain open.

Additional planning records: [commercial model](commercial-model.md) and [Supabase setup](supabase-setup.md).

See [research and release proposals](research-and-release-proposals.md) for the next payment/data/operation choices and explanations of research budgets and release gates.

See [Phase 1 implementation progress](phase1.md) for current client capabilities, commands, test evidence and remaining increments.

See [Phase 2 research progress](phase2.md) for implemented source/specification review, approval, native worker, reports, test evidence and remaining work.

See [model providers](model-providers.md) for all 12 adapters, credential environment variables, official/local API addresses and verification limits.

See [strategy agents](strategy-agents.md) for orchestration/Reports and the [Phase 3–5 acceptance ledger](phase3-5.md) for the full authorized scope and remaining work.

See [bounded studies](phase3-studies.md) for reviewed experiments, validation-only selection and holdout/exposure controls, and [data-source rights](data-source-rights.md) for the commercial dataset dependency.

See [dated applicability](phase3-applicability.md) for exact-variant evidence, current-context uncertainty, private findings and local research candidate commands.
