# stratosphere plan

This directory contains the initial plan for **stratosphere**, the component of the **tradingsphere** suite dedicated to agent-driven quantitative strategies.

**Status:** planning update, October 13, 2026. No application functionality has been implemented yet. The [decision log](decisions.md) records confirmed product and technology choices. Detailed schema, component boundaries, phase ordering, and operational criteria remain implementation proposals unless explicitly marked confirmed.

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
