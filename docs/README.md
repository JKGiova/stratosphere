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

## Maintaining the plan

- Update the status when a feature has been implemented and verified.
- Record technical decisions, their rationale, and consequences in the roadmap; introduce dedicated decision records when needed.
- Keep requirements, proposals, and measured results separate.
- Link future tasks and pull requests to the relevant phase and completion criterion.
- Keep only examples without credentials or personal account data in the repository.
- Write all documentation and development material in English and use the lowercase names stratosphere and tradingsphere.
