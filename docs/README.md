# stratosphere plan

This directory contains the current personal-use plan and historical development records for **stratosphere**, the component of **tradingsphere** dedicated to agent-driven quantitative strategies. [Personal-use scope](personal-use.md), D57 and the current roadmap supersede earlier hosted/commercial requirements.

**Status:** Phase 0 complete, October 14, 2026, within its [compatibility/contract proof limits](phase0.md). [Phase 1](phase1.md) has responsive TUI/model/source increments; [Phase 2](phase2.md) has the reviewed BTC/USDT hourly SMA workflow; Phases 3–5 have stable strategy agents, local storage, bounded studies, source/report readers and authored observation/accounting increments. These are partial phases: real feeds, broker paper orders/reconciliation and broader source/conversion coverage remain unfinished. Phases 6–7 are unimplemented. The [personal-use cleanup](personal-use.md) is verified; feature work stops for the intermediate bug-fixing session. The [decision log](decisions.md) and [Phase 3–5 ledger](phase3-5.md) distinguish requirements from measured scope.

## Reading order

1. [Product vision and plan](product-plan.md): what the platform should enable.
2. [Architecture and agents](architecture.md): how to separate research, testing, management, and execution.
3. [Sources and strategy lifecycle](strategy-lifecycle.md): how to turn content into versioned strategies.
4. [Data and validation](testing-and-validation.md): how to produce comparable evidence.
5. [Portfolio and execution](portfolio-and-execution.md): how to move from signals to live positions.
6. [Database model](database-model.md): tables, fields, identity, provenance, and evidence.
7. [Terminal interface](terminal-interface.md): chat, slash commands, and workflow intervention.
8. [Personal strategy library](community-catalog.md): local evidence/reuse, with the retired community plan retained historically.
9. [Decision log](decisions.md): confirmed decisions versus proposals and open items.
10. [Personal application and connectors](authentication-and-connectors.md): provider authorization without application login.
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

See [planning questions](planning-questions.md) for the historical questionnaire and decisions.md for its answers. D57 retires the hosted/commercial questions; remaining technical and operating decisions follow the current roadmap.

Historical records, retired as current requirements: [commercial model](commercial-model.md) and [Supabase setup](supabase-setup.md). They do not authorize hosted-resource deletion.

See [research and release proposals](research-and-release-proposals.md) for historical payment/data/operation proposals and explanations of research budgets and release gates. Payment collection is retired.

See [Phase 1 implementation progress](phase1.md) for current client capabilities, commands, test evidence and remaining increments.

See [Phase 2 research progress](phase2.md) for implemented source/specification review, approval, native worker, reports, test evidence and remaining work.

See [model providers](model-providers.md) for all 12 adapters, credential environment variables, official/local API addresses and verification limits.

See [strategy agents](strategy-agents.md) for orchestration/Reports and the [Phase 3–5 acceptance ledger](phase3-5.md) for the full authorized scope and remaining work.

See [bounded studies](phase3-studies.md) for reviewed experiments, validation-only selection and holdout/exposure controls, and [data-source rights](data-source-rights.md) for provider-purpose restrictions and the historical commercial findings.

See [dated applicability](phase3-applicability.md) for exact-variant evidence, current-context uncertainty, private findings and local research candidate commands.

See [Liquid paper testing](liquid-paper-testing.md) for the later perpetual route, read-only browser connection and the remaining account/paper verification. Connecting the catalog does not activate trading or implement a paper adapter.

See [authored-paper ledger](phase5-paper-ledger.md) for internal durable reservations, virtual attribution and native currency checks. The production feed/runtime and operational paper gateway remain pending.

See [source reviews](source-reviews.md) for specialist review of retained snapshots, exact quote references and private Reports integration. This does not enable converted execution or testing of extracted rules.

Liquid’s [catalog inspection and private export](liquid-paper-testing.md#catalog-inspection-and-export) expose observed tool definitions for the next connector-verification step. They do not call provider tools or implement paper execution.

See [registry-anchored studies](phase3-registry-studies.md) for direct studies of registered definitions/variants, editable simulation costs, versioned anchors and native verification without a dummy parent research job.

See [reviewed closed-bar processing](phase5-closed-bars.md) for the internal authored stream journal, exact review, gap/freshness gates and fresh-process continuation. It adds no real feed or order delivery.

See [reviewed authored signal decisions](phase5-signal-decisions.md) for position-aware reservations, shared exposure/loss gates and one-time delivery claims. Provider transport remains unimplemented. The [authored account reviews](phase5-paper-reviews.md) add separate explicit advisory budget/risk roles over read-only existing journals; they cannot alter accounts or approve execution.

See [Liquid account diagnostics](liquid-account-diagnostics.md) for the explicit fixed address-read operation, private historical evidence and remaining actual-provider/ownership/environment verification. Catalog inspection stays non-invoking.

See [measured report analysis](report-analysis.md) for owning-agent interpretation of frozen simulation facts, complete evidence paging, immutable private Reports and local-model context/reliability limits.

See [authored account inspection](paper-inspection.md) for explicit read-only positions, orders, fills and health, exact retained economics, dated pagination and responsive navigation without model disclosure or account creation.

See [Alpaca paper development](alpaca-paper-testing.md) for the selected first Bitcoin-spot paper route, masked inline credentials and fixed paper account/asset diagnostics. The Liquid provider observation/current-catalog check remains separate from future perpetual support and account/execution authority.

See [authored observation service](observation-service.md) for reviewed standalone process ownership, durable delivery/crash recovery, silence/heartbeat stops and read-only health. This service has no provider feed or order transport.
