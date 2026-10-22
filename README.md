# stratosphere

**A personal terminal workspace for agent-driven quantitative research and trading development.**

stratosphere is the quantitative-strategy component of **tradingsphere**. The repository stays private. Under [D57](docs/decisions.md), this is for the user's own use: no commercialization, platform commission, hosted application login, multi-user database or automatic community publication.

## Current implementation

The responsive Textual TUI provides Chat, Library, Focus, Tests, Positions and Reports. Tab/Shift+Tab cycle views; the terminal retains its existing size. Model setup lives in chat with masked credentials, explicit provider selection and Markdown replies. The [12 adapters](docs/model-providers.md) include local Ollama; provider protocol tests and actual model reliability remain distinct.

The local SQLite library retains exact strategy identities, provenance, one stable agent per strategy, separate histories, jobs and report references. Approved bounded SMA backtests/studies use NautilusTrader and retain inspectable results, costs, failures and validation/holdout evidence. Sources and model interpretations remain subject to the limitations in their respective records.

[Authored account inspection](docs/paper-inspection.md) and the [reviewed observation worker](docs/observation-service.md) expose retained accounting, reservations, queue recovery, ownership and heartbeat/silence stops. They are fixture/account-processing foundations, not a broker paper service.

Liquid is the only trading connector: read-scope browser authorization, catalog inspection and a separate address diagnostic are implemented. Ownership, real feeds, perpetual paper execution, complete reconciliation and the operational keep/close exit workflow remain unfinished. Prior spot research cannot be relabeled as perpetual evidence. Connecting or inspecting a provider does not grant trading authority.

**Phase 0 is complete within its stated scope. Phases 1–5 remain partial/in progress.** The full personal research/source/paper objective is tracked in [Phases 3–5](docs/phase3-5.md); this cleanup does not mark those phases complete.

## Run the existing client

From the configured repository environment:

```sh
.venv/bin/python -m stratosphere
```

Select the model/provider in the inline form, or dismiss it with Escape to inspect supported non-model operations. Use /help for implemented commands. See [Phase 1 setup](docs/phase1.md#try-it), [Phase 2 research](docs/phase2.md) and [Intel macOS verification](docs/intel-macos.md) for dependencies and limits. Do not paste keys into chat.

Research and reporting retain online gates. Local storage and the explicitly supported Ollama model do not introduce a general offline application mode. No Supabase/application account is required by the TUI.

## Personal workflow and remaining scope

- Preserve sources and missing information; convert external logic only with behavioral conformance.
- Use one agent per exact strategy with shared validated chat/slash operations.
- Review exact test plans and budgets before native execution; retain all attempts and measured evidence.
- Compare variants without transferring holdout results or treating model interpretation as verified fact.
- Progressively verify crypto, equities/ETFs and forex data/account capabilities.
- Verify personal paper operation, risk/accounting, restart, reconciliation and shutdown before any separately authorized live activation.

Broker fees, spreads, slippage, funding and native rounding remain trading costs; there is no stratosphere service fee. Provider data restrictions still apply. Historical protocol bytes and reports remain unchanged even where they mention earlier commercial assumptions.

## Documentation

| Document | Purpose |
| --- | --- |
| [Personal-use scope](docs/personal-use.md) | Current requirements, retired features and the bug-fixing checkpoint |
| [Roadmap](docs/roadmap.md) | Personal phases and completion criteria |
| [Acceptance ledger](docs/phase3-5.md) | Full Phase 3–5 scope, measured increments and missing verification |
| [Architecture](docs/architecture.md) | Local operations, agents, persistence and execution boundaries |
| [Strategy agents](docs/strategy-agents.md) | Stable agents, separate histories and Reports |
| [Research](docs/phase2.md) / [bounded studies](docs/phase3-studies.md) | Approved native tests, variants and immutable evidence |
| [Source intake](docs/phase4-sources.md) / [conformance](docs/source-conformance.md) | Actual material, access limits and conversion blockers |
| [Report inspection](docs/report-evidence.md) / [analysis](docs/report-analysis.md) | Complete sealed measurements and advisory specialist interpretation |
| [Local storage](docs/database-model.md) / [personal library](docs/community-catalog.md) | Strategy identity, provenance and local candidate evidence |
| [Provider connections](docs/authentication-and-connectors.md) | Model/broker access without application login |
| [Liquid testing](docs/liquid-paper-testing.md) | Current read-only diagnostics and remaining integration requirements |
| [Observation worker](docs/observation-service.md) / [account inspection](docs/paper-inspection.md) | Authored queue/lifecycle and read-only health |
| [Data rights](docs/data-source-rights.md) / [capabilities](docs/market-data-capabilities.md) | Permitted purpose, actual profiles and verification limits |
| [Terminal interface](docs/terminal-interface.md) | Responsive panels, onboarding and keyboard interaction |
| [Decision log](docs/decisions.md) / [full documentation index](docs/README.md) | Current decisions and historical records |

Historical Supabase schemas/access proofs and the [former pricing plan](docs/commercial-model.md) are retained as retired material. The scope change does not delete hosted resources, local databases or evidence, and does not change visibility/licensing.

## Current checkpoint

Finish and verify the in-progress personal-use cleanup, then move to the user-led intermediate bug-fixing session. New feed/adapter features are outside this checkpoint. Extensive testing remains required; there is no deadline or claim that personal trading is ready.
