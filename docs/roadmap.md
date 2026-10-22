# Roadmap and completion criteria

Last updated: October 22, 2026. stratosphere is now a **personal tool**, as confirmed in [D57](decisions.md). The repository stays private. Local SQLite stores strategies, provenance, agent histories and report references; private files/account journals retain evidence and operational state. Hosted user databases, application login, automatic publication and platform billing are retired requirements.

## Current checkpoint

Phase 0 is complete within its [verified scope](phase0.md). Phases 1–5 have implemented increments, not complete phases. The [Phase 3–5 ledger](phase3-5.md) retains the full personal research/source/paper requirements and distinguishes software fixtures from provider verification.

The personal-use documentation/UI cleanup is complete within its [verified scope](personal-use.md#cleanup-verification--october-22-2026). The next session is the user-led intermediate bug-fixing session. Do not start a new feed, order adapter, strategy family or other feature increment at this checkpoint.

## Active phases

| Phase | Scope | Completion criterion |
| --- | --- | --- |
| 0 — Compatibility and contracts | Pinned Python/engine, Textual, canonical identities and bounded proofs | Measured platform/contract evidence within the documented limits |
| 1 — TUI, local library and sources | English Linux/macOS TUI, model setup, chief-agent chat, slash commands, local strategy identities and all five source integrations | Persistent local deduplication/provenance/agent isolation and verified source access/failure behavior; no application login |
| [2 — Research](phase2.md) | Source or idea → specification → conforming implementation → NautilusTrader backtest → report | Reproducible costs, exact rules, immutable references and user approval |
| 3 — Iteration and local recommendations | Bounded variants, validation-only selection, holdout discipline and dated local candidates | All attempts retained; compatibility, empirical evidence and current context remain distinct |
| 4 — Source maturity and market coverage | YouTube, Instagram, X, TradingView and GitHub acquisition/conversion; progressive market adapters | Exact source provenance, behavioral conformance, provider failures and permitted personal data use verified |
| 5 — Live data and paper | Eligible feed, persistent runtime, budget/risk proposals, deterministic controls and broker paper adapter | Sustained operation, native accounting, restart/reconciliation, concurrent-strategy attribution and keep/close exit workflow verified |
| 6 — Personal live execution | Verified provider adapter, mandate, one order gateway and deterministic controls | New explicit activation authorization plus partial-fill, outage, risk, reconciliation and shutdown checks; no platform billing |
| 7 — Operational evolution | Broader adapters, drift monitoring and controlled implementation changes | Consistent portfolio attribution and no silent changes to pinned instances |

The current authorization covers Phases 3–5, not live orders. Retiring commercial features does not complete source conversion, feed entitlement, paper execution or the rest of those phases.

## Personal application boundaries

- Keep the responsive TUI, one stable agent per exact strategy, separate histories and inspectable Reports.
- Preserve immutable specifications, exact duplicate reuse, provenance and variant lineage in the local library.
- First-run setup selects a model; provider keys and optional broker connections remain separate. No application account, hosted login or user-profile database is required.
- Keep online operation and the selected local Ollama exception. Local storage does not introduce a general offline product mode.
- Alpaca is first for Bitcoin spot paper development; Liquid perpetuals follow later. BTC/USD remains separate from prior BTC/USDT evidence.
- No stratosphere service commission is planned. Broker/exchange fees, spreads, slippage and funding still belong in tests and execution accounting.
- Preserve the settled TUI exit choice: keep positions or request closure of all, with actual outcomes and residual exposure visible.
- Keep credentials outside models, acquired source execution and native worker environments.

## Remaining implementation decisions

| Detail | Required before |
| --- | --- |
| Actual account/feed eligibility, native increments, provenance and permitted personal use | New market-data or broker integration |
| Wider strategy families and faithful external-code/media conversion | Advertising supported conversion or execution |
| Model interpretation quality and bounded research budgets | Broader agent-operated experiments |
| Persistent runtime/portfolio reconciliation and sustained tests | Complete paper operation |
| Risk limits, netting, allocation and emergency policies | Paper/live operation as applicable |
| Generated-code isolation and executable identity | Running externally acquired/generated implementations |
| Local evidence backup/recovery and secure provider credential lifecycle | Personal operational readiness |
| Verified suite interfaces | Any future tradingsphere component integration |

## Testing and readiness

There is no deadline. Extensive testing remains required for personal operation, including source/conversion fidelity, reproducibility, agent isolation, broker accounting, risk gates, outages, restart and shutdown behavior. A high return or a green fixture suite alone is not readiness.

Historical commercial pricing, hosted access proofs and Supabase provisioning records are retained as retired material. They are not deferred prerequisites. See [personal-use scope](personal-use.md), the [decision history](decisions.md) and [provider data rights](data-source-rights.md).
