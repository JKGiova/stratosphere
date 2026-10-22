# Personal-use scope

Confirmed in [D57](decisions.md): stratosphere is now for the user's own use. The earlier commercial/shared-service plan is retired where it conflicts with this decision.

## Current requirements

Keep the private repository, English responsive TUI, selectable model providers, local Ollama option, one stable agent per exact strategy, isolated histories, source provenance, exact approvals, reproducible NautilusTrader evidence and Reports. Retain progressive source/market coverage, deterministic portfolio/risk controls, paper verification and the settled keep/request-all-closures exit choice.

The application remains online as previously requested. Personal use does not add a general offline mode, remove provider authentication or authorize live orders. Broker account access and market-data eligibility still require their own verification.

## Retired requirements

- Selling the application and the 0.5% opening-trade platform fee capped at $1.
- Platform billing, payment collection, fee-credit balances and billing-dependent operation.
- Hosted application login, Google/GitHub application identities and a user-profile database.
- A shared hosted registry/API, automatic strategy publication, cross-user discovery and moderation.
- Hosted synchronization and multi-user ownership tests as release prerequisites for this personal application.

Broker/exchange commissions, data/model charges, spreads, slippage, funding and native rounding remain real costs. No paid capacity or purchase is authorized by this change.

## Local storage remains

The TUI already uses private local SQLite for exact strategy identities, provenance, agent histories, jobs and report references. Private files retain sources and measured artifacts; separate account/service journals retain authored observations, reservations and runtime evidence. Removing those stores would lose the application's memory and audit trail. They remain the personal application's storage, without a hosted users table or login service.

Historical public-ID names remain stable identifiers, not a publication operation. Old canonical contracts, SQL migrations and sealed report bytes stay readable and unchanged. Some frozen v1 research assumptions mention a future platform fee; that historical wording does not enable a fee and must not be rewritten inside sealed evidence. Current personal operation has no stratosphere service charge; simulation fee fields still represent reviewed trading costs.

## Hosted resources and historical documents

The previously provisioned Supabase project and its tested schema are historical development resources. This cleanup does not inspect their current health or delete them. Their setup/migration records remain archived. No hosted database or local evidence is destroyed, and repository visibility/licensing remains unchanged.

Historical verification paragraphs retain what was tested at that time. They do not reinstate retired hosted-login, sharing or billing acceptance requirements. The current [roadmap](roadmap.md) and [Phase 3–5 ledger](phase3-5.md) control active scope.

## Bug-fixing checkpoint

The subsequent D58 request removes Alpaca and keeps only Liquid. Its prior diagnostic/source references below describe the completed cleanup's historical context; they do not restore that connector. Liquid feed/paper implementation remains unfinished, and spot research evidence stays distinct from perpetuals.

The user's current instruction is to finish in-progress work and then start an intermediate bug-fixing session. This personal-use transition closes that in-progress work; verification is recorded below. The reviewed authored-worker increment is already committed and verified. Alpaca stream validation/acquisition was investigated but no implementation was started; real feeds and broker paper execution remain future feature work. New feature increments stop at this checkpoint for the user-led bug-fixing session.

## Cleanup verification — October 22, 2026

The updated TUI/strategy/research/data checks passed **103 tests**. Ruff check, formatting and diff checks passed. An independently installed wheel was exercised in two fresh processes at 180×48 and 45×12: inline model onboarding, local-library navigation and historical report inspection worked without application login or constructing a model provider. The online gate was an authored fixture in this check, not a real connectivity/provider test.

The copied historical workspace retained exact strategy/agent identities, private histories, two analyses with 48 facts each and ten measured study cells. All retained artifact files stayed byte-identical; local database records stayed equivalent apart from the existing report-index refresh timestamp and JSON serialization order. The five research/study/runtime/decision/service implementation digests, 17 canonical examples/contracts/migrations and wheel source files matched their pre-cleanup pins. Package inspection excluded configured credentials, private reports and environment files.

No native simulation, model request, provider request or order was started for this cleanup. This evidence verifies the presentation/scope transition and historical readers; it does not complete a phase, test a real feed or establish broker execution readiness.
