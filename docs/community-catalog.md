# Shared catalog and recommendations

## Confirmed behavior

Registered strategies are automatically shared in the catalog. Users can receive recommendations based on successful strategies built by other users and use those strategies as starting points for research.

Exact duplicates reuse one strategy identity and collect additional source associations. Any behavioral variation, even a small one, gets a distinct identity with a lineage link. Catalog sharing, validation status, and personal live activation are independent.

## Proposed publication boundary

Publish the strategy public ID, explanatory name, readable/formal rules, mathematical definition where available, lineage, suitable evidence summaries, and eligible source references. Label drafts and incomplete specifications explicitly; inclusion in the catalog is not endorsement.

Do not publish private chats, source authentication material, broker credentials, personal account identifiers, capital/risk configuration, or raw account records. Store required private records under user access controls. Public author attribution, private-source handling, redaction, and content-use policy still need definition. A retrieved source is not automatically licensed for redistribution.

Deduplication must not expose confidential content from another user's records. Shared matching operates on the publishable registered specification. Unregistered transient drafts need a defined retention/publication policy before implementation.

## Recommendation evidence

Avoid a leaderboard that sorts only by return. Proposed factors include:

- Compatibility with requested instruments, data, timeframes, and provider capabilities.
- User risk/holding preferences and current market context.
- Reproducible out-of-sample results net of realistic costs.
- Drawdown, robustness, sample length, trade count, and result concentration.
- Distinction between backtests, paper observations, and verified live results.
- Evidence freshness, coverage, and number of attempted variants.

Scoring weights and verification requirements remain open. If evidence is insufficient, label a strategy as a research candidate rather than claiming proven success. Source claims or unverified user metrics do not become verified platform results.

## Reuse and personal operation

Selecting a catalog strategy retrieves its exact specification, sources, conforming implementations, and accessible runs. Keep personal instances separate from global strategy records.

Selecting the same rules for another account does not duplicate the strategy. A user-created behavioral fork gets a new strategy ID. A new source alone does not. Past performance from one asset or protocol is not transferred to another without evaluation.

Changes by another author never silently replace a user's pinned implementation or live configuration. Registration and recommendation do not authorize orders; paper/live promotion still follows validation and explicit activation.

## Shared service

The catalog uses Supabase-managed PostgreSQL. A catalog service is the proposed boundary between terminal clients and shared records; direct Data API access is not yet selected. Clients must not receive unrestricted database access or privileged secret/service-role keys. Supabase Auth OAuth login is selected independently of trading connections. Provider selection and session/authorization integration, application-service hosting, synchronization, quotas, and outage behavior remain open. Define RLS and least-privilege grants before exposing tables; automatic strategy sharing does not make personal records public.

Before launch, define moderation, duplicate abuse handling, source rights, deletion/retention rules, and what performance summaries can be published. The catalog is included in product scope; a paid strategy marketplace is not.
