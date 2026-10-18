# Supabase PostgreSQL database model

Current implementation: the [private Supabase foundation](../supabase/README.md) is provisioned and ownership-tested. The TUI currently uses a private local development registry for agent identities/histories and research references; authenticated hosted synchronization remains pending. Tables and relationships beyond that foundation below remain planned until their corresponding implementation is verified.

## Scope and status

**Confirmed:** Supabase provides the managed PostgreSQL database that stores the shared strategy registry, exact strategy identity, accumulated provenance, and records needed to reuse prior research. This is a proposed logical schema with field meanings, not executable SQL or an applied migration.

Separate strategy definitions, implementations, experiments, applicability assessments, and personal deployments. Registered strategy definitions are automatically shared; personal data is not.

## Identity and specification

Each complete strategy has an internal key and a long random numeric public ID. Phase 0 selects 24 random decimal digits, preserved as strings including leading zeroes, and UUIDv7 internal keys. Public IDs are not sequential counters or authorization tokens. Generate them randomly, enforce uniqueness, and retry collisions. Only the contract/generation helper exists; persistent constraints and transaction handling remain Phase 1 work.

The exact specification includes parameters, mandatory market/asset filters, timing, exits, behavioral sizing, and missing-data behavior. A chosen deployment asset/capital does not change identity if the specification explicitly permits that choice. Fixed strategy parameters or a fixed instrument constraint do change identity when altered.

### formal_spec groups

| Group | Meaning |
| --- | --- |
| indicators | Named/versioned indicator definitions, inputs, formulas, and parameters |
| entry_rules | Entry conditions, direction, combinations, and precedence |
| exit_rules | Stops, profit targets, signal exits, time exits, and precedence |
| position_sizing | Behavioral sizing rules and parameter definitions, if part of the strategy |
| signal_timing | Bar-close/intrabar/event semantics and order timing |
| timeframes | Required timeframes or explicitly allowed configurable choices |
| market_conditions | Mandatory regime, volatility, volume, or liquidity filters |
| instrument_constraints | Required asset categories, fixed instruments, or instrument properties |
| data_requirements | Inputs, frequency, warm-up, and availability requirements |
| execution_requirements | Shorting, order types, lot semantics, and other required capabilities |
| missing_data_policy | Behavior when inputs are absent or stale |
| assumptions | Explicit interpretations added during reconstruction |

Store structured documents using validated schemas, proposed as JSONB for flexible fields. Mathematical prose is explanatory; it cannot replace the executable specification.

## Core tables

Fields below describe the application model; exact SQL types, defaults, enums, and migrations remain to be finalized.

### users

| Field | Meaning |
| --- | --- |
| user_id | Stable application user/profile key, independent of any broker identity |
| auth_user_id | Proposed unique reference to Supabase auth.users.id, verified through the application session |
| username | Public-facing user name; uniqueness/case policy to define |
| created_at | Registration timestamp |
| preferences | General market, risk, holding-horizon, and interaction preferences; not strategy rules |

Supabase Auth owns login identities and authentication sessions. This users table is an application profile, not a separate password database. The proposed auth_user_id is unique and references auth.users; ownership checks resolve the verified Supabase identity to the application user_id. A client-supplied user/broker ID is not authentication. Provider identity linking, deletion/retention, and secret storage still require detailed design. Do not place broker secrets in preferences.

### strategies

| Field | Meaning |
| --- | --- |
| strategy_id | Internal strategy key |
| public_id | Unique long random numeric identifier shown in the TUI |
| name | Explanatory name; not an identity key |
| description | Human-readable explanation |
| formal_spec | Complete structured behavioral specification |
| mathematical_definition | Indicator formulas and mathematical conditions where representable |
| spec_schema_version | Version of the specification format |
| spec_hash | Digest of a complete canonical specification under a defined canonicalization policy |
| parent_strategy_id | Optional lineage link to the strategy this variant was derived from |
| created_by | First registrant; does not erase later contributors |
| visibility | Shared/catalog-visible for registered strategies, not an opt-in/private setting |
| status | Definition readiness, such as draft, specified, implemented, or retired; not global live state |
| created_at | Creation timestamp |

Behavioral identity is immutable after registration. Name/description corrections can be tracked without changing rules. Unresolved drafts require an explicit readiness marker and are not eligible for exact deduplication or claims of validation.

### sources

| Field | Meaning |
| --- | --- |
| source_id | Source snapshot key |
| platform | YouTube, Instagram, X, TradingView, GitHub, article, or direct input |
| url | Original address when available |
| title | Source title or identifying label |
| author | Source author when known |
| published_at | Publication timestamp when known |
| retrieved_at | Acquisition timestamp |
| content_ref | Location of retained text, transcript, images, or code |
| content_hash | Digest of acquired content |
| access_status | Complete, partial, or inaccessible acquisition |

Retain snapshots when a URL's content changes. Credentials are excluded; access rights and redistribution eligibility need metadata/policy before launch. A changed snapshot must not overwrite evidence used by a prior run.

### strategy_sources

| Field | Meaning |
| --- | --- |
| strategy_source_id | Association key |
| strategy_id | Linked strategy |
| source_id | Linked source snapshot |
| evidence_map | Links between rule identifiers and passages, timestamps, or code lines |
| relationship_type | Complete description, partial description, example, or reference |
| verification_status | Proposed or verified association |
| added_by | User adding the association |
| created_at | Association timestamp |

Enforce one association per strategy/source snapshot; preserve new submission records when a link already exists. Partial sources can corroborate individual rules without proving full equivalence.

### strategy_submissions

| Field | Meaning |
| --- | --- |
| submission_id | Submission key |
| user_id | Submitting user |
| original_request | Original user request, with private access and redaction rules |
| source_id | Optional initial source; multiple sources use association records |
| strategy_id | Resolved identity; nullable while unresolved |
| resolution | New strategy, exact duplicate, variant, or needs clarification |
| resolution_reason | Reason and comparison evidence |
| created_at | Submission timestamp |

Keep contributor history even when the submission is an exact duplicate. Original chat/request text is not automatically public.

### datasets

| Field | Meaning |
| --- | --- |
| dataset_id | Immutable data snapshot key |
| provider | Data provider |
| instruments | Included instruments and identifiers |
| data_type | Bars, ticks, order book, or alternative data |
| granularity | Data frequency/granularity |
| start_at / end_at | Covered interval |
| storage_ref | Location of retained data files |
| content_hash | Snapshot digest |
| metadata | Timezone, transformations, schemas, and availability semantics |
| quality_report | Gaps, duplicates, and other findings |

Large data lives in versioned files/object storage; PostgreSQL retains references and metadata. Prices available now cannot substitute for the historical snapshot of an experiment.

### experiment_runs

| Field | Meaning |
| --- | --- |
| experiment_id | Run key |
| strategy_id | Evaluated strategy identity |
| requested_by | Requesting user |
| implementation_ref | Exact implementation, engine/runtime, and dependency manifest reference |
| dataset_id | Data snapshot reference |
| mode | Backtest, replay, forward test, or paper |
| configuration | Instruments, simulated capital, dates, fees, spread, slippage, and settings |
| validation_protocol | Development/validation/holdout windows, baseline, and criteria |
| metrics | Values computed by the engine |
| artifacts_ref | Trades, equity curve, logs, and report location |
| findings | Empirical observations, regime definitions, limitations, and rationale |
| status | Queued, running, completed, failed, or canceled |
| started_at / finished_at | Run timestamps |

Record distinct runs rather than overwriting results. A run requiring multiple datasets needs a run-dataset association in the physical schema; a single dataset_id is only the primary snapshot. Live/paper evidence requires retained input/event windows, not just a provider name.

### strategy_assessments

| Field | Meaning |
| --- | --- |
| assessment_id | Applicability assessment key |
| strategy_id | Assessed strategy |
| user_id | User whose constraints are considered |
| instrument | Instrument or universe considered |
| assessed_at | Assessment timestamp |
| market_snapshot_ref | Current context used: trend, volatility, spreads, liquidity, and feed timestamps |
| user_constraints_snapshot | Exact personal constraints considered |
| evidence_refs | Supporting experiment/implementation references |
| compatibility_checks | Required data, instrument, and provider capability checks |
| conclusion | Compatible, incompatible, or insufficient evidence, with current-context suitability distinguished |
| reasoning | Explanation, uncertainties, and proposed tests |
| expires_at | Optional expiry; relevant new events can invalidate an assessment sooner |

Technical compatibility is not evidence of expected profitability. Keep mandatory rule checks separate from empirical suitability. Assessments are time-bound; they are not permanent properties of the strategy.

### strategy_instances

| Field | Meaning |
| --- | --- |
| instance_id | Personal usage/deployment key |
| user_id | Owner |
| strategy_id | Selected shared identity |
| implementation_ref | Pinned implementation |
| selected_instruments | Deployment choices explicitly permitted by the specification |
| account_ref | Account reference, without credentials |
| capital_allocation | Personal allocated capital |
| risk_limits | Additional personal limits |
| mode | Observation, paper, or live |
| status | Configured, active, suspended, or deactivated |
| created_at / activated_at | Creation and activation timestamps |

Changing account or allocated capital is configuration, not a new shared strategy. Changing behavioral sizing/entry/exit rules creates a variant. Instance state is per user and separate from catalog readiness.

## Relationships

| Relationship | Cardinality |
| --- | --- |
| users → strategies | One first registrant to many strategy records |
| users → strategy_submissions | One user to many submissions |
| strategies ↔ sources | Many-to-many through strategy_sources |
| strategies → strategies | Optional parent with multiple variants |
| strategies → experiment_runs | One identity to many runs |
| datasets → experiment_runs | One snapshot to many runs; multi-input runs need an association |
| strategies → strategy_assessments | One identity to many dated assessments |
| users/strategies → strategy_instances | Each personal instance belongs to one user and one identity |

Use foreign keys and appropriate indexes on queried association/ownership paths. References written as artifact locations are not a substitute for relational integrity for records stored in PostgreSQL.

## Exact deduplication and concurrency

1. Resolve ambiguities or keep an explicitly incomplete draft.
2. Validate and canonicalize the complete specification without rounding or dropping behavior.
3. Use similarity search only to find candidates.
4. Compare complete canonical specifications; mathematical equivalence is not assumed from similar prose.
5. In one transaction, reuse the identical identity or create a new one, then associate sources and record the submission.
6. On a concurrent uniqueness conflict, reload and compare; do not create accidental duplicate strategies.

Phase 0 selects and tests [strategy/1 canonicalization](phase0-contracts.md). Hash equality is a lookup optimization; compare full canonical specifications before accepting equality. Enforce the selected digest/schema uniqueness with conflict reload/comparison for complete records. Treat an unexpected hash collision as an error requiring resolution, not as an identical strategy. Names, authors, sources and performance are excluded from the behavioral fingerprint. The physical schema and transactional race verification remain unimplemented.

## Conditions versus evidence

| Information | Storage |
| --- | --- |
| Mandatory market/asset conditions | strategies.formal_spec |
| Empirical regime performance | experiment_runs.findings and artifacts |
| Personal preferences and mandate | users.preferences and strategy_instances |
| Current asset context and recommendation reasoning | strategy_assessments with dated snapshots |

Reuse stored evidence only under its original configuration. A different fixed parameter produces a new strategy; a different permitted test/deployment choice produces a new run/instance.

## Additional implementation records

The physical schema will also need implementation manifests, durable jobs/checkpoints/interventions, conversations, market snapshots, account references, and order/fill/position/portfolio-decision records. These are required design work, not already implemented tables.

Normalize integrity-critical relationships and define access controls. Use JSONB for schema-validated flexible structures, with indexes driven by actual queries rather than blanket indexing every document. Use timezone-aware timestamps and explicit precision for financial values. Avoid destructive cascades through immutable provenance and research history; deletion/retention policies remain open.

Supabase is confirmed as the managed PostgreSQL platform. The logical schema is unchanged. Supabase Auth with OAuth login is confirmed. The organization and Frankfurt region are selected. OAuth provider/callback configuration, Storage, other platform products, project plan/provisioning, migrations, and extension choices remain to be specified.

Before exposing tables through the Data API, define least-privilege grants and enable RLS on exposed schemas. Shared strategies and private submissions, preferences, assessments, and instances need different policies. Keep privileged secret/service-role keys and database passwords on trusted backend infrastructure, never in distributed terminal clients or agent inputs. The original conceptual model did not provision infrastructure. The current [private Supabase schema](../supabase/README.md) is provisioned with 13 RLS-enabled tables; authenticated application synchronization remains pending.

Security reference: [Supabase data access guidance](https://supabase.com/docs/guides/database/secure-data).

## Optional account connections

Proposed account_connections fields are described in [authentication and connectors](authentication-and-connectors.md). Each connection belongs to an application user and records the provider, verified external principal/account references, environment, granted permissions, secret-storage reference, and lifecycle state. Linking/disconnecting a broker does not change application identity or shared strategy ownership.

Map strategy_instances.account_ref to the corresponding authorized account record in the physical schema. A connection may expose multiple accounts; account rows and permission/environment checks must be modeled explicitly rather than using a free-form ID as authority. Tokens are private secrets, not ordinary JSON profile fields, and are excluded from shared catalog data.

The [local applicability increment](phase3-applicability.md) now retains identity-bound dated findings with immutable payloads and exact-variant evidence references. The Supabase assessments table also rejects updates/deletion through an invoker trigger with a fixed empty search path; ownership isolation is verified with rolled-back fixtures. This is database conformance, not login/session or client synchronization verification.
