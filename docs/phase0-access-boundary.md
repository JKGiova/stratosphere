# Phase 0 shared-service access boundary

Decision: use a hosted application API over HTTPS/JSON between the distributed TUI/local workers and the shared registry. Supabase remains the database/Auth provider. Internal registry tables are not directly exposed to terminal clients. The API implementation framework/hosting, migrations and OAuth login are deferred. No service, authentication integration, Supabase project or database policy is deployed by Phase 0.

## Responsibility and authority

| Boundary | Responsibility |
| --- | --- |
| TUI/local research worker | Submit validated documents, display shared DTOs, keep model keys and optional broker credentials local, compute experiments locally |
| Hosted API | Verify the future Supabase session, map its subject to the application user, authorize every operation, resolve exact identity/provenance transactionally, publish explicit catalog projections |
| Supabase/PostgreSQL | Store authoritative shared identities and private records, enforce uniqueness/foreign keys and least-privilege grants/RLS |
| Generated-code runner | Later isolated compute with approved data/contracts; no database, service-role, session, model or broker secrets |

Clients cannot select the registrant/owner, immutable strategy ID, readiness badge, catalog visibility or approved source-publication decisions. Client-supplied IDs, broker IDs, decoded JWT JSON and user-editable user_metadata are not authority. Verified registration automatically shares the definition; private original requests, source bodies, conversations, accounts, preferences, assessments and deployments remain separate. No broker is needed for catalog or research access.

The API requires authenticated user access for shared catalog reads and private operations before real multi-user exposure. Login remains deferred; the development harness supplies explicit test principals and cannot authenticate a human. Anonymous/unauthenticated catalog deployment is not introduced. Session/token verification, issuer/audience/expiry checks, signing-key rotation, revocation and authorization-to-profile mapping are mandatory future integration tests. Do not infer production security from pure policy fixtures.

Backend database credentials stay on trusted infrastructure. Select a narrowly privileged application database role without BYPASSRLS; set trusted identity context per transaction, reset it through connection pooling, and test isolation. Do not propagate arbitrary client claims into that context. Administrative migration credentials are separate. If any objects later enter an exposed Data API schema, combine explicit grants with RLS and ownership predicates; shared-read rules differ from private ownership rules. Private UPDATE requires an allowed original owner and an allowed resulting owner, plus SELECT access. Views and functions must not accidentally bypass the same boundary. The transport decision does not choose a service-role bypass as the normal authorization mechanism.

## Wire-contract proof

RegistrationRequest contains a complete StrategyDocument only. Additional identity/owner/visibility/approval fields fail validation. Public ID is a 24-digit string. Internal UUIDv7 keys and submitter identity do not belong in the catalog DTO.

CatalogProjection enumerates name, description, exact formal_spec, fingerprint, specified/retired readiness, a disclaimer that registration is not validation, and service-approved source references. Its projector copies an explicit allowlist rather than dumping an internal row and removing known secrets. New private internal fields therefore do not become public by default. Drafts and mismatched behavior fingerprints fail. Additional approved evidence leaves behavioral identity unchanged.

The projection does not detect secrets embedded by a user in intentionally public names/rules/assumptions. Phase 1 must provide the automatic-sharing notice, separate private context from publishable inputs, and validate/redact accidentally embedded credentials before registration. Source bodies/URLs, copyrighted material, performance evidence and personal trade records need their own rights/privacy review before any publication decision. A client or LLM cannot self-approve that review. Shared definition registration remains automatic rather than opt-in.

assert_harness_owner tests only the invariant that a trusted subject matches the owner and that an update cannot transfer ownership. It does not parse or verify tokens, call Supabase, deploy an authorization handler, or replace RLS. Tests cover anonymous access, another user, ownership reassignment, client authority fields, draft projection and private-column exclusion. No production API/security acceptance is claimed.

## Phase 1 registry constraints selected here

Complete strategy/1 specifications are immutable identities. Persist the schema/canonicalization version and full canonical bytes along with SHA-256. Enforce a unique digest within a schema version; on a conflict, reload and compare full canonical bytes. Identical specs reuse the identity; a differing payload with the same digest aborts as a collision. Candidate similarity cannot select equality. Registration, public-ID allocation/collision retry, source links and submission history must be one transaction, with concurrent-conflict reload/retry. One link per strategy/source snapshot; repeat submissions remain distinct private history. Published provenance is an explicitly approved projection of those accumulated links.

Physical tables, application-user mapping, migrations, provenance/implementation/run/job storage, transactional race tests, RLS/role tests and durable persistence are Phase 1 implementation work. Login, public hosting, release access and fee collection remain scheduled as in roadmap.md. Controlled harnesses do not authorize an unauthenticated external service.

## Current Supabase guidance checked October 14, 2026

Reviewed the [changelog](https://supabase.com/changelog), [Data API grants and RLS](https://supabase.com/docs/guides/api/securing-your-api), [RLS ownership/user_metadata guidance](https://supabase.com/docs/guides/database/postgres/row-level-security) and [JWT verification guidance](https://supabase.com/docs/guides/auth/jwts), including the MCP documentation search. The relevant [TLS cipher deprecation](https://supabase.com/changelog/cbc-tls-ciphers-deprecated) is a future client-compatibility check; it requires no setting change for this undeployed boundary. The skill's exposed-schema, grants, view/function and key-exposure checklist applies before deployment; no SQL/query verification is applicable to a documentation/pure-contract proof without a database mutation.
