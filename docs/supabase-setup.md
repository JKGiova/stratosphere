# Supabase setup

Status: **project and private registry foundation provisioned and verified**, October 18, 2026. The TUI's hosted application-service synchronization and login remain unimplemented.

## Current target and state

- Organization: **tradingsphere**, created on the Free plan at the user's request.
- Organization ID: `vwhviuvctapgdoztkgtj`.
- Project: **stratosphere**, reference `iaouphzeaiqyunfwbprm`.
- Region: Frankfurt, `eu-central-1`.
- Quoted project cost at creation: **$0/month**. No paid capacity was enabled.
- Verified health: `ACTIVE_HEALTHY`; PostgreSQL 17.11.
- [Project dashboard](https://supabase.com/dashboard/project/iaouphzeaiqyunfwbprm).

The private `stratosphere` schema contains 13 registry/agent/research tables with RLS and no anonymous/authenticated schema grants. Service-mediated registration preserves exact canonical identity, variants and agent identity. Ownership/immutability/conformance tests ran against the actual project and rolled back all authored fixtures and temporary grants. Security advisors reported no findings; the report ownership FK index was added following performance advice. See [database development](../supabase/README.md) and [strategy agents](strategy-agents.md) for migrations, actual tests and limits.

Login, Google/GitHub app registration, hosted application APIs, token validation, secure client session storage, service hosting and authenticated synchronization are still deferred. No privileged database key is distributed to the TUI. Local agent records currently use the private development workspace database; provisioning Supabase does not itself connect a client or enable public sharing.

## Historical provisioning change

The previous selected organization was Matti Barbershop. Its unrelated inactive project was left untouched. The earlier cost-check blocker was resolved when the reconnected connector exposed the required tools. The user then requested a new organization named tradingsphere, signed into its dashboard, and reauthorized the connector for that organization. The project was created there on the quoted free allowance. The old organization choice and unavailable-tool blocker are superseded; Frankfurt remains the selected region.

## Remaining setup

1. Build and verify the selected HTTPS/JSON application service with real session/ownership enforcement before exposing shared/private records to users.
2. Extend versioned contracts/migrations alongside Phase 3 studies, source integrations, paper accounts/instances and the operational ledger; do not advertise tables as a working integration before application verification.
3. Configure the previously selected Supabase Auth Google/GitHub browser flow at its later authorized stage; preserve PKCE/state, expiry/revocation and cross-user isolation requirements.
4. Select and test artifact storage, retention, backups and observability. Storage, Realtime and Edge Functions are not implicitly selected.
5. Keep login configuration and commercial collection separate from current research/paper development. No fee collection or live trading was enabled by this setup.
