# Supabase setup

Status: provisioning requested and target selected; no project, schema, policy, authentication setting or secret has been created or changed.

## Selected target

- New project name: stratosphere.
- Organization: Matti Barbershop, selected by the user.
- Region: Frankfurt, eu-central-1, selected by the user.
- Organization plan: free, confirmed through the connector; this does not establish the cost or quota eligibility of a new project.
- The unrelated inactive mattibarbershop project must remain untouched.

## Current provisioning blocker

The connector's get_cost call returned UNAVAILABLE: the server did not expose that tool through tools/list. The create_project contract requires a cost confirmation obtained after get_cost. Do not fabricate a confirmation ID or infer a project price from the organization plan. Project provisioning remains pending until a supported creation/cost-confirmation path is available.

## Setup work after provisioning

1. Verify project health, PostgreSQL version and available interfaces. Keep environment secrets outside the repository and conversation log.
2. Finalize registry canonicalization and schema contracts; implement versioned migrations for profiles, strategies, source snapshots/associations, submissions, datasets, implementations, experiment runs, jobs/checkpoints/interventions, assessments and personal instances.
3. Add private provider connections, operational ledger and billing records when their contracts are defined. Do not store plaintext credentials in flexible profile data.
4. Define least-privilege grants and tested RLS/ownership policies before exposing application tables; distinguish catalog reads from private records and privileged service writes.
5. Register the selected Google and GitHub OAuth applications. Set the hosted login domain and exact redirect/callback flow; verify PKCE/state, secure credential persistence, refresh, expiry and revocation on Linux/macOS.
6. Implement the Phase 0 selected [HTTPS/JSON application API boundary](phase0-access-boundary.md) and select its framework/hosting. Do not distribute privileged keys or assume the database provides an application login page or research/trading workers.
7. Decide artifact storage, retention, backups and observability. Storage, Realtime and Edge Functions are not implicitly selected.
8. Test user isolation, duplicate registration races, source accumulation, login without trading accounts, outages and migration reproducibility. Record implementation status and actual results.

The region and organization are settled. Google and GitHub OAuth providers are also selected. The shared access boundary is selected in Phase 0; hosted-login domain/deployment, API implementation/hosting, project budget and physical record contracts remain open.

## Current scheduling

Login configuration and fee-collection work are deferred. This does not revoke the requested database project/registry work, but the existing project-provisioning blocker remains. Keep provider-login/domain setup out of the current research/TUI planning pass.
