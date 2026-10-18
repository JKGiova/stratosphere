# Database development

The stratosphere project is provisioned in the **tradingsphere** organization on the free allowance, Frankfurt (`eu-central-1`). The project reference is `iaouphzeaiqyunfwbprm`; it is an identifier, not a credential. Login providers, shared application hosting and client database access are not configured.

The migrations create the private `stratosphere` schema for strategy identities/lineage, source snapshots/associations, submissions, stable agents and owner-scoped sessions/turns, research jobs, operation outcomes, reports and assessments. All 13 tables have RLS enabled. The schema grants no usage to anonymous or authenticated clients; only the trusted application service can register definitions. No privileged key is distributed to the TUI. Models query the local development workspace through validated Python operations; hosted application-service synchronization is still pending.

Migration files were initially created using the pinned official Supabase CLI 2.121.0, applied through the Supabase connector, and renamed to the authoritative versions returned by the remote migration history. No generated row IDs or credentials are embedded in migrations. Future database changes must append migrations rather than editing applied SQL.

Run `tests/strategy_agent_registry.sql` in a controlled development database to verify canonical identity/deduplication, source accumulation, parent links, stable per-strategy agents, report immutability, cross-owner foreign keys and RLS. The harness creates authored fixtures and temporary role grants in a transaction, then rolls back every change. It does not implement OAuth or prove login/session verification. Final checks must confirm no retained fixtures and no client schema grants.

Application services must validate full canonical `StrategySpec` contracts before invoking registration. The SQL routine rejects noncanonical wire representations and protects the stored identity hash; it is not a substitute for engine/conversion conformance. Shared catalog DTOs must enumerate allowed fields and exclude private source material, conversations, accounts and limits.
