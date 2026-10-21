# Authentication and optional operational connectors

Last updated: October 13, 2026. This is a plan, not an implemented integration.

## Confirmed decisions

- **Supabase Auth with OAuth login** authenticates stratosphere users.
- **Alpaca and Liquid** are optional operational connectors.
- A trading account is not required for strategy research, the community catalog, or backtesting.
- Account linking does not activate live trading.
- Execution follows budget management, risk valuation, deterministic checks, and the controlled gateway.

Google and GitHub are confirmed initial login providers. Do not interpret “Supabase OAuth” as a requirement to build a proprietary OAuth authorization server.

## Three independent authorities

| Authority | What it proves/allows | What it does not allow |
| --- | --- | --- |
| Supabase Auth session | Verified app identity and authorized application data access | Trading on an external account |
| Provider account connection | Access to particular provider accounts under granted scopes | Bypassing application risk controls or automatically enabling live mode |
| stratosphere operational mandate | Permitted strategies, instruments, capital, limits, and approval mode | Exceeding provider permissions or overriding deterministic checks |

An ID identifies an account; a verified token/session proves control. Never accept a claimed Supabase, Liquid, or Alpaca ID as authentication.

## Proposed terminal login flow

1. The TUI starts a Supabase Auth OAuth login and creates the required anti-forgery/PKCE state for the selected flow.
2. The user authenticates and consents with the external provider in a browser.
3. The verified callback/code exchange yields a Supabase application session.
4. Application access validates token authenticity, intended issuer/audience, expiry, and current authorization.
5. Resolve the verified Supabase subject to the stable application profile.

A browser consent page is not a migration to a web product. Choose and verify terminal redirect handling, registered URLs, SDK/runtime support, token refresh, and OS-backed local credential storage before implementation. Do not promise native device-code support or a specific callback implementation without testing it.

Supabase manages authentication; stratosphere still owns resource authorization, client/session integration, and secure use of tokens. Persist app ownership through the Supabase identity, not mutable email addresses or broker account IDs.

## Identity and linking

Keep an application user/profile with a unique reference to Supabase auth.users. Supabase holds social identities; the application does not maintain its own password credentials.

Link additional sign-in identities only through verified provider/Supabase flows. Do not merge accounts solely on an unverified email or a user-provided external ID. Ownership must remain stable if the user disconnects a broker or changes login methods.

Treat Supabase session tokens, social-provider tokens, and trading-provider tokens as distinct credentials with different audiences, refresh behavior, and revocation. A social login does not require access to the user's repositories or financial accounts.

## Optional connector comparison

| Connector | Intended role | Verification needed |
| --- | --- | --- |
| Alpaca | Authorized account state and supported order operations through its trading interfaces | App onboarding, supported instruments/accounts, paper/live scopes, API/event handling, and engine adapter |
| Liquid at liquid.trade | Authorized account/position context and supported operations through the selected API/MCP flow | Custom-client authorization, tradable products, confirmations, event/reconciliation access, automation limits, and engine adapter |

Alpaca documents OAuth access to its Trading API, including paper/live account authorization and a trading scope. Keep confidential token exchanges and application secrets on a trusted backend rather than distributing them in the TUI.

Liquid Co-Invest documents account access and execution flows; standard Co-Invest requires explicit confirmations, while other modes have separately enabled automation limits. Verify which flow is supported for stratosphere. Do not remove or bypass provider confirmations to achieve unattended operation.

Liquid perpetual markets referencing stocks/FX are different instruments from owned equities/ETFs or spot currency positions. Multi-market support must preserve instrument semantics and actual provider capabilities. Direct Hyperliquid support is not proof of a working Liquid adapter or access to every venue Liquid routes to.

Neither optional provider is selected as the identity authority for stratosphere. Users may connect neither, one, or both; the operational routing policy chooses an explicit authorized path per account.

## Proposed connection metadata

A private account_connections record should include:

| Field | Meaning |
| --- | --- |
| connection_id | Internal connection key |
| user_id | Application owner, resolved from the verified Supabase session |
| provider | Alpaca or Liquid |
| external_subject | Provider identity verified during connection, where available |
| external_account_refs | Authorized account references; normalize account rows in the physical schema |
| environment | Provider-supported paper/simulation/live environment, explicitly verified |
| granted_scopes | Permissions actually granted, not just requested |
| credential_ref | Pointer to protected token/secret storage; not plaintext credentials |
| status | Pending, connected, expired, revoked, disconnected, or error |
| connected_at / last_verified_at | Connection and last validation timestamps |
| revoked_at | Revocation timestamp where applicable |

Enforce ownership, environment, scope, and capability checks on every operational action. Provider account IDs are not authorization tokens. If one connection exposes multiple accounts, map permissions and instances to the correct account instead of assuming universal access.

This logical addition does not create tables or migrations.

## Budget, risk, and order path

Candidate strategy/portfolio intents first receive a budget-management proposal and then a risk-valuation assessment. The deterministic gate checks fresh state, available/reserved capital, exposure, concentration, order validity, connection scopes, and the user's mandate.

Only approved intents reach the gateway, engine integration, and selected provider. Maintain one authoritative submission/reconciliation path per account. Agents and provider-side assistants cannot submit a second independent stream of orders.

Execution events update capital, positions, and pending-order reservations. Previously authorized protective orders and emergency controls must operate deterministically without a fresh model call, under their preapproved policy; they do not gain permission to open unrelated positions. Risk limits and essential protections must function when models are unavailable. Detailed sizing, allocation, concurrency, netting, and fallback policies remain open.

## Session, revocation, and private-data security

- Never distribute Supabase secret/service-role keys or confidential OAuth client secrets to users.
- Keep operational credentials outside prompts, traces, shared catalog records, and generated-code sandboxes.
- Use least-privilege scopes and RLS/grants that distinguish shared strategy reads from private ownership.
- Do not use user-editable metadata as authorization authority.
- Logout, connector disconnection, mandate revocation, runtime shutdown, and liquidation are different actions.
- Define and test cancellation/reconciliation behavior for pending orders and existing positions before revoking operational access.
- User deletion is not assumed to invalidate already issued access tokens; session handling and retention require explicit policies.
- Current permissions must be checked on sensitive operations; stale cached claims must not silently expand access.

Supabase Auth selection does not select Supabase Storage, Realtime, or Edge Functions. The organization and Frankfurt region are selected. Hosting, project plan/provisioning, SDK versions, RLS policies, provider app registration, and connector-specific integration remain open.

## Acceptance criteria

- Login works without Alpaca or Liquid accounts; no proprietary password system is introduced.
- A valid user cannot read or operate another user's private records/accounts.
- Bare user/account IDs and expired/untrusted tokens cannot grant access.
- Account linking does not activate paper/live trading implicitly.
- Revoked scopes or mandates block new unauthorized operations.
- No order bypasses the budget/risk workflow and deterministic gate.
- Partial fills, retry deduplication, reconnects, and ambiguous outcomes reconcile correctly.
- Provider-required confirmations are honored.
- Actual instrument behavior is represented in historical and paper validation.

## Documentation references

- [Supabase social login](https://supabase.com/docs/guides/auth/social-login)
- [Supabase PKCE flow](https://supabase.com/docs/guides/auth/sessions/pkce-flow)
- [Supabase JWT guidance](https://supabase.com/docs/guides/auth/jwts)
- [Supabase data access security](https://supabase.com/docs/guides/database/secure-data)
- [Alpaca OAuth and Trading API](https://docs.alpaca.markets/us/docs/using-oauth2-and-trading-api)
- [Liquid Co-Invest documentation](https://www.liquid.trade/coinvest-docs)

## Confirmed onboarding and provisioning target

First-run onboarding covers model configuration, hosted browser login and optional trading connections. Google and GitHub are selected; hosted-login domain, deployment and callback configuration remain open. Connection setup may be skipped, preserving research use without a broker. The user selected a dedicated stratosphere project in Matti Barbershop, Frankfurt (eu-central-1); provisioning is blocked by the unavailable connector cost-check capability. No project or auth settings have been created. See [Supabase setup](supabase-setup.md).

## First connector and execution modes

The user selected Alpaca first for Bitcoin-spot paper integration on 21 October 2026, with Liquid later for perpetuals; see [Alpaca development](alpaca-paper-testing.md). The product should offer configured mandate-based operation or explicit order approval, subject to provider capabilities. [Liquid Co-Invest documentation](https://www.liquid.trade/coinvest-docs), reviewed October 13, 2026, distinguishes standard confirmation-based execution from Co-Invest Computer simulation/staged/live modes. Those documented modes do not prove stratosphere custom-client automation or a NautilusTrader adapter. Preserve provider confirmations and verify the applicable integration before promising unattended operation.

## Scheduling correction

The user deferred login discussion, configuration and implementation to later roadmap work. Preserve Supabase Auth, Google/GitHub providers and browser-hosted login as final-product decisions. Current core development does not require choosing a login domain or configuring providers. Verified auth/ownership remains required before exposing a shared-user service.

## Liquid paper testing increment

The original Liquid-first real-feed paper selection is superseded by the Alpaca-first spot route. The TUI retains public metadata checks and a memory-only, browser-authorized **read-scope MCP catalog connection**, separate from deferred Supabase application login. The catalog connection invokes no provider tools and cannot submit an order. A separate explicit account diagnostic was user-performed and its retained response inspected on 21 October; it reports an address with review-required prose, without ownership, feed entitlement or paper-mode verification. The fresh export retains the same 38 tool definitions. See [Liquid paper testing](liquid-paper-testing.md) for the current evidence, spot-market mismatch and required paper-isolation checks.
