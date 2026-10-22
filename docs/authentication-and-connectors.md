# Personal application and provider connections

Current scope: **personal use only**, confirmed in [D57](decisions.md). stratosphere has no hosted application login, social sign-in, users table or multi-user service requirement. The previous Supabase Auth/application-identity plan is retired, not deferred.

## Local application state

The TUI opens the user's private local workspace for strategies, isolated agent histories, sources, jobs and report references. It does not obtain or distribute a database service key. Local storage is not authentication to a broker or permission to trade.

First-run setup configures the selected model/provider. Active model keys stay in memory; local Ollama needs its own running server/installed model. Model pricing and connectivity are provider-specific. Online research/reporting gates remain in place.

## Optional provider connections

| Provider | Current implementation | Remaining verification |
| --- | --- | --- |
| Alpaca | Masked in-memory paper credentials, fixed paper-host account/crypto-asset GETs and private exports | Eligible actual feed, native BTC/USD semantics/fees, paper transport and reconciliation |
| Liquid | Browser read-scope catalog authorization and a separate reviewed address-read diagnostic | Ownership/environment, feed access, per-request paper isolation and a perpetual adapter |

Alpaca is the first Bitcoin-spot paper route; Liquid perpetual support comes later. Research can run without either connection. Connecting, inspecting or reopening a diagnostic never activates trading. Provider OAuth/browser consent may still be necessary even though application login has been removed.

The [Alpaca user-performed check](alpaca-paper-testing.md#user-performed-paper-check--21-october-2026) reports an active USD crypto account and tradable fractional BTC/USD. The [Liquid observations](liquid-account-diagnostics.md) remain review-required and do not establish account control or paper mode. Historical exports cannot restore credentials, link an account or grant authority.

## Credential and execution boundaries

Keep model keys, broker keys and Liquid sessions distinct and scoped to their intended hosts. Never place credentials in source material, model prompts, reports or native-worker environments. Preserve least-privilege scopes, provider confirmations, fixed reviewed read shapes and cancellation/account rechecks. A supplied account ID is not authentication.

Actual paper/live operations must check account identity, environment, granted capabilities, current state and the user's exact mandate. Personal use does not waive jurisdiction, account/data restrictions, provider confirmations or execution safeguards. No paid data/capacity or live orders are authorized by the scope change.

Budget/risk models propose; application code validates. Deterministic gates must check available/reserved funds, aggregate exposure, concentration, loss, quote freshness, native increments/costs and idempotency before one authoritative order gateway. Pending/ambiguous orders and partial fills require actual reconciliation.

Disconnecting credentials, stopping a worker, revoking a mandate and requesting closure are distinct. Preserve the settled keep-positions/request-all-closures choice and actual outcomes. Complete broker lifecycle behavior remains unimplemented.

## Historical hosted resources

The [Supabase provisioning record](supabase-setup.md), migrations and access-boundary proofs are retained historical work. Hosted login/synchronization is not a remaining dependency for the personal application. This cleanup does not delete or inspect the current hosted project, modify permissions or expose its data.

See [personal-use scope](personal-use.md), [architecture](architecture.md), [Alpaca development](alpaca-paper-testing.md) and [Liquid testing](liquid-paper-testing.md).
