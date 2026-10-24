# Credentials

## Save and forget

The selected model/provider/official endpoint/key, Alpaca paper keys and optional fixed Liquid read grant use macOS
Keychain or Linux Secret Service through `keyring`. There is no plaintext, config-file or chained
backend fallback. Only the selected model configuration is retained, not a provider password library.

- `/model` sets and saves the selected configuration; startup restores it without a model request.
- `/model-forget` removes saved model access when no operation is running.
- `/liquid-connect` explicitly authorizes read access in the browser and saves a supported grant.
- `/liquid-disconnect` waits for connector writes, cancels work and forgets active/saved access.
- `/alpaca-paper-connect` uses a masked form, fixed paper endpoint and native storage.
- `/alpaca-paper-forget` removes saved paper keys after the runner stops; it never closes positions.
- Quitting retains saved credentials. Provider revocation is managed separately with Liquid.

Unavailable/unlocked secure storage is reported explicitly; access may remain session-only after
setup. Linux requires a functioning unlocked Secret Service session. macOS can prompt for Keychain
access. Previously memory-only versions cannot recover tokens they discarded.

## Liquid restore and renewal

Pin issuer/resource/client/read scope and bounded lifetimes. Revalidate metadata and negotiate a
fresh session/catalog on restore; never load a saved diagnostic as a connection. Renew only through
the fixed advertised token endpoint and validate exact read scope plus confirmed token rotation.

Use an account-wide OS lock shared across research roots. Consume the old saved grant before
refresh dispatch, retain the confirmed replacement before catalog negotiation and finish secure
writes before cancellation/disconnect releases the lock. Do not retry ambiguous refreshes or
open a sign-in browser automatically on startup. Expiry/revocation may require explicit sign-in.
Renewal occurs before an account diagnostic freezes its session, never inside the read.

Callback codes, PKCE/state, registration secrets and MCP session identifiers stay in memory.
Credentials stay outside models, SQLite, reports, source artifacts and worker environments.
Native secure storage is personal OS storage, not an independent multi-user security boundary.

## Verification limits

Synthetic macOS Keychain write/read/delete and installed-package restart checks passed. Actual
user Liquid refresh/rotation and Linux native-backend verification remain pending. Fixtures and
public authorization metadata do not verify them or grant feed/trading access.
