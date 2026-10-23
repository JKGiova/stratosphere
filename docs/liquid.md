# Liquid integration

## Implemented access

Liquid is the only trading connector. Current access is read-only:

- Browser OAuth authorization and secure grant retention.
- Fresh MCP catalog negotiation, deterministic browsing and explicit private export.
- A separate explicit address-read diagnostic using the reviewed fixed `get_account` request.

No real-time structured feed, perpetual paper adapter or order transport is implemented.
An exported catalog or reported address does not verify legal account control, environment,
feed entitlement, paper isolation or permission to trade.

## Use

1. Run `/liquid-connect`, approve only read access and inspect the result.
2. Use `/liquid-tools` or `/liquid-tool <name>` to inspect definitions without invoking them.
3. Use `/liquid-catalog-save` only when an explicit private diagnostic export is needed.
4. Run `/liquid-account` explicitly for the fixed address diagnostic. List/open historical attempts
   through `/liquid-account-reads` and `/liquid-account-show`.
5. `/liquid-disconnect` forgets active and saved access; quitting retains the saved grant.

Never paste tokens or callback codes into chat. See [credentials](credentials.md) for renewal.

## Diagnostic boundaries

Catalog definitions, schemas and annotations are untrusted data, outside model contexts/histories.
Schema references never trigger network fetches. Catalog browsing has no invocation path.

Only the separate explicit account reader may call the reviewed fixed tool shape with fixed
arguments. Freeze the active read session, request IDs and schema pins before dispatch. Reject
expired/changed sessions and credential echoes; do not refresh or reauthorize during a read.
Retain failed/canceled dispatch uncertainty and plain provider confirmations as private untrusted
text. Do not open returned links/widgets/resources. Historical observations never restore access.

A persistent wallet-wide paper setting is not per-request protection against another client
switching to live mode. Existing BTC/USDT spot results cannot become Liquid perpetual evidence.
Provider/native precision, fee currencies, funding/margin and paper isolation need separate tests.
The remaining work is in [plan.md](../plan.md).
