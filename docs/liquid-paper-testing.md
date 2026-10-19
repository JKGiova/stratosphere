# Liquid paper testing and connection status

Status: **read-only connection foundation implemented; hosted account and paper execution unverified**. The user selected Liquid paper mode for the first real-feed paper test. This preserves Liquid as the first connector; Alpaca remains optional later. Application login and fee collection remain deferred.

## Testing choices

| Route | Useful evidence | Requirements and limits |
| --- | --- | --- |
| Liquid paper mode, selected | First-provider connection, account state and eventually simulated orders against real prices | Liquid account and explicit OAuth consent; custom-client identity, paper isolation, confirmations, instruments and reconciliation must still be verified |
| Alpaca Paper Only, optional later | Separate paper API/account tests | Free paper account and distinct paper credentials/endpoint; free stock data is IEX-only, not consolidated exchange coverage; this would be a separate connector |
| Authored local fixtures, available now | Native execution behavior, deterministic controls, failure/restart tests | No broker account or acquired market dataset; cannot establish real-feed integration or market performance |

Provider documentation describes [Liquid's paper mode](https://www.liquid.trade/coinvest-docs) with simulated capital and live prices. Its mode persists across conversations and can be changed by another connected client. A pre-order status read therefore does **not** prove that a subsequent operation is confined to paper. Per-request simulation binding, applicable confirmations and event/reconciliation behavior must be verified before enabling an external action. Liquid perpetuals are distinct from spot BTC/USDT and owned stocks or currencies; no historical spot results or fixture fills transfer to them.

[Alpaca's paper documentation](https://docs.alpaca.markets/us/docs/paper-trading) describes globally available email-based Paper Only accounts, separate paper credentials and `https://paper-api.alpaca.markets`. Its documented fill model and [free IEX coverage](https://docs.alpaca.markets/us/docs/about-market-data-api) differ from a real execution venue. No Alpaca account was created or linked.

## Implemented operations

Run the usual TUI and use these commands independently of model configuration:

1. `/liquid` checks current public OAuth metadata against the fixed standard Co-Invest endpoint. No account, registration or MCP tool call occurs.
2. `/liquid-connect` registers a public native OAuth client, opens the system browser, requests only `read`, and waits up to four minutes for explicit browser consent. Sign in or create your Liquid account in that provider-controlled browser flow. Do not paste credentials into chat. This step needs no funded live test or trade activation in stratosphere; if the provider demands live funding/activation, stop and report that requirement rather than proceeding.
3. A successful read connection inspects only the MCP tool **catalog**. Focus shows the connection scope and tool count. `/liquid-tools` lists captured definitions and `/liquid-tool <exact-name>` opens one definition in Focus. Escape dismisses model setup if needed; these commands require no configured model. The client has no `tools/call` path and never invokes a provider tool, changes account/paper mode or submits an order. Tool names, descriptions and schemas do not enter model prompts.
4. `/liquid-catalog-save` explicitly exports the captured definitions to a new private `liquid-catalogs/<id>/` directory under the local research root. Focus displays the full path to `catalog.json`; the adjacent `manifest.json` records its digest. Nothing is exported automatically.
5. `/liquid-disconnect` cancels pending sign-in and forgets local session credentials. `/cancel`, Ctrl+K and quitting also cancel pending connection work. Provider-side consent is separate: revoke it in Liquid when desired.

The implementation supports the standard endpoint `https://coinvest.liquid.trade/mcp`. Co-Invest Computer is not an implemented alternative. OAuth metadata changes, unsupported public-client registration, unconfirmed/additional token scopes, an unbounded/expired token, incompatible protocol negotiation and malformed/repeated catalog pages fail explicitly. There is no trade-scope fallback.

Tokens, client-registration responses, authorization codes and refresh tokens are not persisted. The short-lived access token remains in memory until disconnect or process exit; expiry blocks use and requires disconnect/reconnection. Local forgetting is not a cryptographic memory-wipe guarantee or provider-side revocation. Active connector credentials are rejected from chat, research proposals and source storage without copying them into the source store's retained secret list. OAuth/registration responses, headers, callback queries and provider error bodies are not recorded in conversation logs or reports. An explicit catalog export contains tool definitions only, with its capture time, fixed resource/protocol, read scope and digest.

## Verified evidence

- Public checks on October 18, 2026: both documented standard/Computer endpoints returned HTTP 401 with their protected-resource metadata location. Their public metadata advertised `read`/`trade`, PKCE S256, authorization-code flow and public-client authentication. This is advertised capability, not account permission verification.
- The production standard-client discovery and registration path accepted a public loopback client and generated a valid read-scope/resource-bound authorization URL. The acceptance run deliberately stopped before browser sign-in; no code/token exchange, account access, provider tool or order occurred.
- The user subsequently reported **Read-only catalog connected** in the real TUI. This confirms the reported connection result; account identity, paper mode, market data and execution remain unverified. A separate diagnostic opened read-only browser authorization but timed out before completion. It obtained no token/catalog and called no account or trading tool; it does not invalidate the distinct TUI result. Actual tool-schema inspection remains pending.
- Local fixtures cover PKCE/resource/state binding, same-origin metadata, redirects/failures, scope/expiry rejection, cancellation, paginated catalogs, protocol negotiation and SSE streams that remain open after returning a result. A real localhost listener test exercises wrong host/path/state/issuer, duplicate parameters and one-use callback behavior.
- The full regression suite passed **659 tests with zero skips**, including the pinned native-engine checks (33 upstream Pandas deprecation warnings). The focused TUI run passed 45 tests. Built wheel/sdist checks confirmed source/CSS equality, configured-credential exclusion and private-state exclusion. The packaged fixture flow and keyboard scrolling passed at 180 × 48 and 45 × 12; both views were visually inspected. Fixtures never prove an actual Liquid account, paper mode, quote semantics or order behavior.

## Required next verification

Verify the user-reported connection independently and inspect the provider's actual tool schemas before selecting any read operation. Verify provider account identity and environment explicitly. OAuth success/catalog access alone must not mark an account as verified, claim a feed entitlement, activate a personal instance or expose trading controls.

The persistent paper runtime, native adapter, complete mandate/budget/risk controls, provider-connected order reconciliation, sustained-feed checks and keep/close exit behavior remain pending in the [full Phase 3–5 acceptance ledger](phase3-5.md). The [authored-paper ledger foundation](phase5-paper-ledger.md) adds local reservations/attribution and native fixture checks, not that production runtime or a Liquid paper adapter.

Protocol references: [MCP authorization](https://modelcontextprotocol.io/specification/2025-06-18/basic/authorization), [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports), [initialization lifecycle](https://modelcontextprotocol.io/specification/2025-06-18/basic/lifecycle). The client negotiates the explicit `2025-06-18` protocol and rejects an unsupported negotiated version.

## Catalog inspection and export

The client now keeps complete tool definitions in immutable canonical JSON for the current read connection. It checks names, required object-root input schemas, optional object-root output schemas, display-field types, annotation types and duplicate names across pages. Duplicate JSON keys and non-finite JSON values fail before capture. Nested schema content and extension metadata remain literal provider data: this is not a complete JSON Schema compiler, and `$ref` locations are not fetched or executed.

Definitions, descriptions and annotations are untrusted. A `readOnlyHint` or a description claiming paper safety grants no permission and does not verify a tool's behavior. The [pinned MCP tool specification](https://modelcontextprotocol.io/specification/2025-06-18/server/tools) distinguishes discovery metadata from invocation. The catalog operation sends only initialization, its notification and `tools/list`; there is no `tools/call` path.

`/liquid-tools`, `/liquid-tool <name>` and `/liquid-catalog-save` require online access and a complete, unexpired in-memory read session. They inspect the snapshot captured at connection time; they do not refresh the catalog or read an account. Reconnect to capture a new provider catalog. Schemas do not enter model prompts, strategy histories, research jobs or the shared registry.

Exports use private directories (0700) and exclusive files (0600) on Linux/macOS, retain earlier exports and write the digest manifest last. A partial export or changed artifact fails diagnostic loading. A saved digest detects inconsistent artifacts; it is not a provider signature or proof of an active connection, account identity, entitlement or paper isolation. Loading a saved catalog cannot recreate a session or enable an operation. The application has no offline catalog mode.

The export boundary rejects echoed access/refresh tokens, authorization codes, PKCE/state/session values and known configured model credentials, including escaped JSON spellings. Tokens and OAuth callbacks remain in memory; they are not export fields. Unexpected provider material remains private diagnostic data, so do not publish catalog files. Default exports live in application state outside the repository; development `reports/` artifacts are ignored and excluded from distributions.

To provide the actual provider contract for the next development step, restart the updated TUI, run `/liquid-connect`, approve only read access, then run `/liquid-catalog-save`. Report the path displayed in Focus; do not paste credentials or authorize trade scope. The saved definitions can then be inspected to select a specific read operation. A named tool or its hint alone does not establish that it is safe or available to the account.

Catalog verification: **785 full local regression tests passed with zero skips**, including native-engine checks (36 upstream Pandas warnings). The final compact inspector presentation passed 68 focused connector/TUI checks. Built wheel/sdist source equality, credential/private-state exclusion, private export/load, offline-operation rejection and keyboard scrolling passed; the inspector was rendered at 180×48 and 45×12. These checks used an explicit provider fixture and make no hosted-account, tool-behavior or paper-execution claim.

## Actual exported schema observations — 19 October 2026

The user supplied the private catalog export path after a successful read-only connection. Diagnostic loading verified its manifest and canonical catalog digest: `5eacfac411242e64f2feb7f4e7e498728e843cd3fcf72fa882e70ec792e20805`. It was captured at **2026-10-19T14:57:22.364946+00:00**, records scope `read` and contains **38 tool definitions**. The raw export stays in private application state outside GitHub, packages and model contexts. This review inspected schemas/descriptions only; it made **zero provider tool calls**.

| Observed tool | Captured contract | Implication for the next increment |
| --- | --- | --- |
| `get_account` | Object input permits only optional `user_intent`; object output requires `tradingAccountAddress: string` and `addressRole: "trading-account"`, without extra properties | Candidate for a separately implemented, explicit read-only account-identity diagnostic. No actual address, authenticated ownership or account environment was read or verified here. |
| `paper_trading_status` | Optional text output without required structured account/environment fields; description says the simulator setting is shared wallet-wide across other MCP clients and does not switch the Liquid web app to paper | A status read cannot bind a later request to simulation or prevent another client changing the mode. |
| `automated_trading_status` | Optional text output; description distinguishes a saved policy from another process's current authorization/refresh activity | Not an operational order mandate or attestation of an active runtime. |
| `show_chart`, `show_orderbook` | Chart accepts symbol, interval and bounded count; outputs expose optional widget status/text, without a required structured candle/book response | Does not establish a streaming data contract, raw OHLCV availability/publication evidence or executable quote semantics. Descriptions require explicit chart/book requests. |
| `enable_trading` | Included with `readOnlyHint=true`, but its setup/widget description includes trading readiness and confirmation behavior | Discovery hints cannot authorize setup, funding, agent keys or state changes; this tool was not invoked. |

There is no order-submission/close/reset/enable-paper definition in **this captured read catalog**. This does not establish what other scopes or the provider as a whole support. Annotations are discovery data; all 38 definitions carry a read-only hint, including setup-facing entries. Neither these hints nor OAuth success prove account identity, feed entitlement, simulation isolation, event delivery, idempotency or reconciliation.

The existing connection still has no `tools/call` path. The next account-read step requires a separate constrained operation with an explicit user command, fixed schema validation, retained provider confirmations, in-memory credentials and no scope escalation. A persistent wallet mode and preflight status read remain insufficient protection for an external paper order. Real-feed and paper-adapter acceptance items stay unchecked.
