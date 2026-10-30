# MT5 data and execution connector

MT5 is the sole active broker connector. The local `StratosphereBridge` Expert Advisor (EA)
exchanges typed filesystem requests/responses with `stratosphere.mt5`. Alpaca/Liquid transports
are archived under `src/stratosphere/legacy` for historical compatibility; their commands,
automatic credential restoration and execution setup are retired. Existing reports, account
journals, identities, secure-store entries and old contracts are retained. They grant no MT5 authority.
Yahoo research remains available separately; it is not automatically converted into CFD execution.

## Install and connect

Keep the existing broker login inside MT5. The connector needs no copied password, API key, DLL,
WebRequest permission, remote service or hosted database. Compile the application EA with its pinned
source identifier and install it in the existing terminal:

```sh
.venv/bin/python tools/mt5/build_bridge.py --install
```

The helper discovers the inspected macOS MetaQuotes Wine installation. For other Wine deployments,
pass `--terminal`, `--wine` and `--prefix`. Native Windows compilation can use `--terminal`; the
Python application currently targets Linux/macOS and its account lease uses POSIX `fcntl`.
Linux bridge deployment and Windows application compatibility have not been verified.
Compiler logs, source/binary hashes and binaries stay in ignored `reports/mt5/bridge-build-*`.
A local source identifier is a compatibility pin, not independent executable attestation.

In MT5 Navigator → Expert Advisors, refresh and attach **stratosphere → StratosphereBridge**
to one chart. Leave `EnableTrading=false`, `AllowRealAccount=false` for connection/data tests.
Only one EA may own the terminal bridge. The EA remains attached and handles typed requests on a
one-second timer while MT5 is open; the Python client does not start or log into MT5.

The client discovers the inspected macOS path. Elsewhere set `STRATOSPHERE_MT5_ROOT` to the
active terminal's `MQL5/Files/stratosphere-bridge` directory, or pass `--root <path>` before the
subcommand. The IPC directory is private to the OS user and must not contain symlinks. This is local
IPC, not a network endpoint or a model tool.

```sh
.venv/bin/python -m stratosphere.mt5 account
.venv/bin/python -m stratosphere.mt5 symbol XAUUSD
.venv/bin/python -m stratosphere.mt5 quote XAUUSD
.venv/bin/python -m stratosphere.mt5 rates XAUUSD H1 --count 512
.venv/bin/python -m stratosphere.mt5 positions
.venv/bin/python -m stratosphere.mt5 orders
```

All 21 standard MT5 intervals are accepted. Rates default to closed bars (shift one). `--end`
requests a window ending at a raw MT5 server/chart epoch and may include an incomplete bar.
The per-request cap is 100,000 bars and the terminal limit still applies. No clock conversion,
gap filling, sorting or deduplication is performed. The client validates positive consistent OHLC,
strict timestamp order and nonnegative volume/spread. `deals <start> <end>` accepts server/chart
epochs, with a 10,000-deal cap; request shorter windows if necessary. Raw responses, including
rejected/invalid observations, remain in the local request journal.

TUI read-only entry points: `/mt5`, `/mt5-status`, `/mt5-quote <symbol>`,
`/mt5-symbol <symbol>`, `/mt5-positions`, `/mt5-orders`, `/portfolios`. Diagnostics are rendered
privately and are not appended to strategy-model histories. The headless connector is the execution
interface; it does not yet run the strategy-agent research/signal pipeline automatically.

A separate [gold Golden Cross pilot](mt5-gold.md) connects an explicit XAUUSD D1 strategy
to this EA through headless status/draft/tick commands. It has its own exact demo deployment and
one-entry sizing adapter; it does not complete the general agent/portfolio workflow.

## Exact deployment approval

A connection never enables trading. A separate `mt5-deployment/1` binds the registered strategy ID,
complete strategy/report digests, Python implementation and EA source identifier, exact
login/server/demo-or-real mode/currency, symbol, expiry and finite limits:

- `max_volume` in broker lots; `max_order_risk` and `max_order_margin` in account currency.
- `max_spread` in the symbol's absolute price units; `min_equity` in account currency.
- Positive `cost_buffer` added to the estimated stop risk; `max_orders` bounds consumed entries.

The canonical client journal/lease and retained deployment evidence live under the native user
state directory (`Library/Application Support/stratosphere/mt5` on macOS,
`.local/state/stratosphere/mt5` on Linux), keyed by exact server/login/mode. Terminals logged
into the same account share that journal and lease; research-root overrides do not isolate it.
An empty, incomplete or corrupt existing journal is rejected instead of recreated.

The connector deliberately provides no implicit/default account risk allocation. Prepare a
`limits.json` with the registered `strategy_id`, exact provider `symbol`, the limits above,
`max_orders` and Unix-UTC `expires`. Draft against the currently connected account:

```sh
.venv/bin/python -m stratosphere.mt5 --output draft.json draft strategy.json report.json limits.json
```

`strategy.json` must be a complete canonical `StrategyDocument`. The draft privately retains the exact strategy/report bytes and binds their digests. Entry approval
and submission reject missing or changed retained evidence. It does not prove the report is faithful, profitable, owned by this
candidate or applicable to broker CFDs. Inspect those limitations and evidence before user approval;
the automatic research-to-MT5 package validator/runner remains a separate development gate.
The draft output wraps `deployment` and its `digest`; save the inner `deployment` object as
`deployment.json`, review it and approve its exact digest:

```sh
.venv/bin/python -m stratosphere.mt5 review deployment.json
.venv/bin/python -m stratosphere.mt5 approve deployment.json <exact-sha256>
```

This is one explicit operator deployment approval, not a model operation. It persists in the local
SQLite journal and a native policy file, both outside model contexts. MT5 additionally requires
`EnableTrading=true`, the EA's standard trading permission and terminal algorithmic trading enabled
before submission. Real accounts require both `approve --allow-real` and EA
`AllowRealAccount=true`; no real account is activated by installation, connection or tests.
FTMO Free Trial/evaluation accounts may report `demo`: that flag does not establish Swing profile,
phase, risk/reset compliance or permission to run an evaluation.

## Check, submit and exit

An order JSON contains `signal_sha256`, `side` (`buy`/`sell`), exact decimal `volume`, mandatory
`stop_loss` and optional `take_profit` (default `0`). The caller owns signal research and binary
review; the connector does not ask a model or infer that a hash proves a reliable signal.

```sh
.venv/bin/python -m stratosphere.mt5 check <deployment-sha256> order.json
.venv/bin/python -m stratosphere.mt5 submit <deployment-sha256> order.json
.venv/bin/python -m stratosphere.mt5 close <deployment-sha256> <exact-position-ticket>
.venv/bin/python -m stratosphere.mt5 revoke <deployment-sha256>
```

`check` invokes broker `OrderCheck` without `OrderSend` and grants no submission authority.
The EA checks account/session, deployment, quote age, native tick/volume increments, stop direction,
spread, equity floor, native `OrderCalcProfit` stop risk plus buffer, `OrderCalcMargin` and broker
`OrderCheck`. Market orders use FOK where offered, otherwise IOC; other filling policies are blocked.
Only a completely flat account without pending orders can open a position. This conservative
connector currently allows one position across the account, with no reinforcement. Mandatory stops
limit nominal risk; gaps, slippage and financing can exceed the estimate. FTMO daily loss/reset and
multi-strategy portfolio allocation are not implemented by these cash limits.

Only an exact journal-owned ticket/magic/instrument can be closed, with no model dependency.
Entry expiry/revocation does not remove the retained policy needed for protective exits. Revocation
stops new entries; it does not close positions. Closing the Python client stops its requests; closing
MT5 stops the EA. Neither action automatically closes positions. Broker-side stops remain submitted,
subject to broker behavior. No unattended exit/monitoring or emergency-close-all service is claimed.

## Ambiguity and evidence

One OS account lease serializes approval/submission. Requests are committed before publication;
MT5 moves each request into persistent `processing` before dispatch. Request IDs cannot replay across
restart. A changed EA session, broker server, account or mode rejects the request. Account switching
requires explicit EA reattachment and a newly bound client/approval.

`OrderSend=true`, an order ticket or a placement acknowledgment is not a confirmed fill.
The client checks terminal success/partial-fill codes against subsequent positions and open orders.
Timeouts, cancellation, conflicting state, outstanding/unknown order outcomes or incomplete closures
latch uncertainty and block new entries across restart. No automatic retry, timeout reset or
absence-of-order inference clears that state. Read positions/orders/deals and retain broker evidence
for operator reconciliation; automatic ambiguity recovery is not implemented. Protective owned
exits are still available. Broker-confirmed rejection consumes its intent and is retained.

Local SQLite, requests, native claims and responses are operational evidence: do not delete them to
unhalt trading, change an ID to replay an uncertain signal, relabel an account or transfer old approvals.
Current verification distinguishes authored peer lifecycle tests, actual compiled EA diagnostics,
read-only connected checks and actual broker orders. Installation and fixture tests do not send broker orders.

The Intel macOS build compiled with zero errors/warnings. Broad regressions passed 1,419 core and
39 engine tests; subsequent bridge hardening passed the final 55-test focused run. An actual
connected Intel macOS check passed on October 29, 2026 against FTMO-Demo: account status,
XAUUSD specification/quote, 205 H1 and D1 closed bars, ten M1 bars, positions, pending orders and
bounded recent deal history. A second quote's timestamp advanced. The reported EA source identifier
matched the reviewed build; this remains a local compatibility observation, not independent
executable attestation. Native trading inputs and terminal algorithmic trading were disabled;
no broker checks or orders were submitted. Private connected responses/hashes are retained under
`reports/mt5/connected-20261029`; compilation/fixture logs remain under
`reports/mt5/connector-verification`. Disconnect/account-switch/restart and sustained feed tests
remain pending.

A separately approved one-time connector diagnostic on the same demo account passed native
OrderCheck, a 0.01-lot XAUUSD buy with a mandatory stop, position read-back, immediate owned close
and final flat-account verification on October 29, 2026. The entry and exit deals were confirmed
in broker history; realized P/L including commissions was -$0.41. Estimated entry stop risk was
$5.37 plus a $2 buffer, and estimated margin was $80.34, within the approved $10/$500 limits.
The native test policy was revoked after closure; no ongoing strategy authority was created.
Private receipts and the one-use diagnostic are retained under
`reports/mt5/order-test-preflight-20261029`. This operator diagnostic did not exercise the normal
finished-strategy deployment approval path. Broker responses reported zero fill price and deal ID;
actual prices and deal IDs were recovered from broker history rather than inferred from the
submission acknowledgments. None of these checks certifies FTMO rules, sustained operation or
the automatic strategy-agent runner.
