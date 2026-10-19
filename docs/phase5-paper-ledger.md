# Authored-paper ledger foundation

Status: **implemented as an internal local library; production paper runtime not implemented**. This increment supplies durable capital reservations, personal allocation references, fill accounting and conservative failure gates. It does not activate a strategy, feed, account connector or background process, and has no order transport or TUI trading command.

## Scope and authority

`paper/ledger.py` accepts only `authored-paper-account/1`, a local cash account with the fixed `BTCUSDT.SIM` market and `authored_fixture_only` evidence. It cannot represent a Liquid perpetual account or submit a live order. A trusted application must own one canonical journal per account before any runtime integration; arbitrary library paths are not global account coordination or authenticated ownership.

Each personal instance pins a registered strategy public ID/hash, implementation digest, local account, allocation, order/quantity bounds and maximum quote age. Registration verifies the actual local registry's immutable strategy identity. These are **references**, not an executable-artifact attestation, complete reviewed trading mandate, suitability assessment or user approval. Allocation/account/instance pins cannot be replaced on reopen. Instance creation does not register a behavioral variant. The implementation digest must be verified against a trusted executable by the future runtime. This ledger does not validate an intent against the pinned strategy's behavioral sizing/rules; that trusted compiler/signal gate is still required. Its authored order fixtures test accounting, not strategy execution.

Intent records bind instance, quantity, side, price/fee bounds, signal/proposal digests and explicit timestamps. Quote age is pinned by the instance rather than supplied as an intent permission. The caller currently supplies timestamps; no verified provider quote, signal, budget/risk agent output or freshness attestation is acquired here. No model is connected to this API.

## Durable operations

- All mutations use SQLite `BEGIN IMMEDIATE`. Concurrent callers against the same journal cannot spend the same instance capital or independently claim the same delivery.
- Account creation and each instance, decision, delivery claim, fill, cancellation request, block and terminal observation append to a hash-linked journal. Update/delete triggers reject ordinary mutation; missing/changed records fail verification. This detects retained-state damage, not a malicious local administrator rewriting the database and hashes.
- Fixed allocations cannot exceed starting account cash. Reservations include the remaining buy quantity at its conservative price bound and unused fee allowance. Actual cash and attributed base positions remain separate from pending reservations. One pending order per instance and flat-only entries prevent reinforcement in this scope; other instances cannot sell its attributed base.
- Intent IDs deduplicate both accepted reservations and blocked decisions. A changed body with a reused ID is rejected. A second delivery claim is rejected, including after reopening. Returning an existing reservation is never permission to resubmit it.
- Delivery claims recheck expiry, pinned quote age, cancellation requests, entry blocks and uncertain state. The operation records a claim **before** any future trusted transport; this library invokes none. Dispatching/ambiguous state blocks additional actions instead of guessing whether delivery happened.
- Fill IDs deduplicate exact observations. Conflicting observations retain evidence and block further actions. Actual reported economics remain visible for late fills, overfills, fee/price breaches or negative balances; they are not discarded because a risk check failed.
- Cancellation requests do not claim a cancellation or release funds. A trusted terminal observation releases the remainder only when its cumulative filled quantity agrees with retained fills. Missing fills/conflicting terminal states remain ambiguous. This is a fixture-observation gate, not implemented broker reconciliation or account-snapshot verification.
- Blocking new entries, requesting cancellation and obtaining a terminal state are distinct. A pure entry block permits a fresh, attributed reduction. Damaged/ambiguous account state requires reconciliation. Automatic clearing of faults, closure requests and emergency procedures are not implemented.

The supported concurrency policy is database commit order for local reservations, one pending order per instance, long-only virtual attribution and shared cash. It does not implement external netting, broker order priority, multi-instrument concentration or a multi-provider portfolio.

## Native currency accounting

Inputs use exact decimal strings and the SIM profile's eight-decimal quantity/price/quote-currency quantum. Amounts are structurally bounded to 24 integer digits/eight fractional digits to keep arithmetic deterministic; those are implementation limits, not broker precision verification. Cash uses the **reported quote notional and quote fee**, not an unrounded price × quantity assumption. Future adapters must derive those values from their trusted execution/accounting source and reconcile actual balances.

Tests use pinned NautilusTrader `CurrencyPair.notional_value` for native fill notionals and compare the resulting journal cash/base with the **actual native account cache** after each streaming batch. This matters at fractional and half-quantum costs: the Python `Money` conversion can differ from a naïve exact-decimal rounding rule. It is not a claim that every amount/venue is numerically conformant.

Buy reservations round the price-bound notional upward; sell fee bounds use conservative minimum proceeds. Cumulative observed notional, fees and remaining reservation may not expand the original order budget. If rounding across partial fills breaches that budget, the ledger retains the actual debit/exposure and blocks further actions. It does not silently grant a rounding buffer or invent a new approved amount. Funding, leverage, base-currency fees, borrowed positions and stratosphere fee collection are outside this profile.

## Verification

The focused suite passed **32 checks**, including three real pinned-native cases. The native fixture uses authored quotes, cash/NETTING, liquidity consumption, no borrowing and market IOC orders. A 0.7 BTC request receives a 0.4 BTC partial fill and a canceled remainder; a later 0.1 BTC reduction retains 0.3 BTC. Ledger cash/base match the native account after each batch for whole, fractional and tiny prices.

The journal is reopened between batches while the native engine remains active. This verifies **journal recovery and streaming engine continuity**, not whole-process native reconstruction, reconnect/replay or sustained real-feed operation. Native event/account IDs in these fixtures are not provider reconciliation evidence.

Other checks exercise concurrent reservation attempts, stable pins, account/amount rejection, denied-decision retention, stale/expired intents, delivery rechecks, entry-only blocking, duplicate/conflicting/late fills, actual fee accounting, currency rounding breaches, cross-strategy position isolation, canceled/rejected/expired terminal observations, missing-fill reconciliation, symlink refusal and damaged-journal retention. The full regression suite passed **691 tests with zero skips**, including 12 pinned-native checks (36 upstream Pandas deprecation warnings). Built wheel/sdist checks verified source/CSS equality, documentation inclusion and configured-credential/private-state exclusion. A separate built-wheel process passed reservation, partial-fill accounting, cancellation and journal reopening using authored inputs; it made no provider, model, feed or order calls.

Run the native verification in an environment containing the pinned engine:

```sh
STRATOSPHERE_REQUIRE_ENGINE=1 python -m pytest tests/test_paper_ledger.py tests/test_paper_native.py -q
```

## Closed-bar processing follow-up

The [reviewed closed-bar journal](phase5-closed-bars.md) now adds an internal observation-processing mandate, exact approval, ordered/freshness gates and retained crossings in the same account transaction. It upgrades journals to version 2 only when a stream is prepared and preserves historical events. It does not deliver orders, provide a real feed, approve a production execution mandate or implement operational TUI controls.

## Remaining Phase 5 work

The [acceptance ledger](phase3-5.md) remains authoritative and unchecked for production paper execution. Still required: trusted executable/personal-mandate review and activation; budget/risk agents and deterministic exposure/concentration/loss/drawdown checks; verified real feeds and account identity/environment; native runtime/gateway ownership; provider paper isolation/confirmations; complete order/event/account reconciliation; whole-process restart and sustained operation; read-only operational views; and the settled keep-positions/request-all-closures TUI exit choice. There is no session-end liquidation policy or running paper daemon in this increment.
