# Reviewed authored signal decisions

Status: **internal authored reservation and risk-gate increment**. This connects retained closed-bar signals to the existing paper ledger. It supplies no Liquid adapter, real feed, background service, model budget/risk specialist, external account verification, operational TUI command or provider order transport. The [full acceptance ledger](phase3-5.md) remains in progress.

## Review and authority

An immutable `authored-signal-decisions/1` plan selects one existing authored account and exact approved stream digests. Each stream already binds its personal allocation, complete registered behavior and implementation. The plan pins its own implementation source digest and requires explicit gross exposure, concentration, loss, drawdown, spread, slippage, fee allowance and time limits. There are no empirical/default promotion thresholds. These limits are authored configuration choices in tests, not suitability recommendations.

Preparation and exact-digest approval require an account without prior orders or fills. This establishes a loss/high-water baseline from its retained starting cash; the code does not invent historical account peaks or migrate an already traded account. A pending plan is immutable. Its preparation upgrades the local journal to version 3; earlier events and version-1/2 readers in the current implementation remain supported. Preparing an additional raw stream preserves the newer version marker and does not add it to the approved decision plan. Older application versions reject the new journal version. Selecting the profile also blocks new legacy reservations while review is pending.

Source files, registry definitions and exact stream pins are verified before review/approval. New decisions verify their selected stream; delivery claims verify all selected definitions again. Source changes require a new compatible review rather than silently continuing an old plan. Source-file guards and local UUIDs are not loaded-executable attestation, authenticated account ownership or global coordination across arbitrary paths. A trusted future service must own one canonical journal per actual account.

This plan approves **authored reservations only**. It is not a live/paper provider mandate, data entitlement, user authentication, source conversion or native worker attestation. Models have no approval or provider-call path in this module.

## Quote and signal handling

Quotes are explicitly `BTCUSDT.SIM` / `authored_fixture_only`. They carry exact eight-decimal bid/ask/size fields, event time and availability; receipt time is supplied separately. Times and displayed liquidity remain authored caller observations, not provider-authenticated evidence. Structural validity does not establish executable prices or native venue filters.

Quote attempts retain invalid temporal ordering, age, revisions and ID conflicts. Exact duplicates and aliases of the same event cannot refresh the current mark, clear a rejected observation or replenish used displayed size. Conflicting IDs/event revisions persist as an account reconciliation fault. A subsequent ordinary fresh quote can recover a transient stale/future/out-of-order observation, but cannot clear a conflict or risk stop. There is no automatic heartbeat to detect an absent feed.

A decision names an exact retained signal and owning personal instance; callers cannot supply a new side, sizing, price or approval. The selected stream must be reviewed, approved and unhalted. The crossing must belong to its latest closed candle and remain inside its reviewed lifetime. A quote must occur after the signal's declared availability/receipt and nominal next-open boundary. Quotes received later cannot grant retrospective fills at an earlier open. Plan and instance quote-age limits both apply.

Entries use the strategy's fixed base quantity only while flat. Exits use the actual attributed position, including a partial opening fill. Existing pending orders block another decision for that instance; this does not cancel them or manufacture a closure. Reinforcement and already-flat exits are retained as blocked decisions. The instance's existing quantity/order-notional bounds still apply to reductions; automatic splitting or an emergency override is not implemented.

Each signal is consumed once, whether reserved or blocked. Consumption and a successful cash reservation commit in one `BEGIN IMMEDIATE` transaction. A crash before commit rolls back both; a retry after commit returns the retained decision and never a new delivery permission. Deterministic intent IDs cannot overwrite previously denied/accepted intent records. Changing a quote or restarting cannot revive a consumed blocked signal.

## Deterministic account gates

All values come from the same account transaction. No model approval is consulted.

- Capital and existing instance bounds use the shared ledger checks, including actual cash, unused fee allowances, other reservations, flat-only entries and attributed reductions.
- Gross exposure includes held base at the current ask and pending entries at the higher of current ask or their reviewed price bound. Pending exits do not release exposure until fills arrive.
- The projected equity floor marks held base at bid, deducts conservative pending entry costs and unused fees, credits their possible base at bid, and deducts pending exit fees/adverse slippage. Concentration compares projected gross exposure with that floor using exact decimal cross multiplication. Nonpositive projected equity blocks entries.
- Entry cost limits include the proposed order before reservation. If worst-case spread/slippage/fees breach the reviewed loss or drawdown budget, the signal is blocked. A hypothetical breach does not become an observed-loss stop.
- Loss is measured from retained starting cash; drawdown uses a durable peak of observed, bid-marked equity. Observed threshold breaches latch an entry stop across recovery/restart. Fresh quotes and actual reported fills can create findings. Fill-based marks explicitly reference the last retained quote and fill ID; they are not a fresh or authenticated market valuation at fill time.
- Displayed size accounts for pending quantities and fills already attributed to that quote. The quote-size check is an authored assumption, not proof of execution, queue priority or future liquidity.
- Entry stops preserve fresh attributed reductions. Quote/account ambiguity, stale data, spread, instance bounds or pending orders may still block a reduction. A blocked exit is not a completed closure.

Price bounds round upward for buys and downward for sells; absolute fee allowances round upward to the SIM quote quantum. These are finite reviewed budgets, not recomputed broker charges. Actual native reported notional/fees remain authoritative in the ledger. Out-of-budget or late fills retain their actual economics and block further action. Better sell prices can also produce fees above a fixed allowance; no automatic expansion of the fee budget is implied.

The local concurrency policy remains transaction commit order, one pending order per instance, shared cash, no borrowing and long-only virtual attribution. It is not broker netting, multi-instrument portfolio optimization, funding/margin control or guaranteed market impact modeling.

## Delivery claims and evidence

Reviewed accounts reject the legacy `PaperLedger.reserve` and `begin_delivery` paths for new activity. `SignalDecisions.begin_delivery` verifies current registry/code, stream state, age, quote/spread/price bounds, displayed size, projected risk and existing ledger controls before retaining a single delivery claim. A changed condition preserves the original reservation; it does not silently release capital. Cancellation, terminal observations and reconciliation remain distinct.

The gateway does **not** submit/cancel an order or invoke Liquid. The native integration test explicitly provides its own authored NautilusTrader fixture transport. It compares the actual native account cache with ledger cash/base after a full IOC fill and reopens the journal to reject repeat decisions/claims. Existing native partial-fill ledger tests and authored partial-fill decision tests remain separate from this one full-fill integration proof.

Risk findings, exact source signals, quote attempts, reviewed limits, projected metrics, blocks, reservations and actual fills remain linked in the private hash-chained account journal. They are not shared community performance reports or provider confirmations.

## Verification and remaining work

Tests exercise review and schema restrictions, unavailable/invented/foreign signals, causality and expiry, duplicate delivery/restart, old denied IDs, atomic crash rollback, cross-strategy shared limits under concurrency, liquidity aliases, account loss/drawdown latches, fresh reductions, changed code/registry and changed risk before a delivery claim. A worst-price/fee exit fixture's actual final cash equals its earlier projected equity floor. A real pinned native fixture verifies one complete signal-to-reservation-to-fill accounting path; it is not a production adapter.

Measured full-suite and installed-wheel results appear in the [acceptance ledger](phase3-5.md). A separate [advisory review increment](phase5-paper-reviews.md) now supplies fixture-verified budget/risk proposals without changing these gates; its live-model reliability remains unverified. [Account inspection](paper-inspection.md) now exposes the existing journal's captured positions, reservations, orders, fills and health without mutation or model disclosure. Still required: reliable specialist validation and reviewed capital changes, trusted executable/provider bindings, real feed rights and publication evidence, actual account reconciliation, persistent service ownership and heartbeat, sustained operation, provider-backed operational views/controls and the confirmed keep/request-all-closures exit choice. There is no additional trading scope or application login in this increment.
