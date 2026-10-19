# Reviewed closed-bar journal

Status: **implemented internal authored-data processing; no real feed, daemon or order gateway**. This advances the persistent runtime requirement in the [Phase 3–5 ledger](phase3-5.md), which remains in progress. There is no new paper-trading command, account entitlement or strategy activation.

## Review and identity

`paper/runtime.py` processes only `authored-btcusdt-hourly/1` observations in an existing `authored-paper-account/1` ledger. A `ClosedBarMandate` pins the existing personal allocation, complete registered definition, local implementation source digest, first UTC hourly boundary, maximum publication delay, maximum receipt age and `halt_without_backfill` policy. The complete definition must match the trusted exact-rational SMA compiler. Declared source formats or indicator names alone are insufficient.

`prepare` retains this immutable draft without reallocating capital or accepting observations. `approve(instance_id, exact_digest)` approves precisely the pending draft; a wrong digest or repeated approval fails. The registry definition and implementation source files are checked before preparation, approval and every new ingestion. A source snapshot taken when the runtime module imports also prevents accepting a newly supplied file hash after that process's files change. Historical state remains inspectable after source changes; new processing requires a new review and compatible allocation. Automatic mandate replacement, fault clearing and migration of an active instance are not implemented.

This is a local application approval boundary for observation processing, not proof of a verified external user, native executable artifact, production activation or paper eligibility. The source-file guard is not attestation against a modified Python process or local administrator. The complete trusted executable/transport binding remains required before orders can be enabled. No model, source reader or credential has a path into these operations.

## Observation policy

An observation contains an exact hourly OHLCV candle, explicit availability time, authored profile/evidence and delivery ID. Existing SIM precision and OHLCV checks apply. Receipt time is supplied separately. These are **authored caller timestamps**, not authenticated provider publication/receipt evidence.

- The first candle must match the reviewed boundary, and subsequent accepted candles must be consecutive hours in supplied order. A gap or out-of-order observation halts that instance. The processor does not sort, invent candles, backfill a gap or silently resume after a reconnect.
- Availability must be at or after the next hourly boundary, after the inclusive microsecond close. Receipt must be at or after availability. Publication delay and receipt age must fit the explicit reviewed limits. Accepted receipt times cannot regress.
- Invalid but structurally readable observations retain their exact payload, receipt and reason. The halt persists across reopening. Subsequent observations remain retained as halted attempts, rather than clearing the condition.
- An exact redelivery returns `duplicate` with no new signal. The same candle under a new delivery ID is retained as an alias without advancing the kernel. Reusing either ID with changed content or revising an accepted candle retains the conflict and halts processing.
- No heartbeat or wall-clock feed monitor runs here. An absent feed is not detected merely by reading the stored state. Current liveness/freshness, provider authentication and reconnect/catch-up orchestration remain runtime work.

All changes share the account's `BEGIN IMMEDIATE` transaction and hash-linked event journal. Candle acceptance and its derived crossing commit together. A pre-commit crash rolls back both; a replay after commit does not emit again. A separate interpreter reconstructs the bounded exact-rational SMA window and previous comparison from retained events and can continue without replaying a crossing as new. Account journals remain version 1 until a stream mandate is prepared; that transaction upgrades them to version 2. Current readers preserve either version, and older readers reject version 2 rather than misreading its new event kinds. Historical event bytes are not rewritten.

## Signals and account state

Outputs are **raw crossings**, including crosses that an execution strategy would ignore while flat or already long. They retain the exact mandate-bound digest, source observation, closed-candle signal time, declared availability/receipt and nominal next-open boundary. The nominal boundary is not an executable quote or permission to fill retrospectively. A crossing does not claim sufficient funds, position compatibility, risk approval or delivery eligibility.

Preparation, approval, ingestion, duplicate delivery and stream halts do not create orders, reserve or release capital, cancel anything, alter positions or liquidate at a day boundary. Existing reservations/fills remain governed by the [paper ledger](phase5-paper-ledger.md). A stream halt is not a completed account shutdown or reconciliation. Different personal instances retain distinct stream state, crossing identities and fault conditions under the shared account transaction.

## Verification and remaining work

The deterministic tests cover exact approval, full identity/allocation pins, changed source files, rejected profiles/policies/times, warmup, freshness, gaps, late revisions, ID aliases/conflicts, concurrent delivery, transaction rollback, damaged journals, versioning and separate strategies. Four authored days are compared against an independent batch rational oracle, with repeated reopenings and continuation in a fresh interpreter. This is accelerated fixture coverage, not a four-day wall-clock soak.

A real pinned NautilusTrader research simulation also verifies the retained crossing timestamps after filtering raw crosses using that fixture's flat/long full-fill state. This does not integrate the journal with a native broker or verify live/paper execution. The existing native partial-fill accounting checks remain separate. An independently installed wheel exercises approved processing and fresh-process continuation with authored inputs; it receives no configured provider credentials.

Measured results are recorded in the [acceptance ledger](phase3-5.md). Remaining work includes actual eligible feeds and account identity, publication/receipt provenance, a persistent service and heartbeat, approved execution mandates, budget/risk proposals and deterministic portfolio gates, position-aware signal-to-intent conversion, delivery/reconciliation, provider paper isolation and the confirmed TUI keep/close choice. No actual Liquid tool schema or commercial market-data entitlement is inferred from these tests.
