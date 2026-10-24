# Internal account and runtime foundations

These components process authored software-test data. They are not a Liquid adapter, real feed,
running broker paper service or order transport. The separate new Alpaca paper runner is described
in [agent workflow](agent-workflow.md); it does not replace or relabel these authored journals.

## Existing components

- A local account ledger retains capital reservations, order/fill economics, ambiguity and faults.
- Closed-bar processing retains per-instance stream state, raw crossings, aliases, revisions and gaps.
- Reviewed decisions consume exact crossings and reserve capital through deterministic gates.
- Budget/risk models provide separate advisory reviews over explicit read-only frozen snapshots.
- A durable observation queue and separately approved worker retain ownership, progress and halts.
- `/paper` opens existing authored journals in Positions; missing/unreadable accounts stay unavailable.

The worker is a development service over existing approved state. It does not create/fund accounts,
approve streams, fetch provider data or activate trading. Its environment excludes credentials.
Reading health cannot start, acknowledge, reset or stop the worker.

## Correctness requirements

Use actual reported native notionals/fees and reconcile currency rounding against balances.
Retain pending capital until terminal observations match retained fills; preserve late, conflicting
or breaching evidence. Duplicate claims cannot deliver twice across restart.

Commit crossing consumption and reservation atomically. Include other strategies' pending entries
and conservative pending-exit costs in exposure/equity. Retain loss/drawdown stops, quote-size
consumption and conflicts across restart. No model or legacy API may bypass reviewed-account gates.

The canonical per-account queue requires a held OS owner lock. Recover the account-commit/queue-ack
crash boundary through retained duplicate handling. Deadlines remain per stream across restart;
aliases, duplicates and heartbeats cannot renew progress. Halt without backfill or capital release
on silence, clock, connectivity, identity or stream faults. Historical IDs/heartbeats do not prove
current process liveness, and a held lock does not prove a healthy feed.

## Inspection and reviews

Inspection uses read-only transactions and complete pagination. Preserve exact fills, fees,
reservations, uncertainty, capture times and digests. A failed refresh clears the Positions preview.
Inspection cannot create/migrate/fund accounts or disclose their evidence to strategy models.

Explicit budget/risk reviews keep role/account contexts separate. Application code validates
coverage, amounts and fact references; reports cannot mutate allocations, approve mandates or
allow orders. Open positions/pending orders lock the existing allocation. Interpretation remains
unverified. See [invariants](invariants.md) for the full subsystem requirements.

## Missing integration

The plan still requires an eligible structured Liquid feed, verified account/paper environment,
perpetual accounting, an idempotent broker gateway, full reconciliation, portfolio attribution,
sustained operation and the keep/request-all-closures exit flow. Current local fixtures prove
software behavior only.
