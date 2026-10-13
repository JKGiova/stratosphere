# Commercial model

Status: confirmed pricing direction; billing is not implemented.

## Confirmed direction

Sell stratosphere with a service commission of **0.5% on each opening trade, capped at $1**. Keep the repository private. Open sourcing is a possible fallback if monetization fails, not an automatic action or an approved license change. Release only after extensive testing; there is no release deadline.

## Decisions needed before implementation

- Define the fee basis: executed notional, capital allocation, margin, or another value. Do not implement a notional-based formula without confirmation.
- Define an opening event: an order, a position opening, each increase in exposure, or another grouping. Specify short openings and reversals.
- Define whether $1 means USD, valuation timestamp, exchange-rate source, currency rounding, and minimum-charge behavior.
- Define grouping and cap handling for partial fills, replacements, retries, simultaneous strategies, and netted orders. Fees must not duplicate after replay or reconciliation.
- Define treatment of rejected/unfilled orders, paper trades, closing trades, manual broker trades, refunds, and corrections. Only opening trades are currently named as chargeable; do not add other charges silently.
- Choose collection: broker-supported fee collection, prepaid balance, periodic external invoice/payment, or another verified mechanism. Do not assume Alpaca or Liquid can collect the proposed fee.
- Define payment-provider choice, user fee consent, billing records, failed-payment policy, dispute handling and whether billing eligibility blocks new entries.
- Define which costs are included versus supplied by users: model calls, infrastructure, data, broker charges, and funding.

## Proposed implementation scope

Use versioned fee policies, immutable execution references, a private billing ledger, idempotent charge events and reconciliation. The fee is computed deterministically outside agents and presented in cost estimates/reports under the approved rules. Keep service fees separate from broker commissions, spreads, funding and slippage. Include applicable service fees in backtest/paper assumptions before promoting a candidate for commercial live use.

Test the approved formula, cap boundaries, fractional execution values, partial fills, retries, reversals, cancellations, concurrent signals, currency conversion and payment failures. These are proposed acceptance areas; exact rules depend on user answers.
