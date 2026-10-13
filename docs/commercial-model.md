# Commercial model

Status: confirmed pricing direction; billing is not implemented.

## Confirmed direction

Sell stratosphere with a service commission of **0.5% on each opening trade, capped at $1**. Keep the repository private. Open sourcing is a possible fallback if monetization fails, not an automatic action or an approved license change. Release only after extensive testing; there is no release deadline.

## Confirmed charge interpretation

The next answer batch confirms a charge based on opening-order trade value rather than profit, one cap per opening order, and no charge for increasing an already-open position. The earlier USD-cap question was answered affirmatively. For an eligible opening order, the target formula is min(0.005 × executed opening-order value in USD, 1 USD). Examples: a $100 opening has a $0.50 fee; a $200 or larger opening reaches the $1 cap. These are service fees, separate from venue costs.

Do not classify every increase in exposure as chargeable. The position scope used to determine whether an order opens a new position—aggregate broker position versus strategy-attributed position—still needs specification.

## Decisions needed before implementation

- Executed opening-order value is selected. Finalize valuation of leveraged/foreign-currency instruments and rounding without substituting profit or collateral as the fee basis.
- The cap is per opening order and reinforcement is exempt. Specify short openings, reversals, hedged accounts and position scope across concurrent strategies.
- Specify USD valuation timestamp, exchange-rate source, currency rounding and sub-cent accounting.
- Define grouping and cap handling for partial fills, replacements, retries, simultaneous strategies, and netted orders. Fees must not duplicate after replay or reconciliation.
- Define treatment of rejected/unfilled orders, paper trades, closing trades, manual broker trades, refunds, and corrections. Only opening trades are currently named as chargeable; do not add other charges silently.
- Choose collection: broker-supported fee collection, prepaid balance, periodic external invoice/payment, or another verified mechanism. Do not assume Alpaca or Liquid can collect the proposed fee.
- Define payment-provider choice, user fee consent, billing records, failed-payment policy, dispute handling and whether billing eligibility blocks new entries.
- Define which costs are included versus supplied by users: model calls, infrastructure, data, broker charges, and funding.

## Proposed implementation scope

Use versioned fee policies, immutable execution references, a private billing ledger, idempotent charge events and reconciliation. The fee is computed deterministically outside agents and presented in cost estimates/reports under the approved rules. Keep service fees separate from broker commissions, spreads, funding and slippage. Include applicable service fees in backtest/paper assumptions before promoting a candidate for commercial live use.

Test the approved formula, cap boundaries, fractional execution values, partial fills, retries, reversals, cancellations, concurrent signals, currency conversion and payment failures. These are proposed acceptance areas; exact rules depend on user answers.

## Collection alternatives under review

No collection method is selected. Evaluate prepaid service-fee credits, aggregated periodic billing, and verified venue-supported collection. Prepaid credits are service credits separate from trading collateral. Per-order card charging is a poor fit for small fees because processors may impose minimum payments; Stripe documents a $0.50 USD minimum in its [currency guidance](https://docs.stripe.com/currencies). Stripe is a possible integration, not an approved provider. Its [Invoicing API](https://docs.stripe.com/invoicing) supports invoice collection, but merchant eligibility, economics, implementation and approval remain to be verified.

Proposed default for review: prepaid fee credits, with optional user-authorized top-ups; if credits are insufficient, block new chargeable openings while continuing permitted risk-reducing management. Payment-failure treatment and credit refunds are unconfirmed. The private server ledger must reconcile verified execution evidence and not rely only on user-editable local reports.
