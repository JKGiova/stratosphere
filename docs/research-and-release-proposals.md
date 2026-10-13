# Research, collection and release proposals

Status: proposals for user review, October 13, 2026. No payment provider, data provider, numerical job budget, paper duration or background-exit policy is approved by this document.

## Payment collection

Compare prepaid service-fee credits, aggregated periodic billing and venue-supported collection. Prepaid credits are the current proposal: separate from trading collateral, with optional user-authorized top-ups. Failed-payment/insufficient-credit policy remains unconfirmed. Do not interpret these credits as custody of trading funds.

Stripe is a candidate rather than an approved integration. It documents minimum payment amounts, including $0.50 USD: [currency guidance](https://docs.stripe.com/currencies). Its [Invoicing API](https://docs.stripe.com/invoicing) supports automated collection of invoices. Provider eligibility, processing economics and fee reconciliation remain to be verified before selection. Liquid's ability to collect the exact stratosphere fee is not established by Co-Invest documentation.

## Local runtime and exit

Confirmed: users run research/trading locally and choose keeping positions or requesting closure of all positions on TUI exit. Proposed keep behavior: leave a separate local worker running under the approved mandate while the computer is online. Alternative: stop management and leave existing positions at the venue. The user has not selected either meaning.

Local logic cannot run while the host is off. Broker-side protective orders must be capability-tested and cannot be assumed to provide all runtime-managed exits. Test pending entries, partial closures, cancellation races, connection loss and residual exposure. Do not report a closure request as a completed liquidation without execution evidence.

[Liquid Co-Invest](https://www.liquid.trade/coinvest-docs), reviewed October 13, distinguishes standard explicit confirmation from Computer simulation/staged/live modes. Verify stratosphere's custom-client support and NautilusTrader integration; this documentation is not proof that either works.

## First data proof

Proposed initial engineering instruments: BTC and ETH, one-hour candles. The initial proof verifies data ingestion, timing, costs, rule conformance and reproducibility, not investment suitability or profitability.

- [Binance public archives](https://github.com/binance/binance-public-data) offer downloadable historical candles and trades with checksums. Pin snapshots and normalize timestamp units. Verify data quality, symbol/instrument semantics and actual access before integration.
- Liquid paper/live validation requires the historical/live data and funding semantics of the actual execution instrument and venue. Binance spot observations must not be transferred as perpetual performance evidence.
- [Alpaca data documentation](https://docs.alpaca.markets/us/docs/market-data-faq) distinguishes IEX from consolidated SIP feeds and their access conditions. This is an equities alternative, not a selection or a change to Liquid-first execution.
- Forex remains product scope; its first instrument/provider is not selected.

The five Phase 1 external content sources remain required regardless of the first market-data proof.

## Job-budget explanation

A job budget is the user's chosen stopping rule for a research task: model-call spend, duration, variant count or resource concurrency. For illustration, a plan could allow $2 of model spend and 20 variants. These values are examples, not approved defaults or application upload quotas.

Proposed behavior: present the limits in the plan, run after one approval, checkpoint results and pause when a limit is reached. Increasing an approved budget requires a new approval. Provider cost estimates and actual usage must be verified before claiming precise enforcement.

## Proposed release gates

1. Representative Linux/macOS onboarding, browser auth, provider model configuration, panels and slash commands pass.
2. All five content source integrations pass accessible examples and explicit inaccessible/unsupported cases.
3. External-code conversions pass behavior checks; historical runs reproduce under frozen inputs, costs and timing.
4. Ownership isolation and private/public projections pass; billing formula/caps, partial-fill grouping and retries reconcile.
5. Paper operation passes crash/reconnect, pending-order reconciliation and keep/close exit cases.
6. Proposed initial 30-day paper stability exercise and invited-user acceptance complete without unresolved critical failures. Duration is unconfirmed and is not a universal strategy-promotion requirement.
7. Commercial live release also requires verified connector permissions, deterministic limits, approved billing/collection and live operational acceptance. Paper success does not authorize real-money orders.

Specific strategy evidence, sample sizes, robustness thresholds, capital/risk mandate, scale and support expectations remain separate unanswered decisions.
