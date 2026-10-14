# Phase 0 canonical contracts

These are executable, versioned contracts and authored examples, not a registry deployment, strategy converter or general strategy compiler. See schemas/ and examples/phase0/ for the wire format and src/stratosphere/phase0/contracts.py for cross-field validation.

## Behavioral identity

strategy/1 contains every formal_spec group from database-model.md. All groups are mandatory, including empty filters and explicit assumptions. Unknown fields, incomplete rules, duplicate IDs, undefined indicator references, non-boolean rules, invalid arity, inconsistent shorting/timeframes and insufficient warmup fail validation. Unresolved drafts are outside this complete-spec schema and cannot obtain a registered identity.

Canonicalization is UTF-8 JSON with sorted object keys, compact separators, no NaN, and unchanged array order. Exact decimal values are strings without exponent notation; only trailing fractional zeroes and negative zero are normalized. No float conversion, precision rounding, case folding, expression reordering, rule removal or mathematical-equivalence inference is performed. The format version is part of the digest. Unknown JSON keys and duplicate object keys fail closed.

The SHA-256 fingerprint covers the entire formal_spec, including assumptions. Name, description, source references, authors, performance, account and deployment capital are separate. A digest selects candidates; complete canonical bytes must match before reuse. An unexpected hash collision is an error. Adding a corroborating source preserves identity and requires a new provenance association/submission in Phase 1; changing a rule, fixed instrument, timing, sizing or missing-data behavior requires a distinct identity. Conservative structural identity can keep mathematically equivalent expressions distinct; similarity never proves equivalence.

Public IDs are selected as **24 random decimal digits**, retained as strings including leading zeroes. They are identifiers, not secrets or permissions. Phase 1 must enforce database uniqueness and transactional collision retry. Internal keys are selected as UUIDv7, independent of public IDs. Neither a persistent allocator nor database constraints are implemented here.

## Rule vocabulary and capability checks

Expressions support exact decimal/boolean literals, bar fields, named indicator references, position states, arithmetic, comparisons, binary boolean operators, negation and crosses. Rules are ordered within their groups; signal_timing defines entry/exit precedence. Crosses use the previous and current observations: above is previous left <= right and current left > right; below is previous left >= right and current left < right. First observations without a prior valid value cannot cross. Division by zero is an execution error, never an imputed signal. Rules must resolve ambiguous actions before registration.

Indicator algorithm/version identifies a definition: SMA v1 is the arithmetic mean of the last period observations; EMA v1 seeds from an initial period SMA and uses alpha=2/(period+1); RSI v1 uses Wilder averages seeded from the first period price differences, with 100 when loss is zero and gain is positive and 50 when both are zero. These definitions are schema vocabulary, not claims that adapters implement all three. The Phase 0 engine adapter will accept only the complete authored SMA example; it must reject other contracts instead of silently ignoring their rules. Additional indicators, stop/target/time exits and instrument-specific semantics require versioned extensions and their own conformance tests before availability is claimed.

The specification distinguishes required asset classes and fixed symbols from allowed deployment choices. Personal capital/permissions do not alter identity when permitted; behavioral sizing parameters do. Costs and simulation mechanics belong to experiment/1. Performance interpretation and promotion remain later phases.

## Dataset and experiment identity

dataset/1 currently has one normalization profile: ECB daily USD per EUR references. It pins the provider URL, exact observation dates, normalized date/rate digest, frequency, UTC availability assumption, attribution and usage limitation. A missing/duplicate/unexpected date, changed rate or rejected observation status blocks the proof. Metadata-only upstream CSV changes need not alter normalized identity; raw responses have their own retained checksums. No volume, intraday OHLC, executable quote or point-in-time release history is inferred.

instrument/1 pins declared SIM currency-pair increments/minima. These are test assumptions, not historical broker rules. experiment/1 binds strategy, dataset and instrument fingerprints to the exact engine/adapter, balances, fees, spread/slippage, next-reference fill convention, last pending signal and open-position end behavior. Repeat runs retain separate report identities while comparing their deterministic payloads.

The example uses 3/8 observation SMA crosses, one long position of 100 EUR, 0.1% simulated taker fees, no shorting/reinforcement and no session-end liquidation. Zero spread/slippage is explicitly a simplified compatibility assumption. It consumes the entire historical snapshot without claiming a holdout, profitability validation, broker capability, or product readiness. stratosphere fees and fee collection are not implemented by this protocol.

## Data-source selection

The initial source is the [ECB Data API](https://data.ecb.europa.eu/help/api/overview), series EXR.D.USD.EUR.SP00.A, January–March 2025. [ECB reuse conditions](https://www.ecb.europa.eu/services/using-our-site/disclaimer/html/index.en.html) allow reuse with accurate attribution and disclosure of modifications; information reproduced in paid documents needs a notice that it is free from the ECB. The proof includes this attribution. Reference rates are [informational rates](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html), not transaction prices. Future tradable-data selection remains open.

Binance Vision was rejected for this commercial product because its [current terms](https://github.com/binance/binance-public-data/blob/master/TERMS_AND_CONDITIONS.md) exclude commercial integration. Coinbase was also rejected after its [market-data terms](https://www.coinbase.com/legal/market_data) limited end-user applications, redistribution and AI-related use without consent. No adapter or redistributed dataset for either source is included. A free endpoint is not sufficient permission for a product integration.
