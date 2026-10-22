# Data capabilities and closed-observation quality

Status: **local inspection and quality checks implemented**. This increment adds no real-time provider, account link, new historical adapter, paper runtime or execution integration. Existing bounded historical proofs remain distinct from a commercially entitled trading feed. Full Phase 4/5 acceptance remains pending in [the ledger](phase3-5.md).

## Inspect current scope

D58 removes the former alternative-broker data profile. Liquid is the only trading connector, but its read-only catalog/address diagnostics are not an eligible price-feed profile. The reference/archive/fixture profiles below remain research or software-verification tools, not additional trading integrations.

Use `/data`, `/data <provider-profile>` or `/data <provider-profile> <purpose>`. The read-only result opens in Focus and scrolls with Page Up/Page Down, including at 45 × 12. It requires online access and makes no model request, price download, registration, account link or order. Profiles are explicit requested examples, not discovered account instruments.

| Profile | Verified/implemented scope | Eligible purpose in this inspector |
| --- | --- | --- |
| `ecb-reference-proof` | Bounded January–March 2025 daily EURUSD reference-rate proof | `reference_proof` only; never tradable forex or paper data |
| `binance-archive` | Existing BTCUSDT spot hourly archive research profile and separately reviewed studies | No new entitlement granted; commercial usage remains blocked by this routing policy |
| `authored-fixture` | Development fixtures, subject to exact-byte binding by the caller | `software_verification` only; no market/performance/real-paper claim |
| `kraken-public` | Documentation reviewed; no implemented/verified adapter or commercial permission | None |

No profile is eligible for `paper` in this increment. A technically matching symbol does not establish entitlement, liquidity, venue filters, a current observation or execution readiness. The routing function recomputes eligibility from packaged policy; model output does not supply it. Market, venue, symbol, base asset, quote currency, interval and price kind must match the bounded profile. Spot/perpetual, USD/USDT and cross-venue aliases cannot transfer capability.

The existing legacy research/study operations keep their own reviewed protocols and historical evidence; this inspector does not execute, rewrite or retroactively certify them. It is not a new market-data source or universal account entitlement service. Future feed/paper operations must use explicit eligibility and verified provenance at their execution boundary.

## Observation and quality contracts

`market-data-batch/1` retains exact market identity, provider symbol, raw/normalizer digests, references, availability provenance, declared session/action/funding scope and observations as received. Bar ends are **exclusive**; the older archive contract retains its separate inclusive microsecond-close convention. Prices/volumes use exact decimal strings and timestamps use strict integer nanoseconds. No silent precision rounding, numeric coercion or fake volume is introduced.

The quality policy explicitly supplies freshness/receipt-lag bounds, warmup, alignment origin, optional exact requested window, and whether close-time availability assumptions are acceptable. Checks retain all input rows and findings. They neither sort/deduplicate nor fill missing intervals/prices. Any finding makes the whole batch unacceptable to this quality check; no partial clean subset is silently promoted.

Findings cover incomplete/running bars, duplicate/unordered/overlapping/missing intervals, width/alignment/window coverage, future timestamps, availability before close, receipt before availability, receipt lag, staleness, missing volume and unverified availability. Receipt-only provenance cannot assert a publication timestamp earlier than receipt. A permitted close-time assumption must actually match the retained close boundary. Recent receipt cannot make an old observation current.

This v1 checker does not implement pinned exchange-calendar/action, perpetual settlement/multiplier/funding or tradable-forex adapters. Labels alone cannot establish those capabilities. Calendar gaps require an actual schedule; reference rates remain nontradable. The old ECB proof continues through its dedicated reference contract, rather than being relabeled as venue OHLCV.

A passing structural/chronological result does **not** verify that a digest matches provider bytes, prove a provider's identity, establish native-engine price/quantity precision, establish a data license or authorize trading. A future trusted adapter/store must bind and verify raw bytes, normalization and instrument/account metadata before use. No broker precision, calendar, corporate-action or funding behavior is inferred from a symbol here.

## Provider evidence and rights

- [Binance dataset terms](https://github.com/binance/binance-public-data/blob/master/TERMS_AND_CONDITIONS.md) and the existing [rights record](data-source-rights.md) keep technical archive access separate from commercial entitlement.
- [Kraken's official API notice](https://docs-legacy.kraken.com/api/docs/guides/global-intro/#notices) requires prior permission for non-personal commercial use of public endpoint data, including market/status data. No permission has been supplied and no new Kraken market/status probe was performed.
- [Kraken OHLC REST](https://docs.kraken.com/api-reference/market-data/get-ohlc-data) documents the newest-720-entry limit and final uncommitted candle. [WebSocket OHLC](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ohlc) updates are trade-driven. Those facts do not implement closed-bar/reconnect/catch-up behavior in stratosphere.

No data license was purchased, paid capacity enabled or provider contacted. Further native/software validation continues on authored fixtures. Entitlement and actual real-time/account verification remain required work, not silently waived release criteria.

## Verification

The results below describe the earlier profile matrix. D58 removes the alternative-broker profile and its current provider references; requesting that former profile now fails as unknown. This does not turn Liquid diagnostics or any research profile into a real paper feed.

The focused data/TUI suite passed **55 checks**: exact profile/currency/venue separation, purpose isolation, strict numeric/timestamp types, quality/freshness/availability failures, unsupported market semantics and model-free online inspection. The full regression passed **617 tests**, zero skips, including nine native-engine tests (33 upstream Pandas deprecation warnings). The first full run exposed a flaky Reports assertion that assumed nonzero scrolling despite equal-timestamp hash ordering; the corrected check verifies that the selected row is actually visible. A subsequent **54-check source/TUI group** passed after making Markdown headings explicitly light in monochrome mode. Ruff/whitespace checks passed. Rebuilt wheel/sdist inspection matched Python/CSS sources and excluded configured credentials/private state/caches; the packaged capability view was inspected at 180 × 48 and 45 × 12 with keyboard scrolling and an injected online fixture gate only for rendering. No real feed or new provider capability is claimed by these fixtures.
