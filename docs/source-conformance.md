# Source conformance checks

Status: **diagnostic rule mapping and execution blocking implemented**. Faithful Pine-to-Nautilus conversion is not verified or enabled. Source acquisition, rule recognition, numerical equivalence, broker-emulator behavior and strategy activation are separate claims.

## Inspect a retained source

Use `/conformance <source-id>` to record missing context, or declare the original chart context explicitly:

```text
/conformance <source-id> crypto_spot BTCUSDT 3600 regular_ohlcv source_defaults
/conformance-show <assessment-id>
```

The source ID comes from `/source`, `/source-text` or `/source-file`. Context is a user declaration about the original chart; it is not inferred from a title or treated as verified provider/account information. Other markets, intervals, synthetic charts and overridden properties remain unsupported by this checker. Both operations require online access. Inspection uses the existing scrollable Focus panel.

The checker reads the verified source snapshot and retains a new dated assessment with its snapshot digest, checker implementation hash, explicit context, exact source-piece/line references, recognized rule fields and blockers. Repeated checks produce distinct records. `execution=blocked`, `actual_pine_runtime_verified=false` and `arithmetic_equivalence_verified=false` are fixed contract values. There is no executable plan, approval, strategy registration, simulation, market download or model request in this operation. Retained source snapshots remain unchanged.

## Strict rule shape

A small Pine 6 shape is recognized deterministically: one single-line strategy declaration, literal close-based SMA periods, explicit fixed quantity, no pyramiding, full long margin, closed-bar calculation, delayed market fills, distinct crossover/crossunder conditions, flat-only long entry and guarded closure with matching order IDs. The entire statement sequence is checked. Additional/missing statements, alternative indicators, input overrides, stop/limit orders, quantity overrides, extra exits/filters, lookahead, unsupported plotting/helpers or different timing produce explicit limitations rather than being removed.

Recognition does not establish that a script compiles in Pine, reproduces an original publication or has the same numerical behavior as our kernel. Comments are data and cannot request approval or code execution. Source text is never evaluated or imported. The original symbol/timeframe come from the explicit chart declaration and are distinguished from code quotes.

Pine input pasted through the older `/new` path is blocked before a model request. A model's `ready` response cannot bypass this check. Ordinary supported natural-language SMA research remains available under its existing reviewed assumptions; this does not make it a faithful translation of a Pine program.

## Numerical verification gap

Pine's [type-system documentation](https://www.tradingview.com/pine-script-docs/language/type-system/#float) describes float arithmetic and comparison rounding to nine fractional digits. The packaged SMA evaluator uses exact rational arithmetic. Matching indicator names/periods alone therefore does not prove equivalent crossing behavior. An authored regression demonstrates a strictly positive exact SMA difference that collapses under a nine-digit comparison model. This is a documented-comparison counterexample, **not an actual Pine runtime test**; the checker does not claim Pine's built-in crossover has been measured end to end.

Pine's [strategy documentation](https://www.tradingview.com/pine-script-docs/concepts/strategies/) also distinguishes closed-bar versus intrabar calculation, recalculation on fills, delayed versus immediate market orders, position reversal, sizing and chart/property overrides. These require explicit conformance rather than inferred defaults or hidden changes.

Enabling converted execution still requires a verified numerical mapping/runtime comparison, preserved whole-source semantics and immutable binding into a versioned executable plan. No `research-plan/2` or executable source-conversion path has been added by this diagnostic increment. Legacy research/study wire formats and prior sealed evidence remain distinct.

## Verification

The deterministic suite tests whole-source rule recognition, unsupported behavioral mutations, missing/unsupported chart context, exact line quotes, numerical boundary evidence, dated immutable records, source binding, retained limitations and the legacy `/new` bypass. TUI checks cover source retention, missing-context and mapped-but-blocked records, inspection at 45 × 12, invalid context, and absence of model/registration/simulation side effects. No actual Pine runtime, authorized TradingView account or source-media conversion is claimed.

The full regression run passed **575 tests**, zero skips, including nine native-engine tests and 33 upstream Pandas deprecation warnings. A stricter subsequent TUI check found that normal Focus selection targeted the outer panel instead of its scrollable body. The target was corrected; the final source/agent/research/TUI regression group passed **201 checks** after that correction. Built-wheel inspection matches Python/CSS sources, excludes configured credentials/private state and verifies keyboard scrolling from the actual selected Focus body at 45 × 12. The package previews use authored source and an injected online gate only for rendering.
