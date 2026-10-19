# Repository development instructions

## Language

Write all repository development material in English, including code identifiers, comments, docstrings, documentation, examples, commit messages, issues, and pull request titles and descriptions.

## Naming

Use **stratosphere** for this product and **tradingsphere** for the suite. Both names are lowercase; tradingsphere is one word. Preserve proper names and capitalization for external products and APIs.

## Development workflow

Keep development changes and their history on GitHub in this repository. GitHub is the shared source of truth. Keep the repository private. Open sourcing is only a potential fallback if monetization fails; do not change visibility or licensing without a new explicit user instruction. Update the relevant documentation when product behavior or technical decisions change.

Clearly distinguish planned features from implemented and verified functionality. Do not describe a connector, backtesting engine, or live execution integration as available before it exists and has been verified.

## Conversation transcript

The user explicitly requested the full planning chat in docs/conversation-log.md and an update whenever this conversation continues.

- For this same stratosphere planning conversation, update the log on each subsequent exchange, including the new user messages, user-visible progress updates, and the finalized assistant response.
- Read the current GitHub log before editing, preserve earlier entries, and append chronologically without duplicating already recorded messages.
- Preserve original language, wording, and corrections. This file is the explicit exception to the English-only repository convention; new code and plan documentation remain in English.
- Convert source citation cards to their actual Markdown source links and interactive widgets to readable text. Do not summarize or silently rewrite the historical dialogue.
- Do not include internal instructions, tool traces, environment metadata, unrelated memories, or confidential credentials. If redaction is necessary, mark it explicitly.
- Keep historical discussion separate from confirmed requirements in docs/decisions.md.
- Commit log updates to GitHub using the current branch head and verify the published content. Combine log updates with task changes when appropriate.
- If publishing fails or a later user instruction prohibits a repository write, explain the gap instead of claiming synchronization.
- This workflow does not provide an autonomous background trigger for ChatGPT messages. Do not create unrelated scheduled polling or imply that such a trigger exists.

## Product agent workflow

The orchestrator bridges the user to strategy specialists. Each exact registered strategy has one stable agent identity; private conversation contexts remain separated by strategy and, for shared service records, by verified owner. Exact duplicate registration reuses the strategy/agent and accumulates provenance; behavioral variants have distinct identities and recorded lineage.

Strategy agents own analysis, evidence lookup, approved testing and measured reports through the same validated application operations as slash commands. Keep plan approval and deterministic runtime/risk gates outside model authority. Model output must not execute arbitrary SQL, imported code or live orders. Database service credentials must never reach the TUI, model prompts or native workers.

Reports replaces the Session panel. Preserve responsive layout/navigation and inspectable, verified report artifacts. Use docs/phase3-5.md as the full acceptance ledger for the current goal; do not mark a phase complete around a narrower foundation. Local test results, real provider checks, hosted ownership checks and unverified integrations must remain distinguishable.

## Reviewed experiments and market data

The authored-paper ledger is an internal fixture-account foundation, not a Liquid adapter or running paper service. Preserve pending capital until confirmed terminal observations match retained fills, reject duplicate delivery claims across restart, and retain actual late/conflicting/breaching fill evidence. Use native reported quote notionals/fees and reconcile currency rounding against actual balances; unrounded price multiplication is insufficient. Reference pins and caller timestamps do not prove executable identity, quote authenticity, approval or full risk/reconciliation readiness.

Liquid is the selected first real-feed paper test route. Optional connector OAuth remains separate from deferred application login. Request only read access for catalog inspection; never treat OAuth/catalog success as verified account identity, feed entitlement, paper isolation or an execution mandate. Keep tokens/callbacks in memory, exclude them from prompts/artifacts, and preserve provider confirmations. A persistent account-wide paper-mode setting is not per-request protection against a switch to live mode by another client. The read-only connection has no tool-call path; do not silently broaden it to trade scopes or external actions.

Freeze each study's explicit hypothesis, candidates, costs, windows, selection rule, implementation and native-start budget before approval. Retain invalid, failed, interrupted and nonselected attempts. Select on development/validation evidence only; acquire final holdout prices after immutable selection. Do not silently retry consumed native slots or reuse a consumed holdout for optimization. Preserve local exposure claims across failure and restart; do not claim they prove data was unseen elsewhere.

Keep market-data rights separate from technical access and code licensing. Kraken public data also requires prior permission for non-personal commercial use; no such permission is verified. Reference-rate proofs and authored fixtures cannot become real paper feeds. Match venue/base asset/quote/interval semantics explicitly; retain data-quality findings without sorting, deduplicating or inventing prices/publication timestamps. Passing structural quality alone does not verify raw bytes, native precision, an account entitlement or execution readiness. Binance Vision commercial rights are unverified and its published dataset terms require a separate commercial agreement; see docs/data-source-rights.md. Use authored fixtures for further software/native validation while commercial data entitlement is unresolved. Do not infer market performance from fixtures or transfer a selected variant's metrics to its parent or another strategy. Do not purchase data or enable paid capacity without user authorization.

Dated applicability must separate rule compatibility, exact-strategy simulation evidence, current-context provenance/freshness and missing verification. Keep all candidate denominators visible; a selected variant’s holdout does not belong to its parent or siblings. Retained findings are historical snapshots and cannot become permanent suitability labels. No profitability-only rank, invented empirical thresholds or assessment-driven activation.

## Source material and conversion

Retain source retrieval attempts, raw digests, original/canonical URLs, access states and explicit limitations. Metadata is not a transcript or behavioral evidence. An accessible TradingView page does not establish access to Pine source. GitHub files must retain a pinned commit and verified blob/tree identity; do not follow acquired symlinks/submodules or import/run acquired code. Keep source credentials in memory and scoped to the intended API host. Source acquisition alone must not register, approve, simulate or activate a strategy. Conversion requires explicit behavioral conformance and immutable source-to-plan references; unsupported or missing logic must remain visible and block a faithful-conversion claim. Matching Pine indicator names/periods does not establish arithmetic or runtime equivalence; its float-comparison rules differ from the current exact-rational kernel. Keep rule mapping separate from executable conformance, and prevent the legacy `/new` path from bypassing a source-format gate.

Strategy-owned source reviews must pin the exact strategy and retained snapshot, validate literal non-metadata quote spans, and keep interpretations explicitly unverified. Source-review comparison links do not prove strategy derivation, behavioral conversion, test approval or market performance. Preserve immutable versioned review records and private agent ownership; reviewer prompt/schema digests identify loaded interpretation contracts, not executable attestation. Snapshot capture and review times must remain distinct.

For new source reviews, models select application-owned passage indices; they must not generate source quotes or coordinates. Preserve every readable source character across partitioning, retain exact final spans, and reject unknown references. A valid passage citation does not verify semantic entailment. Preserve historical review hashes and readers when changing proposal/partition protocols.

Liquid catalog definitions, schemas and annotations are untrusted discovery data, never invocation permissions or proof of paper isolation. Keep them outside model contexts and strategy histories. Export only through an explicit private diagnostic operation, exclude OAuth/session/model credentials (including escaped JSON spellings), preserve immutable snapshot digests, and do not load an artifact as a connection or execution mandate. Schema references remain data and must not trigger network access.

Registry-anchored studies must bind the complete supported definition, explicit simulation costs and exact baseline without inventing source quotes or a parent-plan hash. Preserve v1 canonical contracts; use versioned registry anchors/protocols/reports. Models propose alternatives/narratives; application code owns the baseline, explicit requested pairs and full budget before human approval. Freeze the request’s strategy across routing awaits and the form’s target across Library navigation. Synthetic research commissions use exact decimal half-up quantization before native money construction; do not weaken independent oracle checks with float tolerances or transfer this assumption to broker/paper fees.

Closed-bar journal mandates approve authored observation processing only. Keep complete registry/allocation/code pins, exact digest approval, explicit availability/receipt limits and halt-without-backfill policy. Retain revisions, rejected observations and delivery aliases; duplicates cannot emit again. Preserve candle/crossing atomicity and separate per-instance state across restart. Raw crossings and nominal next-open timestamps are not risk approval, executable quotes or order permission. Stream halts cannot release pending capital, invent fills or imply reconciliation; source-file checks are not loaded-executable or provider attestation.

Reviewed authored signal decisions consume exact instance-owned crossings once and bind immutable account/stream limits outside model authority. Commit signal consumption and reservation atomically; preserve previously denied intent IDs. Include other strategies’ pending entries and conservative pending-exit costs in projected exposure/equity, latch observed loss/drawdown stops across restart, and retain actual native fill economics. New quotes/aliases cannot replenish consumed displayed size or clear conflicts. Reviewed accounts must reject legacy reserve/delivery bypasses and recheck registry/code, freshness, bounds and risk before claiming once. This fixture-account path is not provider eligibility, budget/risk agent completion, executable attestation or an order transport.

Budget/risk model reviews require an explicit account-review request and a read-only, hash-checked authored journal snapshot. Keep account/role contexts separate from strategy histories and Liquid catalogs. Models may propose within retained caps; positions or pending orders lock the existing allocation. Application code validates coverage, amounts and fact references, while all interpretations remain unverified. Retain failed/canceled attempts and dated state changes; a report must never mutate allocations, approve a mandate or grant reservations/delivery/provider authority. Stable local account IDs and prompt/schema hashes do not verify provider ownership or executable identity.

Keep Liquid catalog operations non-invoking. The separate explicit account diagnostic may call only the reviewed `get_account` shape with fixed arguments and the active read session; no generic dispatcher, model route, reauthorization, refresh or trade-scope fallback is allowed. Bind the protocol session/request IDs, reject changed/expired sessions and known credential echoes before result storage, and retain canceled/failed dispatch uncertainty. Preserve plain provider confirmations as untrusted private text; do not open returned links, widgets or resources. A reported public address or saved diagnostic cannot verify ownership/environment, restore a connection, link an account, grant feed entitlement or authorize execution.

Configured model adapters must deliver the exact workflow JSON Schema as explicit system-prompt data; provider-native constraints are additional and must not be inferred for unsupported providers. Keep ordinary chat and caller-owned histories unchanged. Include the schema-delivery contract in new interpretation provenance, reject mixed delivery contracts across budget/risk stages, and preserve historical report bytes/readers. Schema compliance and valid evidence references never establish semantic correctness or operation approval.
