# Contracts and retained evidence

The authoritative executable formats live in [schemas](../schemas/),
[authored examples](../examples/) and the corresponding Python contract classes.
Keep persisted format versions, historical readers and sealed artifact bytes compatible.

## Strategy identity

`strategy/1` requires the complete supported behavior: instrument constraints, timing, indicators,
entry/exit, filters, sizing, risk rules and assumptions. Unresolved drafts cannot acquire a complete
identity. Reject unknown/duplicate fields, undefined references and inconsistent rules.

Canonical JSON uses UTF-8, sorted object keys, compact separators, no NaN and preserved array order.
Exact decimal strings normalize only trailing fractional zeros and negative zero. Do not reorder
expressions, round through floats, case-fold or infer mathematical equivalence. SHA-256 covers
all behavior/assumptions; name, source, performance and account capital are separate. Reuse needs
matching complete canonical bytes, not only a candidate digest. Behavioral variants get distinct
identities and acyclic lineage; duplicates reuse the stable strategy/agent and add provenance.

See [`phase0/contracts.py`](../src/stratosphere/phase0/contracts.py) and
[`registry/workspace.py`](../src/stratosphere/registry/workspace.py).

## Research and interpretation

Research/study plans bind strategy, implementation, costs, dataset/windows, selection and finite
native-start budget. Approval is outside model authority. Preserve legacy v1 readers and versioned
registry anchors/protocols; do not fabricate a parent-plan hash or source quote.

Report inspection reads owning sealed artifacts without recalculation or new native/holdout use.
Source review, caption normalization, registration and measured-analysis receipts preserve their
original hashes/readers. Prompt/schema digests identify interpretation contracts, not executable
or provider attestation. New contracts require explicit versions and compatibility tests.

## Journals and credentials

Account/service journals preserve immutable observations, consumption, reservations, fills,
revisions, failures and crash-recovery boundaries. Unknown versions and damaged state fail visibly.
Native credentials never enter these records or worker/model contexts.

## Retired hosted contracts

Existing hosted wire schemas, SQL migrations and ownership tests are retained under `schemas/`,
`src/stratosphere/phase0/` and [supabase](../supabase/README.md). They are historical compatibility
material, not an application login/database requirement. Do not edit applied migrations, delete
hosted resources or grant clients service-key access as part of local cleanup.

## Replacement workflow contracts

`workflow/contracts.py` owns `agent-research-proposal/1`, historical `agent-research-plan/1`,
automatic `agent-research-plan/2`,
`portfolio-definition/1`, `portfolio-risk-snapshot/1`, `strategy-signal-verdict/1`,
`portfolio-capital-proposal/1` and `portfolio-position-proposal/1`. Campaign reports use
`agent-research-report/1`; validated sizing previews use `position-preview/1`.
These formats do not migrate or relabel legacy strategy, research, study or account evidence.
`workflow/portfolios.py` adds `finished-paper-strategy/1`, pinning exact research evidence,
provider/account/mode, implementation, execution policy and full limits. Application research receipts
use `research-policy-authorization/1` and explicitly grant no order authority. The new personal
paper SQLite journal has its own version 1; historical authored-paper journals are unchanged.
See [agent workflow](agent-workflow.md) for behavior and verification limits.

## MT5 connector contracts

`mt5/bridge.py` owns `stratosphere.mt5.bridge/1` typed request/response rows and a durable local
request journal. `mt5/trading.py` owns exact `mt5-deployment/1` approvals. Neither interprets old
`finished-paper-strategy/1` approvals as MT5 authority. The EA checks native policy/account/session
pins before invoking an allowlisted operation. Source identifiers/local hashes are compatibility
evidence, not independently verified executable identity. Details: [MT5 connector](mt5-bridge.md).
