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
