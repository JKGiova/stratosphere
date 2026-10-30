# Behavioral invariants

Mandatory subsystem requirements for development and bug fixes. These rules preserve supported
behavior and retained evidence; they are not claims that the wider product is finished. Read the
relevant section before modifying its code. Scope/priorities live in [plan.md](../plan.md).

## Secure credential lifecycle

Remember credentials only through the native OS credential store. Bind saved Liquid grants to the
fixed issuer/resource/client and read scope; revalidate metadata and negotiate a fresh catalog on
restore. Serialize rotating-token consumption with an account-wide OS lock shared across research
directories, consume the old saved grant before dispatch, and retain a confirmed replacement before
catalog negotiation. Do not retry ambiguous refreshes or auto-open a sign-in browser on startup.
Finish secure-store writes before cancel/forget releases the lock; explicit disconnect removes the
saved grant while quitting retains it. Keep saved credentials outside artifacts/models/workers and
preserve exact frozen account-read session pins.

## Stable strategy ownership

The orchestrator bridges the user to strategy specialists. Each exact registered strategy has one
stable agent identity; private conversation contexts remain separated by strategy. Historical
shared-service contracts retain their owner boundaries, but a new hosted service is outside the
personal-use scope. Exact duplicate registration reuses the strategy/agent and accumulates
provenance; behavioral variants have distinct identities and recorded lineage.

## Application authority

Strategy agents own analysis, evidence lookup, application-authorized testing and measured reports
through the same validated application operations as slash commands. Keep plan approval and deterministic
runtime/risk gates outside model authority. Model output must not execute arbitrary SQL, imported
code or live orders. Database service credentials must never reach the TUI, model prompts or native
workers.

## Report UI and verification

Reports replaces the Session panel. Preserve responsive layout/navigation and inspectable, verified
report artifacts. Use plan.md as the development plan and completion criteria; do not mark a phase
complete around a narrower foundation. Local test results, real provider checks, hosted ownership
checks and unverified integrations must remain distinguishable.

## Reservation and fill economics

The authored-paper ledger is an internal fixture-account foundation, not a Liquid adapter or running
paper service. Preserve pending capital until confirmed terminal observations match retained fills,
reject duplicate delivery claims across restart, and retain actual late/conflicting/breaching fill
evidence. Use native reported quote notionals/fees and reconcile currency rounding against actual
balances; unrounded price multiplication is insufficient. Reference pins and caller timestamps do
not prove executable identity, quote authenticity, approval or full risk/reconciliation readiness.

## Historical Liquid read scope

Alpaca/Liquid active routing is retired in favor of MT5. The following requirements remain
mandatory for retained historical readers/contracts and do not authorize restoring their network
transports or credentials into active setup. Optional connector OAuth
remains provider authorization; application login is retired. Request only read access for catalog
inspection; never treat OAuth/catalog success as verified account identity, feed entitlement, paper
isolation or an execution mandate. Model settings/API keys, Alpaca paper keys and Liquid
access/refresh grants use only the native OS credential store, with no plaintext/chained backend
fallback. Keep callback codes, PKCE/state and MCP session identifiers in memory; exclude all
credentials from prompts/artifacts/workers and preserve provider confirmations. A persistent
account-wide paper-mode setting is not per-request protection against a switch to live mode by
another client. The read-only connection has no tool-call path; do not silently broaden it to trade
scopes or external actions.

## Reviewed experiment protocol

Freeze each study's explicit hypothesis, candidates, costs, windows, selection rule, implementation
and native-start budget before approval. Retain invalid, failed, interrupted and nonselected
attempts. Select on development/validation evidence only; acquire final holdout prices after
immutable selection. Do not silently retry consumed native slots or reuse a consumed holdout for
optimization. Preserve local exposure claims across failure and restart; do not claim they prove
data was unseen elsewhere.

## Data rights and quality

Keep market-data rights separate from technical access and code licensing. Evaluate permitted
personal use and the actual account/feed entitlement; an enterprise commercialization agreement is
no longer a general project prerequisite. This does not grant unrestricted redistribution or waive
provider restrictions: Binance Vision's personal historical-research allowance does not authorize
live proprietary execution. See [data rights](data-source-rights.md). Reference-rate proofs and
authored fixtures cannot become real paper feeds. Match venue/base asset/quote/interval semantics
explicitly; retain data-quality findings without sorting, deduplicating or inventing
prices/publication timestamps. Passing structural quality alone does not verify raw bytes, native
precision, an account entitlement or execution readiness. Use authored fixtures for deterministic
software/native validation, without treating their metrics as market performance or transferring a
selected variant's metrics to its parent or another strategy. Do not purchase data or enable paid
capacity without user authorization.

## Dated assessments

Dated applicability must separate rule compatibility, exact-strategy simulation evidence, current-
context provenance/freshness and missing verification. Keep all candidate denominators visible; a
selected variant’s holdout does not belong to its parent or siblings. Retained findings are
historical snapshots and cannot become permanent suitability labels. No profitability-only rank,
invented empirical thresholds or assessment-driven activation.

## Acquisition and conversion

Retain source retrieval attempts, raw digests, original/canonical URLs, access states and explicit
limitations. Metadata is not a transcript or behavioral evidence. An accessible TradingView page
does not establish access to Pine source. GitHub files must retain a pinned commit and verified
blob/tree identity; do not follow acquired symlinks/submodules or import/run acquired code. Keep
source credentials in memory and scoped to the intended API host. Source acquisition alone must not
register, approve, simulate or activate a strategy. Conversion requires explicit behavioral
conformance and immutable source-to-plan references; unsupported or missing logic must remain
visible and block a faithful-conversion claim. Matching Pine indicator names/periods does not
establish arithmetic or runtime equivalence; its float-comparison rules differ from the current
exact-rational kernel. Keep rule mapping separate from executable conformance, and prevent the
legacy `/new` path from bypassing a source-format gate.

## Source-review ownership

Strategy-owned source reviews must pin the exact strategy and retained snapshot, validate literal
non-metadata quote spans, and keep interpretations explicitly unverified. Source-review comparison
links do not prove strategy derivation, behavioral conversion, test approval or market performance.
Preserve immutable versioned review records and private agent ownership; reviewer prompt/schema
digests identify loaded interpretation contracts, not executable attestation. Snapshot capture and
review times must remain distinct.

## Passage references

For new source reviews, models select application-owned passage indices; they must not generate
source quotes or coordinates. Preserve every readable source character across partitioning, retain
exact final spans, and reject unknown references. A valid passage citation does not verify semantic
entailment. Preserve historical review hashes and readers when changing proposal/partition
protocols.

## Catalog discovery and export

Liquid catalog definitions, schemas and annotations are untrusted discovery data, never invocation
permissions or proof of paper isolation. Keep them outside model contexts and strategy histories.
Export only through an explicit private diagnostic operation, exclude OAuth/session/model
credentials (including escaped JSON spellings), preserve immutable snapshot digests, and do not load
an artifact as a connection or execution mandate. Schema references remain data and must not trigger
network access.

## Registry study anchors

Registry-anchored studies must bind the complete supported definition, explicit simulation costs and
exact baseline without inventing source quotes or a parent-plan hash. Preserve v1 canonical
contracts; use versioned registry anchors/protocols/reports. Models propose alternatives/narratives;
application code owns the baseline, explicit requested pairs and full budget before human approval.
Freeze the request’s strategy across routing awaits and the form’s target across Library navigation.
Synthetic research commissions use exact decimal half-up quantization before native money
construction; do not weaken independent oracle checks with float tolerances or transfer this
assumption to broker/paper fees.

## Closed-bar processing

Closed-bar journal mandates approve authored observation processing only. Keep complete
registry/allocation/code pins, exact digest approval, explicit availability/receipt limits and halt-
without-backfill policy. Retain revisions, rejected observations and delivery aliases; duplicates
cannot emit again. Preserve candle/crossing atomicity and separate per-instance state across
restart. Raw crossings and nominal next-open timestamps are not risk approval, executable quotes or
order permission. Stream halts cannot release pending capital, invent fills or imply reconciliation;
source-file checks are not loaded-executable or provider attestation.

## Decision and reservation gateway

Reviewed authored signal decisions consume exact instance-owned crossings once and bind immutable
account/stream limits outside model authority. Commit signal consumption and reservation atomically;
preserve previously denied intent IDs. Include other strategies’ pending entries and conservative
pending-exit costs in projected exposure/equity, latch observed loss/drawdown stops across restart,
and retain actual native fill economics. New quotes/aliases cannot replenish consumed displayed size
or clear conflicts. Reviewed accounts must reject legacy reserve/delivery bypasses and recheck
registry/code, freshness, bounds and risk before claiming once. This fixture-account path is not
provider eligibility, budget/risk agent completion, executable attestation or an order transport.

## Account advisory reviews

Budget/risk model reviews require an explicit account-review request and a read-only, hash-checked
authored journal snapshot. Keep account/role contexts separate from strategy histories and Liquid
catalogs. Models may propose within retained caps; positions or pending orders lock the existing
allocation. Application code validates coverage, amounts and fact references, while all
interpretations remain unverified. Retain failed/canceled attempts and dated state changes; a report
must never mutate allocations, approve a mandate or grant reservations/delivery/provider authority.
Stable local account IDs and prompt/schema hashes do not verify provider ownership or executable
identity.

## Fixed account diagnostic

Keep Liquid catalog operations non-invoking. The separate explicit account diagnostic may call only
the reviewed `get_account` shape with fixed arguments and the active read session; no generic
dispatcher, model route, in-read reauthorization/refresh or trade-scope fallback is allowed. Read-
only grant restoration/renewal may occur before the diagnostic freezes its session; never replace a
session during an active address read. Bind the protocol session/request IDs, reject changed/expired
sessions and known credential echoes before result storage, and retain canceled/failed dispatch
uncertainty. Preserve plain provider confirmations as untrusted private text; do not open returned
links, widgets or resources. A reported public address or saved diagnostic cannot verify
ownership/environment, restore a connection, link an account, grant feed entitlement or authorize
execution.

## Schema delivery

Configured model adapters must deliver the exact workflow JSON Schema as explicit system-prompt
data; provider-native constraints are additional and must not be inferred for unsupported providers.
Keep ordinary chat and caller-owned histories unchanged. Include the schema-delivery contract in new
interpretation provenance, reject mixed delivery contracts across budget/risk stages, and preserve
historical report bytes/readers. Schema compliance and valid evidence references never establish
semantic correctness or operation approval.

## Sealed report inspection

Detailed report inspection must read the owning job's sealed artifacts and preserve exact
candidate/variant, window/cost, row order, failure state and immutable references. Display
pagination cannot truncate retained evidence or transfer a selected holdout to its parent/siblings.
Do not recalculate historical results, consume native/holdout slots, modify approval/selection or
append raw tables to model histories merely to view evidence. Inspection hashes establish local
artifact consistency, not provider entitlement or executable attestation.

## Caption ingestion

Supplied caption ingestion must preserve raw bytes, exact payload/metadata spans, original cue order
and enclosing file-local bounds. Keep quality findings without sorting/deduplicating cues; malformed
tracks cannot silently fall back to untimed text. Metadata is not behavioral evidence, cue intervals
are not word-level or verified media-clock timing, and supplied attribution is not provider
acquisition or publication identity. Preserve historical normalization/review contracts and prevent
canceled parser workers from publishing late results.

## Explicit native registration

Native source registration must require an explicit user operation and the complete supported
canonical JSON definition, never a model-generated action or inferred code conversion. Bind the
retained acquisition request/snapshot and full behavior; commit identity, one stable agent,
provenance, declared acyclic lineage and an immutable receipt atomically. Same-ID replay cannot add
another submission, and an auxiliary event failure cannot require re-registration. Registration
grants no approval, native start, paper mandate, source-performance claim or executable/provider
attestation. Preserve active selection, private histories and historical receipt readers. Existing
study candidates reuse their identities with comparison provenance, without reparenting ancestors or
inventing a new derivation edge.

## Measured report interpretations

Measured report analyses must resolve the owning stable strategy agent before model waits, freeze
the complete hash-checked candidate/window/cost evidence and retain exact application-owned fact
references. Keep all denominators, failures, selected-only holdout attribution and historical
semantics; never silently truncate candidate evidence or move raw simulation tables/private
histories into the model request. Valid fact citations do not establish semantic correctness,
current suitability or approval. Retain canceled/failed attempts, recheck evidence before
publication, preserve immutable analysis receipts/readers across restart, and keep this advisory
operation outside testing, allocation and execution authority.

## Read-only account inspection

Authored account inspection must read an existing journal through a read-only transaction and
preserve exact reported fill notionals/fees, pending reservations, ambiguity, faults and all
paginated rows. Keep capture times and journal digests visible; refreshes are separate snapshots,
not a feed or running-service proof. Missing or unreadable accounts cannot appear as verified zero
positions, and a failed refresh must clear the Positions preview. Inspection must not
create/migrate/fund an account, modify approval/state, disclose account evidence to models or infer
provider ownership, reconciliation, current P/L or execution permission from retained pins.

## Connector scope

MT5 is the sole active data/execution connector. Retire Alpaca and Liquid commands, credential
restoration and active setup; preserve historical formats/readers and exact identities under
legacy compatibility. Old paper approvals cannot authorize MT5. Keep the existing terminal login
inside MT5; no password export, DLL/network fallback or credential-bearing worker environment.

The local EA and application must bind exact server/login/mode/session. Native trading is disabled
by default; real-account activation requires explicit authorization in both the app and terminal.
Only an exact user-approved MT5 deployment binds strategy/evidence/implementation/EA and finite
volume, cash-risk, margin, spread, equity and order-count limits. Mandatory native SL, one account
position at a time and no reinforcement are current conservative execution constraints. Freeze
request IDs before publication, claim natively before OrderSend, retain all confirmations and halt
new entries on ambiguous outcomes. Never replay an uncertain request. Exact owned protective exits
remain separate from entry expiry/revocation and do not depend on model availability.

Broker snapshots, an EA source digest, native checks and fixtures do not prove executable
attestation, faithful backtests, account ownership, complete archives, FTMO Swing profile or loss/reset
compliance. The headless connector is not the automatic strategy-agent runner. Preserve raw server
chart timestamps; no inferred historical UTC conversion or generic CFD-to-spot mapping.

## Observation worker ownership and recovery

Authored observation workers require exact service/stream approval, the canonical per-account
private queue and a held OS owner lock. Keep deadline progress per stream across restarts;
duplicates, aliases and heartbeats cannot renew it. Preserve the account-commit/queue-ack crash
boundary and recover through the retained duplicate gate. Halt without backfill or capital release
on silence, clock, connectivity, identity or stream faults. Keep worker environments credential-free
and separate live lock probes from historical heartbeat/session evidence. This developer service
grants no provider feed, order authority, global executable attestation or production-duration
reliability; reading health must not start, acknowledge, reset or stop it.

## Historical bounded tool runtime

Freeze strategy ownership before model waits. Use typed capabilities over the existing validated
services, count nested calls against the same total budget/deadline and retain safe failed/canceled/
interrupted traces without automatic replay. Historical study/test protocols retain human review;
new v2 agent research uses application authorization as specified below. Models cannot approve
or replenish budgets or resume consumed work. Keep account/Liquid/credential, registration and
arbitrary shell/SQL/file/code capabilities outside the strategy tool set. Preserve historical
reader/format contracts and distinguish fixture behavior from actual model reliability.

## Research and automatic paper workflow

The orchestrator bridges exact strategy owners. New v2 research campaigns use finite application
authorization, not human or model approval. Freeze every candidate/rejected attempt, dataset,
cost/window, code and at most twelve candidates/twenty-four native starts. Receipts explicitly grant
no order authority. Historical v1/manual protocols keep their approval contracts. No silent consumed-
start replay, recursive budget replenishment, profitability-only selection or invented final holdout.

Only the user approves the finished strategy, evidence, implementation, provider instrument/account
and full paper limits. That one approval permits automatic paper orders within those limits. Verify
sealed exact candidate evidence; never transfer fixed-quantity Yahoo returns to a variable-size
provider policy. Local hashes/fixtures do not establish connected account or executable attestation.

Keep one risk-agent context per portfolio, separate from private strategy histories and credentials.
Strategy capital and position capital remain distinct and constrained by application limits and
account-wide reservations. Models only propose reliable/not_reliable for pinned signal facts,
strategy capital and position capital. Recheck ownership, freshness, assignments, account exposure,
policy and available funds after inference and before reservation. Invalid/unavailable entry reviews
deny entry. Deterministic protective exits must not depend on an LLM.

Paper credentials use native storage only. Keep one personal account journal and OS lease across
research roots/strategies. Persist consumed signals and reserved client IDs before submission; never
retry an uncertain POST, including after a 404 read-back. Preserve changed provider confirmations,
partial/late/conflicting fills and reserved capital until reconciliation. External/unknown exposure,
stream gaps/revisions, stale data and observed loss stops cannot silently clear across restart.
Historical Alpaca/Liquid paper contracts remain readers only; active orders use the separate MT5
deployment/bridge contract above. No historical grant or paper mandate transfers authority.

## Compiled strategy-agent development

A complete sealed intake can pass to its exact registered strategy specialist under finite
application research policy. Validate source-rule indices and typed program capabilities before
registration/compilation. The application emits code; only bytes exactly equal to the versioned
compiler output may run in credential-free children. Never execute arbitrary model/acquired Python
or let the model alter a loader, worker path, account, environment or source/evidence check.
Preserve v1 source/owner/runtime/scope pins, three program slots, twelve cells and twenty-four
native-start slots. V2 freezes four scopes, 32 model calls including formalization, 128 reserved
native starts, four searches, three technical errors and 3,600 active loop seconds. Application code
owns frozen progress thresholds; the first insufficient complete revision stops and retains the best
predecessor. Technical repair is separate and bounded. Record slots before use; failures consume them.
V2 continuation reconstructs hash-chained receipts and reconciles completed native children; uncertain
starts cannot replay. Exact re-delivery cannot replenish any budget. No arbitrary Python or shell tools.
Every behavior/market/interval variant gets exact ownership and lineage; do not move its returns
to the parent. Revisions see bounded own measured summaries and cannot change frozen market, cost,
sizing or execution scope. Retain blocked datasets, failed cells, code, partials and all variants.
Source citations and compiled/native checks do not prove faithful conversion. Descriptive year and
prior-direction buckets are not predictive regimes, independent holdouts or a global-best claim.
A selected historical case grants no final approval, portfolio assignment or MT5 authority.

Public research fallbacks use fixed public adapters and fresh anonymous processes only; never reuse
browser profiles, login state or operator credentials. Retain every backend attempt and safe raw
response. Access/rate denials, private addresses and sensitive output are terminal, not permission
to escalate. Optional readers stay within application time/byte/search ceilings; no arbitrary CLI,
shell, browser evaluation, acquired code or broker tools enter the strategy model surface.

The gold connector pilot keeps XAUUSD CFD/D1 identity separate from historical Yahoo/ETF research.
Its full application-owned regime/startup/stop/sizing overlay participates in strategy identity.
Status/export never requests sizing or activates trading. Entry requires an exact demo-only,
one-entry deployment expiring within 24 hours, fresh matching account/session/window/exposure and
the existing native gateway. Reserve advisory slots before inference; failed/canceled/uncertain
operations cannot replay. The risk role receives anonymous bounded facts only, never raw account
diagnostics or credentials. Protective exits require exact journal ownership and no model.
This one-cycle connector is not a scheduler, faithful CFD backtest, FTMO certification or the
general research-to-portfolio/position-management workflow.
