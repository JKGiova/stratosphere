# Studies anchored to registered definitions

Implemented local increment within the supported BTCUSDT spot hourly SMA family. An existing strategy or study-derived variant can now own a reviewed study without a separate `/new` research job. This does not complete the [full Phase 3–5 scope](phase3-5.md), verify source conversion, grant data rights or activate paper/live trading.

## Workflow

Select the strategy agent, then use `/study`. If the strategy has a prior standalone research plan, the existing path and its reviewed costs remain available. `/study <24-digit-strategy-id>` explicitly opens the new registry path; `/study <32-character-parent-research-job-id>` preserves the original research-plan path.

The registry form pins the chosen strategy, complete definition, baseline periods and fixed quantity. It exposes starting cash, simulated fee rate, spread and slippage as editable research assumptions. It also accepts the hypothesis, candidate order, development month and finite native-start budget. Review the frozen protocol, then approve its exact digest once. Selecting another Library entry while the form is open cannot retarget it.

A test request with no pending plan opens this review path rather than requiring the user to recreate the strategy through `/new`. A natural-language experiment request reaches the same specialist and form. For general proposals the model supplies alternatives and a hypothesis; application code always includes the actual registered baseline and computes the complete budget. Duplicate/baseline alternatives and unsupported model parameters are rejected, without silent repair or execution.

Explicit `fast/slow` pairs in a request, such as `Compare 3/8 with 5/12 and 6/15`, are application input. The model supplies a narrative/month proposal and cannot replace those pairs. The registered baseline is included first by default, then unique requested pairs retain their first-mention order. URLs and decimal fragments are excluded from this notation parser. This is a bounded syntax, not a general natural-language specification parser. The form remains editable, including candidate order and month; the frozen protocol defines the eventual tie order and all authorized starts.

Model hypotheses are unverified proposals, not results. During live testing a model omitted the baseline, and another proposal changed explicitly requested pairs and contradicted its own hypothesis. These observations motivated application-owned baseline and explicit-pair handling. The final model check preserved the application-owned pairs but still called a fast period a slow period and inaccurately described which period remained fixed. Human review remains necessary; these checks do not establish general hypothesis quality. No checksum pin, price download, approval or native start occurs merely because a model produces a proposal; production online availability checks still fetch public checksum metadata.

## Identity and wire formats

`bounded-study/2` embeds a `registered_definition/1` anchor with the complete canonical definition, its digest, supported parameters, display name, reviewed costs and pinned engine version. It carries no fabricated original-plan digest, source digest, extraction or source quote. Matching indicator names alone is insufficient: the entire definition must equal the trusted compiler's output, including instrument, timing, exits, sizing, data policy and assumptions.

The registered baseline must appear in a version-2 protocol. Candidate definitions are compiled from trusted parameters directly. Exact duplicates retain their existing strategy/agent; changed periods receive their own identities and lineage. Definition/owner consistency is checked around checksum acquisition, before approval and before running. A changed/corrupt definition cannot acquire prices or start a native cell under the old review.

The existing budget, window/reset rules, validation-only selection, separate final holdout, exposure claims, retained invalid/failed attempts and no-retry checkpoints apply to both protocol versions. Restart reads either version and preserves consumed slots. Version-2 reports use `study-report/2`; report rendering and dated evidence lookup retain exact selected-variant attribution. A winner's holdout must not become its parent's or sibling's evidence.

`research-plan/1` and `bounded-study/1` retain their original serialized fields/defaults and canonical fingerprints. A captured golden fixture verifies both hashes. Historical sealed reports remain inspectable; implementation changes require new review for future execution. The new path adds no hosted synchronization or authentication.

## Synthetic fee correction

The expanded native test exposed a half-quantum fee discrepancy under non-default costs: the default native money constructor passes a Decimal through a binary float. Research simulation now computes price × quantity × reviewed fee exactly, rounds half up to eight decimals and constructs native money with `Money.from_decimal`. Version 2 names this policy `decimal-price-quantity-half-up-8/1`. Base and doubled-cost runs use it consistently.

The independent oracle keeps exact decimal assertions, without a numerical tolerance. Formatting differences such as `0.0020007` versus `0.00200070` compare as equal decimal values. Previously sealed results retain their original implementation references and measured values; they are not rewritten. This is a synthetic research fee assumption, not verified broker fees, the future stratosphere commission or a paper-ledger transport policy.

## Verification scope

Tests cover legacy hashes, full-definition matching, missing baselines, changed definitions, exact approval, private routing, explicit pairs, edited costs, form target pinning, escaped credentials, native output, restart and selected-variant evidence. Switching Library selection during router latency cannot retarget the original request or authorize a newly invented destination. Strategy names must match complete words, not substrings of unrelated text.

Native fixture checks compare all ten cells of a two-candidate study against an independent batch SMA/cash oracle: signals, fills, fees, every equity point, held base and pending signals. An independently installed wheel also exercises the real TUI from registry selection through cost review, one approval, ten native subprocesses and Reports, including 180×48 and 45×12 layouts. The worker interpreter imports that installed wheel; its dependency path does not activate the checkout's editable installation.

Real local Ollama checks cover two registered agents, explicit-pair preservation, private context and restored histories with no standalone research jobs, study checksum pins, price downloads, approvals or native starts. Public checksum availability probes are separate. Measured results are recorded in the [acceptance ledger](phase3-5.md); successful routing/schema checks do not verify scientific quality or market performance. All execution data in this increment is authored fixture data. Commercial data entitlement, wider strategy families, hosted service work and the real-feed paper runtime remain pending.
