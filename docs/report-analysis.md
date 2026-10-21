# Strategy-owned measured report analysis

Status: implemented local workflow, with verification recorded below. This extends the strategy specialist's analysis and Reports workflow within the supported research/SMA study scope. The full [Phase 3–5 acceptance ledger](phase3-5.md) remains in progress.

## Use the owning specialist

After a research job or bounded study completes, run:

```text
/analyze-report <job-id>
```

`Analyze report <job-id>` (optionally `Please`) invokes the same operation through the orchestrator. The application resolves the report's exact registered owner before awaiting the model. A study belongs to its parent agent; the metrics of each candidate and selected final holdout still belong to their exact variant. Neither command switches the active Library selection or copies conversation histories into the review.

Completed analyses appear separately in Reports. `/report-analyses` lists attempts, including failed/canceled or incomplete/unpublished work. `/report-analysis-show <analysis-id>` verifies and displays the analysis; add a positive fact-page number to inspect every frozen input fact in pages of 20. `/report-data <job-id>` continues to expose all sealed simulation rows. These online operations do not start simulations, consume native/holdout slots, request new prices, revise a selection, approve a plan, alter allocations or invoke account/data/order provider tools. Only an explicit new analysis request makes a model call; opening or paging a retained analysis does not.

A model-produced router action cannot manufacture this operation. The explicit deterministic chat form and slash command both delegate to the owning stable strategy agent. Imported source content, a generic model chat reply or a quoted instruction does not grant execution authority.

## Frozen measurements and interpretation boundaries

The reader verifies the owning job's completion, full artifact seal, exact plan/implementation/strategy references, registry projection, candidate checkpoints, selection and every retained cell's identity. It then freezes `measured-report-facts/1`: report-byte and plan hashes, exact strategy and agent, capture time, complete registered behavior and simulation configuration, complete candidate denominators/statuses, chronological dataset pins, per-candidate/window/cost metrics, selected-only holdout, baselines, assumptions and limitations. The facts preserve retained decimal strings, statuses, nulls and candidate order. No metrics are recalculated and no empirical thresholds are invented. Continuous research segments retain their continuous-account convention; they are not independent holdouts.

Each application-owned fact has an ordered ID, a retained report JSON pointer, scope and exact value. Metric objects explicitly omit only their raw `trades` field; this projection is retained in `omitted_fields`. Raw fills/equity/signals/trades remain in their sealed report and never enter the analysis request or private chat history. The complete fact set is sent together: there is no silent candidate truncation or pagination of the model input. Large reports therefore need a model/context configuration that can accept the complete request.

The model proposes one to six concise categorized English findings (at most 320 characters, each citing one to four distinct fact IDs) and at most three questions (200 characters each). These are explicit output-contract limits, not upload or candidate-input limits; the complete frozen evidence remains inspectable. Oversized output fails validation rather than being truncated. The application rejects unknown/duplicate references, malformed structured output and known credential echoes, then revalidates the full evidence after the model wait. Cancellation prevents publication, including when a provider swallows the cancellation and returns late. Prompt, proposal/report schemas and explicit schema-delivery contract are retained as interpretation provenance, not executable attestation.

A valid fact reference establishes the retained measurement's identity, not semantic entailment. Every narrative remains explicitly **unverified**. A model may misread an amount or draw an unsupported conclusion despite valid references; this feature does not automate approval, optimization, recommendation, suitability or activation. Historical evidence does not become current market evidence when reanalyzed. Authored/native test fixtures establish software behavior, not market performance, commercial data entitlement, actual account access or executable identity.

## Private retention and failure handling

Local immutable analysis receipts bind the owning strategy, stable agent and job in `workspace.sqlite3`. Private request, evidence, analysis and manifest files live in `report-analyses/<analysis-id>` under the research directory. Directories use mode 700 and files 600. Configured model/session credentials are checked before evidence disclosure and before result storage, including JSON Unicode spellings. No provider error body enters retained prompts, reports or UI messages.

Failed/canceled model attempts remain visible without a completed analysis. An interrupted attempt is incomplete; it is not silently resumed or retried. A database publication failure cannot appear as a completed receipt. Once the immutable database receipt commits, a missing auxiliary completion event does not require another model call. Restart reopens the exact retained analysis, verifies all files and its owning measured report, and preserves historical prompt/schema hashes. Changing the current interpretation prompt does not rewrite a prior report.

This table and service are local only. No authenticated shared-user report-analysis service or Supabase synchronization is claimed.

## Ollama context setup

Ollama's OpenAI-compatible endpoint configures local context through a model definition. For a larger local context, use a new model name rather than silently reducing the evidence:

```text
FROM <your-installed-model>
PARAMETER num_ctx 16384
```

Create it with `ollama create <new-name> -f <path-to-Modelfile>` and select that name in `/model`. Choose capacity appropriate to the actual report and your machine; 16K is an example, not a universal requirement or a model-quality guarantee. See [Ollama's local context instructions](https://docs.ollama.com/api/openai-compatibility#setting-the-local-context-size) and [Modelfile reference](https://docs.ollama.com/modelfile).

For the exact recognized local context-overflow HTTP 400 response, the client now gives a fixed actionable message instead of the generic model failure. It does not echo provider text, numeric contents or credentials, alter context configuration or retry the call. Other provider failures retain their existing handling.

## Verification

The full local regression run passed **1,182 tests with zero skips** and 40 upstream Pandas warnings. After the final concise-output bounds were added, **292 affected checks passed**, including five new rejection cases; these overlapping runs are separate evidence, not a single 1,187-test regression run. The current suite collects 1,187 tests. Lint and formatting passed. The first sandboxed run could not bind the existing OAuth callback listener; the successful full run included listener access.

An independently installed wheel completed both explicit entry points with fixture model responses, retaining two analyses with 48 facts each over a previously measured ten-cell authored native study. Two fresh interpreters verified exact analysis digests, the stable owning agent, complete facts and unchanged sealed measurements. Reports, complete fact pagination, scrolling and Tab navigation passed at 180×48 and 45×12. Wheel/source parity and archive checks excluded private state and configured credentials. This acceptance check used two fixture model calls, zero actual provider/trading calls and zero new native starts; it did not rerun or consume the study's holdout.

Actual local Ollama verification did **not** complete an analysis. The installed Spark model's 4,096-token runtime context rejected the complete 11,281-token request. A temporary alias using 16,384 tokens then reached the original 60-second read timeout. With local reads allowed within the existing 180-second total budget, both the original and final concise-output contracts timed out after partial JSON. Failures were retained without completed analyses; temporary aliases were removed and the original model names preserved. No model download or paid request occurred. Live-model reliability and interpretation quality remain unverified.

These checks establish the local report-analysis workflow and its retention boundaries. Authenticated shared synchronization, faithful external strategy conversion, real-feed entitlement, provider paper isolation and execution/reconciliation remain separate incomplete Phase 3–5 requirements.
