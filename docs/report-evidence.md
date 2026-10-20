# Inspecting retained simulation evidence

Status: read-only inspection for completed local `research-report/1` and bounded `study-report/1` / `study-report/2` reports. This does not add a simulation engine, new strategy family, data entitlement, model reliability, paper adapter or execution authority. The full [Phase 3–5 ledger](phase3-5.md) remains in progress.

## Commands and agent operation

`/report <job-id>` and selecting a report still show the measured summary. Its footer links to `/report-data <job-id>`, which lists available cells and their states. Use the complete 32-character job ID. The syntax is:

```text
/report-data <job-id> [section] [cell|-] [page]
```

Sections are `overview`, `attempts`, `metrics`, `trades`, `fills`, `equity`, `signals` and `baselines`. Examples (replace `JOB_ID` with the actual ID):

```text
/report-data JOB_ID
/report-data JOB_ID metrics base 1
/report-data JOB_ID equity stress 2
/report-data JOB_ID attempts - 1
/report-data JOB_ID metrics c0:validation:2 1
/report-data JOB_ID trades holdout:1 1
/report-data JOB_ID baselines - 1
```

Research cells are `base` and `stress`; an omitted cell for a result section resolves to `base`. Research has no candidate-attempt section. Study result sections require an explicit `c<index>:development:<cost>` or `c<index>:validation:<cost>` cell, where cost is `1` or `2`. `holdout:1` / `holdout:2` always refer to the frozen selected candidate. There is no arbitrary-candidate holdout selector. Overview, attempts and baselines use `-` for the cell; baseline entries retain their own window/cost scope. The research baseline is the retained base-cost benchmark only.

`Inspect report <job-id> [section] [cell|-] [page]` (optionally prefixed with `Please`) uses the same query and specialist operation. This explicit full-match grammar makes no model request. A model-generated route cannot invoke the new inspector. Slash inspection also needs no model output. The bridge resolves the job's registered owner and delegates to that stable strategy agent, records the operation there, and leaves the user's current strategy selection unchanged. A different specialist cannot inspect it as its own job. The owning parent can inspect its study's candidate cells, whose exact variant IDs remain visible. Raw result pages never enter strategy/general model histories or automatically replace model evidence context.

## Display and fidelity

Pages contain at most 20 retained records, with zero-based original row indices, total rows, page count and explicit previous/next commands. This is display pagination, not an upload or artifact-size limit. Every row is reachable; an invalid page fails explicitly. JSON records preserve exact decimal strings, integer nanosecond timestamps and retained row order. Inspection does not sort, deduplicate, resample, infer missing prices, reconstruct trades or calculate new metrics.

Metrics include the retained gross/net results, modeled costs, drawdown, turnover, exposure, trade distribution and chronological segments. Trades use the retained entry/exit pairing; fills and signals come from the exact cell response. Equity comes from that cell's curve, including separate doubled-cost results. Pending signals, open positions, balances and available runtime versions remain in cell context. Empty rows do not imply a successful experiment. Failed/interrupted/canceled cells display their retained status/detail instead of an invented empty successful result; invalid/unexecuted cells cannot be opened as results. Attempts expose every candidate and its recorded selection/cell state. Denominators and selection remain visible with each study page.

Each view identifies the owning strategy/job, exact result variant, window/cost, plan digest, retained report-byte digest and, for a result cell, its sealed artifact digest. Context retains cost configuration, implementation/version references, data references, assumptions and limitations. A study's selected holdout is never attributed to its parent or siblings. Retained evidence is historical and does not grant current suitability, approval or entitlement. These local checks do not prove a malicious host cannot replace all artifacts or authenticate a provider/native executable.

The inspector uses the existing online/completion/seal checks, rejects symbolic links and ambiguous job identities, and checks registered job/strategy/plan references. Where a registry report projection exists it must match. Study protocols, candidate checkpoints/identities, selection and requested cell checkpoints must agree with the report. The requested response must match its engine, protocol/plan, candidate/window and cost references. It reads historical contracts without requiring today's implementation digest, and never rewrites reports, starts native slots, fetches archive prices, alters approval/selection or consumes a holdout.

Results open in the existing Reports body with readable vertical records, scrolling and the existing responsive layout/Tab navigation. Reopening the summary returns to the existing summary view. Navigation commands and source/context references stay inspectable in the page; terminal controls are filtered and external hyperlinks are disabled.

## Verification

See the [acceptance ledger](phase3-5.md) for measured results. Tests distinguish lifecycle fixtures from the pinned NautilusTrader subprocess. They cover complete row traversal, exact candidate/window/cost and variant ownership, historical artifact bytes, empty/failed/unexecuted results, online gates, corruption/symlinks, argument validation, model-free forced/chat parity, operation ownership/history separation and responsive keyboard/scrolling behavior. Native authored fixtures provide software evidence only, with no market-performance or data-rights claim.
