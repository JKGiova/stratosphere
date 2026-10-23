# stratosphere development plan

Updated: October 23, 2026. This is the only active development plan.

## Product scope

Build a reliable personal terminal workspace to research strategies, inspect measured evidence
and eventually operate explicitly authorized trading through Liquid. Keep an English responsive
TUI, agent chat, slash commands and inline setup. Linux and macOS come first; other operating
systems and broader tradingsphere integration are later work.

Use local SQLite, private artifacts, separate strategy histories and native secure credential
storage. No application account, hosted user database, platform fee or automatic sharing.
Research/reporting require connectivity; local Ollama is the supported local-model exception.
The repository stays private. There is no release deadline.

## Actual status

| Area | Available foundation | Missing or requiring verification |
| --- | --- | --- |
| Terminal/models | Responsive panels, Tab navigation, inline setup, Markdown and 12 model adapters | Consistent forms, cancellation/restart and actual-provider reliability |
| Library/agents | Exact identities, duplicate reuse, provenance, lineage and isolated saved histories; routed analysis and dedicated workflows | The interrupted bounded tool-loop implementation needs final review and stabilization; no DeepSeek Harness migration is complete |
| Research | Approved NautilusTrader BTC/USDT hourly long-only SMA runs, bounded variants, costs and holdout controls | Broader strategy families/windows/providers and verified external-code conversion |
| Sources | Supplied text/files/captions, pinned GitHub material and limited public readers | Complete authorized acquisition and faithful conversion; Instagram, media and Pine coverage remain incomplete |
| Reports | Retained research/study evidence, detailed inspection and advisory reviews | Reliable interpretations and consistent presentation of exact ownership, failures and limitations |
| Credentials | Native secure storage and read-only Liquid grant renewal logic | Actual user refresh/rotation and Linux native-store verification |
| Liquid | Read-only OAuth/catalog and a fixed address diagnostic | Verified account/environment, feed eligibility, structured data, perpetual paper adapter and reconciliation |
| Account/runtime | Internal authored account ledger, risk/reservation gates, inspection and observation worker | Provider integration, sustained operation and complete operational controls |
| Live trading | Unavailable | Separate authorization and verification after paper acceptance |

“Authored” means deliberately constructed software-test inputs, not market-performance evidence.
See [usage](docs/usage.md) for commands and [invariants](docs/invariants.md) for mandatory boundaries.

## Immediate priority: stabilization

Do not expand the feature set during this pass.

1. Establish a clean-checkout baseline. Record revision, environment, native engine build and test results.
   Review remaining local agent changes separately; interruption is not completion.
2. Reproduce each bug with the smallest useful case. Fix one coherent problem at a time, add a
   meaningful regression check and exercise the actual user path.
3. Test onboarding, secure-store errors, model changes, cancellation, navigation, resizing and
   quit/restart. Failed refreshes must clear stale previews.
4. Verify strategy ownership across model waits, forms, Library changes and restarts. Check
   command/chat consistency, duplicate handling and incomplete operations.
5. Exercise source → supported specification → approval → simulation → report, including unsupported
   input, pause/resume, cancellation and evidence inspection without another simulation.
6. Review the bounded agent runtime: typed tools, progress, shared budgets, nested calls, duplicate
   guards, safe failure traces, isolation and explicit human handoffs. A paused approved job must
   offer explicit resume rather than silently resume or demand another approval.
7. Produce a short baseline report and update this status table. Keep unresolved defects explicit.

Known verification limits: small local models can repeat tool calls or return invalid structured
output; a localhost OAuth test needs listener permission in restricted environments; fixture tests
and short worker checks do not prove production-duration operation or general model quality.

### Completion criteria

- Required local checks pass for the exact revision; environmental exceptions are separately verified.
- Supported installed-package workflows pass, including cancellation and fresh-process restart.
- Large/small terminal checks pass without stale state, lost controls or strategy-context crossover.
- Models cannot invent operations, grant approval, cross ownership boundaries or exceed budgets.
- Known defects have reproducible cases and a visible disposition.
- Documentation and `/help` agree with tested behavior.

## Later milestones

Resume a milestone only after stabilization and a clear user-selected scope.

### 1. Research and source completeness

- Extend strategy families with explicit behavioral conformance cases.
- Mature YouTube, Instagram, X, TradingView and GitHub access, including supplied-material fallbacks.
- Preserve source bytes, provenance, access limits, unsupported logic and all experiment attempts.
- Verify market-specific rights, precision, quality, timing and instrument semantics.
- Select using development/validation only; evaluate the frozen final holdout once.
- Evaluate interpretation against actual evidence. Valid JSON/citations are insufficient.

Complete when each advertised source/strategy/data combination has reproducible success and
failure cases, exact inspectable reports and no inferred capabilities.

### 2. Liquid real-feed paper trading

- Verify account/environment, permitted personal data use and actual structured feed capabilities.
- Implement perpetual increments, fee currencies, funding/margin, freshness, gaps and reconnect policy.
- Bind instances to exact strategy/code/account/market/capital/mandate pins.
- Use one idempotent paper gateway with partial fills, rejections, cancellations, expiry and ambiguity reconciliation.
- Enforce aggregate capital/reservation/exposure/concentration/loss/quote gates independently of models.
- Define concurrent-strategy netting, priority and attribution.
- Verify outages, restart, damaged state, overnight/multi-day holdings and sustained operation.
- Implement the settled exit choice: keep positions or request closure of all. Show actual closure
  failures and residual exposure; stopping, canceling and closing remain distinct.
- Display actual provider-backed positions, orders, limits, health and reconciliation state.

Complete when paper isolation and the whole feed-to-account lifecycle pass independent sustained,
restart, failure and shutdown checks. Read-only OAuth is not this milestone.

### 3. Personal live execution

Requires a new explicit activation instruction. Independently verify live transport, account
permissions, emergency controls, partial fills, outages, reconciliation and shutdown. Paper mode
or a linked account never implies live permission.

### 4. Operational maturity

Add backup/recovery, upgrade compatibility, drift monitoring and controlled implementation changes.
Expand operating systems, adapters and suite interfaces only with their own acceptance cases.

## Release gate

No release based only on a green unit suite or profitable backtest. Require a stable installed
client, reproducible research, reliable source/evidence handling, verified credential lifecycle
and the operational acceptance appropriate to advertised features. State limitations plainly.
Never advertise paper/live execution before it is implemented and verified.
