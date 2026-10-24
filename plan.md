# stratosphere development plan

Updated: October 24, 2026. This is the single active development plan.

## Product scope

stratosphere is a private personal terminal workspace in tradingsphere. Linux and macOS come
first. Keep English UI/docs, responsive panels, agent chat, slash commands, local SQLite,
private research artifacts and native OS credential storage. No billing, application login,
hosted user database, automatic strategy sharing or release deadline.

Historical research uses Yahoo/yfinance. Planned portfolio connections are Alpaca paper/live,
Liquid paper/live and FTMO two-step Swing evaluation. These are different provider/account/mode
identities; declaring one never connects, funds or activates it. Live execution needs separate
explicit activation. No paid data/capacity or challenge purchase is authorized by this plan.

## Requested agent architecture

1. The **orchestrator** bridges user intent and strategy agents; it does not own their private
   reasoning, approve work, allocate account money or issue orders.
2. One **strategy agent per exact definition** receives the brief, proposes bounded research,
   investigates candidate markets/assets/intervals/periods and interprets its own evidence.
   Exact duplicates reuse the agent; behavioral variants retain explicit lineage.
3. Application code freezes the candidate universe, datasets, costs, windows, implementations,
   selection policy and finite native/model budgets. User approval precedes backtests.
4. A report retains every attempted candidate and supports a precise final strategy package.
   User approval of that package precedes assignment to a portfolio. Research approval is not
   deployment approval, and a profitable comparison is not independent holdout validation.
5. Each **portfolio risk agent** proposes how much account capital is available to each assigned
   strategy, subject to explicit user policy and account-wide deterministic limits/reservations.
6. On a verified strategy trigger, its strategy agent returns **reliable/not_reliable** against
   pinned facts. Missing/invalid/stale reviews deny new entries. A reliable verdict is advisory,
   not a probability guarantee or execution permission.
7. For a reliable entry, the portfolio agent proposes position capital. Application code checks
   current capital, cash, open/pending exposure, increments, fees, loss/drawdown/concentration and
   stop/gap risk, then atomically reserves an approved intent. Only a verified adapter may send it.
   Protective exits/emergency actions must not wait for inference.

The canonical detailed workflow is [docs/agent-workflow.md](docs/agent-workflow.md).
Mandatory behavior is in [docs/invariants.md](docs/invariants.md).

## Current implementation

| Area | Implemented or retained | Missing / verification limits |
| --- | --- | --- |
| New research bridge | TUI `/create` delegates a typed bounded Yahoo SMA campaign; stable exact owners, duplicate reuse and variant lineage | Broader strategy families, actual-provider model reliability, general research tools and regime/trend analysis |
| Research campaign | Frozen eligible plans and blocked attempts; one exact campaign approval; finite base/stress starts; cancellation/restart without replay; Tests/Reports views | Independent holdout and selection protocol, richer comparative interpretation |
| Strategy chat | Own registered definition/history and own verified campaign results | Full source/research tool loop and operational trigger consumer |
| Portfolio risk layer | Typed provider/mode/limit/snapshot contracts, binary reviews, advisory capital proposals and deterministic position previews | Persisted portfolio lifecycle, review receipts, final-package approval/assignment and authoritative reservations |
| Historical research | Yahoo hourly/daily long-only SMA native simulations; legacy Binance studies/readers | Dividends/splits, verified calendars, broader instruments and strategy conformance |
| Credentials/models | Native secure storage; existing model transports and Liquid read-grant lifecycle | New connector credentials and environment/account binding; real Linux rotation checks |
| Existing account services | Authored accounting, reservations, observation/runtime and advisory foundations | Provider reconciliation, operational agent integration and sustained feed-to-account verification |
| Connectors | Liquid read-only catalog/address diagnostics | Alpaca paper/live, Liquid feed/paper/live, FTMO platform bridge; no order adapter is implemented |
| Live execution | Unavailable | Explicit activation and operational acceptance after paper verification |

The new research route supersedes the old natural-language action router and January-2025
creation handoff in the active TUI. Historical agent modules and explicit legacy commands remain
compatibility helpers. Preserve all versioned schemas, saved definitions, migrations, histories,
sealed reports and journals. Do not delete evidence or rerun historical work during migration.

## Implementation sequence

### 1. Research handoff replacement and stabilization — in progress

- Exercise new creation → exact agent ownership → reviewed campaign → native runs → report.
- Test duplicate/variant identities, blocked datasets, tampering, competing approvals, cancellation,
  startup with another active client, damaged evidence and fresh-process report inspection.
- Verify large/small TUI, model setup/change, forms, task cancellation and report-state clearing.
- Keep every candidate/window/cost owner exact; never promote an earlier exploratory price replay
  or selected sibling's result into the final strategy's evidence.
- Run focused tests then relevant compatibility regressions. Distinguish authored fixtures,
  actual model/provider checks and release readiness.

### 2. Final strategy package and portfolio lifecycle

- Define a versioned executable strategy package with exact entry/exit/sizing behavior, provider
  instrument mapping, supported environment and research evidence/limitations.
- Current backtests use fixed quantity. Dynamic portfolio sizing changes executable behavior;
  implement and test a new sizing policy rather than silently relabel fixed-quantity results.
- Add final-report review/approval, exact candidate selection and inactive portfolio assignment.
- Persist stable portfolio risk-agent contexts, account bindings, policy approvals and immutable
  model-review receipts. Preserve the distinction between advisory proposals and applied policy.
- One physical account must have one authoritative exposure/reservation view across strategies
  and logical portfolios. Include occupied, reserved, late and ambiguous exposure.

### 3. First real-feed paper portfolio

- Choose the first adapter (Alpaca paper, Liquid paper or FTMO two-step Swing platform route).
  User preference and paper-order approval mode are pending; no account access has been requested.
- Verify actual account/environment, permitted feed access and exact instruments. Preserve native
  secure storage, credential-free research workers and isolated paper/live endpoints.
- Integrate closed-bar triggers, binary specialist reviews and portfolio sizing with freshness,
  finite inference budgets and deterministic account-wide limits.
- Implement idempotent reservations/submission, partial fills, cancellations, rejections, expiry,
  ambiguous outcomes and restart reconciliation. No silent retries of uncertain orders.
- Test keep-positions versus close-all exit behavior, including failed closures and residual risk.
- Verify sustained operation, outages, risk stops and recovery before advertising paper support.

### 4. Other adapters and FTMO-specific acceptance

- Add other provider/mode combinations with their own acceptance cases; never infer one from another.
- FTMO needs the user's actual platform and a versioned two-step Swing rule/account binding.
  Verify phase, contract sizes, commissions/swaps, daily-reset timezone and loss calculations.
  Evaluation is not a generic live brokerage connection or an authorization to buy a challenge.
- Research universal "best markets/trends" only within a frozen candidate and validation protocol;
  reserve an independent holdout for a frozen selection, not repeated exploration.

### 5. Live execution and operational maturity

Requires separate explicit live activation after paper acceptance. Verify live credentials,
transport, risk/emergency controls, reconciliation and shutdown independently. Add backup/restore,
upgrade compatibility, drift monitoring and controlled implementation changes.

## Release gate

No phase/release is complete from fixture checks or a profitable replay. Require a stable installed
client, reproducible research, reliable evidence ownership, verified credential lifecycle and
operational acceptance for each advertised feature. State limitations plainly. Do not advertise
connected portfolios, paper orders or live trading before they are implemented and verified.
