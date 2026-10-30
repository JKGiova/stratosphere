# Operational overnight agents

This headless adapter replaces the active Gold Golden Cross pilot. It connects application-owned
close-to-open strategies to the existing MT5 EA. The orchestrator does not execute trades. Each
exact instrument strategy has its own stable registry identity, agent, trigger receipts and sizing
history. A bounded account coordinator serializes them; it does not implement portfolio reallocation
or the general research-to-execution workflow.

## Source and conversion

Bruce Knuteson's [Strikingly Suspicious Overnight and Intraday Returns](https://arxiv.org/pdf/2010.01727)
compares historical close-to-next-open and open-to-close returns across 21 stock markets. Figure 1
covers available histories through September 2020. It is a descriptive paper, **not a complete
executable strategy or evidence of current net profitability**. Its discussion of alleged market
manipulation is not implemented. The paper does not supply stops, sizing or leverage limits.

The implemented adaptation buys a long index CFD before its underlying cash-market close and exits
at the next regular cash-market open. CFD financing, prices, spreads and fills differ from the
paper's indices/ETFs. Do not transfer the chart returns, ETF auction prices or old Yahoo backtests
to this deployment. In particular, Nasdaq 100 is not Nasdaq Composite; the automatic intersection
excludes `US100.cash`, although the adapter supports an explicitly chosen operator variant.

## Exact universe and calendar

`universe` reads the attached EA's complete, bounded broker catalog and applies a reviewed mapping.
It does not infer availability from a website, substring match or unavailable symbol. The connected
FTMO demo catalog checked on October 30, 2026 matched these eight benchmark families:

| MT5 CFD | Paper benchmark family | Cash calendar | Timezone |
| --- | --- | --- | --- |
| `US500.cash` | S&P 500 / SPDR ETF | XNYS | America/New_York |
| `AUS200.cash` | S&P/ASX 200 / SPDR fund | XASX | Australia/Sydney |
| `FRA40.cash` | CAC 40 | XPAR | Europe/Paris |
| `GER40.cash` | DAX; membership has changed | XETR | Europe/Berlin |
| `UK100.cash` | FTSE 100 / iShares ETF | XLON | Europe/London |
| `JP225.cash` | Nikkei 225 | XTKS | Asia/Tokyo |
| `HK50.cash` | Hang Seng | XHKG | Asia/Hong_Kong |
| `N25.cash` | AEX | XAMS | Europe/Amsterdam |

Other reviewed aliases (FTSE MIB and TSX 60) were absent. Unmapped/different benchmarks are
reported explicitly. The evidence is a benchmark-family intersection, not identical paper instruments.

The pinned `exchange-calendars==4.13.2` supplies regular-session times, local DST, holidays and early
closes. Each reviewed deployment freezes its complete UTC entry/exit schedule. The application does
not derive historical UTC from the current MT5 server offset. The calendar is a cash-market clock,
not a guarantee that the broker is accepting CFD orders: fresh quotes and native checks still gate
execution. Unexpected broker/market closures remain an operational limitation.

## Trading profile and limits

- Enter only within `[cash close - 5 minutes, cash close - 30 seconds)`. Never catch up later in the
  night or invent a previous fill. Native request expiry also ends inside this window.
- Exit an exactly owned long at/after the next cash-session open, including after weekends and
  holidays. No shorts, reinforcement, trailing or arbitrary model code.
- Attach an initial native stop at 1% below the current bid, rounded down to the broker tick. This
  protective overlay is application-defined and changes the unprotected paper return decomposition.
- Bound notional at 10% of current USD equity, plus exact deployment volume, nominal stop-risk,
  margin, spread, minimum-equity and cost-buffer gates. Foreign CFD notional uses a fresh broker FX
  conversion to USD; MT5's native profit/margin calculations remain authoritative.
- The model cannot change account leverage. Raw swap mode/rate/triple-day information is advisory;
  historic financing, realized gaps/slippage and full round-trip costs are not validated. Nominal
  stop risk plus the fixed buffer is not a guaranteed maximum realized loss.
- Demo only. Each exact deployment lasts at most seven entry-days and permits at most five entry
  attempts and ten reserved advisory calls. A basket freezes all member limits and the sum of their
  finite budgets. It never renews them automatically.

The basket permits **one position across the entire account**. When flat, select the earliest due
window, then the least-attempted eligible member, then symbol order. While occupied, overlapping
signals are skipped and reported; they do not create another position or become late entries.
Covering all eight markets does not guarantee that all eight trade during one deployment.

## Agent and recovery behavior

A due, approved trigger reserves its durable slot before inference. Its strategy agent returns a
binary reliability review. If accepted, an isolated risk role proposes broker-step lots or skips.
It receives only the application profile, signal, bounded anonymous numeric sizing facts and explicit
evidence limitations. Account/server IDs, raw account diagnostics, credentials and account tools
stay outside model context. The selected model/provider is pinned in the review and loaded from the
native OS secure store; no plaintext credentials, automatic model switch or environment fallback.

The application validates both replies and refreshes account/session/exposure, time, quote and sizing
after inference. It then uses the existing account-serialized native preflight, one submission and
fill reconciliation. Failed, rejected, skipped, canceled or interrupted triggers do not replay,
including after SL, restart or a new deployment. Uncertain native outcomes halt new entries.

Owned scheduled exits do not need a model and remain available after entry expiry, revocation,
retirement or an entry halt. Missing model access cannot block that exit path. Clock jumps latch
an entry halt while preserving owned-exit processing. Other service/transport faults stop the
service and retain halt/health evidence; restart cannot silently clear those latches or native
uncertainty. Recovery needs inspected evidence, not deletion/recreation of journals or trigger files.

`serve` holds an account coordinator lock and every member owner lock. Individual `tick` is a
diagnostic single cycle; the EA alone does not run the Python agents. The headless service can run
outside the TUI. MT5 and the computer must remain running for timed exits. Stopping Python does not
close positions; the submitted broker SL remains subject to broker execution and gaps. OS sleep,
network outages and unexpected closures do not provide a guaranteed exit time.

## Prepare, approve and run

Compile/install the [EA](mt5-bridge.md), refresh/reattach it and inspect the current connection.
Connection and catalog reads grant no trading authority:

```sh
.venv/bin/python -m stratosphere.mt5.overnight universe
.venv/bin/python -m stratosphere.mt5.overnight --symbol FRA40.cash status
```

Prepare private limits. The single-agent `draft` takes the eight fields documented in the MT5
deployment contract. Basket `draft` accepts those same common bounds, or a JSON object containing
`common` (the seven non-spread fields) and `spreads` (one exact `max_spread` string per matched symbol).
Spreads are price distances in each CFD's quote units; money/risk/margin/equity bounds are USD.

```sh
.venv/bin/python -m stratosphere.mt5.overnight_basket draft /private/limits.json
.venv/bin/python -m stratosphere.mt5.overnight_basket review /private/basket.json
```

Draft exports definitions, application-generated scripts, exact account/EA/runtime bindings, frozen
calendars, model identity, feasibility bounds and immutable review evidence. Stale quotes while
preparing outside trading windows defer feasibility explicitly; runtime freshness gates remain
strict and no quote age is rewritten. Draft does not call a model,
approve a deployment, run a service or submit an order. A final user approval must bind the exact
basket digest and all member strategy/evidence/account/implementation/limit digests:

```sh
.venv/bin/python -m stratosphere.mt5.overnight_basket approve /private/basket.json EXACT_BASKET_DIGEST
.venv/bin/python -m stratosphere.mt5.overnight_basket serve /private/basket.json
```

EA `EnableTrading=true` and terminal Algo Trading are also required; keep `AllowRealAccount=false`.
The adapter rejects real accounts regardless of unrelated connector settings. One exact batch review
can approve all listed member deployments; it cannot silently add another symbol or renew a budget.
An OS login service may run this exact approved command, with private logs and no automatic crash
restart. Health JSON distinguishes running/stopped state; a stale file alone does not prove liveness.

## Verification boundaries

Authored tests cover independent global calendar/DST/holiday/early-close oracles, USD conversions,
exact catalog intersection, ownership/lineage retention, approvals/budgets, two advisory roles,
post-model gates, overlap handling, restart/SL deduplication, uncertain submissions, shutdown and
model-independent exits. They use a filesystem EA peer, not a broker fill guarantee.

Actual connected checks verified the updated EA catalog and individual market metadata. Separate
real calls to the operator's saved Ollama model completed binary review and sizing using an authored
account/quote fixture, with no orders. An earlier malformed evidence reference was rejected and
retained. These checks do not establish profitability, full FTMO loss/reset compliance, sustained
operation or actual strategy-order acceptance. Private evidence stays under the canonical account
state and ignored `reports/mt5/overnight-201001727`; operational account IDs never enter Git.
