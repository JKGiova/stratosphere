# Gold Golden Cross connection

Retired from the operator's active catalog in favor of the [overnight agents](mt5-overnight.md).
The exact definition, stable agent, reports and journals remain historical evidence. A retired
Gold identity cannot prepare or enter a new deployment; exactly owned protective exits remain.
The following describes the retained pilot and its compatibility behavior.

This headless pilot connects one application-owned XAUUSD daily strategy to the already attached
`StratosphereBridge` EA. It is separate from the orchestrator/research-to-portfolio workflow and
implements no portfolio allocation, FTMO loss/reset engine or general position-management service.
MT5 remains the data and execution provider; no Yahoo/ETF result is relabeled as CFD evidence.

## Exact strategy

The registered `strategy/1` definition has the distinct `commodity_cfd` asset class and
`MT5:XAUUSD` symbol. The application reads exactly 260 closed D1 candles through the EA, excluding
the forming candle. It computes SMA 50 and SMA 200 on closes with exact rational arithmetic:

- SMA 50 above SMA 200: desired position is long.
- SMA 50 equal to or below SMA 200: desired position is flat.
- A flat account may enter an existing bullish regime at startup; it does not invent a past fill
  or wait indefinitely for another crossover. New-cross flags are also retained.
- No shorting or reinforcement. An owned position exits when the closed-bar regime becomes flat.

The native stop is initially the latest bid minus twice the simple mean of the last fourteen true
ranges, rounded down to the broker tick. Each true range is the maximum of high-low and the distances
from high/low to the preceding close. This is **not Wilder ATR or verified MetaTrader iATR parity**.
This protective overlay and startup behavior are explicit parts of this execution profile, not a
claim to reproduce the returns of earlier golden-cross backtests. Notional exposure is capped at
10% of current equity, with tighter deployment and broker limits authoritative.

The complete overlay participates in the strategy fingerprint. Exact reattachment reuses one
registered strategy and stable agent; script, definition, account/EA binding and observations are
retained privately. The emitted Python entry point is application-generated and pins the definition.
No model/acquired Python is compiled or executed. This CFD/fractional-sizing profile is deliberately
rejected by the historical Yahoo development adapter. The additive CFD vocabulary preserves prior
strategy definitions and identities; it does not expand historical simulation support.

## Run and inspect

The existing EA must be attached; no replacement EA or manual chart setup is added by this feature.
Run from the source checkout:

```sh
.venv/bin/python tools/mt5/gold_golden.py status
```

Equivalent module entry point:

```sh
.venv/bin/python -m stratosphere.mt5.gold status
```

`--root <bridge-directory>` before the subcommand selects an explicitly inspected bridge directory.
Status registers/exports the strategy, reads fresh account/symbol/closed bars/exposure/quote and
prints the signal plus the private generated-script path. It never requests model sizing, approves
anything or submits an order. Executing that generated script with the same Python environment and
`status` reaches the same application interface.

Raw server/chart epochs are retained, without a historical UTC conversion. Quotes outside the
EA-compatible age bound, forming/stale windows, insufficient bars and gaps longer than four days
block evaluation. Remaining holiday/session and historical-cost completeness are unverified.

## Explicit pilot deployment

This pilot supports **demo accounts only**, one entry per exact deployment and expiry within
24 hours of preparation. Connection alone grants no authority. Prepare a private JSON with exactly
these eight fields, choosing the limits yourself:

```json
{
  "max_volume": "0.01",
  "max_order_risk": "200",
  "max_order_margin": "500",
  "max_spread": "2",
  "min_equity": "99000",
  "cost_buffer": "5",
  "max_orders": 1,
  "expires": 0
}
```

These are illustrative USD-demo limits, not authorization or a sizing recommendation. Replace zero
with an explicit future Unix-UTC expiry within 24 hours and review every amount. Price-distance
spread is in XAUUSD price units, volume is broker lots, and risk/margin/equity limits are in account
currency. USD account, profit and margin currencies are currently required for sizing.

```sh
.venv/bin/python tools/mt5/gold_golden.py draft /private/path/limits.json
```

The result includes a private deployment-file path and its exact digest. Review it using the
[existing MT5 approval interface](mt5-bridge.md#exact-deployment-approval), then explicitly approve
that exact file/digest if appropriate. Preparation retains the complete definition and connection
review, including the absence of faithful CFD backtests and FTMO certification. Existing connector
or legacy strategy approvals cannot authorize this pilot.

```sh
.venv/bin/python -m stratosphere.mt5 review /private/path/deployment.json
.venv/bin/python -m stratosphere.mt5 approve /private/path/deployment.json EXACT_DIGEST
.venv/bin/python tools/mt5/gold_golden.py tick EXACT_DIGEST
```

EA trading input and terminal Algo Trading must also be enabled for an approved entry. Real-account
activation remains unsupported by this pilot, even if an unrelated connector policy allows it.
No leverage setting is changed.

## One evaluation and advisory sizing

`tick` performs one evaluation and exits. A bearish flat account stays flat, without a model call.
A bullish flat account needs the exact approved deployment before requesting sizing. The isolated
risk role receives only application-calculated signal facts and anonymous numeric lot/risk/notional
bounds, never account diagnostics, account/server identifiers, positions or credentials. It proposes
`enter` plus lots or `skip`; it cannot approve or submit. Existing model configuration is loaded
lazily from the OS secure store, with no plaintext/environment credential fallback or model switch.
The application reserves one advisory slot per deployment/signal before inference and retains the
result or failure. Failed, canceled, skipped or interrupted slots cannot silently replay.

After an entry proposal, the application refreshes data and exposure. Changed session, signal-window
content, exposure, expired review or invalid/excessive broker-step size blocks entry. It recomputes
the initial stop and quantity bounds against the refreshed quote. The existing account-serialized
gateway then performs broker OrderCheck, mandatory native stop-risk/margin checks and at most one
submission, with durable identity and fill/exposure reconciliation. Uncertain outcomes halt further
entries across restart. A consumed entry budget is checked before another advisory call.

Only an exactly journal-owned long position can be held or closed. An owned bearish exit does not
call a model and remains available under a revoked/expired entry policy, subject to the connector's
exact implementation/account checks. External exposure and pending orders block the connection.

This is **one cycle per invocation**, not an always-on scheduler or an EA-resident signal engine.
The bridge EA alone does not run this Python strategy or call its risk role. After a successful
entry, another approved tick is needed to evaluate a later crossover exit; stopping Python does not
close the position. Broker-side SL remains submitted, subject to execution/gap behavior. There is no
trailing stop, general position rebalancing or emergency-close-all implementation in this pilot.

## Verification

Authored tests cover independent SMA/true-range/sizing calculations, explicit CFD identity,
registration reuse, missing approval, finite limits, advisory privacy, invalid quantities, native
preflight rejection, changed signal, deterministic owned exit, external exposure, canceled advisory
and uncertain-submit recovery without replay. The file-based test EA is an authored peer, not a
real broker. Legacy/native backtests remain separate compatibility checks.

An actual read-only connection on October 30, 2026 registered/exported the strategy and evaluated
260 closed daily XAUUSD candles from the installed FTMO demo EA. The observed regime was bearish,
with no positions or pending orders. No strategy order, sizing-provider call or deployment approval
was performed. Private observations and verification logs are retained under
`reports/mt5/gold-golden-20261030` and the canonical per-account MT5 state directory. This verifies
the signal connection, not profitable CFD research, the new strategy's live order branch, sustained
operation or the complete agent workflow.
