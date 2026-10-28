# Agent workflow

The active goal remains orchestrator → exact strategy research agent → finished report → one user
approval → portfolio risk agent → binary signal review → position sizing → deterministic gates.
MT5 is the sole broker data/order connector. [MT5 connector](mt5-bridge.md) owns current transport,
commands, approval and execution limits. The connector is callable headlessly; the automatic agent
runner is not yet migrated to it. The single development plan is [plan.md](../plan.md).

## Implemented research

The orchestrator routes supported SMA briefs to finite Yahoo/Nautilus research campaigns. Each
exact strategy retains a stable isolated agent; exact duplicates reuse ownership and variants retain
lineage. New v2 campaigns run automatically under frozen application budgets: at most twelve
candidates and twenty-four native starts. Retain invalid, failed, canceled and nonselected attempts;
no consumed-start replay or automatic budget replenishment. Historical v1 study readers preserve
their original approval contracts.

Use `/create`, `/flow`, `/flow-show`, `/flow-cancel` and the historical research/report/source
commands in [usage](usage.md). Successful research is not approval or evidence of suitability for a
different broker instrument. A Yahoo spot/ETF study cannot silently become an FTMO gold CFD study.

## Deployment and missing integration

An MT5 connector deployment binds exact strategy/report bytes, account/server/mode/currency,
implementation/EA, instrument and finite limits. Only the user can approve it. Native trading is
disabled at installation, and real-account activation is separate. The current manual deployment
path does not establish faithful report ownership/conversion, FTMO rule compliance or an automatic
strategy executor. It must not be represented as the completed agent pipeline.

The next integration needs a broker-CFD research dataset/instrument contract, sealed exact report
validation, execution-conformant strategy behavior, closed-bar/signal ownership, account-specific
portfolio risk/allocation and deterministic protective exits. Models only propose and interpret;
the application owns evidence checks, reservations, approval, sizing bounds and submission. Broker
account/credential diagnostics stay outside strategy-model tools and histories.

## Retired broker workflow

Alpaca and Liquid are removed from active setup and routing. Retained v1 final packages, paper
journals, advisory contexts, provider observations and their readers preserve original identities;
they are not converted or reapproved for MT5. `/strategy-show` and `/portfolio-orders` inspect
historical evidence only. The former `/strategy-finish`, `/strategy-approve`, `/portfolio-run`,
Alpaca setup and Liquid connection commands cannot start transport or reach models. Legacy broker
fixtures remain compatibility tests, not current provider acceptance.
