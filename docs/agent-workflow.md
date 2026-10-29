# Agent workflow

The active goal is orchestrator intake → exact strategy research agent → finished report → one user
approval → portfolio capital allocation → strategy trigger and strategy-agent investment proposal
→ position risk review/sizing → deterministic execution gates. The orchestrator does not place
orders. The strategy agent evaluates a trigger; the position risk agent proposes exposure, capital
and leverage within application-owned limits. This trading lifecycle remains unimplemented for MT5.
MT5 is the sole broker data/order connector. [MT5 connector](mt5-bridge.md) owns current transport,
commands, approval and execution limits. The connector is callable headlessly; the automatic agent
runner is not yet migrated to it. The single development plan is [plan.md](../plan.md).

## Strategy conversation and source intake

The orchestrator can discuss and extract a source-grounded research brief for strategy families
beyond SMA. `/discover <idea>` searches Wikipedia and Crossref without new API credentials. Only a
short model-proposed public query is sent to those indexes; private strategy histories and account
diagnostics are excluded. At most three results per index are returned; the model may choose at
most two exact result indices. Search metadata is retained for discovery, not treated as strategy
rules. Failed, paywalled and unreadable sources remain explicit; no hidden cached source replaces them.
This is bounded reference discovery, not comprehensive web or literature search.

`/paper-read "path/to/paper.pdf" [brief]` reads an explicitly supplied local PDF. `/source-file`
also accepts PDFs; `/intake <source-id> [brief]` interprets retained material. Local PDFs are parsed
locally by a credential-free subprocess; only extracted passages go to the selected model, not a
hosted document parser. Limits are 32 MiB, 250 physical pages, 30 seconds and two million extracted
characters. Encrypted/corrupt files fail; blank/image-only pages are disclosed. OCR, visual equation
verification and table/reading-order fidelity are not implemented. No embedded action, attachment
or acquired code is executed.

An explicit public strategy/paper URL can also be supplied in conversation. Public HTTPS retrieval
has finite byte/time/redirect budgets, validates public DNS peers and pins each connection while
preserving hostname TLS verification. It does not inherit browser credentials or environment proxies.
Existing provider readers retain their original contracts, including GitHub commit/blob verification.

Extraction separates entry, exit, market, timing, sizing and risk rules. Models select application-owned
passage indices; the application attaches exact text, offsets, source hashes and page/URL locators.
Missing rule categories add questions. A valid reference proves a literal passage exists, not that
its interpretation or implementation is correct. The user-supplied idea is retained and distinguished
from acquired evidence. Briefs are sealed, versioned and readable with `/intake-show <brief-id>`.
Source acquisition itself grants no registration, backtest, approval or order authority. A complete
brief can pass to the separate application-authorized [strategy development](strategy-development.md)
stage, which formalizes/registers a definition before bounded code generation and testing.

Follow-up answers reuse verified retained sources and create a new brief with explicit parent lineage;
the old brief stays immutable. Conversation drafts remain isolated by the selected strategy, accept
at most six follow-up answers, and clear on `cancel`, `never mind`, `/clear` or application restart.
Unrelated chat does not consume the draft. Unsupported `/create` ideas enter this intake path rather
than being silently replaced by SMA. Complete briefs now hand off to the strategy specialist;
[development](strategy-development.md) owns the exact language, budget and verification limits.

## Implemented research

The orchestrator routes supported SMA briefs to finite Yahoo/Nautilus research campaigns. Each
exact strategy retains a stable isolated agent; exact duplicates reuse ownership and variants retain
lineage. New v2 campaigns run automatically under frozen application budgets: at most twelve
candidates and twenty-four native starts. Retain invalid, failed, canceled and nonselected attempts;
no consumed-start replay or automatic budget replenishment. Historical v1 study readers preserve
their original approval contracts.

When the library is empty, explicit `backtest`, `research` or `test` requests can enter the
planner without selecting a nonexistent strategy. `/create` also starts a new idea. If the planner
needs details, the bridge retains the original brief and subsequent answers in an ephemeral draft,
isolated by the strategy selected when the request began. The router distinguishes an answer from
unrelated chat; a clarification cannot change the pinned research owner. Other strategy histories
are not supplied. At most six follow-up answers are accepted, and the complete brief is limited to
12,000 characters. `cancel` or `never mind` discards the current pending draft; clearing the current
conversation also discards its draft. Drafts do not survive application restart.

No campaign starts while questions remain. Once ready, the complete clarified brief is handed to
bounded research. Failed model calls retain the previous draft, while a consumed preparation/run
is not automatically replayed as another clarification. A new `/create` starts a fresh brief.
Source intake can describe other families. Legacy `/create` campaigns remain SMA-only; the
specialist compiler supports the SMA/EMA/RSI expression language described in its focused guide.

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
