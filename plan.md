# stratosphere development plan

Updated: November 4, 2026. This is the single active development plan.

## Product and decisions

stratosphere is a personal headless strategy engine in tradingsphere, initially for Linux and macOS.
The source repository is public; credentials, account data and operational artifacts remain private.
Keep English, local SQLite, private artifacts and native OS credential storage. External agent
frameworks own conversation, strategy research coordination and model selection. The engine owns
validated strategy programs, market data, backtests, evidence, approvals, deterministic risk gates
and the MT5 connector. The rebuild covers both backtesting and execution through a common strategy
definition. The TUI, slash-command router, internal research crew and persistent tool runner are
removed. General external-agent tools/MCP are not implemented yet. Research uses explicit typed
program jobs; frozen operator-approved MT5 adapters retain their separate model/risk dependencies. No billing, app login, hosted user
database, strategy sharing or release deadline.
Unused conversation parsers, presentation helpers and runner-only research helpers are removed.
Historical contracts and evidence readers remain; development checkpoints have no recovery mutator.
NautilusTrader wheel production belongs to the separate `nautilus-trader-macos-intel` repository.
stratosphere consumes its pinned Intel artifact and matching runtime dependencies; Linux and Apple
Silicon retain upstream wheels. No engine build tooling or vendored wheel belongs in this project.

MT5 is the sole active broker data and order connector, including the FTMO account. Alpaca and
Liquid are retired from active commands/setup; their formats, journals, evidence and readers remain
under legacy compatibility. Yahoo/yfinance and NautilusTrader historical research remain available
separately; a spot/ETF result cannot silently become a broker-CFD deployment. No historical approval
transfers authority to MT5. Broker login stays inside the terminal. Connection/installation never
activates trading. Real-account activation requires separate explicit application and terminal
permission. No challenge purchase is authorized.

The target external-agent workflow is orchestrator intake → exact strategy research agent
→ final report/package
→ one user approval → portfolio capital allocation → strategy trigger/agent investment proposal
→ position risk review (exposure, capital and leverage) → application gates → execution within limits.
The orchestrator never submits orders; models only propose and deterministic application code enforces
limits and owns broker submission. Research runs under finite app limits, without a
human approval prompt. There is no per-order approval within a reviewed MT5 deployment. Existing historical
protocols retain their original readers and authorization semantics.

Current entry points: [headless usage](docs/usage.md). Target lifecycle and retained internal
contracts: [agent workflow](docs/agent-workflow.md);
[strategy-agent development](docs/strategy-development.md) owns code generation and comparative research.
Mandatory boundaries: [invariants](docs/invariants.md). Other guides: [docs index](docs/README.md).

## Current implementation and evidence

| Area | Implemented | Limits / remaining verification |
| --- | --- | --- |
| Headless interface | Package help, MT5 connector dispatch and compatibility inspection; no TUI dependency or slash-command router | General research CLI/tools, external-agent/MCP integration and remote connectivity remain pending |
| Source and compatibility operations | Public source readers, PDF extraction, sealed briefs, explicit one-shot model adapters and stable registry ownership remain | Internal conversation/router/crew modules are removed. External agent integration and faithful conversion remain unfinished; retained model helpers are not an embedded crew |
| Legacy SMA research | `research-campaign/2` campaigns; at most 12 candidates / 24 native starts, all attempts retained, no consumed-start replay, sealed reports | No generic optimizer, independent holdout or global-best claim; real model reliability still needs checks |
| Strategy development | V3 Python operations accept an externally supplied validated definition, freeze datasets and run one application-compiled program with base/stress comparisons; reports retain exact owners, attempts and scopes | At most 4 targets / 8 native starts / 0 model calls / 1,200 execution seconds. No internal refinement or checkpoint continuation. V1/v2 reports and consumed receipts remain read-only. Long-only fixed units, hourly/daily; source conformance, independent holdout and CFD execution remain unverified |
| Final strategy | Exact selected report/definition/account/instrument/implementation and complete paper limits; one human approval and inactive rejection | Historical fixed-quantity evidence does not validate variable paper-policy returns; retirement/upgrades remain unfinished |
| Broker setup | Sole active MT5 role; Alpaca/Liquid commands and automatic credential restoration retired | Historical journals, contracts and readers remain; their approvals cannot activate MT5 |
| MT5 bridge | Local typed EA/client, exact account/session binding and private durable journal; actual Intel macOS FTMO demo reads passed for account/specs/advancing quotes/M1-H1-D1 candles/positions/orders/deals with trading disabled | Linux deployment, disconnect/account-switch/restart, sustained feed freshness, timestamp/session/cost semantics and independent executable attestation remain unverified |
| MT5 execution | Exact operator deployment approval, finite order/volume/cash-risk/margin/spread/equity bounds, mandatory SL, native checks, FOK/IOC entry, owned close/revoke, shortened signal deadlines and no uncertain replay; actual one-time demo round-trip passed | One account position at a time; connector diagnostics do not validate finished-strategy execution, FTMO daily/reset rules or portfolio allocation |
| Overnight agents | Paper-grounded close-to-open CFD adaptation; exact catalog intersection/calendars, generated scripts, stable owners, binary signal review/risk sizing, bounded coordinator and owned exits. V2 separates manual exposure from the single agent position, skips occupied symbols/capacity shortages and supports explicit recovery without budget renewal | Separate operator-defined adapter, not the general research pipeline. Historical v1 demo startup passed, then manual exposure stopped it. V2 fixture verification and native compilation are separate from new exact activation, actual strategy fills, sustained operation, CFD profitability and FTMO compliance |
| Retired Gold pilot | XAUUSD daily golden-cross identity, historical reader/code, agent and private evidence retained; retirement removes active catalog routing and blocks new entries | Historical observation/connector evidence grants no overnight authority |
| Historical services | Original research/studies, source/evidence readers, authored ledger/runtime, schemas and journals retained | Historical provider fixtures are not active accounts or transferred approvals |
| Gold data | Actual MT5 export validated for all 21 intervals; H2–D1 reach June 2004 | Short intervals hit 100k terminal limit; full archives/UTC/session/cost semantics remain unverified; this is acquisition, not a CFD backtest |

## CLI/MCP rebuild: component plans

These are proposed work packages for the CLI/MCP product, not implemented capabilities.
This section is the canonical plan for every component chat; focused guides remain reference material.
The current chat coordinates shared decisions and dependencies. Component chats initially inspect
and discuss their assigned scope without editing the shared checkout or starting implementation.
Accepted decisions and contract changes return here before dependent work starts. Open decisions
below require agreement; the initial local-first direction does not authorize remote broker exposure.

### Product milestones and integration order

- **M1 — external research:** an externally supplied supported strategy passes model-free intake,
  runs bounded historical research and yields the same inspectable report through CLI and MCP.
  Use existing supported historical scopes first; label authored fixtures and Yahoo research honestly.
  M1 grants no CFD equivalence, deployment approval or trading authority.
- **M2 — faithful MT5 deployment:** exact MT5 dataset/session/cost semantics, validated CFD research,
  complete final evidence, account-specific deterministic risk and one operator-approved deployment.
  Native check-only and separately authorized demo checks precede any operational readiness claim.
- **M3 — operational readiness:** sustained feed, restart/reconciliation, owned exits, inspected
  uncertainty recovery and verified dated account/FTMO rules for the supported configuration.

Start 01 by agreeing the shared contracts. Designs for 02–04, 06–07 and compatibility work can be
explored alongside it, with contract-dependent implementation waiting for the relevant decisions.
Build M1 through 02 → 03/04 → 05 → 08; 09 checks compatibility throughout. M2 joins 03/04 with
06/07; 09 owns integrated operational acceptance. Dependencies below gate implementation, not
read-only planning, and do not authorize simultaneous changes to a shared file.

| ID | Component chat | First verifiable outcome | Implementation dependencies |
| --- | --- | --- | --- |
| 01 | [Engine interface and jobs](#01-engine-interface-and-jobs) | One shared research job contract | None for design |
| 02 | [Strategy intake and registry](#02-strategy-intake-and-registry) | Model-free authored intake and exact identity | 01 |
| 03 | [Market data and MT5 snapshots](#03-market-data-and-mt5-snapshots) | Frozen dataset with honest semantic limits | 01 |
| 04 | [Backtesting and evidence](#04-backtesting-and-evidence) | Bounded run and sealed exact-owner report | 01, 02, 03 |
| 05 | [CLI and MCP adapters](#05-cli-and-mcp-adapters) | Equivalent installed CLI/MCP research operations | 01; 02–04 for full research |
| 06 | [Portfolio risk and deployment approval](#06-portfolio-risk-and-deployment-approval) | Inspectable deployment proposal; no activation | 01–04; existing MT5 execution constraints |
| 07 | [MT5 execution and position lifecycle](#07-mt5-execution-and-position-lifecycle) | Read-only recovery and check-only execution | 03, 06; read-only checks independent |
| 08 | [External crew integration](#08-external-crew-integration) | One external crew reads its own research report | 05; 01–04 for research |
| 09 | [Compatibility, operations and release](#09-compatibility-operations-and-release) | Installed-product and historical-reader acceptance | Throughout; integrated gates follow claimed scope |

### 01 Engine interface and jobs

**Owns:** the shared application interface used by CLI, MCP and tests, plus research job identity and lifecycle.

**Current:** explicit v3 preparation/execution and durable stores exist; callers still assemble internal services themselves.

**Deliver:** versioned request/result/error contracts for capabilities, validation, preparation, execution, status, cancellation and report references. Reuse existing validation and stores behind a small interface. Freeze configuration, exact owners, finite budgets and implementation pins before work. Define cancellation and process-death semantics without adding historical resume or uncertain replay.

**Inputs / outputs:** validated intake, strategy and dataset references → durable job ID, state, structured failures and immutable result references.

**Acceptance:** CLI and MCP can call the same operation with equivalent results; duplicate delivery neither reacquires data nor replenishes work; concurrent claims, cancellation and restart retain consumed reservations. Unknown schema versions fail explicitly. Status is read-only.

**Blocked by:** none for contract design; concrete integration consumes 02–04.

**First slice:** expose one existing historical research job through the shared interface, retaining the current v3 ceilings.

**Decisions to settle:** foreground execution versus a bounded job process, application construction/configuration and structured error taxonomy.

### 02 Strategy intake and registry

**Owns:** typed external strategy submission, source/brief provenance, stable registry identity, duplicates and explicit variant lineage.

**Current:** the registry, public readers and sealed briefs exist; the legacy intake producer still accepts a model transport.

**Deliver:** an explicit model-free submission path for an external crew's proposed rules and supported strategy definition. Distinguish caller-authored declarations, literal source evidence and verified behavior. Preserve existing brief readers; never mint evidence by accepting an agent assertion. Keep unresolved/unsupported logic visible and reject executable preparation until required fields and rule mappings are valid.

**Inputs / outputs:** supported structured definition, optional public source evidence and declared parent → validated intake/strategy references or actionable validation findings.

**Acceptance:** a caller can supply a supported strategy without invoking an internal model; exact duplicates reuse identity; changed behavior has a concrete variant owner; invalid rule mapping or lineage is rejected before acquisition/execution. Historical sealed intake bytes remain readable.

**Blocked by:** 01 contracts; source-format/conformance rules remain authoritative.

**First slice:** register an explicitly authored supported strategy and sealed intake, honestly labeled as authored rather than source-faithful conversion.

**Decisions to settle:** minimum intake payload, whether source-free authored proposals are supported, and how external interpretations attach to retained passages.

### 03 Market data and MT5 snapshots

**Owns:** immutable research datasets and exact instrument, interval, timestamp, session and cost metadata.

**Current:** Yahoo historical acquisition and pinned snapshots exist separately; MT5 supports local reads/exports with remaining timestamp and coverage limitations.

**Deliver:** validated snapshot operations that retain acquisition attempts, coverage/gaps, raw timestamps, provenance and immutable digests. MT5 remains the sole active broker connector; existing Yahoo research stays explicitly historical and separate. A research-safe capability describes supported instruments without exposing account diagnostics, credentials or private account identifiers to strategy tools.

**Inputs / outputs:** explicit data request and reviewed instrument mapping → frozen dataset reference plus quality/semantic findings.

**Acceptance:** missing/ambiguous mappings, stale data, gaps and truncated coverage remain visible; repeats cannot silently refetch a frozen job; no inferred broker-to-spot equivalence or guessed server-time UTC conversion. Reopening snapshots changes no evidence. Read-only MT5 checks leave trading disabled.

**Blocked by:** 01 reference contracts; MT5 CFD research additionally needs verified terminal/session/cost semantics.

**First slice:** expose existing supported historical snapshots, then add a separately verified MT5 snapshot path.

**Decisions to settle:** initial symbol/interval scope, snapshot storage portability and explicit server-time/session interpretation.

### 04 Backtesting and evidence

**Owns:** validated signal compilation, isolated native runs, measured comparison and exact report ownership.

**Current:** v3 jobs run one application-compiled program over up to four targets/eight native starts with zero model calls; v1/v2 remain read-only.

**Deliver:** expose existing bounded runs and paged reports through 01, retaining failed/blocked targets, generated code, base/stress economics, every attempt and exact concrete variant owners. Define the additional execution/cost semantics required before an MT5 CFD result can support a deployment proposal. Independent holdouts and new strategy families remain later extensions.

**Inputs / outputs:** exact strategy/intake/dataset references and frozen scope → sealed report, attempted-cell references and explicit verification limitations.

**Acceptance:** only bytes matching the application compiler run in credential-free workers; failures consume reserved starts; no uncertain replay or replenished budget. Report tampering fails, historical reports reopen unchanged, and measured values retain candidate/window/cost ownership.

**Blocked by:** 01, 02 and 03 contracts; initial work reuses existing supported historical data.

**First slice:** one externally submitted supported strategy completes base/stress runs and yields an inspectable report.

**Decisions to settle:** minimum public report fields and the explicit scope required for a faithful MT5 CFD simulation.

### 05 CLI and MCP adapters

**Owns:** the two product entry points over the shared engine interface.

**Current:** package help, MT5 operator commands and compatibility inspection exist; general research CLI/MCP do not.

**Deliver:** installable CLI commands and an MCP adapter for the same allowlisted research operations. Proposed initial transport is local stdio; remote transport needs a separate decision. Machine output uses versioned JSON, stable exit/error behavior and bounded paged results. MCP protocol stdout is separate from logs. Expose capabilities so unsupported operations are explicit.

**Inputs / outputs:** typed arguments or MCP requests → the same job/result/error contracts, without duplicating engine rules.

**Acceptance:** one supported workflow works through both adapters with equal identity, evidence and budget semantics; malformed input, cancellation, duplicate calls, broken pipes and process termination are covered. Account diagnostics, credential access, human approval and broker order submission remain outside strategy MCP tools. Do not equate a CLI flag with human authorization.

**Blocked by:** 01; complete research demo consumes 02–04. Begin with help/capabilities/status once their contracts settle.

**First slice:** prepare/run/status/report through CLI, then equivalent MCP tools.

**Decisions to settle:** installed command naming, MCP library/transport, synchronous versus job-handle responses and private configuration discovery.

### 06 Portfolio risk and deployment approval

**Owns:** deterministic allocation/exposure checks, final deployment proposals and exact operator authorization.

**Current:** historical paper accounting/contracts and guarded MT5 deployments exist; a general research-to-MT5 portfolio workflow is unfinished.

**Deliver:** validate a proposal binding exact completed strategy, complete evidence, instrument, account/server/mode, implementation/EA and finite limits. External strategy/risk roles submit proposals; application code validates and reserves under aggregate account constraints. Keep private account diagnostics out of strategy contexts. Preserve the trusted operator approval path independently of model-controlled request fields.

**Inputs / outputs:** exact evidence plus proposed deployment/size → reviewable proposal, deterministic denials or a separately authorized deployment reference.

**Acceptance:** altered bindings, stale/insufficient evidence, missing mappings, conflicting exposure and unavailable risk facts deny entry. A report, model reply or historical Alpaca/Liquid approval grants no MT5 authority. Real activation stays separately authorized; protective exits remain independent.

**Blocked by:** 01–04 contracts and verified constraints of the existing MT5 gateway; faithful CFD evidence gates the general deployment slice. Agree the interface with 07 during design; proposal inspection does not depend on building the new execution path.

**First slice:** construct and reject/inspect proposals without enabling trading.

**Decisions to settle:** authoritative capital model, trusted human-approval mechanism and initial single-position scope. Verify dated FTMO profile/rules and day-reset equity accounting before any FTMO compliance claim.

### 07 MT5 execution and position lifecycle

**Owns:** exact local EA/session binding, native checks, durable order uncertainty and owned protective exits.

**Current:** the bridge and finite approved deployments exist, including separate pinned overnight adapters; they are not the general external-crew runtime.

**Deliver:** connect approved compiled strategies through explicit deterministic trigger/decision contracts and existing risk gates. External reviews may propose entry/size within a frozen contract; unavailable/invalid reviews deny entry. Preserve native request reservation before submission, no reinforcement, exact ownership and exits independent of models, entry expiry and revocation.

**Inputs / outputs:** exact approved deployment and fresh validated signal/risk decision → durable checked/submitted/confirmed/ambiguous outcome and owned position state.

**Acceptance:** disconnect, account/session switch, late replies, restart, partial/conflicting fills and ambiguous submission halt entries without retrying uncertain requests. Never close external/manual exposure. Existing approved exit access survives compatibility changes. Simulation cannot confer executable attestation or account ownership.

**Blocked by:** 03 and 06 for the general path; read-only connector verification can start independently.

**First slice:** read-only restart/account-switch verification, then proposal/check-only integration; actual demo orders require their own exact authorization.

**Decisions to settle:** supported first signal program/instrument, execution implementation/EA binding and safe upgrade policy for existing approvals.

### 08 External crew integration

**Owns:** a reproducible integration with one external agent framework using the public product interface.

**Current:** the framework called “grokbot” has not been identified or compatibility-verified; no framework dependency is selected.

**Deliver:** verify the intended project's identity and official CLI/MCP capabilities, then build a minimal external example covering intake, strategy proposal, bounded research and evidence interpretation. Keep conversations, model choice and crew coordination in that framework. Use the versioned interface without privileged access to engine internals.

**Inputs / outputs:** external task/strategy proposal → exact strategy/job/report references and an honest research summary; deployment remains a separately approved path.

**Acceptance:** a fresh example runs against the installed product, preserves isolated exact strategy ownership and handles schema rejection, unsupported tools, duplicate delivery and exhausted budgets. Role labels alone confer no authority. Model contexts omit account diagnostics/credentials; logs exclude secrets. Separate deterministic fixture checks from an actual framework/model smoke check.

**Blocked by:** 05 with 01–04 for the first research workflow. Framework identification can begin immediately from primary sources.

**First slice:** one crew submits one supported strategy and reads its report without trading.

**Decisions to settle:** exact framework repository/version, local deployment arrangement and role/tool allowlists. Select an equivalent only after verifying the intended framework's suitability.

### 09 Compatibility, operations and release

**Owns:** retained readers/approval pins, installation, operational health and cross-module acceptance.

**Current:** core/native fixtures and package builds pass locally; cross-platform and sustained broker operation are separate verification gates.

**Deliver:** an inventory separating active engine modules from compatibility readers and pinned operator adapters. Plan any model-dependency extraction with preserved historical formats and exact owned exits. Define private state locations, read-only health, bounded process ownership, inspected recovery and implementation upgrades. Consume pinned external NautilusTrader artifacts; wheel production stays in its own repository.

**Inputs / outputs:** integrated product and retained evidence fixtures → installable artifacts, compatibility results and explicit release limits.

**Acceptance:** clean installation and engine-free startup work; CLI/MCP research parity passes; old v1/v2 evidence reopens unchanged; consumed reservations and operational journals survive restart. Linux/macOS verification, sustained MT5 freshness, interruption/recovery and exit behavior are recorded separately from fixtures. No automatic operational resource deletion, approval renewal or live activation.

**Blocked by:** compatibility design can start now; release acceptance consumes 01–08 for the claimed feature set.

**First slice:** retain historical archive/approval-exit regressions while packaging the first CLI/MCP research milestone.

**Decisions to settle:** supported launch platforms, process supervision, diagnostic redaction and staged release criteria. Multiple owned positions/emergency controls require explicit later scope.

## Verification and release gate

Run focused tests for each change, then necessary integration/compatibility checks. Keep failed logs
and environmental limits. Keep native integration tests in the engine-marked suite, including
headless engine paths; engine-free compatibility must not depend on an installed native engine.
The cross-platform application matrix installs verified dependency wheels without compiling them;
external wheel release readiness and application acceptance remain separate checks.
Source-intake verification on October 29, 2026 includes actual local PDF parsing, authored HTTP/model
fixtures and an actual public Wikipedia/Crossref search plus RSI page retrieval. These checks do not
establish live-model extraction reliability or faithful strategy conversion. Specialist development
now has actual generated-code Nautilus fixtures and a separate actual AMZN/TSLA Yahoo comparison
with authored model replies: four cells / eight native starts completed. A real local Ollama check from an authored baseline also completed two native starts; an invalid
revision was rejected. Small-model formalization/refinement reliability remains unresolved. The
general research-to-MT5 agent pipeline remains unconnected. The separately bounded
[overnight adapter](docs/mt5-overnight.md) does not establish that integration or CFD profitability.
See the [development guide](docs/strategy-development.md) for research limits and evidence.

Distinguish authored fixtures/native simulation, actual model calls,
provider checks and installed-client readiness. No phase or release is complete from a narrower test,
profitable replay or adapter implementation. Require sustained operation, recovery/exit controls,
verified credentials and evidence ownership for every advertised feature.
