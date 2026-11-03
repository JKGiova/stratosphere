# stratosphere development plan

Updated: November 3, 2026. This is the single active development plan.

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

## Next development gates

1. **Finish compatibility separation.** The general research crew and model tool loop are removed;
   explicit program jobs own compilation, frozen native work and evidence validation. Retained
   one-shot source/report helpers and exactly pinned MT5 model/risk adapters must be separated
   without breaking old readers, deployment approvals or protective exits. External frameworks own
   the crew and model lifecycle. Do not reactivate historical research checkpoints.
2. **Expose a small external-agent interface.** Support structured research operations, job status,
   reports and deployment proposals through the same engine operations. Keep account diagnostics,
   credentials and human approval outside strategy-model tools. MCP is an intended adapter, not an
   implemented feature. Select local versus remote connectivity separately; publish no broker endpoint
   or credentials as a side effect of this rebuild.
3. **Extend connected MT5 verification.** Initial real read-only client/EA checks passed with
   trading disabled. Verify disconnect/account-switch/restart behavior, sustained feed and private
   evidence retention. A separately approved one-time demo connector round-trip passed; next verify
   the normal finished-strategy deployment path and limits. Do not infer sustained readiness from
   one successful diagnostic or acceptance from authored tests.
4. **Connect the agent workflow to MT5.** Explicit compiled-program historical research now
   runs separately from external agent coordination and broker execution. Build a faithful CFD research and final-report validator,
   exact registered strategy/report ownership and instrument mapping, timestamp/session/cost
   semantics, signal evidence and MT5-specific portfolio risk/allocation. The bounded overnight
   adapter connects explicit operator-defined strategies; it does not complete the automatic
   research-to-execution pipeline. Retain old
   research and final-approval contracts without routing them to MT5.
5. **Implement FTMO evaluation rules.** Verify actual Free Trial/two-step Swing profile and phase,
   dated rules, timezone/day-reset equity accounting, commissions/swaps, daily/total loss stops,
   restart/reconciliation and exposure ownership. Do not treat a demo flag as Swing certification.
6. **Finish operational lifecycle.** Add sustained monitoring, inspected ambiguity recovery,
   implementation upgrades, safe stream rewarming, portfolio allocations/multiple owned positions,
   emergency close-all and keep/close exit controls. Current connector exit is an explicit owned
   ticket close; expiry/revocation block entry but retain protective exit policy. Process exit never
   automatically closes positions or continues application monitoring.
7. **Broaden faithful research.** Additional strategy families, broker instruments, session and
   corporate-action semantics, regimes, frozen-selection holdouts and measured dynamic sizing.
   Preserve all failed/rejected attempts and separate data access, model reliability and profitability.

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
