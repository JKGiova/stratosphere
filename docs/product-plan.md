# Product vision and plan

Current scope: **personal use only**, confirmed in [D57](decisions.md). stratosphere remains the private quantitative-strategy component of tradingsphere. There is no commercialization, application login, multi-user database, automatic community publication or platform service commission.

## Objective

Help the user create, develop, modify and improve quantitative strategies through agents; test them on historical and real-time data; and eventually manage explicitly authorized personal trading. Portfolio management coordinates the capital and positions of selected strategies.

The permanent interface is an English terminal TUI with chief-agent chat and slash commands. Linux and macOS come first; eventual support for other operating systems remains planned. Online operation remains required, with the existing explicit local Ollama exception.

## Personal library and agents

The orchestrator bridges requests to one stable agent per exact strategy. Private histories, provenance, tests and reports persist locally. An exact duplicate reuses its identity/agent and accumulates sources; a behavioral variation receives a distinct identity with lineage.

Local SQLite and private artifact/account journals remain necessary for this evidence. They do not constitute a hosted user database. Recommendations compare strategies in the user's own library, with dated evidence and uncertainty rather than a profitability-only leaderboard.

## Intended workflow

1. Supply an idea or authorized material from YouTube, Instagram, X, TradingView, GitHub or another supported source.
2. Preserve the actual material and access limitations, reconstruct rules and resolve ambiguities.
3. Produce an exact specification and a behaviorally conforming NautilusTrader implementation.
4. Review a bounded test plan, approve it once, run tests and inspect measured reports.
5. Compare variants with validation-only selection and a separate, unreused final holdout.
6. Evaluate eligible real-time observations and paper operation with native accounting and deterministic risk controls.
7. Enable personal live trading only through a separately authorized, verified execution path.

Chat and slash commands use the same validated operations. Models cannot grant approval, run acquired code/SQL or submit independent orders. Research cannot silently modify an active strategy.

## Markets, connections and costs

Crypto, equities/ETFs and forex remain the intended progressive scope; each market/provider needs verification. Short-term positions may last minutes, hours, days or about a week; there is no universal end-of-day liquidation.

Alpaca is the first Bitcoin-spot paper route; Liquid perpetual support comes later. Provider authentication, actual account eligibility, feed permission and order authority remain separate. Connecting a broker is optional for research and never activates trading.

The withdrawn 0.5%/$1 stratosphere fee is not a current requirement. Actual broker fees, spreads, slippage, funding, currencies and rounding still matter. Personal use does not grant unrestricted data redistribution or waive account/provider restrictions.

## Current state and readiness

The application has a working research TUI, model adapters, stable local agents, approved bounded SMA research/studies and inspectable evidence. Source acquisition/conversion and broader strategy workflows remain partial. The authored worker/risk/accounting foundation is verified within its fixture scope; an eligible real feed, broker paper transport and complete reconciliation remain unfinished.

The [Phase 3–5 ledger](phase3-5.md) defines remaining work without claiming phase completion. There is no deadline; personal operational readiness still requires extensive source, simulation, risk, restart and broker tests. The confirmed exit choice remains keeping positions or requesting closure of all, with actual closure failures/residual exposure visible.

See [personal-use scope](personal-use.md), [architecture](architecture.md), [roadmap](roadmap.md) and the [historical decision log](decisions.md). Hosted schemas/contracts and prior pricing records are historical evidence, not active requirements or authorization to delete existing resources.
