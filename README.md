# stratosphere

**Quantitative strategies created, tested, and managed through agents.**

stratosphere is the component of the **tradingsphere** suite dedicated to the complete lifecycle of a quantitative trading strategy: starting from an idea or external source, turning it into executable rules, evaluating it on historical and real-time data, improving it, and activating it on a live account with coordinated portfolio management.

Planned sources include YouTube videos, Instagram Reels, articles, X posts and threads, TradingView strategies, and GitHub repositories. Agents reconstruct the rules, highlight missing information, and propose implementations and variants. A portfolio management agent evaluates the signals from active strategies together and proposes allocations and position openings, reductions, and closures within limits enforced by the risk system.

## Project status

**Planning stage.** This repository contains the initial plan and documentation structure. It does not yet contain a working application, connectors, backtesting engines, or order execution integrations. The features described here are development objectives; the technology stack, markets, and first broker remain to be selected.

## Planned capabilities

- Import sources and preserve references to the evidence behind extracted rules.
- Create, modify, and improve strategies through specialized agents.
- Version strategy specifications and implementations, with explicit assumptions and parameters.
- Run reproducible backtests with costs, data quality checks, and out-of-sample validation.
- Run forward tests and paper trading on real-time data.
- Coordinate capital, exposures, and positions across multiple strategies.
- Activate validated versions for live trading, with monitoring, reconciliation, and operational shutdown controls.

## Strategy workflow

1. Import a source or describe an idea.
2. Extract the rules and resolve ambiguities.
3. Generate a verifiable specification and implementation.
4. Run backtests, validation, and comparisons between versions.
5. Evaluate the selected version with live data and simulated capital.
6. Explicitly activate it for live trading within a defined risk mandate.
7. Monitor results and positions; develop improvements as new versions.

Backtest results and claims made by a source do not guarantee future returns.

## Documentation

| Document | Contents |
| --- | --- |
| [Plan index](docs/README.md) | Reading order and document status |
| [Product vision and plan](docs/product-plan.md) | Objectives, use cases, boundaries, and requirements |
| [Architecture and agents](docs/architecture.md) | Components, responsibilities, and contracts |
| [Sources and strategy lifecycle](docs/strategy-lifecycle.md) | Importing, specifications, versions, and improvements |
| [Data and validation](docs/testing-and-validation.md) | Backtests, live evaluation, and promotion criteria |
| [Portfolio and execution](docs/portfolio-and-execution.md) | Allocation, positions, risk, and orders |
| [Roadmap and decisions](docs/roadmap.md) | MVP, subsequent phases, and open decisions |

## First development objective

Complete a reproducible workflow from **article or user-provided text → specification → executable strategy → backtest → report**, using one market and one data format. Multimedia integrations, paper trading, and live execution follow in the phases defined in the [roadmap](docs/roadmap.md).

## Development conventions

GitHub is the shared source of truth for development. Code, comments, documentation, commit messages, and GitHub development discussions must be written in English. The product and suite names are always written as **stratosphere** and **tradingsphere**, respectively.
