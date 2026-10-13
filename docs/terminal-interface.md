# Permanent terminal interface

## Product contract

**Confirmed:** the TUI is the permanent main interface. Its center is a chat with a chief agent, plus slash commands inspired by terminal coding assistants. There is no planned migration to a browser dashboard. TUI library and exact layout remain open.

The user can supply strategy ideas and external sources, inspect recommendations, review formal rules, select experiments, intervene in ongoing workflows, and inspect portfolio state without leaving the terminal.

## Login and optional account setup

Application login uses Supabase Auth OAuth, independent of broker accounts. A browser may be used for the external sign-in/consent step; research, chat, and portfolio interaction remain in the permanent TUI. The exact PKCE/callback flow and local token storage must be verified before implementation.

After login, users can research and backtest without connecting Alpaca or Liquid. Optional account linking has its own consent, permissions, environment, and connection status. Live activation requires a separate mandate/confirmation. Disconnecting a trading account does not delete the user's strategies or app profile.

Proposed manual operations include /login, /logout, /connections, /connect <provider>, and /disconnect <connection-id>. These are not implemented commands. Distinguish logout, connector revocation, runtime shutdown, and position closure in the UI.

## Proposed surfaces

| Surface | Contents |
| --- | --- |
| Chief-agent chat | Requests, clarifications, operation results, evidence references, and decisions |
| Strategy/catalog view | Public ID, name, rules, sources, variants, and recommendation explanations |
| Jobs view | Job IDs, strategy/implementation, parameters, progress, state, costs, and interventions |
| Reports view | Trades, metrics, development/out-of-sample comparisons, and limitations |
| Portfolio view | Personal instances, allocations, positions, orders, limits, and feed health |

## Chat and operation routing

The chief agent translates natural-language requests into structured operations. Slash commands route directly to the same operation layer. Both paths validate arguments, authorization, scope, budgets, and current state. A slash command must not require an LLM call just to execute an unambiguous operation.

Incomplete arguments open a terminal form or chooser. Preview the strategy, implementation, instrument, time range, costs, and job/instance target before consequential actions. Execution reports include stable IDs so later requests can refer to the same work.

## Proposed command vocabulary

| Command | Intended operation |
| --- | --- |
| /new | Start specification from an idea |
| /import <url> | Acquire a source and extract rules |
| /strategies | Browse the user's submissions and linked catalog strategies |
| /recommendations | Explore relevant community strategies and their evidence |
| /show <strategy-id> | Inspect rules, sources, variants, and experiments |
| /backtest <strategy-id> | Select configuration and launch a historical experiment |
| /compare | Compare strategies, implementations, or experiment runs |
| /improve <strategy-id> | Start a bounded improvement workflow |
| /jobs | Inspect running and completed jobs |
| /pause <job-id> | Request a research pause at a safe checkpoint |
| /resume <job-id> | Continue a paused research workflow |
| /cancel <job-id> | Cancel a research job and retain available results |
| /portfolio | Inspect personal capital, exposures, and positions |
| /help | Explain commands and required arguments |

Command spelling, flags, and activation/shutdown commands are proposals, not implemented interfaces. Specify trading controls separately; /pause on a research job is not a position-close command.

## Workflow intervention

Persist the original request, selected experiment configuration, checkpoints, and interventions. A request such as “stop parameter search and test EUR/USD first” must identify the affected job, pause/cancel safely, retain completed evidence, and create or queue the new test.

Do not retroactively mutate a finished experiment. Changes to behavioral rules create a new strategy identity; changes to a test configuration create a new experiment. Active trading keeps its pinned strategy/implementation until an explicit controlled transition.

Proposal for job states: queued, running, pause_requested, paused, completed, failed, cancel_requested, canceled. Worker cooperation, cancellation granularity, and restart behavior need acceptance tests.

## Process lifecycle and privacy

Proposed design: closing/detaching the TUI does not terminate the trading runtime or durable research jobs; reconnecting restores state from the backend. Provide an explicit shutdown action with clearly stated effects. Final process supervisor, transport, and deployment remain open.

Chats, source access credentials, account details, and personal mandates are not catalog content. Automatic sharing applies to registered strategy definitions and eligible provenance. Show the publication policy during onboarding/import, before collecting sensitive material.

## Acceptance criteria

- Natural language and slash commands invoke equivalent validated operations.
- Users can change experiment priorities without losing completed results.
- An intervention is auditable and clearly scoped to a job or instance.
- TUI disconnect/reconnect does not corrupt operational state.
- Catalog browsing and recommendations do not automatically activate trading.

## Confirmed roadmap refinements

Use an English-only online TUI inspired by the conversational terminal experience of Claude Code and Codex. Linux and macOS are the first supported clients; all operating systems remain the longer-term goal. Alongside chat and forced slash-command operations, expose panels for opening strategies, ongoing/completed tests and actual trading positions. These surfaces are required rather than tentative.

First-run onboarding configures the user's selected model/provider, hosted browser login and trading connections. Trading connections remain optional; offer an explicit skip choice for research without a broker. No offline functionality is planned. Hosted login deployment, OAuth provider and terminal callback details still need decisions.

## Confirmed execution and onboarding refinements

Users provide their own model API keys. Google and GitHub are selected for browser login; no hosted-login domain exists yet. Research executes an approved plan and asks clarifying questions when needed. Execution uses separate configured permissions. On TUI exit, users choose keeping positions or requesting closure of all positions. The meaning of continued background management and the handling of pending orders remain open.
