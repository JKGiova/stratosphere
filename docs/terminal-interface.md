# Permanent terminal interface

## Product contract

**Confirmed:** the TUI is the permanent main interface. Its center is a chat with a chief agent, plus slash commands inspired by terminal coding assistants. There is no planned migration to a browser dashboard. Textual is selected. The current workspace follows the four-pane terminal style of [agentic-trading-workflow](https://github.com/JKGiova/agentic-trading-workflow).

The user can supply strategy ideas and external sources, inspect recommendations, review formal rules, select experiments, intervene in ongoing workflows, and inspect portfolio state without leaving the terminal.

## Current implementation

[Phase 1 increment 1](phase1.md) implements model onboarding, streaming conversational chat, deterministic navigation commands and capability panels. Agent tools, registry/source operations, research jobs and trading connections remain unimplemented. The command vocabulary below describes the wider planned interface; only the commands listed in phase1.md are available.

### Workspace layout and navigation

The redesigned client uses a navy background, cyan focus borders, a shared navigation strip, numbered pane titles, a model/request status strip and a keyboard footer. At 80×24 and larger, four panes stay visible. Larger terminals give chat more vertical space:

| Alternate shortcut | Pane | Current behavior |
| --- | --- | --- |
| F1 | Strategy library, upper left | Empty table and explicit disconnected-library state |
| F2 | Strategy focus, upper right | Empty selection state; rules/source/version inspection remains planned |
| F3 | Agent chat, lower left | Literal streamed messages, request errors/cancellation and an inline command input |
| F4 | Activity, lower right | Tests, Positions and Session views; the first two disclose unavailable integrations, while Session shows the selected model, actual request state and completed conversation turns |

**Tab** cycles through Library → Focus → Chat → Tests → Positions → Session, then wraps. **Shift+Tab** reverses that order. Clicking the navigation strip selects the same views. F1–F4 remain alternate direct pane shortcuts. All navigation is local UI routing and makes no model request.

Below 80 columns or 24 rows, the selected pane fills the workspace and the brand header is hidden to preserve reading space. Tab/Shift+Tab work in either layout. Resizing preserves input drafts, cursor position and conversation history. Streaming completion and cancellation preserve focus when browsing another pane. Scrollable content remains accessible in small panes. Inside model setup, Tab/Shift+Tab retain standard form-field navigation; they cannot switch the underlying workspace.

`/strategies` selects the library; `/jobs` and `/portfolio` select the relevant activity view. `/` from a non-input pane returns to chat and begins a command if the draft is empty; an existing draft is preserved. The inline `›` composer shows the send/command hint or the active response's stop shortcut. Submitting a draft while the model is busy or unconfigured preserves it for retry. Ctrl+K cancels the model response, and Ctrl+Q exits. Model setup uses the same visual theme, a provider picker, an editable model ID and a masked key, with a session/privacy notice before the credential field. It can be reopened with `/model`.

The interface does not populate synthetic strategies, tests, balances or positions. An unavailable account is not displayed as an empty live portfolio. Session status distinguishes model configuration from a completed online request. This visual redesign adds no research or trading integration.

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

Users provide their own model API keys. Google and GitHub are selected for browser login; no hosted-login domain exists yet. Research executes an approved plan and asks clarifying questions when needed. Execution uses separate configured permissions. On TUI exit, users choose keeping positions or requesting closure of all positions. The keep/close choice is settled and must not be reopened. Internal lifecycle and pending-order handling must honor that choice.

Login implementation is deferred to later pre-release work; preserve it in the final onboarding design while focusing current development on model setup, chat, commands, panels and research.
