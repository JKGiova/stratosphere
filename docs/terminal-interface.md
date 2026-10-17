# Permanent terminal interface

## Product contract

**Confirmed:** the TUI is the permanent main interface. Its center is a chat with a chief agent, plus slash commands inspired by terminal coding assistants. There is no planned migration to a browser dashboard. Textual is selected. The current visual direction follows [Gloomberb](https://github.com/gloom-sh/gloomberb)'s default monochrome terminal style, adapted to stratosphere’s responsive workspace.

The user can supply strategy ideas and external sources, inspect recommendations, review formal rules, select experiments, intervene in ongoing workflows, and inspect portfolio state without leaving the terminal.

## Current implementation

[Phase 1 increment 1](phase1.md) implements model onboarding, streaming conversational chat, deterministic navigation commands and capability panels. Agent tools, registry/source operations, research jobs and trading connections remain unimplemented. The command vocabulary below describes the wider planned interface; only the commands listed in phase1.md are available.

### Workspace layout and navigation

The client uses black/charcoal surfaces, white focus accents, square dividers, full-width panel title bars, a shared navigation strip, request status and a keyboard footer. The brand is plain text: clicking it opens no menu. The terminal window stays at the size chosen by the user; startup sends no resize/maximize request and runs no desktop automation.

There are six independent panels: Library, Focus, Chat, Tests, Positions and Session. The client selects the first layout that fits the following minimum terminal dimensions, measured in **columns × rows**. Each panel is allocated at least 48 columns and 14 rows in multi-panel layouts; four rows are reserved for interface controls.

| Layout | Minimum terminal size | Visible panels |
| --- | --- | --- |
| Six, 3 columns × 2 rows | 144 × 32 | All six panels |
| Four, 2 columns × 2 rows | 96 × 32 | Library, Focus, Chat and the selected Tests/Positions/Session panel |
| Two horizontal, side by side | 96 × 18 | Chat and the most recently selected other panel |
| Two vertical, stacked | 48 × 32 | Chat and the most recently selected other panel |
| One | Smaller sizes | The selected panel |

Side-by-side takes priority when both two-panel arrangements fit. Selecting a hidden panel reveals it in the appropriate slot. The initial secondary choices are Library in two-panel mode and Tests in four-panel mode. Panels remain mounted across layout changes, preserving input drafts/cursor positions, entered setup values, messages and the active view. The brand is hidden in one-panel mode to preserve reading space. Scrollable content remains accessible in small panels.

**Tab** cycles Library → Focus → Chat → Tests → Positions → Session and wraps; **Shift+Tab** reverses that order. Clicking the navigation strip selects the same panels. F1–F4 select Library, Focus, Chat and Tests respectively. All navigation is local UI routing and makes no model request. Streaming completion and cancellation preserve focus when another panel is selected.

`/strategies` selects Library; `/jobs` selects Tests; `/portfolio` selects Positions. `/` from a non-input panel returns to chat and begins a command if the draft is empty; an existing draft is preserved. The `›` composer shows the send/command hint or the active response’s stop shortcut. Submitting a draft while busy or unconfigured preserves it for retry. Ctrl+K cancels a response; Ctrl+Q exits, including during setup.

### Model setup inside agent chat

First-run model setup appears as an inline form in the agent conversation. There is no separate welcome screen or modal. `/model` reopens the same form in chat with update wording. The provider picker, model ID, masked key, privacy/publication notice, validation and cancellation remain available. Keys never enter the conversation history. The composer is hidden and disabled while the form is open, and normal Tab/Shift+Tab traverse its fields when it has focus. Enter advances from model to key and submits from the key field; Escape cancels. Users may still select other panels. Focused setup fields scroll into view, including after resizing a small terminal. Settings remain session-only.

### Model reply formatting

Assistant replies render Markdown while streaming: headings, emphasis, lists, fenced code blocks with syntax coloring, inline code and tables. Unfinished Markdown is rerendered as text arrives. User messages and command/status text remain literal. Terminal controls are stripped, Rich markup is not interpreted in model text, and rendering neither fetches images nor automatically opens links. Formatting does not execute code or change the stored model conversation. Failed/canceled replies remain marked incomplete and excluded from model history.

The interface does not populate synthetic strategies, tests, balances or positions. Unavailable account data is explicitly unknown. Session displays the configured model, actual request state and completed conversation turns; configuration alone is not a verified online request. This redesign adds no research or trading integration.

### Terminal window size at launch

Automatic maximization has been removed at the user’s request, superseding the previous native macOS Terminal/XTWINOPS launcher. The full-screen TUI adapts to the existing terminal character grid on Linux and macOS. No native launch script, Automation permission or terminal window-size control is needed.

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
