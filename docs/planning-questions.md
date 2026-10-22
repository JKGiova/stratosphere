# Roadmap planning questionnaire

Historical questionnaire. Under D57, hosted login/user databases, automatic sharing, commercial pricing and payment questions are retired and need no answers. Use [personal-use scope](personal-use.md) and the current [roadmap](roadmap.md) for remaining personal research/source/paper work. The current checkpoint closes existing cleanup before user-led bug fixing.

Status: partially answered, October 13, 2026. Confirmed answers are in decisions.md (D20–D33). The questionnaire remains the historical question set; unconfirmed options do not authorize deployment, spending or trading.

The existing roadmap gives us eight phases. We’ll turn each into tasks, dependencies, and acceptance criteria after your answers:

| Phase | Deliverable |
| --- | --- |
| 0 | Verified runtime, engine, data integration, and core contracts |
| 1 | TUI, login, shared registry, and ownership controls |
| 2 | Text/article → strategy → backtest → reproducible report |
| 3 | Improvement workflows and evidence-based recommendations |
| 4 | Additional sources and market integrations |
| 5 | Live data, paper trading, and portfolio coordination |
| 6 | Explicitly activated live execution and operational controls |
| 7 | Monitoring, broader integrations, and controlled upgrades |

**Answer by number, in batches if you prefer.** You can write “choose for me” for technical decisions or “defer until phase X.” Please don’t include credentials. The confirmed stack, permanent TUI, multi-market scope, automatic strategy sharing, and optional Alpaca/Liquid connections remain settled.

**Launch and priorities**

1. Is the first release for you alone, an invited group, or public users? Approximately how many users and simultaneous jobs should it support initially?
2. What would make the first release useful to you personally? Is the text-to-backtest workflow enough, or must recommendations, videos, or paper trading be included?
3. Do you have a target date? What monthly budgets should we plan for infrastructure, market data, and models?
4. Should this repository remain public? Do you anticipate charging for application access or compute later, even though the strategy catalog is not a paid marketplace?

**Runtime and hosting**

5. Which client operating systems must work first: macOS, Linux, Windows? Do you need remote-terminal/SSH support?
6. Where should research jobs and eventual trading run: your computer, a server you control, or our hosted service? Must they keep running when your computer is off?
7. Do you already have hosting or a Supabase project? What deployment region or data-residency requirements apply? Describe existing resources without sharing secrets.
8. What should work offline: viewing saved strategies/reports, drafting strategies, local backtests? When the shared catalog is unavailable, should registrations queue locally or wait?
9. Should I select and verify runtime versions, TUI framework, packaging, database keys/ID length, canonical format, storage, job queue, and API boundary, or do you have preferences?

**Models and agent behavior**

10. Which model providers do you want? Should users bring their own API keys, use service-funded models, or have both options? Is local-model support needed?
11. Should agents ask before every research step, approve one plan then work within it, or proceed autonomously within a budget? Which actions always need confirmation?
12. What limits should apply per job: model spend, runtime, concurrent jobs, and number of improvement variants? What happens when a limit is reached?
13. When strategy rules are ambiguous, should the agent always ask, or may it create clearly labeled assumption-based variants? Should it support scheduled research and automatic improvement later?

**Terminal experience and authentication**

14. What languages should the app support? Do you prefer chat-first with expandable results, or persistent chat/strategies/jobs/portfolio panels?
15. Which operations are essential at launch? What report exports do you need: terminal views, JSON, CSV, HTML, PDF? Any keyboard or accessibility requirements?
16. Which OAuth providers should launch first: GitHub, Google, or both? Is opening a browser for sign-in acceptable in your intended local and remote-terminal setup?
17. Should registration be open or invitation-only? Do we need admin/moderator roles, multiple-device sessions, and explicit session revocation?

**Sources and strategy definitions**

18. In what order should we add YouTube, Instagram, X, TradingView, and GitHub? Do you have representative sources or strategies to use as acceptance examples?
19. Should users upload text, code, CSV data, images, audio, or video? Which formats and size limits matter first? For inaccessible sources, is manual upload/paste sufficient initially?
20. Should the first engine support only our structured strategy language and approved indicators, or also arbitrary generated/user Python? Do you have required custom indicators or code-import use cases?
21. Which behaviors must the first strategy schema express: long/short, stops, trailing stops, multiple timeframes, multiple instruments, limit orders, intrabar rules? What initial timeframes matter most?
22. Should incomplete drafts stay private until registration? Who may edit shared names/descriptions, correct provenance, or retire a definition? May anyone fork any registered strategy?

**Market data and validation**

23. Which market, instruments, and example strategy should we use for the first end-to-end test? How should crypto, equities/ETFs, and forex integrations be ordered afterward?
24. Do you already have data providers or datasets? How much history and what granularity do you need? Must users be able to import their own datasets?
25. Should backtests use a platform-defined protocol, user-defined protocols, or both? Who sets cost assumptions, baselines, splits, and promotion thresholds, and what must users be allowed to override?
26. What qualifies a strategy for paper and then live use? Do you have required sample sizes, observation periods, robustness checks, or risk limits, or should we draft configurable criteria for review?

**Community catalog and privacy**

27. Is the catalog readable without login? Should creators appear by username, pseudonym, or anonymously? Do you want favorites, ratings, comments, or collections?
28. Which recommendation factors matter most: robustness, drawdown, net return, evidence quality, freshness, market compatibility, or holding horizon? How should unverified user-submitted results appear?
29. Which evidence may be shared: summaries, reproducible configurations, trade lists, charts, paper results, or aggregated live results? How should private or redistribution-restricted sources be handled?
30. Who handles spam, misleading claims, duplicate abuse, and disputes? What should deletion remove versus anonymize or retain, and how long should private chats, drafts, source snapshots, and research artifacts remain?

**Portfolio and execution**

31. Which optional connector comes first, Alpaca or Liquid? What account types, instruments, and country of account registration must we verify? Is any existing account used by other trading software?
32. Should live trading require approval for every order, run autonomously under an approved mandate, or support both? Are short selling, leverage, derivatives, and multiple accounts needed initially?
33. Should allocation use fixed strategy budgets, risk-based sizing, or agent proposals within bounds? How should opposing signals, priority, reserved capital, and manual trades in the same account be handled?
34. Which configurable limits do you require: position size, strategy allocation, gross/net exposure, concentration, daily loss, drawdown, leverage? Who may change them, and when do changes take effect?
35. What should happen to open positions during stale data, model failure, broker disconnection, limit breaches, logout, or shutdown? Should stopping entries, canceling orders, and closing positions each require separate confirmation?

**Operations and development**

36. Where should alerts go: TUI, email, mobile notifications, or another channel? Which events require immediate alerts, and who responds when the runtime is unattended?
37. What availability, recovery time, backup retention, and acceptable data loss should we target? What operational history must users be able to inspect or export?
38. Do other tradingsphere components already exist? What information or operations must they share, and is integration required for the first release?
39. Do you prefer development branches and PRs, or direct commits to `main`? Should I implement a whole approved milestone autonomously, or pause after individual tasks? May I use sub-agents?
40. Do you want additional `AGENTS.md` rules for required checks, dependency approval, documentation, commits, or review? At the end, I’ll list its complete current rules and clearly identify any additions we agree on.

I saved the questionnaire in [planning-questions.md](/Users/giovanni/Documents/Coding/tradingsphere/stratosphere/docs/planning-questions.md). Your answers will become confirmed decisions; unanswered items will remain explicit dependencies in the roadmap.

## First answer batch

Confirmed: extensive testing before release with no deadline; commercial opening-trade commission; private repository; Linux/macOS first and eventual all-OS support; no offline functionality; English UI; chat/slash commands plus strategy/test/position panels; first-run onboarding and hosted browser login; user-selectable providers and NVIDIA NIM DeepSeek V4.1 Flash for testing; all five sources in Phase 1; no application-imposed upload limits; convert external strategies to NautilusTrader. Supabase organization and Frankfurt region were subsequently selected. See D20–D33.

Still open: release audience/scale and measurable testing gates; compute hosting and budget; provider credential funding/handling; agent autonomy and bounded job policies; OAuth providers and hosted domain; detailed strategy contracts; first market/data; validation thresholds; catalog evidence/moderation/retention; trade approval and portfolio risk/operational policies; billing event semantics and collection; alert/recovery requirements; suite integration and GitHub development workflow.

## Second answer batch

D34–D40 confirm opening-order rather than profit fees, no reinforcement fee, users' local compute, exit keep/close choice, one research-plan approval with clarifying questions, Google/GitHub login, user-supplied model keys, Liquid first and configurable/permission-based execution. Collection options, first market/data, job-budget values and measurable release gates require discussion. See research-and-release-proposals.md.

## Latest planning correction

Login and fee-collection discussion/implementation are deferred to later phases. Preserve confirmed choices and do not ask those questions in the current pass. The TUI-exit requirement is settled: keep positions open or close them all. Do not reframe it into another user choice. Current planning focuses on core TUI, source acquisition, agents, strategy conversion, research/backtesting and initial market data.
