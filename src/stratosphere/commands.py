"""Deterministic slash-command routing without a model call."""

import re
import shlex
from dataclasses import dataclass

PANELS = {
    "strategies": "Your local strategy library is empty. No strategies registered yet.",
    "jobs": "No local research runs loaded. Use /new to prepare a supported plan for approval.",
    "reports": "No completed research reports. Approve and complete a test plan first.",
    "portfolio": (
        "No trading account connected. Position data is unavailable; trading is not available yet."
    ),
}
HELP = """/help — Show commands
/data [provider-profile] [purpose] — Inspect data capabilities and unresolved requirements
/paper-review — Ask budget/risk agents for an advisory authored-account review
/paper-reviews — List paper-review attempts
/paper-review-show <review-id> — Open a retained advisory report
/paper [positions|orders|fills|health] [page] — Inspect the existing authored account online
/portfolio — Refresh authored positions; no provider account is connected
/source <url> [github-ref] — Acquire a read-only source snapshot; inspect access limits
/sources — List retained source attempts
/source-show <source-id> — Inspect a retained source snapshot online
/source-text <text> — Retain supplied text as unverified source material
/source-file <path> — Retain an explicit UTF-8 file; quote paths containing spaces
/source-captions <webvtt|srt> <path> [url] — Retain timed caption cues and attribution
/source-register <source-id> [name|-] [parent-id] — Register supported native JSON
/source-registrations — List native definition registration attempts
/source-registration-show <registration-id> — Inspect an immutable registration receipt
/conformance <source-id> [market symbol seconds chart-type properties] — Check conversion gaps
/conformance-show <assessment-id> — Inspect a retained conversion assessment
/source-review <source-id> [strategy-id] — Ask that strategy's agent to review source evidence
/source-reviews — List retained source-review attempts
/source-review-show <review-id> — Inspect a verified source review
/yahoo — Set up a historical Yahoo SMA backtest (stocks, crypto, forex)
/yahoo-show <job-id> — Inspect a Yahoo plan or report
/yahoo-approve <job-id> <plan-sha256> — Approve and run the exact Yahoo plan
/yahoo-cancel <job-id> — Cancel a pending or running Yahoo operation
/new [text] — Draft a supported research plan in chat
/create <brief> — Delegate a bounded Yahoo SMA research campaign to strategy agents
/discover <strategy idea> — Find public sources and extract a cited strategy research brief
/paper-read <path> [brief] — Extract strategy rules from a local PDF; quote paths containing spaces
/intake <source-id> [brief] — Extract a strategy brief from retained source material
/intake-show <brief-id> — Inspect a sealed strategy intake brief online
/flow — List new strategy-agent research campaigns
/flow-show <campaign-id> — Review all candidate plans or the completed report
/flow-approve <campaign-id> <plan-sha256> — Legacy v1 campaign approval only
/flow-cancel <campaign-id> — Cancel a campaign; consumed starts cannot replay
/portfolios — Inspect the active MT5 role; legacy evidence remains separate
/mt5 — Show the headless data/order connector entry point
/mt5-status — Inspect the attached MT5 account privately
/mt5-quote <symbol> — Read the current MT5 bid/ask and quote age
/mt5-symbol <symbol> — Read current native instrument specifications
/mt5-positions — Read actual MT5 positions
/mt5-orders — Read actual MT5 pending orders
/strategy-show <package> — Review exact final strategy, evidence and limits
/portfolio-stop — Stop monitoring/submission; positions remain open
/portfolio-orders — Inspect retained automatic paper order outcomes
/study [strategy-id|parent-job-id] — Review a bounded SMA study with a separate final holdout
/assess [market symbol interval-seconds] — Record dated applicability for the selected agent
/recommend <market> <symbol> <interval-seconds> — Ask each local strategy agent for candidates
/assessment <assessment-id> — Open a retained applicability snapshot
/approve <job-id> <plan-sha256> — Approve exactly the reviewed plan
/pause <job-id> — Pause research and retain completed checkpoints
/resume <job-id> — Resume the same approved plan online
/show <job-id> — Inspect a local research job
/report <job-id> — Display a completed research report
/analyze-report <job-id> — Ask the owning agent to analyze sealed measured evidence
/report-analyses — List retained report-analysis attempts
/report-analysis-show <analysis-id> [fact-page] — Inspect a verified advisory analysis
/report-data <job-id> [section] [cell|-] [page] — Inspect sealed simulation rows
/agent [strategy-id] — List or select a dedicated strategy agent
/reports — Open retained reports panel
/model — Choose a model provider and save its settings/key securely
/model-forget — Forget saved model settings/key and clear the active model
/strategies — Open strategies panel
/jobs — Open ongoing/completed tests panel
/portfolio — Open positions panel
/cancel [job-id] — Cancel a model response or local research job
/clear — Clear in-memory conversation
/quit — Exit
Other text is sent to your selected online model.
New research runs automatically within finite limits.
MT5 trading requires its own exact user-approved deployment and native trading enablement."""


@dataclass(frozen=True)
class Command:
    action: str
    message: str = ""
    args: tuple[str, ...] = ()


def route_command(text: str) -> Command:
    words = text.strip().split()
    name = words[0] if words else ""
    if name.startswith(("/liquid", "/alpaca")) or name in {
        "/portfolio-run",
        "/strategy-finish",
        "/strategy-approve",
    }:
        return Command(
            "message",
            "Alpaca/Liquid execution is retired. Use the MT5 connector; "
            "historical approvals cannot activate it. See /mt5.",
        )
    if name == "/mt5":
        return Command(
            "message",
            "MT5 data/order bridge: python -m stratosphere.mt5 --help. "
            "Attach StratosphereBridge EA; trading is disabled by default.",
        )
    if name in {"/mt5-status", "/mt5-positions", "/mt5-orders"} and len(words) == 1:
        return Command("mt5-read", args=(name.removeprefix("/mt5-"),))
    if name in {"/mt5-quote", "/mt5-symbol"} and len(words) == 2:
        return Command("mt5-read", args=(name.removeprefix("/mt5-"), words[1]))
    if name in {
        "/portfolios",
        "/alpaca-paper-connect",
        "/alpaca-paper-forget",
        "/portfolio-stop",
        "/portfolio-orders",
    }:
        return (
            Command(name[1:])
            if len(words) == 1
            else Command("message", "Use /help for paper commands.")
        )
    if name == "/portfolio-run":
        return (
            Command(name[1:], args=(words[1],))
            if len(words) == 2 and words[1] in {"alpaca", "liquid", "ftmo"}
            else Command("message", "Usage: /portfolio-run alpaca (paper only)")
        )
    if name == "/strategy-finish":
        if (
            len(words) in {4, 8}
            and re.fullmatch(r"[a-f0-9]{32}", words[1])
            and re.fullmatch(r"(?:[0-9]{24}|[a-f0-9]{32})", words[2])
            and words[3] in {"alpaca", "liquid", "ftmo"}
        ):
            return Command(name[1:], args=tuple(words[1:]))
        return Command(
            "message",
            "Usage: /strategy-finish <campaign> <strategy-or-job> alpaca "
            "[strategy-cap position-cap risk-cap daily-loss]",
        )
    if name in {"/strategy-show", "/strategy-approve", "/strategy-reject"}:
        count = 3 if name == "/strategy-approve" else 2
        if (
            len(words) == count
            and re.fullmatch(r"[a-f0-9]{32}", words[1])
            and (count == 2 or re.fullmatch(r"[a-f0-9]{64}", words[2]))
        ):
            return Command(name[1:], args=tuple(words[1:]))
        return Command("message", "Use /help for final-strategy command arguments.")
    if name in {"/flow", "/flow-show", "/flow-approve", "/flow-cancel"}:
        expected = {"/flow": 1, "/flow-show": 2, "/flow-approve": 3, "/flow-cancel": 2}
        if len(words) == expected[name]:
            return Command(name[1:], args=tuple(words[1:]))
        return Command("message", "Use /help for workflow command arguments.")
    if name == "/paper":
        if (
            len(words) <= 3
            and (len(words) == 1 or words[1] in {"positions", "orders", "fills", "health"})
            and (len(words) < 3 or re.fullmatch(r"[1-9][0-9]{0,8}", words[2]))
        ):
            return Command("paper", args=tuple(words[1:]))
        return Command("message", "Usage: /paper [positions|orders|fills|health] [page]")
    if name in {
        "/liquid",
        "/liquid-connect",
        "/liquid-disconnect",
        "/liquid-tools",
        "/liquid-catalog-save",
        "/liquid-account",
        "/liquid-account-reads",
    }:
        return (
            Command(name[1:])
            if len(words) == 1
            else Command("message", "Liquid commands take no arguments; never paste credentials.")
        )
    if name == "/liquid-account-show":
        if len(words) == 2 and re.fullmatch(r"[a-f0-9]{32}", words[1]):
            return Command("liquid-account-show", args=(words[1],))
        return Command("message", "Usage: /liquid-account-show <complete-read-id>")
    if name == "/liquid-tool":
        if len(words) == 2 and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", words[1]):
            return Command("liquid-tool", args=(words[1],))
        return Command("message", "Usage: /liquid-tool <exact-name-from-/liquid-tools>")
    if name in {"/paper-review", "/paper-reviews"}:
        return Command(name[1:]) if len(words) == 1 else Command("message", f"Usage: {name}")
    if name == "/paper-review-show":
        if len(words) == 2 and re.fullmatch(r"[a-f0-9]{32}", words[1]):
            return Command("paper-review-show", args=(words[1],))
        return Command("message", "Usage: /paper-review-show <complete-review-id>")
    if name == "/report-analysis-show":
        if (
            len(words) in {2, 3}
            and re.fullmatch(r"[a-f0-9]{32}", words[1])
            and (len(words) == 2 or re.fullmatch(r"[1-9][0-9]*", words[2]))
        ):
            return Command("report-analysis-show", args=tuple(words[1:]))
        return Command("message", "Usage: /report-analysis-show <complete-id> [fact-page]")
    if name == "/analyze-report":
        if len(words) == 2 and re.fullmatch(r"[a-f0-9]{32}", words[1]):
            return Command(name[1:], args=(words[1],))
        return Command("message", f"Usage: {name} <complete-id>")
    if name == "/report-analyses":
        return (
            Command("report-analyses")
            if len(words) == 1
            else Command("message", "Usage: /report-analyses")
        )
    if name == "/data":
        return (
            Command("data", args=tuple(words[1:]))
            if len(words) <= 3
            else Command("message", "Usage: /data [provider-profile] [purpose]")
        )
    if name == "/source-text":
        return (
            Command("source-text", args=(text.strip().split(maxsplit=1)[1],))
            if len(words) > 1
            else Command("message", "Usage: /source-text <text>")
        )
    if name in {
        "/source",
        "/sources",
        "/source-show",
        "/source-file",
        "/source-captions",
        "/source-register",
        "/source-registrations",
        "/source-registration-show",
        "/conformance",
        "/conformance-show",
        "/source-review",
        "/source-reviews",
        "/source-review-show",
    }:
        try:
            source_words = shlex.split(text)
        except ValueError:
            return Command("message", "Close quoted source arguments and retry.")
        arity = {
            "/source": {2, 3},
            "/sources": {1},
            "/source-show": {2},
            "/source-file": {2},
            "/source-captions": {3, 4},
            "/source-register": {2, 3, 4},
            "/source-registrations": {1},
            "/source-registration-show": {2},
            "/conformance": {2, 7},
            "/conformance-show": {2},
            "/source-review": {2, 3},
            "/source-reviews": {1},
            "/source-review-show": {2},
        }
        if len(source_words) in arity[name]:
            return Command(name[1:], args=tuple(source_words[1:]))
        return Command("message", "Use /help for source command arguments.")
    if name in {"/yahoo", "/yahoo-show", "/yahoo-approve", "/yahoo-cancel"}:
        expected = {"/yahoo": 1, "/yahoo-show": 2, "/yahoo-approve": 3, "/yahoo-cancel": 2}[name]
        if len(words) == expected and (expected == 1 or re.fullmatch(r"[a-f0-9]{32}", words[1])):
            if expected != 3 or re.fullmatch(r"[a-f0-9]{64}", words[2]):
                return Command(name[1:], args=tuple(words[1:]))
        return Command("message", "Use /yahoo or /help for Yahoo plan commands.")
    if name in {"/create", "/discover", "/paper-read", "/intake", "/intake-show"}:
        if name == "/paper-read":
            try:
                parts = shlex.split(text)
            except ValueError:
                return Command("message", "Close the quoted PDF path and retry.")
            if len(parts) < 2 or not parts[1].strip():
                return Command("message", "Usage: /paper-read <path> [strategy brief]")
        if name in {"/intake", "/intake-show"} and (
            len(words) < 2
            or not re.fullmatch(r"[a-f0-9]{32}", words[1])
            or (name == "/intake-show" and len(words) != 2)
        ):
            return Command("message", f"Use /help for {name} arguments.")
        return (
            Command(name[1:], args=(text.strip().split(maxsplit=1)[1],))
            if len(words) > 1
            else Command("message", f"Use /help for {name} arguments.")
        )
    if name == "/new":
        return Command("new", args=(text.strip().split(maxsplit=1)[1] if len(words) > 1 else "",))
    if name == "/agent" and len(words) <= 2:
        return Command("agent", args=tuple(words[1:]))
    if name == "/study" and len(words) <= 2:
        return Command("study", args=tuple(words[1:]))
    if name == "/assess":
        if len(words) in {1, 4}:
            return Command("assess", args=tuple(words[1:]))
        return Command("message", "Usage: /assess [market symbol interval-seconds]")
    if name == "/recommend":
        return (
            Command("recommend", args=tuple(words[1:]))
            if len(words) == 4
            else Command("message", "Usage: /recommend <market> <symbol> <interval-seconds>")
        )
    if name == "/assessment":
        return (
            Command("assessment", args=tuple(words[1:]))
            if len(words) == 2
            else Command("message", "Usage: /assessment <assessment-id>")
        )
    if name == "/report-data":
        if 2 <= len(words) <= 5:
            return Command("report-data", args=tuple(words[1:]))
        return Command("message", "Usage: /report-data <job-id> [section] [cell|-] [page]")
    arities = {"/approve": 2, "/pause": 1, "/resume": 1, "/show": 1, "/report": 1}
    if name in arities:
        if len(words) != arities[name] + 1:
            return Command(
                "message",
                f"Usage: {name} <job-id>" + (" <plan-sha256>" if name == "/approve" else ""),
            )
        return Command(name[1:], args=tuple(words[1:]))
    if name == "/cancel" and len(words) == 2:
        return Command("cancel", args=(words[1],))
    if name not in {"/help", "/model", "/model-forget", "/cancel", "/clear", "/quit"} | {
        f"/{panel}" for panel in PANELS
    }:
        return Command("message", "Unknown command. Use /help for available commands.")
    if len(words) != 1:
        return Command("message", f"Usage: {name} (no arguments).")
    action = name[1:]
    if action == "help":
        return Command("message", HELP)
    return Command(action, PANELS.get(action, ""))
