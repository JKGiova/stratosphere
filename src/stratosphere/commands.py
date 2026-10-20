"""Deterministic slash-command routing without a model call."""

import re
import shlex
from dataclasses import dataclass

PANELS = {
    "strategies": "The shared library is not connected yet. No strategies loaded.",
    "jobs": "No local research runs loaded. Use /new to prepare a supported plan for approval.",
    "reports": "No completed research reports. Approve and complete a test plan first.",
    "portfolio": (
        "No trading account connected. Position data is unavailable; trading is not available yet."
    ),
}
HELP = """/help — Show commands
/data [provider-profile] [purpose] — Inspect data capabilities and unresolved requirements
/liquid — Check public connection metadata; no account or tool access
/liquid-connect — Open browser for read-only MCP catalog authorization
/liquid-disconnect — Cancel connection and forget session credentials
/liquid-tools — Inspect the connected tool catalog; no tool calls
/liquid-tool <name> — Inspect one exact tool definition
/liquid-catalog-save — Save a private catalog snapshot; no credentials
/liquid-account — Explicitly read the provider's reported trading-account address
/liquid-account-reads — List private account diagnostic attempts
/liquid-account-show <read-id> — Inspect a retained historical diagnostic online
/paper-review — Ask budget/risk agents for an advisory authored-account review
/paper-reviews — List paper-review attempts
/paper-review-show <review-id> — Open a retained advisory report
/source <url> [github-ref] — Acquire a read-only source snapshot; inspect access limits
/sources — List retained source attempts
/source-show <source-id> — Inspect a retained source snapshot online
/source-text <text> — Retain supplied text as unverified source material
/source-file <path> — Retain an explicit UTF-8 file; quote paths containing spaces
/source-captions <webvtt|srt> <path> [url] — Retain timed caption cues and attribution
/conformance <source-id> [market symbol seconds chart-type properties] — Check conversion gaps
/conformance-show <assessment-id> — Inspect a retained conversion assessment
/source-review <source-id> [strategy-id] — Ask that strategy's agent to review source evidence
/source-reviews — List retained source-review attempts
/source-review-show <review-id> — Inspect a verified source review
/new [text] — Draft a supported research plan in chat
/study [strategy-id|parent-job-id] — Review a bounded SMA study with a separate final holdout
/assess [market symbol interval-seconds] — Record dated applicability for the selected agent
/recommend <market> <symbol> <interval-seconds> — Ask each local strategy agent for candidates
/assessment <assessment-id> — Open a retained applicability snapshot
/approve <job-id> <plan-sha256> — Approve exactly the reviewed plan
/pause <job-id> — Pause research and retain completed checkpoints
/resume <job-id> — Resume the same approved plan online
/show <job-id> — Inspect a local research job
/report <job-id> — Display a completed research report
/report-data <job-id> [section] [cell|-] [page] — Inspect sealed simulation rows
/agent [strategy-id] — List or select a dedicated strategy agent
/reports — Open retained reports panel
/model — Choose a model provider, API address and session key
/strategies — Open strategies panel
/jobs — Open ongoing/completed tests panel
/portfolio — Open positions panel
/cancel [job-id] — Cancel a model response or local research job
/clear — Clear in-memory conversation
/quit — Exit
Other text is sent to your selected online model.
Research requires explicit plan approval; no live execution exists."""


@dataclass(frozen=True)
class Command:
    action: str
    message: str = ""
    args: tuple[str, ...] = ()


def route_command(text: str) -> Command:
    words = text.strip().split()
    name = words[0] if words else ""
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
            "/conformance": {2, 7},
            "/conformance-show": {2},
            "/source-review": {2, 3},
            "/source-reviews": {1},
            "/source-review-show": {2},
        }
        if len(source_words) in arity[name]:
            return Command(name[1:], args=tuple(source_words[1:]))
        return Command("message", "Use /help for source command arguments.")
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
    if name not in {"/help", "/model", "/cancel", "/clear", "/quit"} | {
        f"/{panel}" for panel in PANELS
    }:
        return Command("message", "Unknown command. Use /help for available commands.")
    if len(words) != 1:
        return Command("message", f"Usage: {name} (no arguments).")
    action = name[1:]
    if action == "help":
        return Command("message", HELP)
    return Command(action, PANELS.get(action, ""))
