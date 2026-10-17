"""Deterministic slash-command routing without a model call."""

from dataclasses import dataclass

PANELS = {
    "strategies": "The shared library is not connected yet. No strategies loaded.",
    "jobs": "No local research runs loaded. Use /new to prepare a supported plan for approval.",
    "portfolio": (
        "No trading account connected. Position data is unavailable; trading is not available yet."
    ),
}
HELP = """/help — Show commands
/new [text] — Draft a supported research plan in chat
/approve <job-id> <plan-sha256> — Approve exactly the reviewed plan
/pause <job-id> — Pause research and retain completed checkpoints
/resume <job-id> — Resume the same approved plan online
/show <job-id> — Inspect a local research job
/report <job-id> — Display a completed research report
/model — Choose OpenRouter (free) or NVIDIA NIM and configure the model key
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
    if name == "/new":
        return Command("new", args=(text.strip().split(maxsplit=1)[1] if len(words) > 1 else "",))
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
