"""Deterministic slash-command routing without a model call."""

from dataclasses import dataclass

PANELS = {
    "strategies": "Strategy registry is not connected in this increment. No strategies loaded.",
    "jobs": "Research job execution is not implemented in this increment. No tests loaded.",
    "portfolio": (
        "Trading connections are not implemented. Positions are unknown; no account connected."
    ),
}
HELP = """/help — Show commands
/model — Choose OpenRouter (free) or NVIDIA NIM and configure the model key
/strategies — Open strategies panel
/jobs — Open ongoing/completed tests panel
/portfolio — Open positions panel
/cancel — Cancel the current model response
/clear — Clear in-memory conversation
/quit — Exit
Other text is sent to your selected online model. Chat has no execution tools yet."""


@dataclass(frozen=True)
class Command:
    action: str
    message: str = ""


def route_command(text: str) -> Command:
    words = text.strip().split()
    name = words[0] if words else ""
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
