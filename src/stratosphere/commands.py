"""Deterministic slash-command routing without a model call."""

from dataclasses import dataclass

PANELS = {
    "strategies": "The shared library is not connected yet. No strategies loaded.",
    "jobs": "Research runs are not available yet. No ongoing or completed tests loaded.",
    "portfolio": (
        "No trading account connected. Position data is unavailable; trading is not available yet."
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
