"""Launch-time requests to the terminal hosting the TUI."""

import os
import subprocess
import sys
from pathlib import Path

MAXIMIZE_WINDOW = "\x1b[9;1t"
MACOS_TERMINAL_SCRIPT = Path(__file__).with_name("macos_terminal.js")


def _maximize_macos_terminal() -> bool:
    """Resize only the Terminal window containing this process's TTY."""
    try:
        device = os.ttyname(sys.stdout.fileno())
        result = subprocess.run(
            ["/usr/bin/osascript", "-l", "JavaScript", str(MACOS_TERMINAL_SCRIPT), device],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
        )
    except OSError, ValueError, subprocess.TimeoutExpired:
        return False
    return result.returncode == 0


def request_maximized_window() -> bool:
    """Request host-window maximization without assuming terminal support.

    Local macOS Terminal uses its native window API and the display's usable area.
    Other hosts receive advisory XTWINOPS. Neither path changes saved preferences.
    The result reports dispatch success, not visual verification by a window manager.
    """
    try:
        if not sys.stdin.isatty() or not sys.stdout.isatty():
            return False
        if os.environ.get("TERM", "").lower() in {"", "dumb", "unknown"}:
            return False
        if (
            sys.platform == "darwin"
            and os.environ.get("TERM_PROGRAM") == "Apple_Terminal"
            and not any(
                os.environ.get(name) for name in ("SSH_TTY", "SSH_CONNECTION", "TMUX", "STY")
            )
            and _maximize_macos_terminal()
        ):
            return True
        sys.stdout.write(MAXIMIZE_WINDOW)
        sys.stdout.flush()
    except OSError, ValueError:
        return False
    return True
