import io
import json
import os
import pty
import subprocess
import sys
from types import SimpleNamespace

import pytest

from stratosphere import terminal, tui


@pytest.fixture(autouse=True)
def isolated_terminal_environment(monkeypatch):
    for name in ("TERM_PROGRAM", "SSH_TTY", "SSH_CONNECTION", "TMUX", "STY"):
        monkeypatch.delenv(name, raising=False)


class TtyBuffer(io.StringIO):
    def isatty(self):
        return True


def test_launch_requests_maximization_before_starting_the_full_screen_tui(monkeypatch):
    output = TtyBuffer()
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setattr(terminal.sys, "stdin", TtyBuffer())
    monkeypatch.setattr(terminal.sys, "stdout", output)
    runs = []

    class App:
        def run(self, *, inline):
            assert output.getvalue() == terminal.MAXIMIZE_WINDOW * (len(runs) + 1)
            runs.append(inline)

    monkeypatch.setattr(tui, "StratosphereApp", App)
    tui.main()
    tui.main()
    assert runs == [False, False]
    assert output.getvalue() == terminal.MAXIMIZE_WINDOW * 2


@pytest.mark.parametrize("redirected", ["stdin", "stdout"])
def test_redirected_streams_do_not_receive_window_controls(monkeypatch, redirected):
    output = TtyBuffer()
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setattr(terminal.sys, "stdin", TtyBuffer())
    monkeypatch.setattr(terminal.sys, "stdout", output)
    redirected_stream = io.StringIO()
    monkeypatch.setattr(terminal.sys, redirected, redirected_stream)
    assert not terminal.request_maximized_window()
    assert output.getvalue() == redirected_stream.getvalue() == ""


@pytest.mark.parametrize("term", [None, "", "dumb", "DUMB", "unknown"])
def test_non_graphical_or_unknown_terminal_is_left_alone(monkeypatch, term):
    output = TtyBuffer()
    if term is None:
        monkeypatch.delenv("TERM", raising=False)
    else:
        monkeypatch.setenv("TERM", term)
    monkeypatch.setattr(terminal.sys, "stdin", TtyBuffer())
    monkeypatch.setattr(terminal.sys, "stdout", output)
    assert not terminal.request_maximized_window()
    assert output.getvalue() == ""


def test_unavailable_output_does_not_prevent_launch(monkeypatch):
    class UnavailableTerminal(TtyBuffer):
        def write(self, text):
            raise OSError("Terminal no longer available")

    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setattr(terminal.sys, "stdin", TtyBuffer())
    monkeypatch.setattr(terminal.sys, "stdout", UnavailableTerminal())
    assert not terminal.request_maximized_window()


def test_maximize_request_reaches_a_real_pseudoterminal(monkeypatch):
    master, slave = pty.openpty()
    try:
        with os.fdopen(os.dup(slave), "w", encoding="utf-8") as output:
            with os.fdopen(os.dup(slave), "r", encoding="utf-8") as input_stream:
                monkeypatch.setenv("TERM", "xterm-256color")
                monkeypatch.setattr(terminal.sys, "stdin", input_stream)
                monkeypatch.setattr(terminal.sys, "stdout", output)
                assert terminal.request_maximized_window()
                assert os.read(master, 64) == terminal.MAXIMIZE_WINDOW.encode()
    finally:
        os.close(master)
        os.close(slave)


def macos_terminal(monkeypatch):
    class NativeTtyBuffer(TtyBuffer):
        def fileno(self):
            return 12

    output = NativeTtyBuffer()
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("TERM_PROGRAM", "Apple_Terminal")
    monkeypatch.setattr(terminal.sys, "platform", "darwin")
    monkeypatch.setattr(terminal.sys, "stdin", NativeTtyBuffer())
    monkeypatch.setattr(terminal.sys, "stdout", output)
    monkeypatch.setattr(terminal.os, "ttyname", lambda descriptor: "/dev/ttys123")
    return output


def test_macos_terminal_uses_native_script_with_calling_tty(monkeypatch):
    output = macos_terminal(monkeypatch)
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(terminal.subprocess, "run", run)
    assert terminal.request_maximized_window()
    command, options = calls[0]
    assert command == [
        "/usr/bin/osascript",
        "-l",
        "JavaScript",
        str(terminal.MACOS_TERMINAL_SCRIPT),
        "/dev/ttys123",
    ]
    assert options["timeout"] == 5 and not options.get("shell", False)
    assert output.getvalue() == ""


@pytest.mark.parametrize("failure", ["denied", "timeout"])
def test_native_failure_falls_back_without_preventing_startup(monkeypatch, failure):
    output = macos_terminal(monkeypatch)

    def run(command, **kwargs):
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, 5)
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(terminal.subprocess, "run", run)
    assert terminal.request_maximized_window()
    assert output.getvalue() == terminal.MAXIMIZE_WINDOW


def test_remote_macos_session_does_not_control_server_desktop(monkeypatch):
    output = macos_terminal(monkeypatch)
    monkeypatch.setenv("SSH_TTY", "/dev/ttys999")

    def unexpected(*args, **kwargs):
        pytest.fail("Remote sessions must not invoke native desktop automation")

    monkeypatch.setattr(terminal.subprocess, "run", unexpected)
    assert terminal.request_maximized_window()
    assert output.getvalue() == terminal.MAXIMIZE_WINDOW


def evaluate_native_helpers(body):
    source = terminal.MACOS_TERMINAL_SCRIPT.read_text()
    helpers, separator, _ = source.partition("function run(argv)")
    assert separator  # The native UI entry point is excluded from these pure helper tests.
    result = subprocess.run(
        ["/usr/bin/osascript", "-l", "JavaScript", "-"],
        input=helpers + "\nfunction run() { " + body + " }",
        capture_output=True,
        text=True,
        check=True,
        timeout=5,
    )
    return json.loads(result.stdout)


@pytest.mark.skipif(sys.platform != "darwin", reason="Requires macOS JXA; tests pure geometry only")
def test_native_screen_geometry_respects_dock_menu_bar_and_secondary_displays():
    actual = evaluate_native_helpers("""
        const screens = [
            {frame: {x: 0, y: 0, width: 1440, height: 900},
             visible: {x: 0, y: 40, width: 1440, height: 837}},
            {frame: {x: 1440, y: 0, width: 1920, height: 1080},
             visible: {x: 1440, y: 0, width: 1920, height: 1056}},
            {frame: {x: -1280, y: 0, width: 1280, height: 1024},
             visible: {x: -1280, y: 0, width: 1280, height: 1000}}
        ];
        return JSON.stringify([
            maximizedBounds({x: 100, y: 100, width: 800, height: 500}, screens),
            maximizedBounds({x: 1350, y: 0, width: 500, height: 500}, screens),
            maximizedBounds({x: -1000, y: 0, width: 900, height: 600}, screens)
        ]);
    """)
    assert actual == [
        {"x": 0, "y": 23, "width": 1440, "height": 837},
        {"x": 1440, "y": -156, "width": 1920, "height": 1056},
        {"x": -1280, "y": -100, "width": 1280, "height": 1000},
    ]


@pytest.mark.skipif(sys.platform != "darwin", reason="Requires macOS JXA; tests mock windows only")
def test_native_window_selection_uses_tty_instead_of_front_window():
    actual = evaluate_native_helpers("""
        const windows = [
            {id: 1, tabs: function() { return [{tty: function() {return '/dev/ttys000';}}]; }},
            {id: 2, tabs: function() { return [{tty: function() {return '/dev/ttys123';}}]; }}
        ];
        let missingRejected = false;
        try { windowForTTY(windows, '/dev/ttys999'); } catch (_) {missingRejected = true;}
        return JSON.stringify({id: windowForTTY(windows, '/dev/ttys123').id,
                               missingRejected: missingRejected});
    """)
    assert actual == {"id": 2, "missingRejected": True}
