import io
import subprocess

from stratosphere import tui


def test_launch_leaves_window_size_to_the_user(monkeypatch):
    output = io.StringIO()
    monkeypatch.setattr("sys.stdout", output)
    monkeypatch.setenv("TERM_PROGRAM", "Apple_Terminal")

    def unexpected(*args, **kwargs):
        raise AssertionError("Startup must not launch desktop automation")

    monkeypatch.setattr(subprocess, "run", unexpected)
    runs = []

    class App:
        def run(self, *, inline):
            runs.append(inline)

    monkeypatch.setattr(tui, "StratosphereApp", App)
    tui.main()
    tui.main()
    assert runs == [False, False]
    assert output.getvalue() == ""
