"""Headless startup must work without UI packages or operational side effects."""

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "args,expected",
    [
        ([], "compatibility"),
        (["--help"], "mt5"),
        (["mt5", "--help"], "deployment"),
        (["compatibility", "--help"], "--require-engine"),
    ],
)
def test_help_works_without_ui_packages_or_creating_state(tmp_path, args, expected):
    # Block UI imports even when they are still installed in a developer's environment.
    script = """
import sys
class NoUI:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'textual', 'rich'}:
            raise AssertionError('Headless startup imported a UI dependency')
sys.meta_path.insert(0, NoUI())
from stratosphere.__main__ import main
main(sys.argv[1:])
"""
    state = tmp_path / "research"
    result = subprocess.run(
        [sys.executable, "-c", script, *args],
        env=os.environ | {"STRATOSPHERE_RESEARCH_DIR": str(state)},
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert expected in result.stdout
    assert not state.exists()


def test_unknown_command_fails_without_creating_state(tmp_path):
    state = tmp_path / "research"
    result = subprocess.run(
        [sys.executable, "-m", "stratosphere", "unknown"],
        env=os.environ | {"STRATOSPHERE_RESEARCH_DIR": str(state)},
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 2
    assert "invalid choice" in result.stderr
    assert not state.exists()
