import pytest

from stratosphere.phase0.compatibility import engine_wheel_support, inspect_runtime


@pytest.mark.parametrize(
    "system,machine,version,libc,libc_version,expected",
    [
        ("Darwin", "arm64", "26.5", "", "", True),
        ("Darwin", "arm64", "15.6", "", "", False),
        ("Darwin", "x86_64", "26.5", "", "", False),
        ("Darwin", "arm64", "unknown", "", "", False),
        ("Linux", "x86_64", "", "glibc", "2.35", True),
        ("Linux", "aarch64", "", "glibc", "2.39", True),
        ("Linux", "x86_64", "", "glibc", "2.34", False),
        ("Linux", "x86_64", "", "musl", "1.2", False),
        ("Linux", "armv7l", "", "glibc", "2.39", False),
        ("Windows", "AMD64", "", "", "", False),
    ],
)
def test_wheel_boundaries_are_explicit(system, machine, version, libc, libc_version, expected):
    supported, reason = engine_wheel_support(system, machine, version, libc, libc_version)
    assert supported is expected
    assert reason


def test_installed_core_matches_lock_and_does_not_claim_backtest_success():
    report = inspect_runtime()
    assert report["errors"] == []
    assert report["packages"]["textual"]["imported"]
    assert "separate" in report["scope"]
