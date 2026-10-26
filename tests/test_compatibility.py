import pytest

from stratosphere.phase0 import compatibility
from stratosphere.phase0.compatibility import engine_wheel_support, inspect_runtime


@pytest.mark.parametrize(
    "system,machine,version,libc,libc_version,expected",
    [
        ("Darwin", "arm64", "26.5", "", "", True),
        ("Darwin", "arm64", "15.6", "", "", False),
        ("Darwin", "x86_64", "26.5", "", "", True),
        ("Darwin", "x86_64", "13.0", "", "", True),
        ("Darwin", "x86_64", "12.7", "", "", False),
        ("Darwin", "x86_64", "unknown", "", "", False),
        ("Darwin", "unknown", "26.5", "", "", False),
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


def test_intel_candidate_does_not_attest_installed_artifact_or_upstream_support(monkeypatch):
    monkeypatch.setattr(compatibility.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(compatibility.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(compatibility.platform, "mac_ver", lambda: ("26.5", (), ""))
    monkeypatch.setattr(compatibility.importlib, "import_module", lambda _: object())
    monkeypatch.setattr(
        compatibility.importlib.metadata, "version", compatibility.PINNED.__getitem__
    )
    report = inspect_runtime(require_engine=True)
    assert report["errors"] == []
    assert report["engine_wheel_available"]
    assert report["engine_runtime_imported"]
    assert report["engine_distribution_kind"] == "external_intel_release_candidate"
    assert "provenance are separate" in report["engine_wheel_reason"]
