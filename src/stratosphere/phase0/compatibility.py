"""Report observed runtime capabilities without substituting an import for a backtest."""

import argparse
import importlib
import importlib.metadata
import json
import platform
import sys
from pathlib import Path

PINNED = {
    "textual": "8.2.8",
    "langgraph": "1.2.15",
    "pydantic": "2.14.1",
    "httpx": "0.28.1",
    "nautilus_trader": "1.231.0",
}


def engine_wheel_support(
    system: str, machine: str, os_version: str = "", libc: str = "", libc_version: str = ""
) -> tuple[bool, str]:
    """Selected wheel targets; availability does not establish runtime or artifact provenance."""
    if system == "Darwin":
        try:
            major = int(os_version.split(".")[0])
        except ValueError:
            return False, "macOS version could not be determined."
        if machine == "x86_64":
            if major < 13:
                return False, "The external Intel wheel targets macOS 13 or newer."
            return True, (
                "External nautilus-trader-macos-intel wheel targets macOS 13+ x86_64; "
                "runtime acceptance and installed artifact provenance are separate checks."
            )
        if machine != "arm64":
            return False, "No selected macOS wheel for this architecture."
        if major < 26:
            return False, "The pinned macOS wheel requires macOS 26 or newer."
        return True, "Published macOS 26+ ARM64 wheel."
    if system == "Linux":
        if machine not in {"x86_64", "aarch64", "arm64"}:
            return False, "No selected Linux wheel for this architecture."
        try:
            version = tuple(int(part) for part in libc_version.split(".")[:2])
        except ValueError:
            version = ()
        if libc != "glibc" or len(version) < 2 or version < (2, 35):
            return False, "The pinned Linux wheel requires glibc 2.35 or newer."
        return True, "Published Linux glibc 2.35+ wheel."
    return False, "This phase verifies Linux/macOS only; other operating systems are deferred."


def inspect_runtime(require_engine: bool = False) -> dict:
    system, machine = platform.system(), platform.machine()
    libc, libc_version = platform.libc_ver()
    supported, reason = engine_wheel_support(
        system,
        machine,
        platform.mac_ver()[0],
        libc,
        libc_version,
    )
    packages: dict[str, dict] = {}
    errors = []
    if sys.version_info[:2] != (3, 14):
        errors.append("The selected Python minor version is 3.14.")
    for name, expected in PINNED.items():
        try:
            observed = importlib.metadata.version(name)
            importlib.import_module(name)
            packages[name] = {"version": observed, "imported": True}
            if observed != expected:
                errors.append(f"{name} does not match its pinned version.")
        except importlib.metadata.PackageNotFoundError:
            packages[name] = {"version": None, "imported": False}
            if name != "nautilus_trader" or require_engine:
                errors.append(f"{name} is not installed.")
        except ImportError:
            packages[name] = {"version": None, "imported": False}
            errors.append(f"{name} could not be imported.")
    engine_imported = packages["nautilus_trader"]["imported"]
    if require_engine and not supported and not engine_imported:
        errors.append(reason)
    return {
        "report_schema": "compatibility/1",
        "python": platform.python_version(),
        "system": system,
        "architecture": machine,
        "macos": platform.mac_ver()[0] or None,
        "libc": libc or None,
        "libc_version": libc_version or None,
        "packages": packages,
        "engine_wheel_available": supported,
        "engine_wheel_reason": reason,
        "engine_runtime_imported": engine_imported,
        "engine_distribution_kind": (
            "external_intel_release_candidate"
            if supported and system == "Darwin" and machine == "x86_64"
            else "published_platform"
            if supported
            else "custom_or_source_build"
            if engine_imported
            else "unavailable"
        ),
        "engine_required": require_engine,
        "errors": errors,
        "scope": "Version/import proof only. Strategy/data behavior is a separate phase 0 gate.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect phase 0 runtime compatibility")
    parser.add_argument("--require-engine", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = inspect_runtime(args.require_engine)
    text = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    print(text, end="")
    raise SystemExit(bool(report["errors"]))


if __name__ == "__main__":
    main()
