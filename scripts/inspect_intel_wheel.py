"""Verify the native wheel architecture and retain build provenance."""

import argparse
import hashlib
import json
import struct
import subprocess
import zipfile
from email.parser import Parser
from pathlib import Path

EXPECTED_COMMIT = "27a8e54e7ac3c57d6cbf8891f0283dfbaee97317"
EXPECTED_TAG = "cp314-cp314-macosx_26_0_x86_64"


def inspect_wheel(path: Path) -> dict:
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        wheel_metadata = [name for name in names if name.endswith(".dist-info/WHEEL")]
        package_metadata = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(wheel_metadata) != 1 or len(package_metadata) != 1:
            raise ValueError("Exactly one distribution metadata directory required")
        metadata = Parser().parsestr(archive.read(package_metadata[0]).decode())
        tags = Parser().parsestr(archive.read(wheel_metadata[0]).decode()).get_all("Tag")
        if metadata["Name"].replace("_", "-") != "nautilus-trader":
            raise ValueError("Unexpected package")
        if metadata["Version"] != "1.231.0" or tags != [EXPECTED_TAG]:
            raise ValueError("Unexpected engine version or platform tag")
        binaries = [name for name in names if name.endswith((".so", ".dylib"))]
        if not binaries:
            raise ValueError("Wheel contains no native engine binaries")
        for name in binaries:
            with archive.open(name) as file:
                header = file.read(8)
            if len(header) != 8 or header[:4] != b"\xcf\xfa\xed\xfe":
                raise ValueError(f"Expected thin 64-bit Mach-O binary: {name}")
            if struct.unpack("<I", header[4:])[0] != 0x01000007:
                raise ValueError(f"Expected Intel x86_64 binary: {name}")
        with path.open("rb") as file:
            digest = hashlib.file_digest(file, "sha256").hexdigest()
        return {
            "wheel_filename": path.name,
            "wheel_sha256": digest,
            "tag": EXPECTED_TAG,
            "native_binaries": len(binaries),
            "all_native_binaries_x86_64": True,
            "engine_version": metadata["Version"],
        }


def output(command: list[str], cwd: Path | None = None) -> str:
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    commit = output(["git", "rev-parse", "HEAD"], cwd=args.source)
    if commit != EXPECTED_COMMIT:
        raise ValueError("Unexpected upstream source revision")
    subprocess.run(["git", "diff", "--exit-code"], cwd=args.source, check=True)
    report = {
        "schema_version": "intel-wheel-build/1",
        **inspect_wheel(args.wheel),
        "upstream_commit": commit,
        "source_modified": False,
        "rustc": output(["rustc", "--version"], cwd=args.source),
        "cargo": output(["cargo", "--version"], cwd=args.source),
        "uv": output(["uv", "--version"], cwd=args.source),
        "clang": output(["clang", "--version"]),
        "rust_lock_sha256": hashlib.sha256((args.source / "Cargo.lock").read_bytes()).hexdigest(),
        "python_build_lock_sha256": hashlib.sha256(
            (args.source / "uv.lock").read_bytes()
        ).hexdigest(),
        "precision": "high-precision",
        "deployment_target": "26.0",
        "provenance_kind": "local_build_manifest_not_a_signed_upstream_attestation",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
