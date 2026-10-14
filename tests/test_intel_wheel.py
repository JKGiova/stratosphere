import hashlib
import runpy
import struct
import zipfile
from pathlib import Path

import pytest

inspect_wheel = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "scripts/inspect_intel_wheel.py")
)["inspect_wheel"]


def wheel(tmp_path, *, cpu=0x01000007, tag="cp314-cp314-macosx_26_0_x86_64", binary=True):
    path = tmp_path / "fixture.whl"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "nautilus_trader-1.231.0.dist-info/METADATA",
            "Name: nautilus_trader\nVersion: 1.231.0\n",
        )
        archive.writestr("nautilus_trader-1.231.0.dist-info/WHEEL", f"Tag: {tag}\n")
        if binary:
            archive.writestr(
                "nautilus_trader/core/example.so", b"\xcf\xfa\xed\xfe" + struct.pack("<I", cpu)
            )
    return path


def test_inspector_checks_native_header_and_digest(tmp_path):
    path = wheel(tmp_path)
    result = inspect_wheel(path)
    assert result["native_binaries"] == 1
    assert result["all_native_binaries_x86_64"]
    assert result["wheel_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    "options",
    [
        {"cpu": 0x0100000C},
        {"tag": "cp314-cp314-macosx_26_0_arm64"},
        {"binary": False},
    ],
)
def test_arm_binary_wrong_tag_and_empty_binary_wheels_are_rejected(tmp_path, options):
    with pytest.raises(ValueError):
        inspect_wheel(wheel(tmp_path, **options))
