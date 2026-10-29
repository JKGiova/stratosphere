"""Compile/install the application EA with a source pin, without terminal credentials."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--terminal", type=Path, help="MT5 installation containing metaeditor64.exe"
    )
    parser.add_argument("--wine", type=Path)
    parser.add_argument("--prefix", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--install", action="store_true")
    args = parser.parse_args()
    mac = sys.platform == "darwin"
    prefix = args.prefix or (
        Path.home() / "Library/Application Support/net.metaquotes.wine.metatrader5" if mac else None
    )
    terminal = args.terminal or (prefix / "drive_c/Program Files/MetaTrader 5" if prefix else None)
    wine = args.wine or (
        Path("/Applications/MetaTrader 5.app/Contents/SharedSupport/wine/bin/wine") if mac else None
    )
    if not terminal or not (terminal / "metaeditor64.exe").is_file():
        parser.error("Provide --terminal with the installed MetaEditor")
    output = (args.output or Path("reports/mt5") / ("bridge-build-" + uuid.uuid4().hex)).resolve()
    output.mkdir(parents=True, mode=0o700, exist_ok=False)
    source = Path(__file__).with_name("StratosphereBridge.mq5").read_bytes()
    digest = hashlib.sha256(source).hexdigest()
    built_source = output / "StratosphereBridge.mq5"
    built_source.write_bytes(source.replace(b"SOURCE_SHA256", digest.encode()))
    log = output / "compile.log"

    # Wine's Z: mapping exposes absolute POSIX paths. Native Windows uses its ordinary paths.
    def native(p):
        return "Z:" + str(p).replace("/", "\\") if os.name != "nt" else str(p)

    command = ([str(wine)] if wine else []) + [
        str(terminal / "metaeditor64.exe"),
        "/compile:" + native(built_source),
        "/log:" + native(log),
    ]
    environment = {
        key: os.environ[key]
        for key in ("HOME", "PATH", "TMPDIR", "LANG", "SystemRoot")
        if key in os.environ
    }
    if prefix:
        environment.update(WINEPREFIX=str(prefix), WINEDEBUG="-all")
    try:
        result = subprocess.run(command, env=environment, capture_output=True, timeout=60)
        (output / "compiler.stdout").write_bytes(result.stdout)
        (output / "compiler.stderr").write_bytes(result.stderr)
    except subprocess.TimeoutExpired as exc:
        raise SystemExit(f"MetaEditor timed out; retain diagnostics at {output}") from exc
    diagnostics = log.read_text(encoding="utf-16") if log.exists() else ""
    binary = output / "StratosphereBridge.ex5"
    if "0 errors, 0 warnings" not in diagnostics or not binary.is_file():
        raise SystemExit(f"Compilation not verified; inspect {log}")
    (output / "build.json").write_text(
        json.dumps(
            {
                "source_sha256": digest,
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "compiler_result": "0 errors, 0 warnings",
                "orders_sent": 0,
            },
            indent=2,
        )
        + "\n"
    )
    if args.install:
        target = terminal / "MQL5/Experts/stratosphere"
        target.mkdir(parents=True, exist_ok=True)
        for item in (built_source, binary):
            shutil.copy2(item, target / item.name)
        bridge = terminal / "MQL5/Files/stratosphere-bridge"
        bridge.mkdir(mode=0o700, exist_ok=True)
        bridge.chmod(0o700)
    print(f"Verified application EA build: {output}; source identifier {digest}")
    if args.install:
        print(
            "Installed. Attach the EA manually; EnableTrading and AllowRealAccount default false."
        )


if __name__ == "__main__":
    main()
