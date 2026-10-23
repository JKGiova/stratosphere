# Intel macOS engine setup

The pinned NautilusTrader 1.231.0 engine needs a custom Intel build; an ARM wheel cannot be renamed
or substituted. Verified build scope is CPython 3.14.7 on macOS 26 Intel, not every historical Mac.

## Build contract

- Source: `27a8e54e7ac3c57d6cbf8891f0283dfbaee97317`, the resolved upstream v1.231.0 tag.
- Target: `x86_64-apple-darwin`, deployment target macOS 26.0, high precision enabled.
- Rust/toolchain/build frontend: those required by the pinned upstream source.
- Build script: [`scripts/build-intel-engine.sh`](../scripts/build-intel-engine.sh).
- Artifact inspector: [`scripts/inspect_intel_wheel.py`](../scripts/inspect_intel_wheel.py).

The isolated build retains source/lock/compiler provenance, license and wheel digest, and checks
native Mach-O payloads. Verify the wheel against its own build manifest. Different valid compiler
builds need not produce byte-identical wheels; the manifest is not upstream signed attestation.

A prior [Intel CI run](https://github.com/JKGiova/stratosphere/actions/runs/38598597381) verified this
bounded engine build. It does not certify later application revisions or every macOS/Python version.
Each engine upgrade needs a new build and behavioral/native-accounting verification.

## Install a verified wheel

Use the exact wheel from a verified local/CI build and check its SHA-256 against that build's
provenance before installation. Example paths below assume that artifact is in `reports/intel/`.

```sh
uv export --quiet --locked --extra engine --no-emit-project --no-emit-package nautilus-trader \
  --output-file reports/intel/requirements.txt
uv venv --python 3.14.7 reports/intel/test-env
uv pip sync --python reports/intel/test-env/bin/python --require-hashes reports/intel/requirements.txt
uv pip install --python reports/intel/test-env/bin/python --no-deps \
  reports/intel/nautilus_trader-1.231.0-cp314-cp314-macosx_26_0_x86_64.whl
uv pip install --python reports/intel/test-env/bin/python --no-deps --editable .
uv pip check --python reports/intel/test-env/bin/python
reports/intel/test-env/bin/python -m stratosphere.phase0.compatibility --require-engine
STRATOSPHERE_REQUIRE_ENGINE=1 reports/intel/test-env/bin/pytest
```

The normal upstream-wheel `uv sync --extra engine` path does not provide this custom Intel artifact.
This is a development installation path, not a released customer installer or general offline mode.
