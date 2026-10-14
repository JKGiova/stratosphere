# Native Intel macOS engine proof

Status: native build and local verification passed; independent Intel CI build is still running. The upstream release has no published Intel macOS wheel. The previously completed Phase 0 audit retains its original scope; this extension verifies a custom native distribution separately.

## Selected build

- Upstream NautilusTrader version: 1.231.0.
- Source commit: 27a8e54e7ac3c57d6cbf8891f0283dfbaee97317, resolved from the annotated v1.231.0 tag.
- Native target: x86_64-apple-darwin, Python 3.14.7, macOS deployment target 26.0.
- Rust: 1.97.1, as selected by the source rust-toolchain.toml.
- Build frontend: uv 0.11.33, as required by the upstream pyproject.toml; backend/build requirements come from that source. stratosphere retains its separate uv 0.13.0 workflow and lock.
- High-precision build enabled. Four Rust build jobs by default; no strategy/application behavior changes.

The build takes place in an isolated checkout and produces a wheel. It does not replace the normal project environment, modify global shell configuration, install a trading connector or execute an order. No model key is required.

## Build and verification workflow

scripts/build-intel-engine.sh checks native host/tool versions and exact upstream revision, creates a separate Python environment, compiles the release wheel and rejects tracked upstream modifications. Rust/Cython source code is compiled rather than converting an ARM binary. The upstream license is retained alongside the artifact.

scripts/inspect_intel_wheel.py checks the distribution/version/platform tag and each native Mach-O header, rejecting an ARM payload even if its filename claims Intel. Its manifest retains the wheel checksum, source commit, Cargo/Python source lock digests and compiler identities. This is a local build manifest, not an upstream signed release attestation or a claim of bit-for-bit reproducible compiler output.

The intel-engine GitHub workflow builds on macos-26-intel, then creates a clean stratosphere environment from uv.lock with verified dependency hashes. It substitutes only the freshly built engine package, validates installed dependency requirements, runs all core/native tests, obtains the online ECB proof and saves a native experiment report. The artifact includes the wheel, upstream license and provenance, plus test reports. Normal installation/distribution to customers remains future packaging work.

The compatibility inspector keeps engine_wheel_available false on Intel; a successfully imported custom build is reported separately from published-wheel capability. Import checks do not replace the native engine behavior gate. Native tests verify exact high precision, independent signal/fill/cost/balance conformance, repeatability and retained open exposure. The native deterministic experiment payload must be compared with the same code/lock on a verified Linux host.

## Limits

Initial support, if proven, applies to macOS 26 Intel with CPython 3.14.7 and this custom build. Earlier macOS versions and other Python versions need separate builds and tests. Each upstream version must be rebuilt/reverified; an Intel wheel does not become an upstream-supported artifact merely because stratosphere's tests pass. Licensing notices for the third-party engine remain attached; the private stratosphere repository's visibility/licensing is unchanged.

## Local evidence

The unmodified upstream source built successfully on macOS 26.5.1 x86_64 with the selected toolchain. The resulting wheel is nautilus_trader-1.231.0-cp314-cp314-macosx_26_0_x86_64.whl; all 111 native binaries have verified Intel Mach-O headers. Local wheel SHA-256: `2b398e05352d33b2e57cd7c9af07ed0ac48fb8acb387b2c278f0e567152c1892`.

The clean environment installed all other packages from the existing uv.lock with hash verification. Dependency validation passed. The engine imported from that environment's site-packages, and **77 tests passed, with zero skips** (74 core, three native-engine tests). The existing upstream Timestamp.utcnow deprecation warning remains visible, as in the prior proof.

The complete deterministic engine payload matches the previously verified Linux payload exactly: `c8a33f029794cdd19a0ffddd5671e70c15fc92a0fdb37f3dd404566945add799`. The actual online ECB acquisition also matches the pinned observation digest. These are the same bounded compatibility/conformance assumptions from phase0.md, not tradable-data or profitability validation.

All 111 binaries' dynamic load commands reference only macOS system libraries/frameworks; no original source or Homebrew dependency is loaded. The Rust dylib retains its build-path self-install name, which is metadata rather than a load dependency. To verify this distinction in practice, the source checkout was temporarily moved away, and all three engine tests passed again in a fresh process. The original source location was then restored.

Local wheel, upstream license, build log, provenance, compatibility JSON, test XML and comparison/engine reports are retained in ignored reports/intel/. The normal .venv and global shell/tool configuration were not replaced. Independent CI evidence must be recorded below after that job completes.

## Install a finished wheel without a compiler

From the repository checkout, with uv 0.13.0 and Python 3.14.7 available, create a separate test environment. Use the exact wheel from a verified build and check its SHA-256 against that build's provenance before installation. CI/local compiler builds need not produce byte-identical archives, so their checksums are recorded separately.

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

The plain `uv sync --extra engine` path still cannot obtain an upstream Intel wheel. It is not the custom-wheel installation path and should not be represented as supported by PyPI. These commands are a development proof setup, not a customer installer or an offline product mode.
