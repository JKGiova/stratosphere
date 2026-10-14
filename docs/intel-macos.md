# Native Intel macOS engine proof

Status: build and verification in progress. The upstream release has no published Intel macOS wheel. Do not describe this source-build attempt as verified support until the native tests and CI pass. The previously completed Phase 0 audit retains its original verified platform scope.

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
