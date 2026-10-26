# Intel macOS engine dependency

stratosphere consumes the Intel wheel from the separate
[nautilus-trader-macos-intel project](https://github.com/JKGiova/nautilus-trader-macos-intel).
Engine compilation, binary repair/audits, corresponding source bundles and wheel releases belong
there. This repository owns application compatibility and backtest acceptance; it does not build
or vendor the engine wheel. Existing local build reports remain historical evidence.

## Pinned artifact

- Release: `v1.231.0-intel.1`, engine `1.231.0`, standard CPython 3.14.
- Wheel: `nautilus_trader-1.231.0-cp314-cp314-macosx_13_0_x86_64.whl`.
- SHA-256: `8cfdced597373a1c050bc85d0bef8fcc005049e8a1cb10d4c11fc1159f5ee8bd`.
- Upstream engine commit: `27a8e54e7ac3c57d6cbf8891f0283dfbaee97317`.
- Runtime constraints: the release's `runtime-cp314.txt`, SHA-256
  `36ef36e4c935094a07112ce26e2563b711026c4be13ba7628934009cac51cf9a`.

`pyproject.toml` selects this URL only for Intel macOS and carries the matching runtime constraints
for that platform. `uv.lock` retains the resolved dependency artifacts/hashes. Linux and Apple Silicon
continue to use upstream PyPI wheels. No moving `latest` release, sibling checkout or source-build
fallback is used. uv source selection applies to uv project commands; a generic pip installation of
the project does not interpret `tool.uv.sources`.

## Install and verify

Install from the published external release:

```sh
uv sync --locked --extra engine --no-build-package nautilus-trader
uv pip check --python .venv/bin/python
.venv/bin/python -m stratosphere.phase0.compatibility --require-engine
STRATOSPHERE_REQUIRE_ENGINE=1 .venv/bin/pytest -m engine
.venv/bin/python -m stratosphere
```

The external release was locally validated on Intel macOS 26.5.1 with Python 3.14.7. Its macOS 13+
binary deployment target is not runtime verification on macOS 13/14/15. stratosphere's Intel CI
runs on macOS 26 with the consumed wheel; a version/import check alone does not prove the installed
artifact's origin, full numerical behavior or older-OS compatibility.

If the release is draft/private or missing, installation must fail rather than compile an engine or
substitute another artifact. A local wheel may be inspected separately, but it must not be committed
or replace the published URL in the shared lockfile. Release publication and compatibility claims
remain the responsibility of the separate wheel project.

## Updates

Review the external release's wheel digest, upstream version/commit, runtime constraints and actual
validation scope. Update the platform source and constraints, regenerate `uv.lock`, and run native
application regressions with the new artifact. Preserve old research reports and their executable
pins; upgrading a dependency never reinterprets historical results or grants trading authority.
