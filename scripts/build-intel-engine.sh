#!/usr/bin/env bash
# Build the pinned upstream engine in an isolated checkout; no project environment mutation.
set -euo pipefail

intel_root=${1:?Usage: build-intel-engine.sh ABSOLUTE_BUILD_DIR PYTHON_EXECUTABLE}
intel_python=${2:?Python 3.14.7 executable required}
intel_commit=27a8e54e7ac3c57d6cbf8891f0283dfbaee97317

[[ "$intel_root" = /* ]] || { printf '%s\n' 'Use an absolute build directory'; exit 1; }
[[ "$(uname -s)" = Darwin && "$(uname -m)" = x86_64 ]] || {
  printf '%s\n' 'Native Intel macOS required'; exit 1;
}
[[ "$(uv --version)" =~ ^uv\ 0\.11\.33($|[[:space:]]) ]] || {
  printf '%s\n' 'Upstream requires uv 0.11.33'; exit 1;
}
[[ "$("$intel_python" -c 'import platform; print(platform.python_version())')" = '3.14.7' ]] || {
  printf '%s\n' 'Python 3.14.7 required'; exit 1;
}
mkdir -p "$intel_root"
if [[ ! -d "$intel_root/source" ]]; then
  git clone --branch v1.231.0 --depth 1 \
    https://github.com/nautechsystems/nautilus_trader.git "$intel_root/source"
fi
[[ "$(git -C "$intel_root/source" rev-parse HEAD)" = "$intel_commit" ]] || {
  printf '%s\n' 'Unexpected upstream revision'; exit 1;
}
git -C "$intel_root/source" diff --exit-code
cd "$intel_root/source"
[[ "$(rustc --version)" = 'rustc 1.97.1 '* ]] || {
  printf '%s\n' 'Pinned Rust 1.97.1 required'; exit 1;
}
uv venv --python "$intel_python" "$intel_root/build-python"
export PYO3_PYTHON="$intel_root/build-python/bin/python"
export MACOSX_DEPLOYMENT_TARGET=26.0
export ARCHFLAGS='-arch x86_64'
export HIGH_PRECISION=true
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4}
uv build --wheel --python "$PYO3_PYTHON" --out-dir "$intel_root/wheels"
git diff --exit-code
cp LICENSE "$intel_root/wheels/UPSTREAM-LICENSE"
