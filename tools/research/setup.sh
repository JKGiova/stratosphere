#!/bin/sh
# Optional, isolated public readers for a source checkout. No login/setup hooks.
set -eu
cd "$(dirname "$0")/../.."
command -v uv >/dev/null
command -v npm >/dev/null
npm ci --prefix tools/research --ignore-scripts --no-audit --no-fund
uv venv --python 3.14 --allow-existing .tools/agent-reach
uv pip sync --python .tools/agent-reach/bin/python tools/research/agent-reach-requirements.txt
