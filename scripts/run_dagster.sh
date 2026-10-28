#!/usr/bin/env bash
# Runs the Dagster UI for this repo (macOS, Linux, or WSL on Windows). No sudo needed.
# Code stays in the repo; the venv and Dagster's state live in ~/.bench-framework.
set -e
REPO="$(cd "$(dirname "$0")/.." && pwd)"
STATE="$HOME/.bench-framework"
mkdir -p "$STATE/dagster_home"
cd "$REPO"
export PATH="$HOME/.local/bin:$PATH"
if ! command -v uv >/dev/null; then
  echo "== installing uv (python package manager)"
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
if [ ! -x "$STATE/venv/bin/dagster" ]; then
  echo "== creating venv + installing Dagster (a few minutes the first time)"
  uv venv --python 3.12 "$STATE/venv"
fi
uv pip install -q --python "$STATE/venv/bin/python" -e . pytest
export DAGSTER_HOME="$STATE/dagster_home" PATH="$STATE/venv/bin:$PATH"
(
  until curl -s -o /dev/null http://localhost:3000/server_info; do sleep 2; done
  if ! ls submissions/*.yaml >/dev/null 2>&1; then
    python -m bench.submit hughes_ignition_comparison --note "first dry run"
  fi
  if command -v open >/dev/null && [ "$(uname)" = Darwin ]; then open http://localhost:3000
  else powershell.exe -NoProfile -Command "Start-Process http://localhost:3000" >/dev/null 2>&1 || true; fi
  echo "== Dagster UI: http://localhost:3000   (submit more runs: scripts/submit.sh --set farsite.spread_adjustment=1.0)"
) &
echo "== starting Dagster (leave this window open; close it to stop)"
exec dagster dev -m bench.definitions -h "$( grep -qi microsoft /proc/version 2>/dev/null && echo 0.0.0.0 || echo 127.0.0.1 )" -p 3000
