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
uv pip install -q --python "$STATE/venv/bin/python" -e ".[k8s]" pytest
export DAGSTER_HOME="$STATE/dagster_home" PATH="$STATE/venv/bin:$PATH"
PORT="${BENCH_PORT:-3070}"          # not 3000: lots of other dev tools use that
if curl -s -o /dev/null "http://localhost:$PORT"; then
  echo "!! port $PORT is already in use (another app, or Dagster already running)."
  echo "   close it, or run with a different port: BENCH_PORT=3071 bash scripts/run_dagster.sh"
  exit 1
fi
URL="http://localhost:$PORT"
mkdir -p artifacts

# ---- where real blocks run: minikube if it's up, otherwise say how to get it ----
export BENCH_BACKEND="${BENCH_BACKEND:-k8s}"
if [ "$BENCH_BACKEND" = k8s ]; then
  if command -v minikube >/dev/null && minikube status >/dev/null 2>&1; then
    if ! minikube image ls 2>/dev/null | grep -q "bench/elmfire:0.1"; then
      echo "== ELMFIRE image not in minikube yet, building it"
      bash scripts/build_blocks.sh elmfire
    fi
    # make repo/artifacts visible inside minikube at /bench-artifacts (pods mount that)
    echo "== mounting $REPO/artifacts into minikube at /bench-artifacts"
    minikube mount "$REPO/artifacts:/bench-artifacts" > "$STATE/minikube-mount.log" 2>&1 &
    MOUNT_PID=$!
    trap 'kill $MOUNT_PID 2>/dev/null' EXIT
    sleep 3
  else
    echo "!! minikube isn't running, so real blocks (ELMFIRE) will fail. Start it with 'minikube start'"
    echo "   and relaunch, or run blocks without k8s: BENCH_BACKEND=local (see README)."
  fi
fi
(
  until curl -s "$URL/server_info" | grep -q dagster; do sleep 2; done
  if ! ls submissions/*.yaml >/dev/null 2>&1; then
    python -m bench.submit hughes_ignition_comparison --note "first dry run"
  fi
  if command -v open >/dev/null && [ "$(uname)" = Darwin ]; then open "$URL"
  else powershell.exe -NoProfile -Command "Start-Process $URL" >/dev/null 2>&1 || true; fi
  echo "== Dagster UI: $URL   (submit more runs: scripts/submit.sh --set farsite.spread_adjustment=1.0)"
) &
echo "== starting Dagster (leave this window open; close it to stop)"
dagster dev -m bench.definitions -h "$( grep -qi microsoft /proc/version 2>/dev/null && echo 0.0.0.0 || echo 127.0.0.1 )" -p "$PORT"
