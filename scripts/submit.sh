#!/usr/bin/env bash
# Submit a run (API stand-in). Usage: scripts/submit.sh [--set block.param=value ...] [--note "..."]
REPO="$(cd "$(dirname "$0")/.." && pwd)"; cd "$REPO"
WF="${WF:-hughes_ignition_comparison}"
exec "$HOME/.bench-framework/venv/bin/python" -m bench.submit "$WF" "$@"
