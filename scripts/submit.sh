#!/usr/bin/env bash
# TEMP(api-stand-in): submit a run with overrides/sweeps.
# Usage: scripts/submit.sh                      (lists workflows)
#        scripts/submit.sh <workflow> [--set block.param=value ...] [--note "..."]
REPO="$(cd "$(dirname "$0")/.." && pwd)"; cd "$REPO"
exec "$HOME/.bench-framework/venv/bin/python" -m bench.submit "$@"
