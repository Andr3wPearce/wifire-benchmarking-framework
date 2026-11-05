"""Runs one step block (a source, method, reference or metric) on its step.json:

    python run_block.py <step.json>

The block's code is `<block>.py` (shipped next to this file), and its entry point is
`run(step, log) -> summary` (docs/block_contract.md, "Step contract"). This writes
`provenance.json` (what ran, with which parameters, how long, the summary) into the
step's out_dir, prints the summary, and exits with code 3 ("unavailable: <reason>") if
the block raises Unavailable. Models have their own entry point (`<model>.py <run_dir>`).

Not part of any artifact's identity: it records what happened, it doesn't shape the data.
"""
from __future__ import annotations

import importlib
import json
import sys
import time
from pathlib import Path

from blockkit import UNAVAILABLE_EXIT, Unavailable


def execute(step: dict, log=print) -> dict:
    """Run one block and write its provenance. An Unavailable block still writes provenance
    (with the reason) and re-raises, so the caller can record it as unavailable."""
    t = time.time()
    out = Path(step["out_dir"])
    prov = {"block": step["block"], "version": step.get("version"),
            "params": step.get("params", {}), "selection": step.get("selection", {})}
    block = importlib.import_module(step["block"])
    try:
        summary = block.run(step, log)
    except Unavailable as e:
        out.mkdir(parents=True, exist_ok=True)
        (out / "provenance.json").write_text(json.dumps({**prov, "unavailable": str(e)}, indent=1, default=str))
        raise
    out.mkdir(parents=True, exist_ok=True)
    (out / "provenance.json").write_text(json.dumps(
        {**prov, "seconds": round(time.time() - t, 1), "summary": summary}, indent=1, default=str))
    return summary


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python run_block.py <step.json>")
    try:
        print(json.dumps(execute(json.loads(Path(sys.argv[1]).read_text())), indent=1, default=str))
    except Unavailable as e:
        print(f"unavailable: {e}")
        sys.exit(UNAVAILABLE_EXIT)
