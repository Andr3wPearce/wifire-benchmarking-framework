"""TEMP(api-stand-in): submit a run by dropping a file; replaced by POST /runs on the API.

    python -m bench.submit hughes_ignition_comparison
    python -m bench.submit hughes_ignition_comparison --set farsite.spread_adjustment=1.0

Writes a small YAML to submissions/; the Dagster sensor picks it up, resolves
it into runs (partitions) and starts whatever isn't already materialized.
"""
from __future__ import annotations

import argparse
import getpass
from datetime import datetime

import yaml

from .overrides import apply_overrides
from .registry import BLOCKS, ROOT
from .resolver import resolve


def _value(s: str):
    return yaml.safe_load(s)       # "1.0" -> 1.0, "[1.0, 2.0]" -> list (a sweep), "true" -> True


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("workflow", help="workflow name (a file in workflows/)")
    ap.add_argument("--set", action="append", default=[], metavar="BLOCK.PARAM=VALUE",
                    help="override a block param for this run; a list value becomes a sweep")
    ap.add_argument("--note", default="", help="why this run (kept in provenance)")
    a = ap.parse_args(argv)

    if not (ROOT / "workflows" / f"{a.workflow}.yaml").exists():
        ap.error(f"no workflow '{a.workflow}' in workflows/")
    overrides: dict = {}
    for item in a.set:
        lhs, _, rhs = item.partition("=")
        block, _, param = lhs.partition(".")
        if block not in BLOCKS or param not in BLOCKS[block].get("params", {}):
            ap.error(f"unknown block param '{lhs}'")
        overrides.setdefault(block, {})[param] = _value(rhs)

    # validate the way the API would (reject before anything is queued)
    spec = apply_overrides(yaml.safe_load((ROOT / "workflows" / f"{a.workflow}.yaml").read_text()), overrides)
    try:
        plan = resolve(spec)
    except ValueError as e:
        ap.error(f"rejected: {e}")
    for w in plan["warnings"]:
        print(f"warning: {w}")

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    out = ROOT / "submissions" / f"{stamp}_{a.workflow}.yaml"
    out.parent.mkdir(exist_ok=True)
    out.write_text(yaml.safe_dump({"workflow": a.workflow, "overrides": overrides,
                                   "submitted_by": getpass.getuser(), "note": a.note}, sort_keys=False))
    print(f"submitted {out.name}: {len(plan['runs'])} runs, {len(plan['skipped'])} skipped")


if __name__ == "__main__":
    main()
