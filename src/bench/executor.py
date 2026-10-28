"""Generic block executor: the only place a block actually runs.

Dry run: log what would run and write the provenance record it would write.
Going live: replace the body of `run_block` with Dagster Pipes launching the
block's image as a Kubernetes Job on NRP, e.g.

    PipesK8sClient().run(context=context, image=payload["image"],
                         base_pod_spec={...resources from the manifest...})

Every block of every kind goes through this one function, so adding a block
never means writing orchestration code.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from .registry import ROOT

ARTIFACT_ROOT = Path(os.environ.get("BENCH_ARTIFACT_ROOT", ROOT / "artifacts"))


def run_block(context, workflow: str, step: str, payload: dict, inputs: list[str] | None = None) -> dict:
    """Pretend to run one block. Returns metadata for the Dagster materialization."""
    # step ids are dot-separated, except run keys (which can hold "1.0"), passed as runs/<key>/...
    out = ARTIFACT_ROOT / workflow / (step if "/" in step else step.replace(".", "/"))
    record = {
        "step": step,
        "block": payload.get("block") or payload.get("model"),
        "would_run_image": payload.get("image"),
        "params": payload.get("params"),
        "inputs": inputs or [],
        "output": str(out),
        "notes": [n for n in [payload.get("reason"), payload.get("rationale")] if n]
        + payload.get("adapter_notes", []),
        "resources": payload.get("resources"),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": True,
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "provenance.json").write_text(json.dumps(record, indent=2))
    context.log.info(f"DRY RUN {step}: would run {record['would_run_image']} with params {record['params']}")
    return {
        "block": record["block"],
        "image": str(record["would_run_image"]),
        "params": record["params"] or {},
        "artifact": str(out),
        "notes": "; ".join(record["notes"]) or "-",
        "dry_run": True,
    }
