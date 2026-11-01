"""fixed_moisture: one fuel-moisture scenario, constant in space and time (moisture slot).

Writes the parameters (fractions) as they are; classes it doesn't set get the model
adapter's defaults, which the adapter reports.
"""
from __future__ import annotations

import json
from pathlib import Path


def run(step: dict, log) -> dict:
    out = Path(step["out_dir"])
    out.mkdir(parents=True, exist_ok=True)
    (out / "fuel_moisture.json").write_text(json.dumps(step["params"]))
    return dict(step["params"])
