"""What the metrics share: the predicted and observed burned areas on the case grid, at
the reference's observation time, and writing score.json. Part of every metric's identity."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np

from blockkit import Unavailable, read_tif, t0


def burned_masks(step: dict):
    """(predicted burned, observed burned, scored_at_min) on the case grid.

    If the case's reference says when it was observed (`reference.observed_utc`), the
    prediction is taken *at that time* (arrival time <= observed - t0), so a perimeter
    from hour 2 is compared with the model's hour 2, not with everything it burned. A
    simulation that ended before the observation can't be scored: unavailable."""
    pred, ref = Path(step["inputs"]["prediction"]), Path(step["inputs"]["reference"])
    toa = read_tif(pred / "time_of_arrival.tif")
    burned_ref = read_tif(ref / "burned.tif").astype(bool)
    if toa.shape != burned_ref.shape:
        raise ValueError(f"prediction grid {toa.shape} != reference grid {burned_ref.shape}")
    observed = (step["case"].get("reference") or {}).get("observed_utc")
    if not observed:
        return np.isfinite(toa), burned_ref, None
    at = (datetime.fromisoformat(observed.replace("Z", "+00:00")) - t0(step["case"])).total_seconds() / 60
    summary = pred / "summary.json"
    ends = json.loads(summary.read_text()).get("stop_time_min") if summary.exists() else None
    if ends is not None and ends < at - 1:
        raise Unavailable(f"the simulation ends at {ends:.0f} min, the reference is observed at {at:.0f} min "
                          "after t0; give the workflow a longer duration_min")
    return np.isfinite(toa) & (toa <= at), burned_ref, round(at, 1)


def write_score(step: dict, score: dict) -> dict:
    out = Path(step["out_dir"])
    out.mkdir(parents=True, exist_ok=True)
    (out / "score.json").write_text(json.dumps(score, indent=1))
    return score
