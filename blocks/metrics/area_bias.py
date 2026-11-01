"""area_bias: (predicted area - observed area) / observed area (metric). Negative means the
model burned too little. Scored at the reference's observation time (see scoring.burned_masks)."""
from __future__ import annotations

from scoring import burned_masks, write_score


def run(step: dict, log) -> dict:
    a, b, at = burned_masks(step)
    cell_ha = step["case"]["resolution_m"] ** 2 / 10_000
    pred_ha, ref_ha = float(a.sum()) * cell_ha, float(b.sum()) * cell_ha
    return write_score(step, {"area_bias": round((pred_ha - ref_ha) / ref_ha, 4) if ref_ha else None,
                              "predicted_ha": round(pred_ha, 1), "observed_ha": round(ref_ha, 1),
                              "scored_at_min": at if at is not None else "end of simulation"})
