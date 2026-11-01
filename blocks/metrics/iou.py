"""iou: intersection over union of the predicted and observed burned areas (metric).

Scored at the reference's observation time (see scoring.burned_masks)."""
from __future__ import annotations

from scoring import burned_masks, write_score


def run(step: dict, log) -> dict:
    a, b, at = burned_masks(step)
    union = int((a | b).sum())
    return write_score(step, {"iou": round(float((a & b).sum()) / union, 4) if union else 0.0,
                              "intersection_cells": int((a & b).sum()), "union_cells": union,
                              "scored_at_min": at if at is not None else "end of simulation"})
