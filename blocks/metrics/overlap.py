"""Overlap metrics: compare the predicted and observed burned areas cell by cell.

Each metric here is a pure function `MatchedPair -> list[ScoreRow]`
(docs/metrics_contract.md), scored at every observation's time over the valid cells
only. They use burned cells alone, never unburned ones, so they don't depend on how
large the domain is (the domain buffer is our choice, not the fire's).

To add a metric: write a scorer like `_iou` (pair, observation, predicted, observed ->
value, diagnostics; raise Undefined when the math has no answer), wrap it with
`score_each_observation`, and add a test file like tests/test_overlap.py.
"""
from __future__ import annotations

from metric_base import Undefined, cells, score_each_observation
from metric_types import MatchedPair


# --------------------------------------------------------------------------- #
# iou
# --------------------------------------------------------------------------- #
IOU_VERSION = "1"        # bump whenever results for the same inputs could change


def _iou(pair, obs, predicted, observed):
    """IoU = |P ∩ O| / |P ∪ O|. Range [0, 1], 1 = identical burned areas (Jaccard index)."""
    intersection, union = cells(predicted & observed), cells(predicted | observed)
    if union == 0:
        raise Undefined("neither the prediction nor the observation burned any valid cell")
    return intersection / union, {"intersection_cells": intersection, "union_cells": union,
                                  "predicted_cells": cells(predicted), "observed_cells": cells(observed)}


def iou(pair: MatchedPair) -> list:
    """Intersection over union of the predicted and observed burned areas, per observation."""
    return score_each_observation(pair, metric="iou", version=IOU_VERSION, unit="1",
                                  better="higher", scorer=_iou)
