"""What every metric shares: scoring each observation in turn, with the statuses
the contract defines. A metric writes only its math (see iou.py)."""
from __future__ import annotations

from typing import Callable, Optional

import numpy as np

from metric_types import Better, MatchedPair, Observation, ScoreRow

# an observation later than the simulation's end (by more than this) can't be scored
SIM_END_TOLERANCE_MIN = 1.0


class Undefined(Exception):
    """Raise from a metric's math when the inputs exist but the math has no answer
    (e.g. IoU when neither area burned). Becomes a row with status "undefined"."""


# (pair, observation, predicted burned, observed burned) -> (value, diagnostics)
Scorer = Callable[[MatchedPair, Observation, np.ndarray, np.ndarray], "tuple[float, dict]"]


def score_each_observation(pair: MatchedPair, *, metric: str, version: str, unit: str,
                           better: Better, scorer: Scorer,
                           extra_flags: tuple = ()) -> list:
    """One ScoreRow per observation, in order.

    * an observation after the simulation ended -> "unavailable", with both times
    * the scorer raises Undefined -> "undefined", with its reason
    * otherwise "ok", with the scorer's value and diagnostics

    Any other exception is a bug and propagates."""
    rows = []
    for obs in pair.observations:
        common = dict(metric=metric, metric_version=version, unit=unit, better=better,
                      scored_at_min=float(obs.time_min), observation_id=obs.id,
                      observation_source=obs.source, prediction_id=pair.prediction_id,
                      reference_id=pair.reference_id,
                      flags=tuple(sorted(set(pair.flags) | set(extra_flags))))
        if obs.time_min > pair.sim_end_min + SIM_END_TOLERANCE_MIN:
            rows.append(ScoreRow(value=None, status="unavailable",
                                 reason=(f"observed at {obs.time_min:.0f} min after t0, but the "
                                         f"simulation ends at {pair.sim_end_min:.0f} min"), **common))
            continue
        try:
            value, diagnostics = scorer(pair, obs, pair.predicted_burned(obs.time_min),
                                        pair.observed_burned(obs))
        except Undefined as e:
            rows.append(ScoreRow(value=None, status="undefined", reason=str(e), **common))
            continue
        rows.append(ScoreRow(value=float(value), status="ok", reason=None,
                             diagnostics=diagnostics, **common))
    return rows


def cells(mask: np.ndarray) -> int:
    return int(np.count_nonzero(mask))


def hectares(mask: np.ndarray, pair: MatchedPair) -> float:
    return cells(mask) * pair.grid.cell_area_m2 / 10_000


def optional_round(x: Optional[float], nd: int = 6) -> Optional[float]:
    return None if x is None else round(float(x), nd)
