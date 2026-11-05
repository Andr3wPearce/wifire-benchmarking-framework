"""Synthetic MatchedPairs for tests, with answers you can work out by hand.

The workhorse is a circular fire with a constant rate of spread (ROS): a cell's
arrival time is exactly its distance from the ignition divided by the ROS, so the
burned area at time t is a disc of radius ROS * t. Two such fires with different
ROS (or ignitions) give partial overlaps with known answers, e.g. concentric discs
of radii r1 <= r2 have IoU = (r1 / r2)^2, up to discretization.
"""
from __future__ import annotations

from typing import Optional, Sequence

import numpy as np

from metric_types import GridSpec, MatchedPair, Observation


def grid(shape=(101, 101), res_m: float = 30.0) -> GridSpec:
    rows, cols = shape
    return GridSpec(crs="EPSG:5070", transform=(res_m, 0.0, 0.0, 0.0, -res_m, rows * res_m),
                    shape=tuple(shape), res_m=res_m)


def distance_m(g: GridSpec, center_rc: Optional[Sequence[float]] = None) -> np.ndarray:
    """Distance (m) from each cell centre to `center_rc` (row, col; default: the grid centre)."""
    rows, cols = g.shape
    cr, cc = center_rc if center_rc is not None else ((rows - 1) / 2, (cols - 1) / 2)
    r, c = np.mgrid[0:rows, 0:cols]
    return np.hypot(r - cr, c - cc) * g.res_m


def circular_toa(g: GridSpec, ros_m_per_min: float, center_rc=None, ignition_min: float = 0.0,
                 sim_end_min: Optional[float] = None) -> np.ndarray:
    """Arrival time (min since t0) of a circular fire; NaN where it hasn't arrived by sim_end_min."""
    toa = ignition_min + distance_m(g, center_rc) / ros_m_per_min
    if sim_end_min is not None:
        toa = np.where(toa <= sim_end_min, toa, np.nan)
    return toa.astype(float)


def circular_observation(g: GridSpec, ros_m_per_min: float, time_min: float, center_rc=None,
                         ignition_min: float = 0.0, source: str = "SYNTHETIC") -> Observation:
    """What a circular fire with this ROS has burned by `time_min`."""
    burned = circular_toa(g, ros_m_per_min, center_rc, ignition_min) <= time_min
    return Observation(id=f"{source}@{time_min:g}min", time_min=float(time_min),
                       burned=burned, source=source)


def pair(toa: np.ndarray, observations: list, g: Optional[GridSpec] = None,
         sim_end_min: Optional[float] = None, valid: Optional[np.ndarray] = None,
         flags=()) -> MatchedPair:
    """A MatchedPair with sensible defaults (all cells valid; sim_end = last observation)."""
    g = g or grid(toa.shape)
    return MatchedPair(grid=g, toa=toa.astype(float),
                       sim_end_min=float(sim_end_min if sim_end_min is not None
                                         else max(o.time_min for o in observations)),
                       valid=np.ones(g.shape, bool) if valid is None else valid,
                       observations=list(observations), prediction_id="test-prediction",
                       reference_id="test-reference", flags=frozenset(flags))
