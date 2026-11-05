"""The types every metric is written against (docs/metrics_contract.md).

Alignment (the framework) builds a `MatchedPair` from a prediction and a reference.
A metric is a pure function `MatchedPair -> list[ScoreRow]`. Neither side needs the
other's code: this module is the whole interface.

Conventions (docs/conventions.md): every array is on the case grid, north-up, row 0
the northern edge; times are minutes since the case's t0; lengths are metres.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Literal, Optional

import numpy as np

Status = Literal["ok", "unavailable", "undefined"]
Better = Literal["higher", "lower", "zero"]      # "zero": closest to 0 is best (e.g. a bias)

FLAG_HIT_DOMAIN_EDGE = "hit_domain_edge"          # the fire may have left the domain


@dataclass(frozen=True)
class GridSpec:
    crs: str                                      # e.g. "EPSG:5070"
    transform: tuple                              # (res, 0, x_min, 0, -res, y_max)
    shape: tuple                                  # (rows, cols)
    res_m: float

    @property
    def cell_area_m2(self) -> float:
        return self.res_m * self.res_m


@dataclass(frozen=True, eq=False)
class Observation:
    """One observation of the real fire, e.g. one FIRIS perimeter, on the case grid."""
    id: str                                       # stable identity, e.g. "FIRIS@2025-01-22T21:04:31Z"
    time_min: float                               # minutes since t0
    burned: np.ndarray                            # bool, grid.shape; True = observed burned
    source: str                                   # "FIRIS"; later "VIIRS", "IR", ...


@dataclass(frozen=True, eq=False)
class MatchedPair:
    """A prediction and what was observed, aligned and ready to score.

    The constructor checks everything alignment guarantees, so a metric can rely on it."""
    grid: GridSpec
    toa: np.ndarray                               # float, grid.shape; minutes since t0, NaN = never burned
    sim_end_min: float                            # when the simulation stopped, minutes since t0
    valid: np.ndarray                             # bool, grid.shape; only these cells count
    observations: list                            # list[Observation], sorted by time_min, non-empty
    prediction_id: str
    reference_id: str
    flags: frozenset = field(default_factory=frozenset)

    def __post_init__(self):
        shape = tuple(self.grid.shape)
        if self.toa.ndim != 2:
            raise ValueError(f"toa must be 2D (rows, cols); got {self.toa.ndim}D "
                             "(ensembles are not supported yet)")
        if self.toa.shape != shape:
            raise ValueError(f"toa shape {self.toa.shape} != grid shape {shape}")
        if not np.issubdtype(self.toa.dtype, np.floating):
            raise ValueError(f"toa must be float (NaN = never burned); got {self.toa.dtype}")
        if self.valid.shape != shape or self.valid.dtype != bool:
            raise ValueError(f"valid must be a bool array of shape {shape}")
        if not self.observations:
            raise ValueError("a MatchedPair needs at least one observation")
        for o in self.observations:
            if o.burned.shape != shape or o.burned.dtype != bool:
                raise ValueError(f"observation {o.id!r}: burned must be a bool array of shape {shape}")
        times = [o.time_min for o in self.observations]
        if times != sorted(times):
            raise ValueError(f"observations must be sorted by time_min; got {times}")
        if len({o.id for o in self.observations}) != len(self.observations):
            raise ValueError("observation ids must be unique")
        if not self.prediction_id or not self.reference_id:
            raise ValueError("prediction_id and reference_id are required (they are the lineage)")

    def predicted_burned(self, time_min: float) -> np.ndarray:
        """Cells the prediction has burned by `time_min`, among the valid cells."""
        with np.errstate(invalid="ignore"):
            return np.isfinite(self.toa) & (self.toa <= time_min) & self.valid

    def observed_burned(self, obs: Observation) -> np.ndarray:
        """Cells observed burned, among the valid cells."""
        return obs.burned & self.valid


@dataclass(frozen=True)
class ScoreRow:
    """One row of the score table: one metric scored against one observation."""
    metric: str
    metric_version: str
    value: Optional[float]                        # None unless status == "ok"
    status: Status
    reason: Optional[str]                         # required unless status == "ok"
    unit: str                                     # "1" for dimensionless, else SI ("ha", "min", "m")
    better: Better
    scored_at_min: float                          # always a number
    observation_id: str
    observation_source: str
    prediction_id: str
    reference_id: str
    flags: tuple = ()
    diagnostics: dict = field(default_factory=dict)   # supporting numbers; never ranked

    def __post_init__(self):
        if self.status == "ok":
            if self.value is None or not np.isfinite(self.value):
                raise ValueError(f"{self.metric}: status ok needs a finite value, got {self.value!r}")
        elif self.value is not None:
            raise ValueError(f"{self.metric}: value must be None when status is {self.status!r}")
        if self.status != "ok" and not self.reason:
            raise ValueError(f"{self.metric}: status {self.status!r} needs a reason")

    def to_dict(self) -> dict:
        d = asdict(self)
        d["flags"] = list(self.flags)
        return d
