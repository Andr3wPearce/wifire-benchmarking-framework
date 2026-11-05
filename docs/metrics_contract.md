# Metrics contract

How a prediction gets scored against what was observed. This document fixes the
interface between **alignment** (ours) and **metrics** (Luis), so both sides can
be built and tested independently.

Status: agreed design, not yet implemented. The current `blocks/metrics/`
(`iou`, `area_bias`, `scoring.py`) predate it and will be migrated (see the end).

## Pipeline

```
prediction (time_of_arrival.tif, summary.json) ┐
                                                ├─> align ─> MatchedPair ─> metrics ─> list[ScoreRow] ─> score table
reference  (one or more timed observations)    ┘   (ours)                  (Luis)
```

| Piece | Owner | Does |
|---|---|---|
| Alignment (`MatchedPair` adapter) | Andrew | reads GeoTIFFs and the case, checks grids, builds the valid mask, sets flags, builds `MatchedPair` |
| Metrics package | Luis | pure functions `MatchedPair -> list[ScoreRow]`, with tests |
| Scoring step | Andrew | one step per run: align once, run the workflow's metric list, write the rows |

## Where the code is

`blocks/metrics/` (see its README): `metric_types.py` is this contract as code (the
types below, with their checks), `metric_base.py` scores each observation with the
statuses, `overlap.py` holds `iou` as the worked example, `metric_synthetic.py` builds
test pairs. Tests are in `blocks/metrics/tests/`.

## Rules for metrics

* **Pure.** A metric takes a `MatchedPair` and returns rows. No file I/O, no network,
  no global state, no randomness.
* **No framework dependencies.** No Dagster, no rasterio, no GDAL in the metrics
  package. NumPy (and SciPy if needed) only. Everything geospatial is resolved
  before the metric sees the data.
* **Never raise for a scoring outcome.** Missing inputs or undefined math are rows
  with a status (below), not exceptions. Exceptions mean a bug.
* **One row per observation**, in the order of `pair.observations`. A metric that
  scores the whole run at once (none yet) returns one row with
  `scored_at_min = sim_end_min`.
* **Score only `valid` cells.** Cells outside `pair.valid` never count toward any
  value.

## `MatchedPair`

Everything is already on the case grid and follows `docs/conventions.md`
(north-up, origin at the NW corner, SI units, case CRS in metres).

```python
@dataclass(frozen=True)
class GridSpec:
    crs: str                    # e.g. "EPSG:5070"
    transform: tuple            # (res, 0, x_min, 0, -res, y_max)
    shape: tuple[int, int]      # (rows, cols)
    res_m: float

@dataclass(frozen=True)
class Observation:
    id: str                     # stable identity of this observation (see Lineage)
    time_min: float             # minutes since the case's t0
    burned: np.ndarray          # bool, grid.shape; True = observed burned
    source: str                 # "FIRIS"; later "VIIRS", "IR", ...

@dataclass(frozen=True)
class MatchedPair:
    grid: GridSpec
    toa: np.ndarray             # float, grid.shape; minutes since t0, NaN = never burned
    sim_end_min: float          # when the simulation stopped, minutes since t0
    valid: np.ndarray           # bool, grid.shape; cells that count
    observations: list[Observation]   # sorted by time_min, at least one
    flags: frozenset[str]       # e.g. {"hit_domain_edge"}
    prediction_id: str
    reference_id: str
```

Predicted burned area at an observation is
`np.isfinite(toa) & (toa <= obs.time_min) & valid`.

### What alignment guarantees

* All arrays have exactly `grid.shape`. A mismatch fails alignment, never a metric.
* `observations` is sorted by `time_min` and non-empty. A case with no reference
  never reaches the metrics: the scoring step is `unavailable`.
* `valid` excludes nodata in either input. It does **not** exclude cells near the
  domain edge; that is reported as a flag instead.
* `flags`:
  * `hit_domain_edge`: the prediction burned at least one cell on the outer ring of
    the grid before `sim_end_min`. The fire may have left the domain, so area-based
    scores may understate the prediction. Scored anyway, flag copied onto every row.

## `ScoreRow`

One row per (metric, observation). This is the shape of the score table.

| Field | Type | Meaning |
|---|---|---|
| `metric` | str | metric name, e.g. `"iou"` |
| `metric_version` | str | bumped whenever results for the same inputs could change |
| `value` | float or None | the headline number; None unless `status == "ok"` |
| `status` | `"ok"`, `"unavailable"`, `"undefined"` | see below |
| `reason` | str or None | required when `status != "ok"` |
| `unit` | str | `"1"` for dimensionless, else SI (`"ha"`, `"min"`, `"m"`) |
| `better` | `"higher"`, `"lower"`, `"zero"` | which way is good (`"zero"`: closest to 0, e.g. bias) |
| `scored_at_min` | float | always a number: minutes since t0 at which this was scored |
| `observation_id` | str | the `Observation.id` scored against |
| `observation_source` | str | copied from the observation |
| `prediction_id` | str | copied from the pair |
| `reference_id` | str | copied from the pair |
| `flags` | list[str] | copied from the pair, plus any the metric adds |
| `diagnostics` | dict | supporting numbers (`union_cells`, `predicted_ha`, ...); shown, never charted |

### Status

| Status | Means | Example |
|---|---|---|
| `ok` | scored | IoU = 0.61 |
| `unavailable` | the inputs needed to score this don't exist | the observation is at 180 min, the simulation ended at 120 min |
| `undefined` | inputs exist, the math has no answer | IoU when both predicted and observed areas are empty; area bias when the observed area is 0 |

The difference matters downstream: `unavailable` is a **data gap** (fix the
workflow or get more data) and goes in the data-gap report; `undefined` is a
property of the fire and the metric.

An observation after `sim_end_min` (more than 1 min) is `unavailable` for every
metric, with a reason naming both times.

## Lineage

The score table must say where every number came from **without** Dagster's graph,
so it survives being exported or joined offline.

* `prediction_id`: the prediction asset's identity key (workflow, fire, block
  versions, params hash), as already used for reuse.
* `reference_id`: the reference asset's identity key.
* `observation_id`: which observation inside that reference (e.g.
  `FIRIS@2025-01-22T14:32Z`).

With these, comparing workflows is a join on `(fire, observation_id, metric)`.
Per-metric Dagster assets are deliberately not used: lineage back to the reference
is carried in the rows, and that is enough.

## Execution

One scoring step per run: alignment once, then the workflow's metric list. The
workflow names its metrics (as today); each metric stays individually versioned and
selectable. The step writes its rows as `scores.json` (a list of `ScoreRow`s: a
handful of rows, readable by eye, no extra dependency) and its summary becomes
Dagster metadata. The cross-fire table for a workflow is built offline by
concatenating its runs' `scores.json` into Parquet; that is where Parquet pays off.

## Testing (metrics package)

Build `MatchedPair`s from small synthetic arrays; no real data needed. Every metric
needs at least:

* a perfect prediction (best value)
* a disjoint prediction (worst value)
* a known partial overlap with a hand-computed answer
* the `undefined` case(s)
* an observation after `sim_end_min` (`unavailable`)
* invalid cells changing the answer when they would otherwise count
* multiple observations returning one row each, in order

## Roadmap

1. **IoU and area bias per observation.** The existing metrics, ported to this
   contract. With a multi-perimeter reference this already gives accuracy over
   time with no new code.
2. **Arrival-time error.** Needs an observed arrival-time raster built from a
   perimeter series (first perimeter containing each cell). That builder belongs
   in the **reference** block; the metric is then `toa_pred - toa_obs` on cells both
   burned. Blocked on references with more than one perimeter per fire.
3. **Shape metrics** (e.g. Hausdorff distance between perimeters).
4. **Spread-rate / direction error.** Later.

### Ensembles (on paper, not built)

`toa` may later gain a leading member axis (`(members, rows, cols)`), enabling
reliability and probabilistic metrics. Until then, metrics may assume 2D `toa`;
alignment will never hand them anything else.

## Migration from the current blocks

| Today | Becomes |
|---|---|
| `scoring.burned_masks()` | alignment (the `MatchedPair` adapter) |
| sim-ended-too-early `Unavailable` for the whole step | per-observation `unavailable` rows |
| `scored_at_min = "end of simulation"` | always a float (`sim_end_min`) |
| `iou = 0.0` on an empty union | `status = "undefined"` |
| one step per metric, free-form `score.json` | one scoring step, `ScoreRow` table |
| `iou.py`, `area_bias.py` | thin blocks calling `overlap.iou`, `overlap.area_bias` |
