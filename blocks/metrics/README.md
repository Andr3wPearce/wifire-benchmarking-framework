# Metrics

How predictions are scored. The interface is `docs/metrics_contract.md`; read it first.

## What's here

| File | What it is |
|---|---|
| `metric_types.py` | **The contract as code**: `GridSpec`, `Observation`, `MatchedPair`, `ScoreRow`. The constructors reject anything the contract rules out, so a metric never has to check its inputs |
| `metric_base.py` | `score_each_observation` (one row per observation, with the `ok` / `unavailable` / `undefined` statuses) and `Undefined` |
| `overlap.py` | Overlap metrics. `iou` is the worked example |
| `metric_synthetic.py` | Synthetic `MatchedPair`s for tests: circular fires with a constant rate of spread, whose answers you can work out by hand |
| `tests/` | `test_overlap.py` is the pattern every metric's tests follow; `test_metric_types.py` covers the types |
| `iou.py`, `area_bias.py`, `scoring.py` + manifests | **The current pipeline's blocks** (what runs on WSTC today). They'll become thin wrappers over the new metrics once the scoring step is built. Don't change them |

## Rules

* A metric is a pure function `MatchedPair -> list[ScoreRow]`: no file or network
  access, no randomness, **NumPy only** (SciPy if needed). No Dagster, rasterio or GDAL.
* Missing inputs and undefined math are statuses, not exceptions: raise `Undefined`
  from your scorer and `score_each_observation` does the rest. Any other exception is a bug.
* Score `valid` cells only (the `MatchedPair` helpers already do).
* Prefer metrics that use burned cells only. Anything counting unburned cells
  (accuracy, Cohen's kappa) depends on the domain buffer, which we chose; such a
  metric must add the flag `depends_on_domain` to its rows.
* Bump a metric's version whenever its results for the same inputs could change.
* Files in a step's pod share one flat directory, so module names must be unique
  across `blocks/` and must not shadow the standard library (hence `metric_types`,
  not `types`). Import siblings by module name: `from metric_types import ...`.
* Python 3.10 (the scoring step runs in the core image).

## Adding a metric

1. Write a scorer in the right family file (`overlap.py` for now):
   `(pair, obs, predicted, observed) -> (value, diagnostics)`, raising `Undefined` when
   the math has no answer.
2. Wrap it: `score_each_observation(pair, metric=..., version="1", unit=..., better=..., scorer=...)`.
3. Test it like `tests/test_overlap.py`: perfect, worst case, a partial overlap with a
   hand-computed answer, each `undefined` case, an observation after the simulation's
   end, invalid cells, several observations.
4. Write its card in the module docstring or the function's: formula, range, which
   way is better, when it is undefined.

## Running the tests

```bash
pip install numpy pytest
python -m pytest blocks/metrics/tests
```
