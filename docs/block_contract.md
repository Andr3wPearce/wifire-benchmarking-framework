# Block contract (v1)

What an implemented block (one with `execution:` in its manifest) gets from the
framework, and what it must give back. There are two shapes: **models** (a run
directory) and everything else (**steps**). ELMFIRE (`images/elmfire`) and the
core blocks (`images/core`) are the reference implementations.

## How a block is run

On the WSTC Dagster every step is its own Kubernetes pod, started from the
block's image (`execution.image` -> `images/wstc/<image>/`). The step writes the
blocks' code (shipped inside the compiled workflow file) to `/tmp/bench/code`
and runs the block's file (`execution.code`) with the image's block Python
(`$BENCH_PYTHON` if the image sets it, else the Python Dagster runs in):

    python core_blocks.py <step.json>        # sources, transforms, reference, metrics
    python elmfire_block.py <run_dir>        # models

`BENCH_ARTIFACT_ROOT` points at the artifact store. The block writes into a
temporary directory; the framework then checks every raster against
[`conventions.md`](conventions.md) and only then moves the artifact into place
under its identity. A block that fails, or writes something that breaks the
conventions, leaves nothing behind that looks finished.

## Step contract (sources, transforms, reference, metrics)

One `step.json`:

```json
{"block": "landfire", "kind": "source", "version": "2024",
 "case": {"incident": "HUGHES", "name": "hughes", "crs": "EPSG:5070", "resolution_m": 30,
          "t0": "2025-01-22T18:53:00Z", "window_h": 24, "domain_wgs84": [-118.70, 34.48, -118.45, 34.68],
          "ignitions": {"reported": {"lat": 34.553, "lon": -118.567, "time_utc": "...", "source": "..."}},
          "reference": {"file": "references/hughes_final_perimeter.geojson"}},
 "params": {}, "selection": {"layers": ["fbfm40", "cc", "ch", "cbh", "cbd", "elevation"]},
 "inputs": {"in": "<absolute dir of the previous step in the chain>"},
 "out_dir": "<absolute dir to write>"}
```

* `params` are already merged with the manifest's defaults and type-checked.
* `selection` is the workflow's data selection for a source (everything on its
  line except `block`, `params`, `version`).
* `inputs`: `in` for a transform or an automatic step; `file` for the reference;
  `prediction` and `reference` for a metric.
* `case` is the fire's facts (cases/<fire>.yaml) plus the workflow's grid
  resolution: the case grid, t0, its ignition points, its reference.
* The block writes canonical artifacts (below) into `out_dir` and returns a
  summary; `core_blocks.execute` adds `provenance.json` with the block, version,
  params, selection, timing and summary. Metrics write `score.json`.
* **Unavailable:** when the fire lacks what the block needs (the case has no such
  ignition, no station is in range, there is no reference), the block raises
  `Unavailable(reason)`. The script then exits with code 3 and prints
  `unavailable: <reason>`; the framework records that, every later step for the
  fire is unavailable too, and the workflow's table says why. A network error or
  a bug is a failure, not "unavailable"; it is retried, never cached.

## Model contract

A run directory containing `run.json`:

```json
{
  "model": "elmfire", "block_version": "pyretechnics-2026.10.20", "run_id": "36204b2bee",
  "case":   {"incident": "HUGHES", "crs": "EPSG:5070", "resolution_m": 30, "t0": "...", "domain_wgs84": [...]},
  "params": {"spread_adjustment": 1.5, "duration_min": 600, "band_duration_min": 60},
  "inputs": {"landscape": "HUGHES/landscape/roads_override-2f329b0d69", "weather": "...",
             "moisture": "...", "ignition": "..."}
}
```

`inputs` are each slot's final artifact (after automatic steps and the
workflow's transforms), relative to `BENCH_ARTIFACT_ROOT`. The model writes a
Prediction to `run_dir/prediction/`.

## Canonical inputs (one directory per slot)

Every raster follows [`conventions.md`](conventions.md): single-band GeoTIFF,
**north-up**, in the case CRS, on the **case grid** (`conventions.case_grid`:
the case's `domain_wgs84` projected and snapped outward to whole cells), in
**SI units**.

| Slot | Files | Units |
|---|---|---|
| landscape | `fuel_model_fbfm40.tif` | FBFM40 code (91–204) |
| | `elevation.tif` | m |
| | `canopy_cover.tif` | fraction 0–1 |
| | `canopy_height.tif`, `canopy_base_height.tif` | m |
| | `canopy_bulk_density.tif` | kg/m³ |
| weather | `wind_series.json` | hourly, uniform in space: `hours[] = {time_utc, wind_speed_10m (m/s), wind_from_direction (deg from grid north, "from")}`; station wind writes this |
| | or `wind_speed_10m.tif` + `wind_from_direction.tif` | gridded wind (m/s; degrees from grid north, "from") |
| moisture | `fuel_moisture.json` | fractions: `dead_1h`, `dead_10h`, `dead_100h`, `live_herbaceous`, `live_woody`, `foliar` (missing ones get the adapter's defaults, which it reports) |
| ignition | `ignition.geojson` | Point features in the case CRS, `properties.time_min` = minutes since t0 |

Not inputs on purpose: **slope and aspect**. Each model adapter recomputes them
from the DEM on the working grid; resampled slope is wrong slope.

## Canonical output: Prediction (`run_dir/prediction/`)

| File | Required | Units |
|---|---|---|
| `perimeter.geojson` | **yes** | burned area polygon(s), case CRS |
| `time_of_arrival.tif` | **yes** (the metrics score burned area from it) | minutes since t0, NaN = unburned |
| `fireline_intensity.tif`, `spread_rate.tif`, `flame_length.tif` | if the model has them | kW/m, m/min, m |
| `summary.json` | **yes** | anything worth showing; becomes Dagster metadata (numbers are charted across runs) |

The framework adds `preview.png` (`src/bench/previews.py`), drawn the same way
for every model.

## Rules

- Report anything the adapter filled in or assumed (defaults, clipped values) in
  the summary. Silent assumptions break comparisons.
- Model quirks stay inside the adapter. Example: pyretechnics' row 0 is the
  **south** edge, so the ELMFIRE adapter flips every grid going in and out.
- Pin the model's dependencies in its image (pyretechnics needs NumPy 1.26;
  NumPy 2 breaks it at import).
- Images are built FROM the server's Dagster image (`dagster/dagster-k8s:1.13.26`), since
  the step process itself runs in them, but don't carry the blocks' code. A block that
  needs another Python (pyretechnics needs >= 3.11; that image is 3.10) gets its own
  environment in the image and points `BENCH_PYTHON` at it (see images/wstc/elmfire).
