# Block contract (v0)

What a **real** block (one with `execution:` in its manifest) gets from the
framework, and what it must give back. ELMFIRE (`images/elmfire`) is the
reference implementation.

## How it's launched

The framework runs the block's image (Kubernetes pod) or its entrypoint (local
subprocess) through **Dagster Pipes**. The block receives one extra,
`run_dir`, relative to the artifact root (`/artifacts` in the pod,
`repo/artifacts` on the host).

## Input: `run_dir/run.json`

```json
{
  "model": "elmfire", "block_version": "pyretechnics-2026.10.20", "run_id": "36204b2bee",
  "case":   {"incident": "HUGHES", "crs": "EPSG:5070", "resolution_m": 30, "mode": "retrospective"},
  "params": {"spread_adjustment": 1.5, "duration_min": 240, "band_duration_min": 60},
  "inputs": {"landscape": "<wf>/landscape/landfire/roads_override", "weather": "...",
             "moisture": "...", "ignition": "..."}
}
```

`params` are already merged with the manifest defaults and validated.
`inputs` point at each slot's final artifact directory (after automatic
reprojection and the workflow's transforms).

## Canonical inputs (one directory per slot)

Every raster follows [`conventions.md`](conventions.md) (orientation, directions, units). In short: single-band GeoTIFF, **north-up**, in the case CRS, on the
case grid, in **SI units**.

| Slot | Files | Units |
|---|---|---|
| landscape | `fuel_model_fbfm40.tif` | FBFM40 code (91–204) |
| | `elevation.tif` | m |
| | `canopy_cover.tif` | fraction 0–1 |
| | `canopy_height.tif`, `canopy_base_height.tif` | m |
| | `canopy_bulk_density.tif` | kg/m³ |
| weather | `wind_speed_10m.tif` | m/s |
| | `wind_from_direction.tif` | degrees clockwise from north, direction wind blows **from** |
| | or `wind_series.json` | hourly, uniform in space: `hours[] = {time_utc, wind_speed_10m (m/s), wind_from_direction (deg from grid north)}`; station wind uses this |
| moisture | `fuel_moisture.json` | fractions: `dead_1h`, `dead_10h`, `dead_100h`, `live_herbaceous`, `live_woody`, `foliar` (missing ones get adapter defaults, which are reported) |
| ignition | `ignition.geojson` | Point features in the case CRS, `properties.time_min` |

Not inputs on purpose: **slope and aspect** are recomputed from the DEM on the
working grid by each model adapter, never resampled.

## Canonical output: Prediction (`run_dir/prediction/`)

| File | Required | Units |
|---|---|---|
| `perimeter.geojson` | **yes** | burned area polygon(s), case CRS |
| `time_of_arrival.tif` | if the model has it | minutes after ignition, NaN = unburned |
| `fireline_intensity.tif`, `spread_rate.tif`, `flame_length.tif` | if the model has them | kW/m, m/min, m |
| `summary.json` | yes | anything worth showing; also sent as Dagster metadata |

## Rules

- Report anything the adapter filled in or assumed (defaults, synthetic inputs)
  in `summary.json`. Silent assumptions break comparisons.
- Model quirks stay inside the adapter. Example: pyretechnics' row 0 is the
  **south** edge, so the ELMFIRE adapter flips every grid going in and out.
- Pin the model's dependencies in the image (pyretechnics needs NumPy 1.26;
  NumPy 2 breaks it at import).

## Step contract (sources, transforms, reference, metrics)

Python-only blocks (`images/core/core_blocks.py`; manifest `execution: {python: ...}`) get
one `step.json`:

```json
{"block": "landfire", "kind": "source", "version": "2024",
 "case": {"incident": "HUGHES", "crs": "EPSG:5070", "resolution_m": 30,
          "t0": "2025-01-22T18:53:00Z", "window_h": 24, "domain_wgs84": [-118.70, 34.48, -118.45, 34.68]},
 "params": {}, "selection": {"layers": ["fbfm40", "cc", "ch", "cbh", "cbd", "elevation"]},
 "inputs": {"in": "<previous step in the chain>"},
 "out_dir": "<where to write>"}
```

and write canonical artifacts into `out_dir`, plus `provenance.json`
(`dry_run: false`, params, and the block's summary). Metrics get
`inputs.prediction` and `inputs.reference` and write `score.json`.

The **case grid** is derived, never stored: `domain_wgs84` projected to `case.crs`
and snapped outward to whole cells (`core_blocks.case_grid`). Every block that
writes rasters puts them on it.
