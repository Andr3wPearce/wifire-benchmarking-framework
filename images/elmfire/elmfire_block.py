"""ELMFIRE model block: the adapter between the framework and SIG's pyretechnics.

The framework hands this block a run directory containing `run.json` (see
docs/block_contract.md, "Model contract"). The adapter does three things:

    prepare  canonical inputs (GeoTIFFs and JSON in SI units, north-up, case grid)
             -> pyretechnics space-time cubes
    run      pyretechnics.eulerian_level_set.spread_fire_with_phi_field
    collect  pyretechnics outputs -> canonical Prediction (rasters + perimeter GeoJSON
             + summary.json) in run_dir/prediction/

Everything model-specific stays in here, never upstream: pyretechnics puts row 0 at
the SOUTH edge (so every grid is flipped in and out), wants wind in km/h, and calls
the wind's "from" direction `upwind_direction`.

Dependencies: pyretechnics (NumPy 1.26), rasterio (images/wstc/elmfire).
Runs standalone for debugging:  python elmfire_block.py <run_dir>
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import rasterio
from rasterio.features import shapes

import pyretechnics.eulerian_level_set as els
import pyretechnics.fuel_models as fm
from pyretechnics.space_time_cube import SpaceTimeCube

# Moisture classes the canonical Moisture artifact may omit, with the defaults
# used when it does (fractions, kg water / kg dry fuel). Logged when used.
MOISTURE_DEFAULTS = {
    "dead_1h": 0.06, "dead_10h": 0.07, "dead_100h": 0.08,
    "live_herbaceous": 0.60, "live_woody": 0.90, "foliar": 1.00,
}
CANOPY_LAYERS = ["canopy_cover", "canopy_height", "canopy_base_height", "canopy_bulk_density"]


def _write_tif(path: Path, arr: np.ndarray, transform, crs: str, nodata=None) -> None:
    with rasterio.open(path, "w", driver="GTiff", height=arr.shape[0], width=arr.shape[1], count=1,
                       dtype=arr.dtype, crs=crs, transform=transform, nodata=nodata,
                       compress="deflate") as dst:
        dst.write(arr, 1)


def _read(path: Path) -> tuple[np.ndarray, dict]:
    with rasterio.open(path) as src:
        return src.read(1), {"transform": src.transform, "crs": src.crs.to_string(),
                             "res": src.res, "shape": (src.height, src.width)}


# --------------------------------------------------------------------------- #
# adapter: prepare / run / collect
# --------------------------------------------------------------------------- #
def prepare(run: dict, log) -> dict:
    """Canonical inputs -> pyretechnics cubes. Returns everything `run_model` needs."""
    params = run["params"]
    inputs = dict(run["inputs"])
    missing = [s for s in ("landscape", "weather", "moisture", "ignition") if not inputs.get(s)]
    if missing:
        raise ValueError(f"run.json has no input for slots {missing}")

    land, weather = Path(inputs["landscape"]), Path(inputs["weather"])
    fuel, g = _read(land / "fuel_model_fbfm40.tif")
    if g["crs"] != run["case"]["crs"]:
        raise ValueError(f"landscape CRS {g['crs']} != case CRS {run['case']['crs']} "
                         "(the framework should have inserted a reproject step)")
    elev, _ = _read(land / "elevation.tif")
    cell_h, cell_w = abs(g["res"][1]), abs(g["res"][0])

    unknown = sorted({int(v) for v in np.unique(fuel) if not fm.fuel_model_exists(int(v))})
    if unknown:
        raise ValueError(f"fuel codes not in pyretechnics' FBFM40 table: {unknown}")

    # slope (rise/run) and aspect (deg clockwise from north, downslope) recomputed
    # from the DEM on this grid, never resampled (see docs/block_contract.md)
    dz_dy_rows, dz_dx = np.gradient(elev.astype("float64"), cell_h, cell_w)
    dz_dnorth = -dz_dy_rows                                  # rows run north -> south
    slope = np.hypot(dz_dx, dz_dnorth)
    aspect = (np.degrees(np.arctan2(-dz_dx, -dz_dnorth)) + 360.0) % 360.0

    canopy = {k: _read(land / f"{k}.tif")[0].astype("float32") for k in CANOPY_LAYERS}
    series_file = weather / "wind_series.json"
    if series_file.exists():
        # hourly, uniform in space (station wind): one value per time band, broadcast over the grid
        series = json.loads(series_file.read_text())
        wind_ms = wind_from = None
        wind_series = series
    else:
        wind_ms, _ = _read(weather / "wind_speed_10m.tif")
        wind_from, _ = _read(weather / "wind_from_direction.tif")
        wind_series = None

    moist_given = json.loads((Path(inputs["moisture"]) / "fuel_moisture.json").read_text())
    moist = {**MOISTURE_DEFAULTS, **moist_given}
    defaulted = sorted(set(MOISTURE_DEFAULTS) - set(moist_given))
    if defaulted:
        log(f"moisture classes not provided, using adapter defaults: "
            f"{ {k: MOISTURE_DEFAULTS[k] for k in defaulted} }")

    ign = json.loads((Path(inputs["ignition"]) / "ignition.geojson").read_text())
    inv = ~g["transform"]
    ign_cells = []
    for f in ign["features"]:
        col, row = inv * tuple(f["geometry"]["coordinates"][:2])
        r, c = int(row), int(col)
        if not (0 <= r < g["shape"][0] and 0 <= c < g["shape"][1]):
            raise ValueError(f"ignition {f['geometry']['coordinates']} is outside the landscape")
        ign_cells.append((r, c))

    duration = float(params["duration_min"])
    band = float(params["band_duration_min"])
    # the simulation clock is minutes since the case's t0; start at the (earliest) ignition time
    ign_times = [float(f.get("properties", {}).get("time_min", 0.0)) for f in ign["features"]]
    start = max(0.0, min(ign_times))
    if min(ign_times) < 0:
        log(f"ignition time {min(ign_times):.0f} min is before the case t0; starting at t0")
    bands = int(np.ceil((start + duration) / band)) + 1
    shape = (bands, *g["shape"])

    def cube(v):
        # GeoTIFF row 0 is the NORTH edge; pyretechnics row 0 is the SOUTH edge (y+1 = north).
        # Flip every grid going in (collect flips the outputs back). Scalars need no flip.
        if isinstance(v, np.ndarray):
            v = np.ascontiguousarray(np.flipud(v), dtype="float32")
        return SpaceTimeCube(shape, v)

    n_rows = g["shape"][0]
    ign_cells = [(n_rows - 1 - r, c) for r, c in ign_cells]   # same flip for ignition cells

    if wind_series is not None:
        # band k covers [k*band, (k+1)*band) min after t0; take the hourly record nearest its start
        from datetime import datetime, timedelta
        def _ts(x):
            return datetime.fromisoformat(x.replace("Z", "+00:00"))
        t_case = _ts(run["case"]["t0"])
        hours = wind_series["hours"]
        def at(k, key):
            t = t_case + timedelta(minutes=k * band)
            return min(hours, key=lambda h: abs((_ts(h["time_utc"]) - t).total_seconds()))[key]
        wind_cubes = {
            "wind_speed_10m": SpaceTimeCube(shape, np.array(
                [at(k, "wind_speed_10m") * 3.6 for k in range(bands)], "float32").reshape(bands, 1, 1)),
            "upwind_direction": SpaceTimeCube(shape, np.array(
                [at(k, "wind_from_direction") for k in range(bands)], "float32").reshape(bands, 1, 1)),
        }
        log(f"wind: {wind_series.get('station')} hourly, {bands} bands from t0, "
            f"{min(at(k, 'wind_speed_10m') for k in range(bands)):.1f}-"
            f"{max(at(k, 'wind_speed_10m') for k in range(bands)):.1f} m/s")
    else:
        wind_cubes = {"wind_speed_10m": cube(wind_ms * 3.6),                       # m/s -> km/h
                      "upwind_direction": cube(wind_from)}

    cubes = {
        "slope": cube(slope),
        "aspect": cube(aspect),
        "fuel_model": cube(fuel),
        **{k: cube(v) for k, v in canopy.items()},
        **wind_cubes,
        "fuel_moisture_dead_1hr": cube(moist["dead_1h"]),
        "fuel_moisture_dead_10hr": cube(moist["dead_10h"]),
        "fuel_moisture_dead_100hr": cube(moist["dead_100h"]),
        "fuel_moisture_live_herbaceous": cube(moist["live_herbaceous"]),
        "fuel_moisture_live_woody": cube(moist["live_woody"]),
        "foliar_moisture": cube(moist["foliar"]),
        "fuel_spread_adjustment": cube(float(params["spread_adjustment"])),
    }
    return {"cubes": cubes, "shape": shape, "resolution": (band, cell_h, cell_w),
            "ignitions": ign_cells, "duration": duration, "start": start, "geo": g,
            "moisture_defaulted": defaulted}


def run_model(p: dict, log) -> dict:
    state = els.SpreadState(p["shape"])
    for cell in p["ignitions"]:
        state.ignite_cell(cell)
    log(f"spreading: grid {p['shape'][1]}x{p['shape'][2]} at {p['resolution'][1]:.0f} m, "
        f"{len(p['ignitions'])} ignition(s), {p['duration']:.0f} min")
    t0 = time.perf_counter()
    out = els.spread_fire_with_phi_field(p["cubes"], state, cube_resolution=p["resolution"],
                                         start_time=p.get("start", 0.0), max_duration=p["duration"])
    out["runtime_s"] = time.perf_counter() - t0
    return out


def collect(p: dict, out: dict, out_dir: Path) -> dict:
    """pyretechnics -> canonical Prediction: rasters + perimeter, plus a summary."""
    out_dir.mkdir(parents=True, exist_ok=True)
    g = p["geo"]
    m = out["spread_state"].get_full_matrices(
        ["time_of_arrival", "fireline_intensity", "spread_rate", "flame_length", "fire_type"])
    m = {k: np.flipud(v) for k, v in m.items()}                 # back to north-up
    toa = m["time_of_arrival"].astype("float32")                 # minutes, NaN = unburned
    for name, units in [("time_of_arrival", "min"), ("fireline_intensity", "kW/m"),
                        ("spread_rate", "m/min"), ("flame_length", "m")]:
        arr = m[name].astype("float32")
        _write_tif(out_dir / f"{name}.tif", arr, g["transform"], g["crs"],
                   nodata=np.nan if name == "time_of_arrival" else None)
    burned = np.isfinite(toa)
    polys = [{"type": "Feature", "properties": {"time_min": float(out["stop_time"])}, "geometry": geom}
             for geom, v in shapes(burned.astype("uint8"), mask=burned, transform=g["transform"]) if v == 1]
    (out_dir / "perimeter.geojson").write_text(json.dumps(
        {"type": "FeatureCollection", "crs": g["crs"], "features": polys}))
    cell_area_ha = p["resolution"][1] * p["resolution"][2] / 10_000
    fli = m["fireline_intensity"][burned]
    summary = {
        "stop_condition": out["stop_condition"],
        "stop_time_min": float(out["stop_time"]),
        "burned_cells": int(burned.sum()),
        "burned_area_ha": round(float(burned.sum() * cell_area_ha), 1),
        "max_fireline_intensity_kw_m": round(float(fli.max()), 1) if fli.size else 0.0,
        "crown_fire_cells": int((m["fire_type"] >= 2).sum()),
        "runtime_s": round(out["runtime_s"], 3),
        "moisture_defaults_used": p["moisture_defaulted"] or "none",
        "outputs": sorted(f.name for f in out_dir.iterdir()),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def execute(run_dir: Path, log, root: Path | None = None) -> dict:
    """Run the model on run_dir/run.json and write run_dir/prediction/. Input paths in
    run.json are relative to the artifact root (BENCH_ARTIFACT_ROOT, set by the framework),
    so the same run.json works wherever the artifacts are mounted."""
    root = root or Path(os.environ["BENCH_ARTIFACT_ROOT"])
    run = json.loads((run_dir / "run.json").read_text())
    run["run_dir"] = str(run_dir)
    run["inputs"] = {k: str(root / v) for k, v in run.get("inputs", {}).items() if v}
    prepared = prepare(run, log)
    out = run_model(prepared, log)
    summary = collect(prepared, out, run_dir / "prediction")
    log(f"done: {summary['burned_area_ha']} ha burned, stop: {summary['stop_condition']}, "
        f"{summary['runtime_s']} s")
    return summary


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: python elmfire_block.py <run_dir containing run.json>")
    print(json.dumps(execute(Path(sys.argv[1]), print), indent=2))


if __name__ == "__main__":
    main()
