"""ELMFIRE model block (pyretechnics), the first block that does real work.

The framework hands this block a run directory containing `run.json` (see
docs/block_contract.md). The block is the model's *adapter*:

    prepare  canonical inputs (GeoTIFFs in SI units) -> pyretechnics space-time cubes
    run      pyretechnics.eulerian_level_set.spread_fire_with_phi_field
    collect  pyretechnics outputs -> canonical Prediction (rasters + perimeter GeoJSON)

Inputs that upstream blocks have not produced for real yet (they are still dry
runs) are filled from a synthetic test landscape, and the run says so loudly in
its output and metadata.

Runs two ways:
  * under Dagster Pipes (inside the Kubernetes Job, or a local subprocess):
    context comes from Dagster, metadata and logs flow back to the UI
  * standalone, for debugging:  python elmfire_block.py <run_dir>
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
from rasterio.transform import from_origin

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


# --------------------------------------------------------------------------- #
# TEMP(dry-run): synthetic fixture (stand-in until LANDFIRE / weather / ignition blocks are real)
# --------------------------------------------------------------------------- #
def make_synthetic_inputs(d: Path, size: int = 200, cell_m: float = 30.0, crs: str = "EPSG:5070") -> dict:
    """A 6 x 6 km test landscape: grass/grass-shrub with a timber-understory
    patch and a road, a gentle hill, a steady SW wind, one ignition point."""
    d.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(42)
    yy, xx = np.mgrid[0:size, 0:size]
    fuel = np.where(rng.random((size, size)) < 0.7, 102, 121).astype("int16")      # GR2 / GS1
    fuel[(yy - 60) ** 2 + (xx - 140) ** 2 < 35 ** 2] = 165                          # TU5 patch
    fuel[:, 95:98] = 91                                                             # NB1 road
    elev = 400 + 120 * np.exp(-(((yy - 120) / 60.0) ** 2 + ((xx - 70) / 60.0) ** 2))
    in_tu = fuel == 165
    layers = {
        "fuel_model_fbfm40": fuel,
        "elevation": elev.astype("float32"),
        "canopy_cover": np.where(in_tu, 0.5, 0.0).astype("float32"),
        "canopy_height": np.where(in_tu, 15.0, 0.0).astype("float32"),
        "canopy_base_height": np.where(in_tu, 2.0, 0.0).astype("float32"),
        "canopy_bulk_density": np.where(in_tu, 0.12, 0.0).astype("float32"),
        "wind_speed_10m": np.full((size, size), 6.0, "float32"),        # m/s
        "wind_from_direction": np.full((size, size), 225.0, "float32"),  # deg, from SW
    }
    transform = from_origin(-2_000_000.0, 2_000_000.0, cell_m, cell_m)
    for name, arr in layers.items():
        _write_tif(d / f"{name}.tif", arr, transform, crs)
    (d / "fuel_moisture.json").write_text(json.dumps({"dead_1h": 0.06}))
    cy, cx = 150, 40
    x, y = transform * (cx + 0.5, cy + 0.5)
    (d / "ignition.geojson").write_text(json.dumps({
        "type": "FeatureCollection", "crs": crs,
        "features": [{"type": "Feature", "properties": {"time_min": 0},
                      "geometry": {"type": "Point", "coordinates": [x, y]}}]}))
    return {"landscape": str(d), "weather": str(d), "moisture": str(d), "ignition": str(d)}


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
    work = Path(run["run_dir"])
    inputs = dict(run.get("inputs", {}))
    # TEMP(dry-run): fill slots whose upstream blocks are still receipts
    fixture_slots = [s for s in ("landscape", "weather", "moisture", "ignition")
                     if not _is_real(inputs.get(s))]
    if fixture_slots:
        log(f"WARNING: upstream slots {fixture_slots} are dry runs; using the synthetic test landscape for them")
        synth = make_synthetic_inputs(work / "synthetic_inputs", crs=run["case"]["crs"])
        for s in fixture_slots:
            inputs[s] = synth[s]

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
    wind_ms, _ = _read(weather / "wind_speed_10m.tif")
    wind_from, _ = _read(weather / "wind_from_direction.tif")

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
    bands = int(np.ceil(duration / band)) + 1
    shape = (bands, *g["shape"])

    def cube(v):
        # GeoTIFF row 0 is the NORTH edge; pyretechnics row 0 is the SOUTH edge (y+1 = north).
        # Flip every grid going in (collect flips the outputs back). Scalars need no flip.
        if isinstance(v, np.ndarray):
            v = np.ascontiguousarray(np.flipud(v), dtype="float32")
        return SpaceTimeCube(shape, v)

    n_rows = g["shape"][0]
    ign_cells = [(n_rows - 1 - r, c) for r, c in ign_cells]   # same flip for ignition cells

    cubes = {
        "slope": cube(slope),
        "aspect": cube(aspect),
        "fuel_model": cube(fuel),
        **{k: cube(v) for k, v in canopy.items()},
        "wind_speed_10m": cube(wind_ms * 3.6),                       # m/s -> km/h
        "upwind_direction": cube(wind_from),
        "fuel_moisture_dead_1hr": cube(moist["dead_1h"]),
        "fuel_moisture_dead_10hr": cube(moist["dead_10h"]),
        "fuel_moisture_dead_100hr": cube(moist["dead_100h"]),
        "fuel_moisture_live_herbaceous": cube(moist["live_herbaceous"]),
        "fuel_moisture_live_woody": cube(moist["live_woody"]),
        "foliar_moisture": cube(moist["foliar"]),
        "fuel_spread_adjustment": cube(float(params["spread_adjustment"])),
    }
    return {"cubes": cubes, "shape": shape, "resolution": (band, cell_h, cell_w),
            "ignitions": ign_cells, "duration": duration, "geo": g,
            "fixture_slots": fixture_slots, "moisture_defaulted": defaulted}


def run_model(p: dict, log) -> dict:
    state = els.SpreadState(p["shape"])
    for cell in p["ignitions"]:
        state.ignite_cell(cell)
    log(f"spreading: grid {p['shape'][1]}x{p['shape'][2]} at {p['resolution'][1]:.0f} m, "
        f"{len(p['ignitions'])} ignition(s), {p['duration']:.0f} min")
    t0 = time.perf_counter()
    out = els.spread_fire_with_phi_field(p["cubes"], state, cube_resolution=p["resolution"],
                                         start_time=0.0, max_duration=p["duration"])
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
        "synthetic_inputs_for": p["fixture_slots"] or "none",
        "moisture_defaults_used": p["moisture_defaulted"] or "none",
        "outputs": sorted(f.name for f in out_dir.iterdir()),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def _is_real(path) -> bool:
    """An upstream artifact is real if it holds data, not just a dry-run provenance record."""
    if not path or not Path(path).is_dir():
        return False
    prov = Path(path) / "provenance.json"
    return not (prov.exists() and json.loads(prov.read_text()).get("dry_run"))


def execute(run_dir: Path, log, root: Path | None = None) -> dict:
    """Paths in run.json are relative to the artifact root, which differs between the
    host (repo/artifacts) and the Kubernetes pod (/artifacts). Absolute paths also work."""
    root = root or Path(os.environ.get("BENCH_ARTIFACT_ROOT", "/artifacts"))
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
    if os.environ.get("DAGSTER_PIPES_CONTEXT"):
        from dagster_pipes import open_dagster_pipes
        with open_dagster_pipes() as pipes:
            root = Path(os.environ.get("BENCH_ARTIFACT_ROOT", "/artifacts"))
            run_dir = root / pipes.get_extra("run_dir")
            summary = execute(run_dir, pipes.log.info, root)
            pipes.report_asset_materialization(metadata={
                k: (json.dumps(v) if isinstance(v, (list, dict)) else v) for k, v in summary.items()})
    else:
        if len(sys.argv) != 2:
            sys.exit("usage: python elmfire_block.py <run_dir containing run.json>")
        print(json.dumps(execute(Path(sys.argv[1]), print), indent=2))


if __name__ == "__main__":
    main()
