"""perimeter_replay: a submitted prediction (timed perimeters) -> a canonical Prediction.

    python perimeter_replay.py <run_dir containing run.json>

Nothing is simulated. Each cell's arrival time is the time of the first submitted
perimeter that contains it (minutes since t0; NaN if none does), rasterized onto the
case grid. Between perimeters nothing is known, so arrival times are quantized to the
submission's time step; scoring at an observation time uses the latest perimeter at
or before it. The summary says so.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import numpy as np


def execute(run_dir: Path, log, root: Path | None = None) -> dict:
    from rasterio.features import rasterize, shapes

    from blockkit import write_tif
    from conventions import case_grid

    root = root or Path(os.environ["BENCH_ARTIFACT_ROOT"])
    run = json.loads((run_dir / "run.json").read_text())
    case = run["case"]
    src = root / run["inputs"]["submission"] / "perimeters.geojson"
    feats = json.loads(src.read_text())["features"]
    transform, grid_shape = case_grid(case)

    toa = np.full(grid_shape, np.nan, dtype="float32")
    for f in feats:                                              # sorted by time_min
        inside = rasterize([(f["geometry"], 1)], out_shape=grid_shape, transform=transform,
                           dtype="uint8").astype(bool)
        toa[inside & np.isnan(toa)] = f["properties"]["time_min"]

    out = run_dir / "prediction"
    out.mkdir(parents=True, exist_ok=True)
    write_tif(out / "time_of_arrival.tif", toa, transform, case["crs"], nodata=np.nan)
    times = [f["properties"]["time_min"] for f in feats]
    steps = sorted({round(b - a, 1) for a, b in zip(times, times[1:])})
    burned = np.isfinite(toa)
    polys = [{"type": "Feature", "properties": {"time_min": float(times[-1])}, "geometry": geom}
             for geom, v in shapes(burned.astype("uint8"), mask=burned, transform=transform) if v == 1]
    (out / "perimeter.geojson").write_text(json.dumps(
        {"type": "FeatureCollection", "crs": case["crs"], "features": polys}))
    cell_ha = case["resolution_m"] ** 2 / 10_000
    # the fire reached the grid's outer ring: the submission may extend past our domain
    edge = bool(burned[0].any() or burned[-1].any() or burned[:, 0].any() or burned[:, -1].any())
    summary = {
        "stop_time_min": float(times[-1]),
        "burned_cells": int(burned.sum()),
        "burned_area_ha": round(float(burned.sum() * cell_ha), 1),
        "perimeters": len(feats),
        "time_step_min": steps[0] if len(steps) == 1 else (steps or "single perimeter"),
        "time_resolution": "quantized: a cell's arrival time is the first perimeter containing it",
        "hit_domain_edge": edge,
        "model": "not run here: a replayed submission (inputs and model not disclosed)",
        "outputs": sorted(p.name for p in out.iterdir()),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    log(f"done: {summary['burned_area_ha']} ha from {len(feats)} perimeters")
    return summary


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: python perimeter_replay.py <run_dir containing run.json>")
    print(json.dumps(execute(Path(sys.argv[1]), print), indent=2))


if __name__ == "__main__":
    main()
