"""Machine-checkable parts of docs/conventions.md.

`check_raster` is run by the framework on every real block's inputs and
outputs. A violation fails the step: better to stop than to compare models
that saw differently oriented or scaled data.
"""
from __future__ import annotations

from pathlib import Path

ORIENTATION = "north-up: row 0 = north edge, col 0 = west edge, transform origin = (min x, max y)"
WIND_DIRECTION = "degrees clockwise from grid north, direction the wind blows FROM"

# plausible ranges in canonical (SI) units: catches unit/scaling mistakes
# such as LANDFIRE canopy cover in % or bulk density x100
VALUE_RANGES = {
    "fuel_model_fbfm40": (91, 204),
    "elevation": (-500.0, 9000.0),
    "canopy_cover": (0.0, 1.0),
    "canopy_height": (0.0, 120.0),
    "canopy_base_height": (0.0, 100.0),
    "canopy_bulk_density": (0.0, 1.0),
    "wind_speed_10m": (0.0, 80.0),
    "wind_from_direction": (0.0, 360.0),
    "time_of_arrival": (0.0, 1e7),
    "fireline_intensity": (0.0, 1e6),
    "spread_rate": (0.0, 1e4),
    "flame_length": (0.0, 500.0),
}


def check_raster(path: Path, case: dict) -> list[str]:
    """Return a list of convention violations for one canonical raster (empty = ok)."""
    import numpy as np
    import rasterio

    problems = []
    name = Path(path).stem
    with rasterio.open(path) as src:
        t = src.transform
        if src.count != 1:
            problems.append(f"{name}: {src.count} bands (one variable per file)")
        if t.b != 0 or t.d != 0:
            problems.append(f"{name}: rotated/sheared transform {tuple(t)[:6]}")
        if t.a <= 0 or t.e >= 0:
            problems.append(f"{name}: not north-up (pixel sizes x={t.a}, y={t.e}; "
                            f"need x > 0, y < 0). Convention: {ORIENTATION}")
        problems += _crs_problems(name, src.crs, case["crs"])
        res = case.get("resolution_m")
        if res and (abs(abs(t.a) - res) > 1e-6 or abs(abs(t.e) - res) > 1e-6):
            problems.append(f"{name}: resolution {abs(t.a)} x {abs(t.e)} m != case grid {res} m")
        if name in VALUE_RANGES:
            data = src.read(1, masked=True).astype("float64").filled(np.nan)
            finite = data[np.isfinite(data)]
            lo, hi = VALUE_RANGES[name]
            if finite.size and (finite.min() < lo or finite.max() > hi):
                problems.append(f"{name}: values {finite.min():g}..{finite.max():g} outside "
                                f"[{lo}, {hi}] (wrong units or scaling?)")
    return problems


def _crs_problems(name: str, crs, case_crs: str) -> list[str]:
    """Compare CRSs by meaning (not by string), and say so when PROJ itself is broken."""
    from rasterio.crs import CRS

    try:
        want = CRS.from_user_input(case_crs)
    except Exception as e:  # PROJ can't even resolve "EPSG:5070": environment problem
        return [f"{name}: can't resolve case CRS {case_crs} ({e}). PROJ's database is missing or "
                "mismatched; check PROJ_DATA/PROJ_LIB (conda sets these)"]
    if crs is None:
        return [f"{name}: no CRS (every raster must carry the case CRS)"]
    if crs == want:
        return []
    hint = (" (CRS came back as a bare LOCAL_CS: usually PROJ_DATA/PROJ_LIB pointing at another "
            "install's database, e.g. conda's)" if "LOCAL_CS" in crs.to_wkt() else "")
    return [f"{name}: CRS {crs.to_string()[:60]} != case CRS {case_crs}{hint}"]


def check_dir(d: Path, case: dict) -> list[str]:
    return [p for f in sorted(Path(d).glob("*.tif")) for p in check_raster(f, case)]
