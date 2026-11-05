"""What the blocks share: the "unavailable" signal, time zero, reading and writing rasters
on the case grid, copying an upstream artifact, and true-vs-grid north.

Shipped next to every block (the compiled workflow writes all of blocks/lib to the step's
code directory), so a block just does `from blockkit import ...`. Part of every block's
identity: changing anything here rebuilds the artifacts of the blocks that import it.
"""
from __future__ import annotations

import math
import shutil
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

# the case grid is a convention, so it lives with the conventions (src/bench/conventions.py,
# shipped next to the blocks in every step)
from conventions import case_grid  # noqa: F401  (re-exported for the blocks)

UNAVAILABLE_EXIT = 3      # exit code meaning "this fire doesn't have what the block needs"


class Unavailable(Exception):
    """Raised by a block when the fire lacks what it needs (no NGFS detection, no weather
    station in range, no reference perimeter). Not an error: the fire's row in the
    workflow's table says "unavailable: <reason>", and the steps after it are skipped."""


def t0(case: dict) -> datetime:
    """The case's simulation clock zero, as an aware UTC datetime."""
    return datetime.fromisoformat(case["t0"].replace("Z", "+00:00")).astimezone(timezone.utc)


def write_tif(path: Path, arr: np.ndarray, transform, crs: str, nodata=None) -> None:
    """A single-band, tiled, deflate-compressed GeoTIFF (docs/conventions.md)."""
    import rasterio
    with rasterio.open(path, "w", driver="GTiff", height=arr.shape[0], width=arr.shape[1], count=1,
                       dtype=arr.dtype, crs=crs, transform=transform, nodata=nodata,
                       compress="deflate", tiled=True) as dst:
        dst.write(arr, 1)


def read_tif(path: Path) -> np.ndarray:
    """Band 1 of a GeoTIFF."""
    import rasterio
    with rasterio.open(path) as src:
        return src.read(1)


def copy_tree(src: Path, dst: Path, skip=("provenance.json",)) -> None:
    """Copy an upstream artifact's files (not its provenance) as the start of a method."""
    dst.mkdir(parents=True, exist_ok=True)
    for f in Path(src).iterdir():
        if f.is_file() and f.name not in skip:
            shutil.copy2(f, dst / f.name)


def geojson_crs(fc: dict) -> str:
    """The CRS a GeoJSON says it is in: our `"crs": "EPSG:5070"`, the old-style named CRS
    member, or WGS84 when it says nothing (RFC 7946)."""
    frm = fc.get("crs") or "EPSG:4326"
    if isinstance(frm, dict):
        frm = frm.get("properties", {}).get("name", "EPSG:4326")
    return frm


def grid_convergence_deg(lon: float, lat: float, crs: str) -> float:
    """Azimuth of TRUE north measured clockwise from GRID north, at (lon, lat).
    A true azimuth a is the grid azimuth a + this (docs/conventions.md, section 2)."""
    from rasterio.warp import transform
    xs, ys = transform("EPSG:4326", crs, [lon, lon], [lat, lat + 0.01])
    return math.degrees(math.atan2(xs[1] - xs[0], ys[1] - ys[0]))
