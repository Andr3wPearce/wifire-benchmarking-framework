"""roads_override: roads and urban cells -> a burnable fuel model (landscape transform).

LANDFIRE maps roads and developed land as non-burnable (NB1, NB3, NB9), which stops fire
at every road. This replaces them with `params.replacement_fuel` (e.g. GR2); water (NB8)
and snow/ice (NB2) stay non-burnable.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np

from blockkit import copy_tree

FUEL_FAMILIES = {"NB": 90, "GR": 100, "GS": 120, "SH": 140, "TU": 160, "TL": 180, "SB": 200}
ROADS_URBAN_CODES = (91, 93, 99)   # FBFM40 non-burnable codes that are not snow/ice (92) or water (98)


def fuel_code(name: str) -> int:
    """'GR2' -> 102 (Scott & Burgan 2005 numbering)."""
    return FUEL_FAMILIES[name[:2].upper()] + int(name[2:])


def run(step: dict, log) -> dict:
    out, src = Path(step["out_dir"]), Path(step["inputs"]["in"])
    copy_tree(src, out)
    import rasterio
    with rasterio.open(out / "fuel_model_fbfm40.tif") as f:
        fuel, profile = f.read(1), f.profile
    code = fuel_code(step["params"]["replacement_fuel"])
    hit = np.isin(fuel, ROADS_URBAN_CODES)
    fuel = np.where(hit, code, fuel).astype(fuel.dtype)
    with rasterio.open(out / "fuel_model_fbfm40.tif", "w", **profile) as f:
        f.write(fuel, 1)
    log(f"{int(hit.sum())} cells of {ROADS_URBAN_CODES} -> {code}")
    return {"cells_changed": int(hit.sum()), "codes_replaced": list(ROADS_URBAN_CODES),
            "replacement": f"{step['params']['replacement_fuel']} ({code})"}
