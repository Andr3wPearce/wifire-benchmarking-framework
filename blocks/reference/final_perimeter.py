"""final_perimeter: the case's observed perimeter, rasterized onto the case grid (reference).

It comes from the case: `reference.file` (shipped with the compiled workflow) or
`reference.url` (downloaded here, and checked against `reference.sha256` when the case
pins one). Unavailable if the case has neither.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np

from blockkit import Unavailable, case_grid, geojson_crs, write_tif
from http_session import session


def run(step: dict, log) -> dict:
    from rasterio.features import rasterize
    from rasterio.warp import transform_geom
    out, case = Path(step["out_dir"]), step["case"]
    ref = case.get("reference") or {}
    if step.get("inputs", {}).get("file"):
        text = Path(step["inputs"]["file"]).read_text()
    elif ref.get("url"):
        r = session().get(ref["url"], timeout=120)
        r.raise_for_status()
        text = r.text
        digest = hashlib.sha256(r.content).hexdigest()
        if ref.get("sha256") and ref["sha256"] != digest:
            raise RuntimeError(f"{ref['url']} has changed (sha256 {digest[:12]} != pinned "
                               f"{ref['sha256'][:12]}): a new perimeter means new scores; update the case")
    else:
        raise Unavailable(f"case {case.get('name', case['incident'])} has no reference perimeter")
    fc = json.loads(text)
    frm = geojson_crs(fc)
    feats = fc["features"] if fc.get("type") == "FeatureCollection" else [fc]
    geoms = [transform_geom(frm, case["crs"], f["geometry"]) for f in feats]
    transform, shape = case_grid(case)
    mask = rasterize([(g, 1) for g in geoms], out_shape=shape, transform=transform, dtype="uint8")
    out.mkdir(parents=True, exist_ok=True)
    (out / "perimeter.geojson").write_text(json.dumps({
        "type": "FeatureCollection", "crs": case["crs"],
        "features": [{"type": "Feature", "properties": {}, "geometry": g} for g in geoms]}))
    write_tif(out / "burned.tif", mask, transform, case["crs"])
    ha = float(mask.sum()) * case["resolution_m"] ** 2 / 10_000
    if mask.any():
        rows, cols = np.nonzero(mask)
        edge = rows.min() == 0 or cols.min() == 0 or rows.max() == shape[0] - 1 or cols.max() == shape[1] - 1
        if edge:
            log("WARNING: the reference perimeter touches the domain edge; widen case.domain_wgs84")
    return {"burned_area_ha": round(ha, 1), "features": len(geoms), "source_crs": frm}
