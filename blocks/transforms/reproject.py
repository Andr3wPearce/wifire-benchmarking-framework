"""reproject: the automatic step the resolver inserts after a source whose native CRS isn't
the case CRS. Not used in workflows by name, so it has no manifest.

Vectors (GeoJSON) -> case CRS; rasters -> case grid (not needed by any block yet);
everything else copied.
"""
from __future__ import annotations

import json
from pathlib import Path

from blockkit import copy_tree, geojson_crs


def run(step: dict, log) -> dict:
    from rasterio.warp import transform_geom
    src, out, case = Path(step["inputs"]["in"]), Path(step["out_dir"]), step["case"]
    copy_tree(src, out)
    done = []
    for f in sorted(out.glob("*.geojson")):
        fc = json.loads(f.read_text())
        frm = geojson_crs(fc)
        if frm != case["crs"]:
            for feat in fc["features"]:
                feat["geometry"] = transform_geom(frm, case["crs"], feat["geometry"])
            fc["crs"] = case["crs"]
            f.write_text(json.dumps(fc))
            done.append(f"{f.name}: {frm} -> {case['crs']}")
    if any(out.glob("*.tif")):
        raise NotImplementedError("raster reprojection: no real block needs it yet")
    return {"reprojected": done or "nothing to do"}
