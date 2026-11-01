"""landfire: LANDFIRE fuels, canopy and elevation for the case domain (landscape slot).

Asks the LANDFIRE Product Service (LFPS) to clip and project the layers on its side,
then puts every layer on the case grid and converts LANDFIRE's scaled integers to SI.
LFPS needs an email address: the run passes it as BENCH_LFPS_EMAIL (a run tag, never
stored in code).
"""
from __future__ import annotations

import io
import json
import os
import shutil
import time
import zipfile
from pathlib import Path

import numpy as np

from blockkit import case_grid, read_tif, write_tif
from http_session import session

LFPS = "https://lfps.usgs.gov/api/job"
# our layer name -> (LFPS product suffix, canonical file, scale to SI, dtype)
LANDFIRE_LAYERS = {
    "fbfm40":    ("FBFM40", "fuel_model_fbfm40", None, "int16"),
    "cc":        ("CC", "canopy_cover", 0.01, "float32"),           # %        -> fraction
    "ch":        ("CH", "canopy_height", 0.1, "float32"),           # m x 10   -> m
    "cbh":       ("CBH", "canopy_base_height", 0.1, "float32"),     # m x 10   -> m
    "cbd":       ("CBD", "canopy_bulk_density", 0.01, "float32"),   # kg/m3 x 100 -> kg/m3
    "elevation": ("Elev", "elevation", 1.0, "float32"),             # m
}
# topography isn't remapped every year; LANDFIRE's current elevation product is LF 2020
LANDFIRE_TOPO_VERSION = "2020"
NONBURNABLE_FILL = 99   # NB9 (bare ground) where LANDFIRE has no fuel value


def lfps_product(layer: str, version: str) -> str:
    """'fbfm40', '2024' -> 'LF2024_FBFM40' (elevation always from LF 2020)."""
    suffix = LANDFIRE_LAYERS[layer][0]
    return f"LF{LANDFIRE_TOPO_VERSION if layer == 'elevation' else version}_{suffix}"


def run(step: dict, log) -> dict:
    """Download the case domain from LFPS, put every layer on the case grid in SI units."""
    import rasterio
    from rasterio.warp import Resampling, reproject

    case, out = step["case"], Path(step["out_dir"])
    version = str(step.get("version") or "2024")
    layers = step.get("selection", {}).get("layers") or list(LANDFIRE_LAYERS)
    email = os.environ.get("BENCH_LFPS_EMAIL")
    if not email:
        raise RuntimeError("LANDFIRE's API needs an email address: set BENCH_LFPS_EMAIL "
                           "(the notebook passes it as launch config)")
    products = [lfps_product(l, version) for l in layers]
    w, s, e, n = case["domain_wgs84"]
    pad = 0.01                                   # ~1 km so reprojection never runs off the edge
    aoi = f"{w - pad} {s - pad} {e + pad} {n + pad}"
    http = session()
    t_start = time.time()
    r = http.get(f"{LFPS}/submit", params={
        "Email": email, "Layer_List": ";".join(products), "Area_of_Interest": aoi,
        "Output_Projection": case["crs"].split(":")[-1], "Resample_Resolution": case["resolution_m"]},
        timeout=60)
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    if r.status_code != 200 or "jobId" not in body:
        raise RuntimeError(f"LFPS submit failed ({r.status_code}): {r.text[:500]}")
    job = body["jobId"]
    log(f"LFPS job {job}: {';'.join(products)} over {aoi}")
    status, last = "", ""
    while time.time() - t_start < 45 * 60:
        st = http.get(f"{LFPS}/status", params={"JobId": job}, timeout=60).json()
        status = st.get("status", "")
        if status != last:
            log(f"LFPS job {job}: {status}")
            last = status
        if "Succeeded" in status:
            break
        if "Failed" in status or "Cancel" in status:
            raise RuntimeError(f"LFPS job {job} {status}: {json.dumps(st)[:800]}")
        time.sleep(10)
    else:
        raise TimeoutError(f"LFPS job {job} still '{status}' after 45 min")
    url = st["outputFile"]
    z = zipfile.ZipFile(io.BytesIO(http.get(url, timeout=600).content))
    tif = next(n for n in z.namelist() if n.lower().endswith(".tif"))
    raw = out / "_lfps"
    raw.mkdir(parents=True, exist_ok=True)
    (raw / "lfps.tif").write_bytes(z.read(tif))

    transform, shape = case_grid(case)
    notes = {}
    with rasterio.open(raw / "lfps.tif") as src:
        if src.count != len(layers):
            raise RuntimeError(f"LFPS returned {src.count} bands for {len(layers)} layers")
        desc = [d or "" for d in src.descriptions]
        for i, layer in enumerate(layers):
            product = products[i]
            # trust band names over band order when LFPS labels them
            band = next((j + 1 for j, d in enumerate(desc) if d.upper() == product.upper()), i + 1)
            _, name, scale, dtype = LANDFIRE_LAYERS[layer]
            data = src.read(band).astype("float64")
            nod = src.nodatavals[band - 1]
            bad = ~np.isfinite(data) | (data < -1000) | ((data == nod) if nod is not None else False)
            data[bad] = np.nan
            dst = np.full(shape, np.nan)
            reproject(data, dst, src_transform=src.transform, src_crs=src.crs, src_nodata=np.nan,
                      dst_transform=transform, dst_crs=case["crs"], dst_nodata=np.nan,
                      resampling=Resampling.nearest if layer == "fbfm40" else Resampling.bilinear)
            missing = int(np.isnan(dst).sum())
            if layer == "fbfm40":
                arr = np.where(np.isnan(dst), NONBURNABLE_FILL, dst).astype(dtype)
                if missing:
                    notes["fuel_cells_filled_nonburnable"] = missing
            else:
                arr = (dst * scale).astype(dtype)
                if layer != "elevation":
                    arr = np.where(np.isnan(arr) | (arr < 0), 0.0, arr).astype(dtype)   # no canopy
                elif missing:
                    notes["elevation_cells_missing"] = missing
            write_tif(out / f"{name}.tif", arr, transform, case["crs"],
                      nodata=None if layer == "fbfm40" else np.nan)
            notes[f"band_{product}"] = desc[band - 1] or f"band {band} (by order)"
    shutil.rmtree(raw)
    fuel = np.asarray(read_tif(out / "fuel_model_fbfm40.tif"))
    codes, counts = np.unique(fuel, return_counts=True)
    top = sorted(zip(counts.tolist(), codes.tolist()), reverse=True)[:5]
    return {"lfps_job": job, "products": products, "grid": f"{shape[0]} x {shape[1]} cells",
            "download_s": round(time.time() - t_start), "top_fuel_models": str(top), **notes}
