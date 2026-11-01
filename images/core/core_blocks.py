"""The framework's Python-only blocks: sources, transforms, the reference and metrics.

Each block is one function `block(step, log) -> summary`. The framework hands it a
`step` dict (see docs/block_contract.md, "Step contract"):

    {"block": "landfire", "kind": "source", "case": {...}, "params": {...},
     "selection": {...}, "version": "2024",
     "inputs": {"in": <dir of the previous step in the chain>, <slot>: <dir>, ...},
     "out_dir": <dir to write>}

and the block writes canonical artifacts (docs/conventions.md) into `out_dir`. `execute`
adds `provenance.json` (what ran, with which parameters, and the block's summary), and
the framework then checks the rasters against the conventions before accepting them.

Models (ELMFIRE, ...) are not here: each model has its own adapter and image.
Dependencies: numpy, rasterio, requests (images/wstc/core).

Runs standalone for debugging:  PYTHONPATH=src/bench python core_blocks.py <step.json>
"""
from __future__ import annotations

import io
import json
import math
import os
import shutil
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

UA = {"User-Agent": "scil-bench (UCSD SCIL wildfire spread-model benchmarking)"}
UNAVAILABLE_EXIT = 3      # exit code meaning "this fire doesn't have what the block needs"


class Unavailable(Exception):
    """Raised by a block when the fire lacks what it needs (no NGFS detection, no weather
    station in range, no reference perimeter). Not an error: the fire's row in the
    workflow's table says "unavailable: <reason>", and the steps after it are skipped."""


# --------------------------------------------------------------------------- #
# shared helpers
# --------------------------------------------------------------------------- #
# the case grid is a convention, so it lives with the conventions (src/bench/conventions.py,
# shipped next to this file in every step)
from conventions import case_grid  # noqa: E402


def t0(case: dict) -> datetime:
    return datetime.fromisoformat(case["t0"].replace("Z", "+00:00")).astimezone(timezone.utc)


def write_tif(path: Path, arr: np.ndarray, transform, crs: str, nodata=None) -> None:
    import rasterio
    with rasterio.open(path, "w", driver="GTiff", height=arr.shape[0], width=arr.shape[1], count=1,
                       dtype=arr.dtype, crs=crs, transform=transform, nodata=nodata,
                       compress="deflate", tiled=True) as dst:
        dst.write(arr, 1)


def _http():
    """A requests session that retries: public services (the Iowa Mesonet, LFPS) time out
    or return 5xx now and then, and a blip shouldn't fail the run. Up to 4 retries per
    request (timeouts, dropped connections, 429/5xx), waiting 10, 20, 40 s in between.
    Not part of any artifact's identity: retrying doesn't change what is produced."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    retry = Retry(total=4, connect=4, read=4, status=4, backoff_factor=5,
                  status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET", "POST"))
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers.update(UA)
    return s


def _copy_tree(src: Path, dst: Path, skip=("provenance.json",)) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for f in Path(src).iterdir():
        if f.is_file() and f.name not in skip:
            shutil.copy2(f, dst / f.name)


# --------------------------------------------------------------------------- #
# landscape: LANDFIRE via the LANDFIRE Product Service (LFPS)
# --------------------------------------------------------------------------- #
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
    suffix = LANDFIRE_LAYERS[layer][0]
    return f"LF{LANDFIRE_TOPO_VERSION if layer == 'elevation' else version}_{suffix}"


def landfire(step: dict, log) -> dict:
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
    http = _http()
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
    fuel = np.asarray(_read(out / "fuel_model_fbfm40.tif"))
    codes, counts = np.unique(fuel, return_counts=True)
    top = sorted(zip(counts.tolist(), codes.tolist()), reverse=True)[:5]
    return {"lfps_job": job, "products": products, "grid": f"{shape[0]} x {shape[1]} cells",
            "download_s": round(time.time() - t_start), "top_fuel_models": str(top), **notes}


def _read(path: Path):
    import rasterio
    with rasterio.open(path) as src:
        return src.read(1)


# --------------------------------------------------------------------------- #
# landscape transform: roads / urban -> a burnable fuel model
# --------------------------------------------------------------------------- #
FUEL_FAMILIES = {"NB": 90, "GR": 100, "GS": 120, "SH": 140, "TU": 160, "TL": 180, "SB": 200}
ROADS_URBAN_CODES = (91, 93, 99)   # FBFM40 non-burnable codes that are not snow/ice (92) or water (98)


def fuel_code(name: str) -> int:
    """'GR2' -> 102 (Scott & Burgan 2005 numbering)."""
    return FUEL_FAMILIES[name[:2].upper()] + int(name[2:])


def roads_override(step: dict, log) -> dict:
    out, src = Path(step["out_dir"]), Path(step["inputs"]["in"])
    _copy_tree(src, out)
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


# --------------------------------------------------------------------------- #
# weather: hourly wind from the nearest ASOS station (Iowa Environmental Mesonet)
# --------------------------------------------------------------------------- #
IEM = "https://mesonet.agron.iastate.edu"
KNOT_MS = 0.514444


def _km(lat1, lon1, lat2, lon2):
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2
         + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin((lon2 - lon1) * p / 2) ** 2)
    return 12742 * math.asin(math.sqrt(a))


def grid_convergence_deg(lon: float, lat: float, crs: str) -> float:
    """Azimuth of TRUE north measured clockwise from GRID north, at (lon, lat).
    A true azimuth a is the grid azimuth a + this (docs/conventions.md, section 2)."""
    from rasterio.warp import transform
    xs, ys = transform("EPSG:4326", crs, [lon, lon], [lat, lat + 0.01])
    return math.degrees(math.atan2(xs[1] - xs[0], ys[1] - ys[0]))


def station_wind(step: dict, log) -> dict:
    """Hourly 10 m wind (m/s, direction FROM, grid north) for the case window, uniform in space."""
    case, out, p = step["case"], Path(step["out_dir"]), step["params"]
    http = _http()
    w, s, e, n = case["domain_wgs84"]
    lat0, lon0 = (s + n) / 2, (w + e) / 2
    network = p.get("network") or "CA_ASOS"
    stations = http.get(f"{IEM}/geojson/network/{network}.geojson", timeout=120).json()["features"]
    cands = []
    for f in stations:
        lon, lat = f["geometry"]["coordinates"][:2]
        sid = f["properties"].get("sid") or f.get("id")
        cands.append((_km(lat0, lon0, lat, lon), sid, f["properties"].get("sname", ""), lat, lon))
    cands.sort()
    if p.get("station"):
        cands = [c for c in cands if c[1] == p["station"]] or cands
    start = t0(case).replace(minute=0, second=0, microsecond=0)
    hours = int(case.get("window_h", 24))
    end = start + timedelta(hours=hours + 1)
    for dist, sid, sname, slat, slon in cands[:5]:
        if dist > float(p.get("max_km", 80)):
            break
        q = {"station": sid, "data": ["sknt", "drct", "gust"], "tz": "Etc/UTC", "format": "onlycomma",
             "latlon": "no", "missing": "M", "report_type": [3, 4],
             "year1": start.year, "month1": start.month, "day1": start.day, "hour1": start.hour,
             "year2": end.year, "month2": end.month, "day2": end.day, "hour2": end.hour}
        r = http.get(f"{IEM}/cgi-bin/request/asos.py", params=q, timeout=120)
        obs = []
        for line in r.text.strip().splitlines()[1:]:
            parts = line.split(",")
            if len(parts) < 5 or "M" in (parts[2], parts[3]):
                continue
            ts = datetime.strptime(parts[1], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            obs.append((ts, float(parts[2]) * KNOT_MS, float(parts[3]),
                        None if parts[4] == "M" else float(parts[4]) * KNOT_MS))
        if len(obs) >= hours // 2:
            break
        log(f"{sid}: only {len(obs)} usable reports, trying the next station")
    else:
        raise Unavailable(f"no {network} station within {p.get('max_km', 80)} km has wind for the window")

    conv = grid_convergence_deg(slon, slat, case["crs"])
    series = []
    for h in range(hours + 1):
        a, b = start + timedelta(hours=h - 0.5), start + timedelta(hours=h + 0.5)
        win = [o for o in obs if a <= o[0] < b] or [min(obs, key=lambda o: abs((o[0] - start - timedelta(hours=h)).total_seconds()))]
        spd = float(np.mean([o[1] for o in win]))
        # vector-mean the direction (calm reports carry no direction)
        u = sum(o[1] * math.sin(math.radians(o[2])) for o in win)
        v = sum(o[1] * math.cos(math.radians(o[2])) for o in win)
        true_from = (math.degrees(math.atan2(u, v)) + 360) % 360 if (u or v) else 0.0
        gust = [o[3] for o in win if o[3] is not None]
        series.append({"time_utc": (start + timedelta(hours=h)).isoformat().replace("+00:00", "Z"),
                       "wind_speed_10m": round(spd, 2),
                       "wind_from_direction": round((true_from + conv) % 360, 1),
                       "gust_10m": round(max(gust), 2) if gust else None, "reports": len(win)})
    out.mkdir(parents=True, exist_ok=True)
    (out / "wind_series.json").write_text(json.dumps({
        "uniform_in_space": True, "band_min": 60, "t0_utc": case["t0"],
        "first_hour_utc": series[0]["time_utc"],
        "station": sid, "station_name": sname, "station_lat": slat, "station_lon": slon,
        "distance_km": round(dist, 1), "grid_convergence_deg": round(conv, 2),
        "units": {"wind_speed_10m": "m/s", "wind_from_direction": "deg from grid north (from)"},
        "hours": series}, indent=1))
    spd = [h["wind_speed_10m"] for h in series]
    return {"station": f"{sid} {sname} ({dist:.0f} km)", "reports": len(obs), "hours": len(series),
            "mean_wind_ms": round(float(np.mean(spd)), 1), "max_wind_ms": round(max(spd), 1),
            "first_hour_from_deg": series[0]["wind_from_direction"],
            "grid_convergence_deg": round(conv, 2)}


# --------------------------------------------------------------------------- #
# moisture: fixed fuel moisture
# --------------------------------------------------------------------------- #
def fixed_moisture(step: dict, log) -> dict:
    out = Path(step["out_dir"])
    out.mkdir(parents=True, exist_ok=True)
    (out / "fuel_moisture.json").write_text(json.dumps(step["params"]))
    return dict(step["params"])


# --------------------------------------------------------------------------- #
# ignition: a point (lat/lon, UTC time) from an external record
# --------------------------------------------------------------------------- #
def case_ignition(step: dict, log) -> dict:
    """One of the case's ignition points (case.ignitions.<which>: lat, lon, time_utc, source),
    in WGS84; the resolver adds the reproject step. Unavailable if the case doesn't have it."""
    out, which, case = Path(step["out_dir"]), step["params"]["which"], step["case"]
    p = case.get("ignitions", {}).get(which)
    if not p:
        raise Unavailable(f"case {case.get('name', case['incident'])} has no '{which}' ignition")
    when = datetime.fromisoformat(p["time_utc"].replace("Z", "+00:00"))
    minutes = (when - t0(case)).total_seconds() / 60
    out.mkdir(parents=True, exist_ok=True)
    (out / "ignition.geojson").write_text(json.dumps({
        "type": "FeatureCollection", "crs": "EPSG:4326",
        "features": [{"type": "Feature",
                      "properties": {"time_min": minutes, "time_utc": p["time_utc"], "source": p.get("source", "")},
                      "geometry": {"type": "Point", "coordinates": [p["lon"], p["lat"]]}}]}))
    return {"which": which, "lat": p["lat"], "lon": p["lon"], "time_utc": p["time_utc"],
            "time_min": round(minutes, 1), "source": p.get("source", "")}


# --------------------------------------------------------------------------- #
# auto steps the resolver inserts
# --------------------------------------------------------------------------- #
def reproject(step: dict, log) -> dict:
    """Vectors (GeoJSON) -> case CRS; rasters -> case grid; everything else copied."""
    from rasterio.warp import transform_geom
    src, out, case = Path(step["inputs"]["in"]), Path(step["out_dir"]), step["case"]
    _copy_tree(src, out)
    done = []
    for f in sorted(out.glob("*.geojson")):
        fc = json.loads(f.read_text())
        frm = fc.get("crs") or "EPSG:4326"
        if isinstance(frm, dict):                       # old-style named CRS member
            frm = frm.get("properties", {}).get("name", "EPSG:4326")
        if frm != case["crs"]:
            for feat in fc["features"]:
                feat["geometry"] = transform_geom(frm, case["crs"], feat["geometry"])
            fc["crs"] = case["crs"]
            f.write_text(json.dumps(fc))
            done.append(f"{f.name}: {frm} -> {case['crs']}")
    if any(out.glob("*.tif")):
        raise NotImplementedError("raster reprojection: no real block needs it yet")
    return {"reprojected": done or "nothing to do"}


# --------------------------------------------------------------------------- #
# reference: the observed final perimeter, rasterized onto the case grid
# --------------------------------------------------------------------------- #
def final_perimeter(step: dict, log) -> dict:
    """The case's reference perimeter (GeoJSON), rasterized onto the case grid. It comes
    from the case: `reference.file` (shipped with the compiled workflow) or `reference.url`
    (downloaded here, and checked against `reference.sha256` when the case pins one)."""
    import hashlib
    from rasterio.features import rasterize
    from rasterio.warp import transform_geom
    out, case = Path(step["out_dir"]), step["case"]
    ref = case.get("reference") or {}
    if step.get("inputs", {}).get("file"):
        text = Path(step["inputs"]["file"]).read_text()
    elif ref.get("url"):
        r = _http().get(ref["url"], timeout=120)
        r.raise_for_status()
        text = r.text
        digest = hashlib.sha256(r.content).hexdigest()
        if ref.get("sha256") and ref["sha256"] != digest:
            raise RuntimeError(f"{ref['url']} has changed (sha256 {digest[:12]} != pinned "
                               f"{ref['sha256'][:12]}): a new perimeter means new scores; update the case")
    else:
        raise Unavailable(f"case {case.get('name', case['incident'])} has no reference perimeter")
    fc = json.loads(text)
    frm = fc.get("crs") or "EPSG:4326"
    if isinstance(frm, dict):
        frm = frm.get("properties", {}).get("name", "EPSG:4326")
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


# --------------------------------------------------------------------------- #
# metrics: prediction vs reference on the case grid
# --------------------------------------------------------------------------- #
def _masks(step: dict):
    """(predicted burned, observed burned, scored_at_min) on the case grid.

    If the case's reference says when it was observed (`reference.observed_utc`), the
    prediction is taken *at that time* (arrival time <= observed - t0), so a perimeter
    from hour 2 is compared with the model's hour 2, not with everything it burned. A
    simulation that ended before the observation can't be scored: unavailable."""
    pred, ref = Path(step["inputs"]["prediction"]), Path(step["inputs"]["reference"])
    toa = _read(pred / "time_of_arrival.tif")
    burned_ref = _read(ref / "burned.tif").astype(bool)
    if toa.shape != burned_ref.shape:
        raise ValueError(f"prediction grid {toa.shape} != reference grid {burned_ref.shape}")
    observed = (step["case"].get("reference") or {}).get("observed_utc")
    if not observed:
        return np.isfinite(toa), burned_ref, None
    at = (datetime.fromisoformat(observed.replace("Z", "+00:00")) - t0(step["case"])).total_seconds() / 60
    summary = pred / "summary.json"
    ends = json.loads(summary.read_text()).get("stop_time_min") if summary.exists() else None
    if ends is not None and ends < at - 1:
        raise Unavailable(f"the simulation ends at {ends:.0f} min, the reference is observed at {at:.0f} min "
                          "after t0; give the workflow a longer duration_min")
    return np.isfinite(toa) & (toa <= at), burned_ref, round(at, 1)


def _write_score(step: dict, score: dict) -> dict:
    out = Path(step["out_dir"])
    out.mkdir(parents=True, exist_ok=True)
    (out / "score.json").write_text(json.dumps(score, indent=1))
    return score


def iou(step: dict, log) -> dict:
    a, b, at = _masks(step)
    union = int((a | b).sum())
    return _write_score(step, {"iou": round(float((a & b).sum()) / union, 4) if union else 0.0,
                               "intersection_cells": int((a & b).sum()), "union_cells": union,
                               "scored_at_min": at if at is not None else "end of simulation"})


def area_bias(step: dict, log) -> dict:
    a, b, at = _masks(step)
    cell_ha = step["case"]["resolution_m"] ** 2 / 10_000
    pred_ha, ref_ha = float(a.sum()) * cell_ha, float(b.sum()) * cell_ha
    return _write_score(step, {"area_bias": round((pred_ha - ref_ha) / ref_ha, 4) if ref_ha else None,
                               "predicted_ha": round(pred_ha, 1), "observed_ha": round(ref_ha, 1),
                               "scored_at_min": at if at is not None else "end of simulation"})


BLOCKS = {
    "landfire": landfire, "roads_override": roads_override, "station_wind": station_wind,
    "fixed_moisture": fixed_moisture, "case_ignition": case_ignition,
    "reproject": reproject, "final_perimeter": final_perimeter,
    "iou": iou, "area_bias": area_bias,
}


def execute(step: dict, log=print) -> dict:
    """Run one block and write its provenance. An Unavailable block still writes provenance
    (with the reason) and re-raises, so the caller can record it as unavailable."""
    t = time.time()
    out = Path(step["out_dir"])
    prov = {"block": step["block"], "version": step.get("version"),
            "params": step.get("params", {}), "selection": step.get("selection", {})}
    try:
        summary = BLOCKS[step["block"]](step, log)
    except Unavailable as e:
        out.mkdir(parents=True, exist_ok=True)
        (out / "provenance.json").write_text(json.dumps({**prov, "unavailable": str(e)}, indent=1, default=str))
        raise
    out.mkdir(parents=True, exist_ok=True)
    (out / "provenance.json").write_text(json.dumps(
        {**prov, "seconds": round(time.time() - t, 1), "summary": summary}, indent=1, default=str))
    return summary


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python core_blocks.py <step.json>")
    try:
        print(json.dumps(execute(json.loads(Path(sys.argv[1]).read_text())), indent=1, default=str))
    except Unavailable as e:
        print(f"unavailable: {e}")
        sys.exit(UNAVAILABLE_EXIT)
