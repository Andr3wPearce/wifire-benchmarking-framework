# TEMP(wstc-minimal): one-off probe of what the real source blocks need from a WSTC step pod:
#   - can a step reach LANDFIRE (LFPS) and Iowa Mesonet (ASOS station wind)?
#   - which LANDFIRE layer names exist for the fuel / canopy / elevation layers?
#   - which ASOS stations are near the Hughes origin, and what does their data look like?
#   - can we run programs from the shared volume (for a cached Python environment)?
# Reads only public endpoints; records small samples as metadata. Remove after use.
import math
import os
import re
import stat
import subprocess
import time
from pathlib import Path

import dagster as dg
import requests

ME = "apearce"
ORIGIN = (34.553008, -118.56687)          # Hughes Fire, CAL FIRE incident page
UA = {"User-Agent": "scil-bench-probe (UCSD SCIL wildfire benchmarking)"}


def _get(url, **kw):
    t = time.time()
    try:
        r = requests.get(url, headers=UA, timeout=30, **kw)
        return r, f"{r.status_code} in {time.time() - t:.1f}s"
    except Exception as e:
        return None, f"no ({type(e).__name__}: {str(e)[:120]})"


def _km(lat1, lon1, lat2, lon2):
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2
         + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin((lon2 - lon1) * p / 2) ** 2)
    return 12742 * math.asin(math.sqrt(a))


@dg.asset(key_prefix=ME, group_name=f"{ME}_data_probe")
def data_probe(context: dg.AssetExecutionContext) -> dict:
    info = {}

    # --- LANDFIRE Product Service ---
    r, info["lfps_health"] = _get("https://lfps.usgs.gov/api/healthCheck")
    if r is not None:
        info["lfps_health_body"] = r.text[:200]
    names = set()
    for url in ["https://lfps.usgs.gov/api/products", "https://lfps.usgs.gov/products"]:
        r, info[f"get {url}"] = _get(url)
        if r is not None and r.ok:
            names |= set(re.findall(r"\bLF20\d\d_[A-Za-z0-9]+\b", r.text))
    want = re.compile(r"_(FBFM40|CC|CH|CBH|CBD|Elev|ELEV)$")
    info["lfps_layers_wanted"] = ", ".join(sorted(n for n in names if want.search(n))) or "none found"
    info["lfps_layer_count"] = len(names)

    # --- Iowa Environmental Mesonet: ASOS stations near the origin ---
    r, info["iem_network"] = _get("https://mesonet.agron.iastate.edu/geojson/network/CA_ASOS.geojson")
    nearest = []
    if r is not None and r.ok:
        for f in r.json().get("features", []):
            lon, lat = f["geometry"]["coordinates"][:2]
            p = f.get("properties", {})
            nearest.append((_km(*ORIGIN, lat, lon), p.get("sid") or f.get("id"), p.get("sname"),
                            p.get("elevation"), p.get("archive_begin")))
        nearest.sort()
        info["iem_nearest"] = "; ".join(f"{s} {n} {d:.0f} km elev {e} since {b}"
                                         for d, s, n, e, b in nearest[:6])
    for _, sid, *_ in nearest[:3]:
        r, info[f"iem_{sid}"] = _get(
            "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py",
            params={"station": sid, "data": ["sknt", "drct", "gust"], "tz": "Etc/UTC",
                    "year1": 2025, "month1": 1, "day1": 22, "hour1": 18,
                    "year2": 2025, "month2": 1, "day2": 23, "hour2": 6,
                    "format": "onlycomma", "latlon": "yes", "missing": "M", "report_type": [3, 4]})
        if r is not None and r.ok:
            info[f"iem_{sid}_sample"] = " | ".join(r.text.strip().splitlines()[:8])

    # --- shared volume: mount flags, and can we execute from it? ---
    try:
        line = next(l for l in open("/proc/mounts") if l.split()[1] == "/opt/dagster/shared")
        info["shared_mount"] = " ".join(line.split()[:4])[:300]
    except Exception as e:
        info["shared_mount"] = f"? {e}"
    d = Path("/opt/dagster/shared/storage") / ME / "_probe"
    try:
        d.mkdir(parents=True, exist_ok=True)
        s = d / "exec_test.sh"
        s.write_text("#!/bin/sh\necho exec-ok\n")
        s.chmod(s.stat().st_mode | stat.S_IEXEC)
        out = subprocess.run([str(s)], capture_output=True, text=True, timeout=20)
        info["shared_exec"] = (out.stdout.strip() or out.stderr.strip())[:200]
        s.unlink()
    except Exception as e:
        info["shared_exec"] = f"no ({type(e).__name__}: {e})"[:200]
    info["pod_tmp_free_gb"] = round(os.statvfs("/tmp").f_bavail * os.statvfs("/tmp").f_frsize / 1e9, 1)

    for k, v in info.items():
        context.log.info(f"{k}: {v}")
    context.add_output_metadata({k: str(v) for k, v in info.items()})
    return info


defs = dg.Definitions(
    assets=[data_probe],
    jobs=[dg.define_asset_job(f"{ME}_data_probe_job", selection=dg.AssetSelection.groups(f"{ME}_data_probe"))],
)
