"""station_wind: hourly 10 m wind from the nearest ASOS station with data (weather slot).

From the Iowa Environmental Mesonet. Uniform over the domain (no WindNinja yet). Speeds
in m/s; directions are "from", rotated from true north to the case grid's north.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from blockkit import Unavailable, grid_convergence_deg, t0
from http_session import session

IEM = "https://mesonet.agron.iastate.edu"
KNOT_MS = 0.514444


def _km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km."""
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2
         + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin((lon2 - lon1) * p / 2) ** 2)
    return 12742 * math.asin(math.sqrt(a))


def run(step: dict, log) -> dict:
    """Hourly 10 m wind (m/s, direction FROM, grid north) for the case window, uniform in space."""
    case, out, p = step["case"], Path(step["out_dir"]), step["params"]
    http = session()
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
