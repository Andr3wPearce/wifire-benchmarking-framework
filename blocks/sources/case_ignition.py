"""case_ignition: one of the case's ignition points (ignition slot).

`params.which` names the point in `case.ignitions` (lat, lon, time_utc, source). Writes
it in WGS84 with `time_min` since t0; the resolver adds the reproject step to the case
CRS. Unavailable if the case doesn't have that point.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from blockkit import Unavailable, t0


def run(step: dict, log) -> dict:
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
