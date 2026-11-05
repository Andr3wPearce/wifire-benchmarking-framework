"""submitted_perimeters: a submitted prediction (timed perimeters) from S3 (submission slot).

The submission format is the whole interface for a model we don't run: one GeoJSON
FeatureCollection per fire, in WGS84 (RFC 7946), each feature a Polygon or MultiPolygon
with `properties.time_utc` (ISO 8601, UTC): the area the fire had burned by then.
Nothing else is read, so nothing else needs to be disclosed.

Writes perimeters.geojson (case CRS, `time_min` since t0, sorted by time).
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

from blockkit import Unavailable, t0


def run(step: dict, log) -> dict:
    from botocore.exceptions import ClientError
    from rasterio.warp import transform_geom
    import s3_store

    case, out = step["case"], Path(step["out_dir"])
    prefix = step["params"]["prefix"].strip("/")
    if not prefix:
        raise ValueError("submitted_perimeters needs params.prefix (where the submission is in S3)")
    bucket, key = s3_store.bucket(), f"{prefix}/{case['name']}.geojson"
    uri = f"s3://{bucket}/{key}"
    try:
        body = s3_store.client().get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
            raise Unavailable(f"no submission for this fire at {uri}")
        raise
    log(f"read {uri} ({len(body):,} bytes)")

    fc = json.loads(body)
    zero = t0(case)
    feats = []
    for i, f in enumerate(fc.get("features", [])):
        geom, when = f.get("geometry") or {}, (f.get("properties") or {}).get("time_utc")
        if geom.get("type") not in ("Polygon", "MultiPolygon") or not when:
            raise ValueError(f"{uri}: feature {i} needs a Polygon/MultiPolygon and properties.time_utc")
        minutes = (datetime.fromisoformat(when.replace("Z", "+00:00")) - zero).total_seconds() / 60
        feats.append({"type": "Feature", "properties": {"time_min": round(minutes, 2), "time_utc": when},
                      "geometry": transform_geom("EPSG:4326", case["crs"], geom)})
    if not feats:
        raise ValueError(f"{uri}: no perimeters")
    feats.sort(key=lambda f: f["properties"]["time_min"])
    out.mkdir(parents=True, exist_ok=True)
    (out / "perimeters.geojson").write_text(json.dumps(
        {"type": "FeatureCollection", "crs": case["crs"], "features": feats}))
    times = [f["properties"]["time_min"] for f in feats]
    steps = sorted({round(b - a, 1) for a, b in zip(times, times[1:])})
    return {"submission": uri, "sha256": hashlib.sha256(body).hexdigest(), "perimeters": len(feats),
            "first_min": times[0], "last_min": times[-1],
            "time_step_min": steps[0] if len(steps) == 1 else (steps or "single perimeter"),
            "model_inputs": "not disclosed (a submitted prediction)"}
