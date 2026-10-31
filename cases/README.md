# Cases

One YAML file per fire; the file name is the case's name. A case holds the fire's
facts, never choices about how to model it (those are workflows).

| Field | Required | Meaning |
|---|---|---|
| `incident` | yes | the incident name |
| `t0` | yes | simulation clock zero, ISO 8601 UTC; every time in the framework is minutes since t0 |
| `window_h` | no (24) | hours of weather to fetch from t0 |
| `crs` | no (EPSG:5070) | the working CRS; CONUS Albers by default |
| `domain_wgs84` | no | [W, S, E, N]. Without it, the domain is the reference perimeter's bounding box + `domain_buffer_km` (default 5 km) |
| `ignitions.<name>` | no | `lat`, `lon`, `time_utc`, `source`. A workflow picks one by name (`case_ignition`, `which: <name>`); a fire without it is "unavailable" for that workflow |
| `reference` | no | `file` (a local GeoJSON, any CRS named in the file, WGS84 if not) or `url` (hosted; `sha256` pins the exact version). Without it, scores are "unavailable" |
