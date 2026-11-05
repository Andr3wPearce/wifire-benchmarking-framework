# Submissions

Predictions from models we don't run, benchmarked exactly like the ones we do. A
submission is a **black box**: for each fire, the area it predicts burned at each time,
and nothing else. No model, no inputs, no settings.

```
submissions/<name>/<version>/<case name>.geojson
```

Each file is a GeoJSON FeatureCollection in WGS84 (RFC 7946). Every feature is a
Polygon or MultiPolygon with one property, `time_utc` (ISO 8601, UTC): the area burned
by then. A version is immutable once it is in S3; a change is a new version.

**Submission files are not in this repository** (they are other people's data): they
live in the S3 bucket, and `submissions/*/` is git-ignored.

How it flows:

1. `bench.submit.build_submit_file(name, version, me, out_dir)` compiles a version into
   one file whose job puts it in the S3 bucket at `<me>/bench/submissions/<name>/<version>/`
   (only step pods have S3 credentials, so the upload is itself a Dagster job).
2. A workflow reads it with the `submitted_perimeters` source and replays it with the
   `perimeter_replay` model:

```yaml
grid:
  resolution_m: 30
inputs:
  submission:
    source: {block: submitted_perimeters, params: {prefix: <me>/bench/submissions/<name>/v1}}
model: {block: perimeter_replay}
reference: {block: final_perimeter}
metrics: [{block: iou}, {block: area_bias}]
```

From there it is benchmarked like any modeled workflow: same references, same metrics,
same table. A cell's arrival time is the first perimeter that contains it, so arrival
times are as coarse as the submission's time step (reported in its summary).
