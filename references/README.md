# References

Observed fire perimeters the cases point at (`reference.file`), one GeoJSON per
fire, in WGS84. Each file is a single observation: one time, one source (several
polygons are fine; they are unioned). Its time goes in the case as
`reference.observed_utc`, and the metrics score the prediction at that time.

| File | Fire | Observation | Source |
|---|---|---|---|
| `hughes_perimeter.geojson` | Hughes, Jan 2025 | 2025-01-22 21:04:31 UTC, 5,043 acres | FIRIS, via the WIFIRE Consolidated Fire Groupings 2025 geopackage (Perimeters id 14616) |
| `sandia_perimeter.geojson` | Sandia, Oct 2025 | 2025-10-08 00:18:00 UTC, 30.4 acres | FIRIS, same geopackage (Perimeters id 14868) |
| `madre_perimeter.geojson` | Madre, Jul 2025 | 2025-07-02 22:48:21 UTC, 8,396 acres | FIRIS, same geopackage (Perimeters id 14727) |
| `kenneth_perimeter.geojson` | Kenneth, Jan 2025 | 2025-01-10 01:24:12 UTC, 960 acres | FIRIS, same geopackage (Perimeters id 14511) |
