# Canonical conventions (hard rules)

Every artifact that crosses a block boundary follows these rules. Sources
convert *into* them; model adapters convert *out of* them into whatever their
model wants (and back). Nothing in between guesses.

The framework checks every raster a block writes against the machine-checkable
parts (`src/bench/conventions.py`: orientation, CRS, the exact case grid,
resolution, plausible value ranges per variable) before the artifact is
accepted, and fails the step if any is violated. Since every block's inputs are
some earlier block's accepted outputs, nothing that breaks the conventions is
ever read.

## 1. Space

| Rule | Value |
|---|---|
| CRS | the case's working CRS (`case.crs`, e.g. EPSG:5070); projected, **metres** |
| Grid | the case grid: one origin, one resolution (`case.resolution_m`), square cells |
| Orientation | **north-up**: row 0 is the **northern** edge, column 0 the **western** edge |
| Affine transform | `(res, 0, x_min, 0, -res, y_max)`: no rotation/shear, **negative y pixel size** |
| Origin | the transform's origin is the **top-left (NW) corner** of the top-left cell: (min x, max y) |
| Index -> coordinates | cell `(row, col)` covers x ∈ [x_min + col·res, x_min + (col+1)·res], y ∈ [y_max − (row+1)·res, y_max − row·res]; values describe the **cell area** (pixel-is-area), centre at +½ cell |
| Axes | x increases **east** with column; y increases **north** as row **decreases** |
| "South" | the last row (`row = n_rows − 1`), i.e. min y |

So the corners are: NW = (min x, max y) = row 0, col 0; SE = (max x, min y) =
last row, last col.

## 2. Directions

All directions are **azimuths in degrees, clockwise from grid north** of the
case CRS, in [0, 360).

| Quantity | Convention |
|---|---|
| Wind direction | **meteorological "from"**: the direction the wind blows **from** (a SW wind = 225°, pushes fire NE) |
| Aspect (if ever stored) | the direction the slope **faces** (downslope), same azimuth convention; flat = NaN |
| Spread / heading direction (outputs) | the direction the fire **moves toward** |

**Grid north vs true north.** Source data in geographic coordinates (station
wind, GRIB, etc.) is referenced to **true** north. Sources must rotate
directions by the meridian convergence angle at each cell when converting to
the case CRS (in EPSG:5070 this is several degrees away from the central
meridian, enough to matter for wind).

## 3. Units (SI)

| Quantity | Unit |
|---|---|
| Lengths, heights, elevation | m |
| Speeds (wind, spread) | m/s for wind at 10 m; spread rate m/min |
| Canopy cover | fraction 0–1 (not %) |
| Canopy bulk density | kg/m³ (LANDFIRE's ×100 scaling removed by the source) |
| Canopy height, base height | m (LANDFIRE's ×10 scaling removed by the source) |
| Fuel moisture | fraction (kg water / kg dry fuel), not % |
| Fireline intensity | kW/m |
| Area | ha in summaries; geometry stays in CRS metres |
| Slope (if ever stored) | rise/run (not degrees, not %) |

Slope and aspect are **not** exchanged between blocks: each model adapter
derives them from the DEM on the working grid.

## 4. Time

| Rule | Value |
|---|---|
| Timestamps | ISO 8601, **UTC** |
| Simulation time | minutes since the case's ignition time t0 |
| Arrival time rasters | minutes since t0; **NaN = never burned** |

## 5. Values and files

| Rule | Value |
|---|---|
| Rasters | single-band GeoTIFF, one variable per file, named as in `docs/block_contract.md` |
| Float nodata | NaN |
| Integer nodata | declared in the file's nodata tag; never a valid code |
| Fuel models | FBFM40 numeric codes (91–204); FBFM13 as 1–13 in a separately named layer |
| Vectors | GeoJSON in the case CRS (`crs` member set) |
| Categorical regrid | majority/nearest; continuous: mean; directions: vector mean |

## Where model quirks live

In the model's block adapter, never upstream. Known so far:

| Model | Quirk | Adapter does |
|---|---|---|
| pyretechnics (ELMFIRE) | row 0 = **south** (`y+1` is north) | flips every grid in, flips outputs back |
| pyretechnics (ELMFIRE) | wind in km/h, `upwind_direction` (= "from") | ×3.6; direction passes through |
