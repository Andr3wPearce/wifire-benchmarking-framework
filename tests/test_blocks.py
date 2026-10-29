"""The blocks themselves, chained on a synthetic landscape (no network): roads override,
ignition + reproject, moisture, ELMFIRE with hourly station-style wind, the reference,
IoU and area bias, and the conventions check on every artifact.

Needs the blocks' packages (images/wstc/*/requirements.txt); skipped otherwise.
"""
import json
import sys
from pathlib import Path

import pytest

np = pytest.importorskip("numpy")
pytest.importorskip("rasterio")

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src" / "bench"), str(ROOT / "images" / "core"), str(ROOT / "images" / "elmfire")]
import conventions                    # noqa: E402  (shipped next to the blocks in every step)
import core_blocks as cb              # noqa: E402

CASE = {"incident": "TEST", "crs": "EPSG:5070", "resolution_m": 30, "t0": "2025-01-22T18:53:00Z",
        "window_h": 6, "domain_wgs84": [-118.66, 34.50, -118.50, 34.62]}
IGNITION = (34.553008, -118.56687)


def run_block(block, out, **kw):
    """Run a core block the way a step does, then check its artifact like the framework does."""
    summary = cb.execute({"block": block, "case": CASE, "out_dir": str(out), **kw}, lambda *_: None)
    assert conventions.check_dir(out, CASE) == [], block
    return summary


@pytest.fixture
def landscape(tmp_path):
    """A LANDFIRE-shaped landscape on the case grid: shrub/grass-shrub, a road column, a lake."""
    transform, shape = conventions.case_grid(CASE)
    rng = np.random.default_rng(0)
    fuel = np.where(rng.random(shape) < 0.6, 142, 122).astype("int16")
    fuel[:, 100:103] = 91                                  # road (NB1)
    fuel[200:240, 200:260] = 98                            # water (NB8): must stay non-burnable
    d = tmp_path / "landfire"
    d.mkdir()
    zeros = np.zeros(shape, "float32")
    for name, arr in {"fuel_model_fbfm40": fuel, "elevation": np.full(shape, 500, "float32"),
                      "canopy_cover": zeros, "canopy_height": zeros, "canopy_base_height": zeros,
                      "canopy_bulk_density": zeros}.items():
        cb.write_tif(d / f"{name}.tif", arr, transform, CASE["crs"])
    return d


def test_case_grid_is_snapped_north_up_and_covers_the_domain():
    transform, (rows, cols) = conventions.case_grid(CASE)
    assert transform.a == 30 and transform.e == -30
    assert transform.c % 30 == 0 and transform.f % 30 == 0
    assert 400 < rows < 600 and 400 < cols < 700


def test_roads_override_replaces_roads_but_not_water(tmp_path, landscape):
    s = run_block("roads_override", tmp_path / "roads", params={"replacement_fuel": "GR2"},
                  inputs={"in": str(landscape)})
    fuel = cb._read(tmp_path / "roads" / "fuel_model_fbfm40.tif")
    assert s["cells_changed"] > 0 and not np.isin(fuel, [91]).any()
    assert (fuel == 98).sum() == 40 * 60                   # the lake is untouched


def test_wind_directions_rotate_from_true_to_grid_north():
    # west of EPSG:5070's central meridian (96 W), true north points east of grid north
    assert 2 < cb.grid_convergence_deg(-118.57, 34.55, "EPSG:5070") < 15
    assert abs(cb.grid_convergence_deg(-96.0, 34.55, "EPSG:5070")) < 0.01


def test_ignition_is_reprojected_and_timed_from_t0(tmp_path):
    raw = tmp_path / "ign_raw"
    s = run_block("triangulated_ignition", raw, params={"lat": IGNITION[0], "lon": IGNITION[1],
                  "time_utc": "2025-01-22T19:23:00Z", "source": "test"})
    assert s["time_min"] == 30.0
    run_block("reproject", tmp_path / "ign", inputs={"in": str(raw)})
    fc = json.loads((tmp_path / "ign" / "ignition.geojson").read_text())
    assert fc["crs"] == "EPSG:5070" and abs(fc["features"][0]["geometry"]["coordinates"][0]) > 1e5


def test_chain_elmfire_spreads_downwind_and_is_scored(tmp_path, landscape):
    eb = pytest.importorskip("elmfire_block")
    roads = tmp_path / "roads"
    run_block("roads_override", roads, params={"replacement_fuel": "GR2"}, inputs={"in": str(landscape)})
    run_block("triangulated_ignition", tmp_path / "ign_raw",
              params={"lat": IGNITION[0], "lon": IGNITION[1], "time_utc": CASE["t0"], "source": "test"})
    run_block("reproject", tmp_path / "ign", inputs={"in": str(tmp_path / "ign_raw")})
    run_block("fixed_moisture", tmp_path / "moist", params={"dead_1h": 0.04})
    # station-style wind: uniform, hourly, a Santa Ana from the NE (40 deg, grid north)
    wx = tmp_path / "wx"
    wx.mkdir()
    hours = [{"time_utc": f"2025-01-22T{18 + h:02d}:00:00Z", "wind_speed_10m": 9.0,
              "wind_from_direction": 40.0} for h in range(6)]
    (wx / "wind_series.json").write_text(json.dumps({"station": "TEST", "hours": hours}))

    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(json.dumps({
        "model": "elmfire", "case": CASE,
        "params": {"spread_adjustment": 1.5, "duration_min": 180, "band_duration_min": 60},
        "inputs": {"landscape": str(roads), "weather": str(wx), "moisture": str(tmp_path / "moist"),
                   "ignition": str(tmp_path / "ign")}}))
    summary = eb.execute(run_dir, lambda *_: None, root=Path("/"))
    pred = run_dir / "prediction"
    assert conventions.check_dir(pred, CASE) == []
    assert summary["burned_area_ha"] > 10

    # a NE wind pushes the fire SW of the ignition
    with __import__("rasterio").open(pred / "time_of_arrival.tif") as src:
        toa, t = src.read(1), src.transform
    rows, cols = np.nonzero(np.isfinite(toa))
    x, y = t * (cols.mean(), rows.mean())
    ix, iy = json.loads((tmp_path / "ign" / "ignition.geojson").read_text())["features"][0]["geometry"]["coordinates"]
    assert x < ix and y < iy

    # scored against a reference that covers the burned area: perfect recall, IoU in (0, 1]
    ref = tmp_path / "ref.geojson"
    lat, lon = IGNITION
    ref.write_text(json.dumps({"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {},
        "geometry": {"type": "Polygon", "coordinates": [[[lon - 0.05, lat - 0.05], [lon + 0.01, lat - 0.05],
                     [lon + 0.01, lat + 0.01], [lon - 0.05, lat + 0.01], [lon - 0.05, lat - 0.05]]]}}]}))
    run_block("final_perimeter", tmp_path / "reference", inputs={"file": str(ref)})
    score_in = {"prediction": str(pred), "reference": str(tmp_path / "reference")}
    iou = run_block("iou", tmp_path / "iou", inputs=score_in)
    bias = run_block("area_bias", tmp_path / "bias", inputs=score_in)
    assert 0 < iou["iou"] <= 1
    assert iou["intersection_cells"] == summary["burned_cells"]
    assert bias["predicted_ha"] == summary["burned_area_ha"] and bias["area_bias"] < 0
