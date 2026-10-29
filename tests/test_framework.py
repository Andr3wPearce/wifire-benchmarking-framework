from pathlib import Path

import pytest
import yaml

from bench.overrides import apply_overrides
from bench.registry import BLOCKS, ROOT
from bench.resolver import resolve
from bench.workflows import load_spec

SPEC = ROOT / "workflows" / "hughes_ignition_comparison.yaml"


def spec(**overrides):
    return apply_overrides(load_spec(SPEC), overrides)


def test_manifests_load():
    assert {"landfire", "farsite", "elmfire", "wrf_fire", "iou"} <= set(BLOCKS)
    for name, m in BLOCKS.items():
        assert m["kind"] in {"source", "transform", "model", "reference", "metric"}, name
        assert "image" in m, name


def test_runs_and_skips():
    plan = resolve(spec())
    assert len(plan["runs"]) == 6            # farsite x2 sweep x2 ignitions + elmfire x2
    assert {s["model"] for s in plan["skipped"]} == {"wrf_fire"}
    assert all("atmos_forcing_3d" in s["reason"] for s in plan["skipped"])


def test_override_changes_runs_and_keys():
    plan = resolve(spec(farsite={"spread_adjustment": 1.0}))
    keys = {r["run_key"] for r in plan["runs"]}
    assert "farsite__spread_adjustment-1.0__ngfs_ignition" in keys
    assert len(plan["runs"]) == 4


def test_auto_transforms_inserted():
    plan = resolve(spec())
    # ignition points come in WGS84, so a reproject step is inserted before the transforms
    steps = [s["step"] for br in plan["slots"]["ignition"] for s in br["steps"]]
    assert "ignition.ngfs_ignition.auto_reproject" in steps
    # station wind converts to canonical itself (grid-north directions): no reproject step
    steps = [s["step"] for br in plan["slots"]["weather"] for s in br["steps"]]
    assert steps == ["weather.station_wind.fetch", "weather.station_wind.windninja"]


def test_bad_param_type_rejected():
    with pytest.raises(ValueError, match="expected float"):
        resolve(spec(farsite={"spread_adjustment": "banana"}))


def test_out_of_range_warns_only():
    plan = resolve(spec(farsite={"spread_adjustment": 9.0}))
    assert any("outside the usual range" in w for w in plan["warnings"])


def test_definitions_load():
    from bench.definitions import defs
    keys = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    assert "hughes_ignition_comparison/prediction" in keys
    assert "hughes_ignition_comparison/weather/station_wind__windninja" in keys


def test_conventions_catch_south_up_and_percent_cover(tmp_path):
    """A south-up raster or canopy cover in % must be rejected at the block boundary."""
    np = pytest.importorskip("numpy")
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import Affine, from_origin

    from bench.conventions import check_raster
    case = {"crs": "EPSG:5070", "resolution_m": 30}

    def write(name, arr, transform):
        p = tmp_path / f"{name}.tif"
        with rasterio.open(p, "w", driver="GTiff", height=arr.shape[0], width=arr.shape[1], count=1,
                           dtype=arr.dtype, crs="EPSG:5070", transform=transform) as d:
            d.write(arr, 1)
        return p

    north_up = from_origin(0, 3000, 30, 30)
    south_up = Affine(30, 0, 0, 0, 30, 0)                      # positive y pixel size
    ok = write("canopy_cover", np.full((10, 10), 0.4, "float32"), north_up)
    assert check_raster(ok, case) == []
    flipped = write("elevation", np.zeros((10, 10), "float32"), south_up)
    assert any("not north-up" in p for p in check_raster(flipped, case))
    percent = write("canopy_cover", np.full((10, 10), 40.0, "float32"), north_up)
    assert any("wrong units" in p for p in check_raster(percent, case))


def test_workflow_name_is_file_name_and_copies_dont_clash(tmp_path):
    copy = tmp_path / "my_copy.yaml"
    copy.write_text(SPEC.read_text() + "\nworkflow: hughes_ignition_comparison\n")
    assert load_spec(copy)["workflow"] == "my_copy"


def test_broken_workflow_says_what_is_missing(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("models: [{block: elmfire}]\n")
    with pytest.raises(ValueError, match="missing.*case"):
        load_spec(bad)
