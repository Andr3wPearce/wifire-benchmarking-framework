"""The framework itself: registry, workflows, resolver, identities, compiled WSTC file,
conventions. Needs pyyaml; the conventions tests need rasterio, the compile test dagster."""
import copy
import json
from pathlib import Path

import pytest

from bench import wstc
from bench.registry import BLOCKS, ROOT
from bench.resolver import implemented, resolve
from bench.workflows import load_spec

SPEC = ROOT / "workflows" / "hughes_red_sky.yaml"
REFERENCE = json.dumps({"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {},
    "geometry": {"type": "Polygon", "coordinates": [[[-118.60, 34.52], [-118.55, 34.52], [-118.55, 34.57],
                                                     [-118.60, 34.57], [-118.60, 34.52]]]}}]})


def spec(**changes):
    """The Red Sky workflow, with optional changes: spec(models=[...]) replaces a section."""
    s = copy.deepcopy(load_spec(SPEC))
    s.update(changes)
    return s


# ---------------------------------------------------------------- registry
def test_manifests_load_and_implemented_blocks_point_at_real_code():
    assert {"landfire", "elmfire", "farsite", "wrf_fire", "iou"} <= set(BLOCKS)
    for name, m in BLOCKS.items():
        assert m["kind"] in {"source", "transform", "model", "reference", "metric"}, name
        if "execution" in m:
            assert (ROOT / m["execution"]["code"]).is_file(), name
            assert (ROOT / "images" / "wstc" / m["execution"]["image"] / "Dockerfile").is_file(), name
    assert implemented("elmfire") and not implemented("farsite")


# ---------------------------------------------------------------- workflows
def test_workflow_name_is_file_name_so_copies_dont_clash(tmp_path):
    copy_ = tmp_path / "my_copy.yaml"
    copy_.write_text(SPEC.read_text() + "\nworkflow: hughes_red_sky\n")
    assert load_spec(copy_)["workflow"] == "my_copy"


def test_broken_workflow_says_what_is_missing(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("models: [{block: elmfire}]\n")
    with pytest.raises(ValueError, match="missing.*case"):
        load_spec(bad)
    no_t0 = tmp_path / "no_t0.yaml"
    no_t0.write_text(SPEC.read_text().replace('  t0: "2025-01-22T18:53:00Z"', ""))
    with pytest.raises(ValueError, match="t0"):
        load_spec(no_t0)


# ---------------------------------------------------------------- resolver
def test_red_sky_plan_one_run_per_ignition_and_wrf_skipped_with_reason():
    plan = resolve(spec())
    assert plan["axes"] == {"ignition": ["triangulated_ignition", "ngfs_ignition"]}
    # run keys: model, non-default params, input choices
    assert sorted(r["run_key"] for r in plan["runs"]) == [
        "elmfire__duration_min-600__ngfs_ignition", "elmfire__duration_min-600__triangulated_ignition"]
    assert {s["model"] for s in plan["skipped"]} == {"wrf_fire"}
    assert all("atmos_forcing_3d" in s["reason"] for s in plan["skipped"])


def test_unimplemented_model_is_skipped_not_dropped():
    s = spec()
    s["models"] = [{"block": "farsite", "params": {"spread_adjustment": [1.2, 1.5]}}, {"block": "elmfire"}]
    plan = resolve(s)
    farsite = [x for x in plan["skipped"] if x["model"] == "farsite"]
    assert len(farsite) == 4                     # 2 values x 2 ignitions, each with its reason
    assert all("no implementation" in x["reason"] for x in farsite)
    assert {r["model"] for r in plan["runs"]} == {"elmfire"}


def test_unimplemented_input_is_an_error():
    s = spec()
    s["inputs"] = {**s["inputs"], "weather": {"source": {"block": "station_wind"},
                                              "transforms": [{"block": "windninja"}]}}
    with pytest.raises(ValueError, match="windninja"):
        resolve(s)


def test_lat_lon_sources_get_a_reproject_step_station_wind_does_not():
    plan = resolve(spec())
    ign = [st["step"] for br in plan["slots"]["ignition"] for st in br["steps"]]
    assert "ignition.ngfs_ignition.auto_reproject" in ign
    wx = [st["step"] for br in plan["slots"]["weather"] for st in br["steps"]]
    assert wx == ["weather.station_wind.fetch"]


def test_bad_param_type_rejected_out_of_range_only_warns():
    s = spec(models=[{"block": "elmfire", "params": {"spread_adjustment": "banana"}}])
    with pytest.raises(ValueError, match="expected float"):
        resolve(s)
    plan = resolve(spec(models=[{"block": "elmfire", "params": {"spread_adjustment": 9.0}}]))
    assert any("outside the usual range" in w for w in plan["warnings"])


# ---------------------------------------------------------------- identity
def _identities(s):
    cp = wstc.compile_plan(s, "test", REFERENCE)
    by_block = {e["name"]: e["identity"] for e in cp["inputs"]}
    return by_block, {k: r["identity"] for k, r in cp["runs"].items()}


def test_landfire_identity_ignores_what_cannot_change_it():
    base_inputs, base_runs = _identities(spec())
    later = spec()
    later["case"] = {**later["case"], "t0": "2025-01-22T20:00:00Z"}
    inputs, runs = _identities(later)
    # same LANDFIRE (and roads override) for a different start time ...
    assert inputs["landscape.landfire.fetch"] == base_inputs["landscape.landfire.fetch"]
    assert inputs["landscape.landfire.roads_override"] == base_inputs["landscape.landfire.roads_override"]
    # ... but the wind window and the runs are different artifacts
    assert inputs["weather.station_wind.fetch"] != base_inputs["weather.station_wind.fetch"]
    assert set(runs.values()).isdisjoint(base_runs.values())


def test_changing_the_domain_changes_landfire_and_everything_downstream():
    base_inputs, _ = _identities(spec())
    wider = spec()
    wider["case"] = {**wider["case"], "domain_wgs84": [-118.75, 34.45, -118.40, 34.70]}
    inputs, _ = _identities(wider)
    assert inputs["landscape.landfire.fetch"] != base_inputs["landscape.landfire.fetch"]
    assert inputs["landscape.landfire.roads_override"] != base_inputs["landscape.landfire.roads_override"]


# ---------------------------------------------------------------- compiled WSTC file
def test_compiled_file_loads_and_gives_each_step_its_blocks_image(tmp_path):
    pytest.importorskip("dagster")
    import importlib.util

    images = {f: f"docker.io/test/bench-{f}:abc" for f in wstc.IMAGE_FAMILIES}
    ref = tmp_path / "ref.geojson"
    ref.write_text(REFERENCE)
    path, cp = wstc.build_workflow_file(SPEC, "test", tmp_path, images, reference=ref)
    assert path.stat().st_size < wstc.UPLOAD_LIMIT
    mod_spec = importlib.util.spec_from_file_location("compiled", path)
    mod = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(mod)
    jobs = {j.name for j in mod.defs.resolve_all_job_defs()}
    assert set(wstc.job_names("test", "hughes_red_sky").values()) <= jobs
    image_of = lambda a: json.loads(a.op.tags["dagster-k8s/config"])["container_config"]["image"]
    assert image_of(mod.prediction) == images["elmfire"]
    assert all(image_of(a) == images["core"] for a in mod.input_assets + mod.score_assets)
    assert set(mod.CODE) == {"core_blocks.py", "elmfire_block.py", "conventions.py", "previews.py"}


# ---------------------------------------------------------------- conventions
def test_conventions_catch_south_up_percent_cover_and_off_grid(tmp_path):
    np = pytest.importorskip("numpy")
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import Affine

    from bench.conventions import case_grid, check_raster
    case = spec()["case"]
    grid, shape = case_grid(case)

    def write(name, arr, transform):
        p = tmp_path / f"{name}.tif"
        with rasterio.open(p, "w", driver="GTiff", height=arr.shape[0], width=arr.shape[1], count=1,
                           dtype=arr.dtype, crs=case["crs"], transform=transform) as d:
            d.write(arr, 1)
        return p

    assert check_raster(write("canopy_cover", np.full(shape, 0.4, "float32"), grid), case) == []
    south_up = Affine(grid.a, 0, grid.c, 0, -grid.e, grid.f - shape[0] * 30)
    assert any("not north-up" in p for p in check_raster(write("elevation", np.zeros(shape, "float32"), south_up), case))
    assert any("wrong units" in p for p in check_raster(write("canopy_cover", np.full(shape, 40.0, "float32"), grid), case))
    shifted = Affine(grid.a, 0, grid.c + 30, 0, grid.e, grid.f)
    assert any("case grid" in p for p in check_raster(write("elevation", np.zeros(shape, "float32"), shifted), case))
