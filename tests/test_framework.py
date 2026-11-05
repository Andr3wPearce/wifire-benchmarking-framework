"""The framework itself: registry, workflows and cases, resolver, identities, compiled WSTC
file, conventions. Needs pyyaml; the conventions tests need rasterio, the compile test dagster."""
import copy
import json
from pathlib import Path

import pytest

from bench import wstc
from bench.registry import BLOCKS, ROOT
from bench.resolver import implemented, resolve
from bench.workflows import case_for_workflow, domain_wgs84, load_case, load_cases, load_workflow

WORKFLOW = ROOT / "workflows" / "elmfire_reported_ignition.yaml"
NGFS_WORKFLOW = ROOT / "workflows" / "elmfire_ngfs_ignition.yaml"
REFERENCE = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {},
    "geometry": {"type": "Polygon", "coordinates": [[[-118.60, 34.52], [-118.55, 34.52], [-118.55, 34.57],
                                                     [-118.60, 34.57], [-118.60, 34.52]]]}}]}


def workflow(**changes):
    w = copy.deepcopy(load_workflow(WORKFLOW))
    w.update(changes)
    return w


def hughes(**changes):
    c = copy.deepcopy(load_case(ROOT / "cases" / "hughes.yaml"))
    c.update(changes)
    return c


def plan(w=None, c=None):
    w = w or workflow()
    return resolve(w, case_for_workflow(c or hughes(), w))


# ---------------------------------------------------------------- registry
def test_manifests_load_and_implemented_blocks_point_at_real_code():
    assert {"landfire", "elmfire", "case_ignition", "farsite", "iou"} <= set(BLOCKS)
    for name, m in BLOCKS.items():
        assert m["kind"] in {"source", "method", "model", "reference", "metric"}, name
        if "execution" in m:
            assert (ROOT / m["execution"]["code"]).is_file(), name
            assert (ROOT / "images" / "wstc" / m["execution"]["image"] / "Dockerfile").is_file(), name
    assert implemented("elmfire") and not implemented("farsite")


# ---------------------------------------------------------------- workflows and cases
def test_workflow_name_is_file_name_and_has_no_fire_in_it():
    w = load_workflow(WORKFLOW)
    assert w["workflow"] == "elmfire_reported_ignition"
    assert "case" not in w and "incident" not in json.dumps(w)


def test_a_workflow_is_one_recipe(tmp_path):
    two = tmp_path / "two_sources.yaml"
    two.write_text(WORKFLOW.read_text().replace(
        "    source: {block: case_ignition, params: {which: reported}}",
        "    sources: [{block: case_ignition}, {block: case_ignition, params: {which: ngfs}}]"))
    with pytest.raises(ValueError, match="one recipe"):
        load_workflow(two)
    sweep = tmp_path / "sweep.yaml"
    sweep.write_text(WORKFLOW.read_text().replace("duration_min: 180", "duration_min: [180, 600]"))
    with pytest.raises(ValueError, match="one recipe"):
        load_workflow(sweep)


def test_case_defaults_and_domain_from_reference_plus_buffer(tmp_path):
    c = hughes()
    assert c["crs"] == "EPSG:5070" and c["name"] == "hughes"
    ref = tmp_path / "ref.geojson"
    ref.write_text(json.dumps(REFERENCE))
    no_domain = {k: v for k, v in c.items() if k != "domain_wgs84"}
    no_domain["reference"] = {"file": str(ref)}
    w, s, e, n = domain_wgs84(no_domain)
    assert w < -118.60 and e > -118.55 and s < 34.52 and n > 34.57          # bbox + buffer
    assert abs((n - 34.57) - 5 / 111) < 1e-3                                  # 5 km default
    with pytest.raises(ValueError, match="no domain"):
        case_for_workflow({**no_domain, "reference": {}}, workflow())


# ---------------------------------------------------------------- resolver
def test_plan_has_one_branch_per_slot_and_reprojects_lat_lon_ignitions():
    p = plan()
    assert [s["step"] for s in p["slots"]["ignition"]] == ["ignition.case_ignition", "ignition.auto_reproject"]
    assert [s["step"] for s in p["slots"]["weather"]] == ["weather.station_wind"]
    assert p["model"]["block"] == "elmfire" and p["model"]["params"]["duration_min"] == 180


def test_recipes_that_cant_run_are_rejected_with_the_reason():
    with pytest.raises(ValueError, match="no implementation"):
        plan(workflow(model={"block": "farsite"}))
    with pytest.raises(ValueError, match="atmos_forcing_3d"):
        plan(workflow(model={"block": "wrf_fire"}))
    w = workflow()
    w["inputs"] = {**w["inputs"], "weather": {"source": {"block": "station_wind"},
                                              "methods": [{"block": "windninja"}]}}
    with pytest.raises(ValueError, match="windninja"):
        plan(w)


def test_bad_param_type_rejected_out_of_range_only_warns():
    with pytest.raises(ValueError, match="expected float"):
        plan(workflow(model={"block": "elmfire", "params": {"spread_adjustment": "banana"}}))
    p = plan(workflow(model={"block": "elmfire", "params": {"spread_adjustment": 9.0}}))
    assert any("outside the usual range" in w for w in p["warnings"])


# ---------------------------------------------------------------- identity
def _identities(w_path, case):
    cp = wstc.compile_plan(load_workflow(w_path), {"hughes": case}, "test")
    f = cp["fires"]["hughes"]
    return {n: s["identity"] for n, s in f["steps"].items()}, f["run"]["identity"]


def test_two_recipes_share_everything_but_the_ignition_and_the_run():
    a, run_a = _identities(WORKFLOW, hughes())
    b, run_b = _identities(NGFS_WORKFLOW, hughes())
    for step in ["landscape.landfire", "landscape.roads_override", "weather.station_wind", "moisture.fixed_moisture"]:
        assert a[step] == b[step], step
    assert a["ignition.case_ignition"] != b["ignition.case_ignition"] and run_a != run_b


def test_landfire_identity_ignores_what_cannot_change_it():
    base, base_run = _identities(WORKFLOW, hughes())
    later, later_run = _identities(WORKFLOW, hughes(t0="2025-01-22T20:00:00Z"))
    assert later["landscape.landfire"] == base["landscape.landfire"]
    assert later["weather.station_wind"] != base["weather.station_wind"] and later_run != base_run
    wider, _ = _identities(WORKFLOW, hughes(domain_wgs84=[-118.75, 34.45, -118.40, 34.70]))
    assert wider["landscape.landfire"] != base["landscape.landfire"]
    assert wider["landscape.roads_override"] != base["landscape.roads_override"]


def test_a_blocks_identity_is_its_file_and_the_shared_code_that_shapes_its_data():
    names = lambda b: {f.name for f in wstc.code_closure(wstc.block_code(b))}
    assert names("landfire") == {"landfire.py", "blockkit.py", "http_session.py", "conventions.py"}
    assert names("iou") == {"iou.py", "scoring.py", "blockkit.py", "conventions.py"}
    assert names("elmfire") == {"elmfire.py"}
    # the HTTP session (retries) ships with landfire but is not part of its identity
    assert "http_session" in wstc.NOT_IN_IDENTITY and "run_block" in wstc.NOT_IN_IDENTITY
    hashes = {b: wstc.block_code_hash(b) for b in ["landfire", "station_wind", "iou", "area_bias", "reproject"]}
    assert len(set(hashes.values())) == len(hashes)


def test_block_folders_match_kinds(tmp_path):
    from bench.registry import load_blocks
    (tmp_path / "metrics").mkdir()
    (tmp_path / "metrics" / "landfire.yaml").write_text("name: landfire\nkind: source\n")
    with pytest.raises(ValueError, match="belongs in blocks/sources/"):
        load_blocks(tmp_path)


# ---------------------------------------------------------------- compiled WSTC file
def test_compiled_file_partitions_by_fire_and_gives_each_step_its_blocks_image(tmp_path):
    pytest.importorskip("dagster")
    import importlib.util

    images = {f: f"docker.io/test/bench-{f}:abc" for f in wstc.IMAGE_FAMILIES}
    path, cp = wstc.build_workflow_file(WORKFLOW, ["hughes"], "test", tmp_path, images)
    assert path.stat().st_size < wstc.UPLOAD_LIMIT
    mod_spec = importlib.util.spec_from_file_location("compiled", path)
    mod = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(mod)
    jobs = {j.name for j in mod.defs.resolve_all_job_defs()}
    assert set(wstc.job_names("test", "elmfire_reported_ignition").values()) <= jobs
    assert mod.FIRES.get_partition_keys() == ["hughes"]
    image_of = lambda a: json.loads(a.op.tags["dagster-k8s/config"])["container_config"]["image"]
    assert image_of(mod.prediction) == images["elmfire"]
    assert all(image_of(a) == images["core"] for a in mod.input_assets + mod.score_assets)
    assert set(mod.CODE) == {"landfire.py", "roads_override.py", "station_wind.py", "fixed_moisture.py",
                             "case_ignition.py", "reproject.py", "elmfire.py", "final_perimeter.py",
                             "iou.py", "area_bias.py", "scoring.py", "blockkit.py", "http_session.py",
                             "run_block.py", "s3_store.py", "conventions.py", "previews.py"}


# ---------------------------------------------------------------- conventions
def test_conventions_catch_south_up_percent_cover_and_off_grid(tmp_path):
    np = pytest.importorskip("numpy")
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import Affine

    from bench.conventions import case_grid, check_raster
    case = case_for_workflow(hughes(), workflow())
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


def test_a_submitted_prediction_is_a_workflow_like_any_other(tmp_path):
    """A model we don't run: its perimeters come in through a source (submitted_perimeters),
    a replay "model" turns them into a prediction, and benchmarking is unchanged."""
    from bench import submit
    wf_path = tmp_path / "replay_example.yaml"
    wf_path.write_text("""
grid: {resolution_m: 30}
inputs:
  submission:
    source: {block: submitted_perimeters, params: {prefix: test/bench/submissions/example/v1}}
model: {block: perimeter_replay}
reference: {block: final_perimeter}
metrics: [{block: iou}, {block: area_bias}]
""")
    cp = wstc.compile_plan(load_workflow(wf_path), load_cases(), "test")
    assert [a["name"] for a in cp["assets"]] == ["submission.submitted_perimeters"]
    assert cp["model"]["block"] == "perimeter_replay" and set(cp["metrics"]) == {"iou", "area_bias"}
    sub = tmp_path / "submissions" / "example" / "v1"
    sub.mkdir(parents=True)
    (sub / "hughes.geojson").write_text(json.dumps({"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"time_utc": "2025-01-22T19:10:00Z"},
         "geometry": {"type": "Polygon", "coordinates": [[[-118.56, 34.57], [-118.55, 34.57], [-118.55, 34.58], [-118.56, 34.57]]]}}]}))
    f, prefix = submit.build_submit_file("example", "v1", "test", tmp_path, root=tmp_path / "submissions")
    assert prefix == "test/bench/submissions/example/v1" and f.stat().st_size < submit.UPLOAD_LIMIT
