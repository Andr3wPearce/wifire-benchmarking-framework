from pathlib import Path

import pytest
import yaml

from bench.overrides import apply_overrides
from bench.registry import BLOCKS, ROOT
from bench.resolver import resolve

SPEC = ROOT / "workflows" / "hughes_ignition_comparison.yaml"


def spec(**overrides):
    return apply_overrides(yaml.safe_load(SPEC.read_text()), overrides)


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
    steps = [s["step"] for br in plan["slots"]["weather"] for s in br["steps"]]
    assert steps == ["weather.station_wind.fetch", "weather.station_wind.auto_reproject",
                     "weather.station_wind.windninja"]


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
