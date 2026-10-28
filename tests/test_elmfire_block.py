"""ELMFIRE block on the synthetic landscape. Needs the block's requirements
(images/elmfire/requirements.txt); skipped otherwise."""
import importlib.util
import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyretechnics")
rasterio = pytest.importorskip("rasterio")
np = pytest.importorskip("numpy")

BLOCK = Path(__file__).resolve().parents[1] / "images" / "elmfire" / "elmfire_block.py"
spec = importlib.util.spec_from_file_location("elmfire_block", BLOCK)
elmfire_block = importlib.util.module_from_spec(spec)
sys.modules["elmfire_block"] = elmfire_block
spec.loader.exec_module(elmfire_block)


def test_synthetic_run_spreads_downwind(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(json.dumps({
        "model": "elmfire", "case": {"crs": "EPSG:5070", "resolution_m": 30},
        "params": {"spread_adjustment": 1.5, "duration_min": 240, "band_duration_min": 60},
        "inputs": {}}))
    summary = elmfire_block.execute(run_dir, lambda *_: None, root=tmp_path)

    assert summary["stop_condition"] == "max duration reached"
    assert summary["burned_area_ha"] > 50
    toa = rasterio.open(run_dir / "prediction" / "time_of_arrival.tif").read(1)
    rows, cols = np.where(np.isfinite(toa))
    # ignition at row 150, col 40; wind from the SW must push the fire NE (up = north)
    assert rows.mean() < 150 and cols.mean() > 40
    # the NB1 road at cols 95-97 stops it
    assert cols.max() < 98
    assert json.loads((run_dir / "prediction" / "perimeter.geojson").read_text())["features"]
