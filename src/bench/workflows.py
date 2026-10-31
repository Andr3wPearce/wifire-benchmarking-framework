"""Loading workflows (recipes) and cases (fires).

A **workflow** is a recipe, with no fire in it: one source per input slot (plus
transforms), one model with fixed parameters, a reference block and metrics, and
the grid resolution. Testing a different version of a recipe means writing a
different workflow. Its name is its file name, so a copy never clashes.

A **case** is one fire: its incident name, simulation clock zero (t0), weather
window, CRS, domain (or a reference perimeter to derive it from), its ignition
points and its reference perimeter. Cases are data; workflows run on any of them.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import yaml

from .registry import ROOT

WORKFLOW_SECTIONS = ["grid", "inputs", "model", "reference", "metrics"]
CASE_FIELDS = ["incident", "t0"]
CASES_DIR = ROOT / "cases"
DEFAULT_CRS = "EPSG:5070"             # CONUS Albers; a case outside the lower 48 sets its own
DEFAULT_BUFFER_KM = 5.0               # domain = reference perimeter's bounding box + this


def load_workflow(path: Path) -> dict:
    """Read workflows/<name>.yaml and check it is a single recipe."""
    path = Path(path)
    wf = yaml.safe_load(path.read_text()) or {}
    missing = [k for k in WORKFLOW_SECTIONS if k not in wf]
    if missing:
        raise ValueError(f"{path.name} is missing section(s) {missing}; a workflow needs "
                         f"{WORKFLOW_SECTIONS} (see workflows/elmfire_reported_ignition.yaml)")
    for slot, cfg in wf["inputs"].items():
        if "sources" in cfg:
            raise ValueError(f"{path.name}: slot {slot!r} has several sources. A workflow is one recipe: "
                             "use one `source`, and make another workflow for the other one")
    for where, params in [("model", wf["model"].get("params", {}))] + [
            (f"inputs.{s}", c["source"].get("params", {})) for s, c in wf["inputs"].items()]:
        lists = [k for k, v in params.items() if isinstance(v, list)]
        if lists:
            raise ValueError(f"{path.name}: {where} params {lists} are lists. A workflow is one recipe: "
                             "one value per parameter, and another workflow for each other value")
    wf["workflow"] = path.stem
    return wf


def load_case(path: Path) -> dict:
    """Read cases/<name>.yaml. The case's name is its file name."""
    path = Path(path)
    case = yaml.safe_load(path.read_text()) or {}
    missing = [k for k in CASE_FIELDS if k not in case]
    if missing:
        raise ValueError(f"{path.name} is missing {missing} (a case needs {CASE_FIELDS})")
    case["name"] = path.stem
    case.setdefault("crs", DEFAULT_CRS)
    case.setdefault("window_h", 24)
    case.setdefault("ignitions", {})
    return case


def load_cases(names: list[str] | None = None, cases_dir: Path = CASES_DIR) -> dict[str, dict]:
    """{name: case} for the named cases, or every case in cases/."""
    names = names or sorted(p.stem for p in Path(cases_dir).glob("*.yaml"))
    return {n: load_case(Path(cases_dir) / f"{n}.yaml") for n in names}


def reference_text(case: dict) -> str | None:
    """The case's reference perimeter (GeoJSON text), if it has one we can read here.
    Hosted references (reference.url) are fetched by the reference step instead."""
    ref = case.get("reference") or {}
    if ref.get("file") and (ROOT / ref["file"]).exists():
        return (ROOT / ref["file"]).read_text()
    return None


def domain_wgs84(case: dict) -> list[float] | None:
    """The case's domain [W, S, E, N]: given explicitly, or the reference perimeter's
    bounding box plus `domain_buffer_km`. None if neither is available."""
    if case.get("domain_wgs84"):
        return [float(v) for v in case["domain_wgs84"]]
    text = reference_text(case)
    if text is None:
        return None
    fc = json.loads(text)
    xs, ys = [], []

    def walk(c):
        if isinstance(c[0], (int, float)):
            xs.append(c[0]); ys.append(c[1])
        else:
            for sub in c:
                walk(sub)
    for f in fc["features"] if fc.get("type") == "FeatureCollection" else [fc]:
        walk(f["geometry"]["coordinates"])
    buf_km = float(case.get("domain_buffer_km", DEFAULT_BUFFER_KM))
    lat = (min(ys) + max(ys)) / 2
    dlat, dlon = buf_km / 111.0, buf_km / (111.0 * math.cos(math.radians(lat)))
    return [round(min(xs) - dlon, 5), round(min(ys) - dlat, 5), round(max(xs) + dlon, 5), round(max(ys) + dlat, 5)]


def case_for_workflow(case: dict, workflow: dict) -> dict:
    """The case as the blocks see it: the fire's facts plus the recipe's grid resolution.
    Raises ValueError if the case can't be gridded (no domain and no readable reference)."""
    domain = domain_wgs84(case)
    if domain is None:
        raise ValueError(f"case {case['name']!r} has no domain_wgs84 and no local reference file to derive it from")
    return {"incident": case["incident"], "name": case["name"], "crs": case["crs"],
            "resolution_m": workflow["grid"]["resolution_m"], "t0": case["t0"],
            "window_h": case["window_h"], "domain_wgs84": domain,
            "ignitions": case.get("ignitions", {}), "reference": case.get("reference", {})}
