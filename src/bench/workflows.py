"""Loading workflow files.

A workflow is configuration only: a case, input slots with their sources and transforms,
models, a reference and metrics. Its name is its file name, so a copied file can never
clash with the original.
"""
from __future__ import annotations

from pathlib import Path

import yaml

REQUIRED = ["case", "inputs", "models", "reference", "metrics"]
CASE_REQUIRED = ["incident", "crs", "resolution_m", "t0", "domain_wgs84"]


def load_spec(spec_path: Path) -> dict:
    """Read workflows/<name>.yaml; its name is <name> (any `workflow:` line is ignored, so a
    copied file can never clash with the original)."""
    spec = yaml.safe_load(Path(spec_path).read_text()) or {}
    missing = [k for k in REQUIRED if k not in spec]
    if missing:
        raise ValueError(f"{Path(spec_path).name} is missing section(s) {missing}; a workflow "
                         f"needs {REQUIRED} (see workflows/hughes_red_sky.yaml)")
    missing = [k for k in CASE_REQUIRED if k not in spec["case"]]
    if missing:
        raise ValueError(f"{Path(spec_path).name}: case is missing {missing} (needs {CASE_REQUIRED})")
    spec["workflow"] = Path(spec_path).stem
    return spec
