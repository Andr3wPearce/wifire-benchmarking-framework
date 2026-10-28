"""Loading workflow files. A workflow's name is its file name."""
from __future__ import annotations

from pathlib import Path

import yaml

REQUIRED = ["case", "inputs", "models", "reference", "metrics"]


def load_spec(spec_path: Path) -> dict:
    """Read workflows/<name>.yaml; its name is <name> (any `workflow:` line is ignored, so a
    copied file can never clash with the original)."""
    spec = yaml.safe_load(Path(spec_path).read_text()) or {}
    missing = [k for k in REQUIRED if k not in spec]
    if missing:
        raise ValueError(f"{Path(spec_path).name} is missing section(s) {missing}; a workflow "
                         f"needs {REQUIRED} (see workflows/hughes_ignition_comparison.yaml)")
    spec["workflow"] = Path(spec_path).stem
    return spec
