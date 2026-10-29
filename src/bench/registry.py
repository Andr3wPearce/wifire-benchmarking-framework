"""The block registry: every block is a YAML manifest in blocks/ (data, not code).

Adding a block means adding a manifest (and, if it should run, its code and its
image family; see blocks/README.md). Nothing in the framework changes.
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml

ROOT = Path(os.environ.get("BENCH_ROOT", Path(__file__).resolve().parents[2]))
BLOCKS_DIR = ROOT / "blocks"


def load_blocks(path: Path = BLOCKS_DIR) -> dict[str, dict]:
    """{name: manifest}. A manifest's `name` must match its file name, so a block can
    always be found from a workflow by name."""
    blocks = {}
    for f in sorted(path.glob("*.yaml")):
        m = yaml.safe_load(f.read_text())
        name = m.pop("name", f.stem)
        if name != f.stem:
            raise ValueError(f"{f.name} declares name '{name}'; the name must match the file name")
        blocks[name] = m
    return blocks


BLOCKS = load_blocks()
