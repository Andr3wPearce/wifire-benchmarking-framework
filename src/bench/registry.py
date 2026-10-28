"""Block registry: every block is a YAML manifest in blocks/ (data, not code).

Adding a block = adding a manifest (and, for real runs, publishing its image).
Nothing in the orchestration code changes.
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml

ROOT = Path(os.environ.get("BENCH_ROOT", Path(__file__).resolve().parents[2]))
BLOCKS_DIR = ROOT / "blocks"


def load_blocks(path: Path = BLOCKS_DIR) -> dict[str, dict]:
    blocks = {}
    for f in sorted(path.glob("*.yaml")):
        m = yaml.safe_load(f.read_text())
        name = m.pop("name", f.stem)
        if name in blocks:
            raise ValueError(f"duplicate block name '{name}' ({f})")
        blocks[name] = m
    return blocks


BLOCKS = load_blocks()
