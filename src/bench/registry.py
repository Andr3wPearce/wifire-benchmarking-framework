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


# blocks/<folder>/ for each kind; blocks/lib/ is shared code, not blocks
KIND_DIRS = {"source": "sources", "method": "methods", "model": "models",
             "reference": "reference", "metric": "metrics"}


def load_blocks(path: Path = BLOCKS_DIR) -> dict[str, dict]:
    """{name: manifest}, from blocks/<kind folder>/<name>.yaml.

    A manifest's `name` must match its file name (so a block can always be found from a
    workflow by name), and its folder must match its `kind`. An implemented block (one
    with `execution:`) has its code next to its manifest, `<name>.py`; its path, relative
    to the repository, is filled in here as `execution.code`."""
    blocks = {}
    for f in sorted(path.glob("*/*.yaml")):
        m = yaml.safe_load(f.read_text())
        name = m.pop("name", f.stem)
        if name != f.stem:
            raise ValueError(f"{f.name} declares name '{name}'; the name must match the file name")
        if name in blocks:
            raise ValueError(f"two blocks named '{name}'")
        if KIND_DIRS.get(m.get("kind")) != f.parent.name:
            raise ValueError(f"{f.relative_to(path)}: a {m.get('kind')} block belongs in "
                             f"blocks/{KIND_DIRS.get(m.get('kind'), '?')}/")
        if m.get("execution"):
            code = f.with_suffix(".py")
            if not code.exists():
                raise ValueError(f"{f.relative_to(path)} has execution: but no {code.name} next to it")
            m["execution"]["code"] = str(code.relative_to(ROOT))
        blocks[name] = m
    return blocks


BLOCKS = load_blocks()
