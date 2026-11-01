# Blocks

A block is a manifest, `<name>.yaml`, and its code next to it, `<name>.py`, in the
folder for its kind. Adding a model, source, transform or metric means adding a block
here, not changing the framework.

```
sources/      fill an input slot: landfire, station_wind, fixed_moisture, case_ignition
transforms/   change a slot's data: roads_override; reproject (the resolver inserts it
              itself, so it has code but no manifest); windninja (manifest only)
models/       elmfire (the pyretechnics adapter); farsite, wrf_fire (manifests only)
reference/    the observation to score against: final_perimeter
metrics/      iou, area_bias, and scoring.py (what they share)
lib/          shared by every block, never a block itself:
                blockkit.py      Unavailable, t0, reading/writing rasters on the case grid
                http_session.py  a requests session that retries (not part of any identity)
                run_block.py     the step runner: imports <block>.py, calls run(), writes provenance
```

A step block's code is a module with `run(step, log) -> summary` (see
[docs/block_contract.md](../docs/block_contract.md)); a model's is a script run on its
run directory. In a step's pod every code file sits in one directory, so a block imports
shared code by module name (`from blockkit import t0`) and names must be unique across
`blocks/`. A block's identity hashes its own file plus the shared modules it imports,
except `http_session` and `run_block`, which don't shape the data.

| Field | Meaning |
|---|---|
| `name` | the block's name in workflows (must match the file name; the folder must match `kind`) |
| `kind` | `source` (makes an input slot's data), `transform` (changes it), `model`, `reference` (the observation to score against), `metric` |
| `slot` | sources only: which input slot it fills (`landscape`, `weather`, `moisture`, `ignition`) |
| `version` | the data or model version; part of every artifact's identity |
| `produces` / `consumes` / `requires` / `uses_if_present` | the variables it makes and needs. The resolver checks a model's `requires` against what the workflow's inputs produce |
| `needs_from_slots` | transforms that need another slot's data (WindNinja needs the landscape's elevation) |
| `params` | each with `type`, `default`, optional `range` (outside it only warns) and `rationale` |
| `native` | CRS / resolution the block works in. A source whose native CRS differs from the case gets an automatic reproject step |
| `resources` | models: CPU and memory for their step pod |
| `execution` | **only for implemented blocks**: `image`, the image family it runs in (`images/wstc/<image>/`). The code is always `<name>.py` next to the manifest. A workflow that uses a block without `execution` is rejected by the resolver, with the reason |
