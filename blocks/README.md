# Blocks

One YAML manifest per block. Blocks are data: adding a model, source, transform
or metric means adding a manifest here (and its code), not changing the framework.

| Field | Meaning |
|---|---|
| `name` | the block's name in workflows (must match the file name) |
| `kind` | `source` (makes an input slot's data), `transform` (changes it), `model`, `reference` (the observation to score against), `metric` |
| `slot` | sources only: which input slot it fills (`landscape`, `weather`, `moisture`, `ignition`) |
| `version` | the data or model version; part of every artifact's identity |
| `produces` / `consumes` / `requires` / `uses_if_present` | the variables it makes and needs. The resolver checks a model's `requires` against what the workflow's inputs produce |
| `needs_from_slots` | transforms that need another slot's data (WindNinja needs the landscape's elevation) |
| `params` | each with `type`, `default`, optional `range` (outside it only warns) and `rationale` |
| `native` | CRS / resolution the block works in. A source whose native CRS differs from the case gets an automatic reproject step |
| `resources` | models: CPU and memory for their step pod |
| `execution` | **only for implemented blocks**: `code` (the file with the block's code) and `image` (the image family it runs in, `images/wstc/<image>/`). A block without it is planned but not run: a model is skipped with the reason, and an input that needs it is an error |
