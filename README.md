# Wildfire spread-model benchmarking framework

SCIL, AI Collaborative for Wildfire, Workstream 2.

A framework for comparing wildfire spread models fairly: the same fire, the same
inputs, the same conventions, scored the same way, with a record of how every
number was produced. You describe an experiment in a **workflow** file; the
framework plans it, runs every step on the WSTC Dagster at NRP, reuses anything
that was already computed, and scores each model run against the observed fire.

The end-to-end example is [`notebooks/red_sky.ipynb`](notebooks/red_sky.ipynb):
the Hughes Fire (January 2025) with real LANDFIRE fuels, station wind, two
ignition sources and ELMFIRE, scored against the final perimeter.

## The idea in four parts

| Part | Where | What it is |
|---|---|---|
| **Blocks** | `blocks/*.yaml` | One manifest per model, data source, transform, reference or metric: what it consumes and produces, its parameters (types, defaults, ranges, rationale), and where its code and image are. Adding a model is adding a block. ([blocks/README.md](blocks/README.md)) |
| **Workflows** | `workflows/*.yaml` | An experiment, as configuration: a case (fire, grid, time window), sources per input slot, models, a reference and metrics. Several sources for a slot make a comparison axis; a list of parameter values makes a sweep. |
| **Conventions** | `docs/conventions.md` | One answer to every question models disagree on (orientation, wind direction, units, time, grid). Sources convert into them, model adapters convert out of them, and every artifact is checked against them before it is accepted. |
| **Identity** | `src/bench/wstc.py` | Every artifact is stored under a hash of everything that determines it. Finished work is reused across runs and workflows, and changing anything that matters changes the hash, so nothing stale is ever served. |

## From workflow to results

```
workflows/hughes_red_sky.yaml
        │  bench.workflows.load_spec      (checks the file's sections and case)
        ▼
spec ──▶ bench.resolver.resolve           (registry checks, defaults, auto reproject steps,
        │                                  sweeps x comparison axes -> runs, skips with reasons)
        ▼
plan ──▶ bench.wstc.compile_plan          (asset keys, identities, artifact paths, images)
        │  bench.wstc.build_workflow_file
        ▼
<me>_<workflow>.py ──▶ d.upload()  (deploy/wstc/dagster_client.py)
        │
        ▼  on the WSTC Dagster: one pod per step, each started from its block's image
inputs job ──▶ runs job (one partition per run) ──▶ report job
```

What a workflow becomes in Dagster (all keys under `<me>/<workflow>/`):

| Asset | Is |
|---|---|
| `<slot>/<source>/<step>` | one per input step: the source's fetch, any automatic reproject, then the slot's transforms |
| `reference/<block>` | the observed perimeter, rasterized onto the case grid |
| `prediction` | **partitioned**, one partition per run (model x params x input choices); Dagster charts any numeric metadata across partitions |
| `score/<metric>` | partitioned like `prediction` |
| `report` | every finished run's scores in one table, plus the skipped runs and why |

## Repository

```
blocks/              block manifests (+ README: the manifest fields)
workflows/           workflows; hughes_red_sky.yaml is the Red Sky example
references/          observed perimeters the workflows score against (GeoJSON)
images/core/         the Python-only blocks: LANDFIRE, roads override, station wind,
                     moisture, ignition points, reproject, reference, IoU, area bias
images/elmfire/      the ELMFIRE adapter (pyretechnics)
images/wstc/         one image recipe per block family (core, elmfire), FROM the
                     server's Dagster version; pushed to Docker Hub
src/bench/
  registry.py        loads blocks/
  workflows.py       loads and checks workflow files
  resolver.py        workflow -> plan
  conventions.py     the case grid and the machine-checkable conventions
  previews.py        the prediction preview image (same look for every model)
  wstc.py            plan -> one uploadable file for the WSTC Dagster; identities; images
  notebook.py        display helpers for the notebook
deploy/wstc/         template_workflow.py (what a compiled workflow looks like) and
                     the WSTC team's client
notebooks/           red_sky.ipynb
docs/                conventions, block contract, what is still temporary
scripts/             build_wstc_image.sh (build + push the images), list_temporary.sh
tests/               test_framework.py (resolver, identity, compile), test_blocks.py
                     (every block on a synthetic landscape)
```

## Running it

1. **Images** (only when `images/wstc/*` changes): with Docker Desktop running and
   `docker login`, run `scripts/build_wstc_image.sh`. It pushes
   `a3pearce/bench-core:<tag>` and `a3pearce/bench-elmfire:<tag>`; the tag is a hash
   of the recipe, and the compiler names the same tag. Both repos must be public.
2. **The notebook**: `jupyter lab notebooks/red_sky.ipynb` from a Python with
   `pip install -e .`, then run top to bottom. It asks for the WSTC dashboard
   password and an email for LANDFIRE's service (a run tag, not stored in code).
3. **The reference perimeter** goes in `references/hughes_final_perimeter.geojson`
   (any CRS, as long as the GeoJSON says which; WGS84 if it doesn't). Without it the
   runs still run; scores say there is no reference.

## Adding things

| You want to add | You do |
|---|---|
| a model | a manifest with `execution` (code + image family), an adapter that follows the model contract (docs/block_contract.md), an image recipe in `images/wstc/<family>/` |
| a data source, transform or metric | a manifest, and a function in `images/core/core_blocks.py` (or its own image if it needs other packages) |
| a fire | a workflow file (and its reference perimeter) |
| a sweep or a comparison | a list of values, or a second source for a slot, in the workflow |

## Tests

```bash
pip install -e ".[blocks,test]"
pytest -q
```

`test_blocks.py` needs the blocks' packages (rasterio, pyretechnics with NumPy 1.26)
and is skipped without them.

## Not yet

See [docs/TEMPORARY.md](docs/TEMPORARY.md). In short: finished artifacts move to the
S3 bucket; many fires become a second partition dimension (a case catalog); FARSITE,
WindNinja and WRF-Fire get implementations; the NGFS ignition gets its real point.
