# Wildfire spread-model benchmarking framework

SCIL, AI Collaborative for Wildfire, Workstream 2.

A framework for comparing wildfire spread models fairly: the same fires, the same
inputs, the same conventions, scored the same way, with a record of how every
number was produced. A **workflow** is a recipe for predicting a fire (data, model,
settings) with no fire in it; **cases** are fires. The framework runs a workflow
on any set of cases on the WSTC Dagster at NRP, reuses anything already computed,
and produces the workflow's **table**: one row per fire, scored against what the
fire really did. Different recipes are different workflows; comparing them means
comparing their tables.

The end-to-end example is [`notebooks/red_sky.ipynb`](notebooks/red_sky.ipynb):
ELMFIRE on real LANDFIRE fuels and station wind, run on the Hughes Fire (January
2025) and the Sandia Fire (October 2025), plus a second recipe (a different ignition source) to show what is shared
and what a fire can't provide.

## The idea in four parts

| Part | Where | What it is |
|---|---|---|
| **Blocks** | `blocks/*.yaml` | One manifest per model, data source, transform, reference or metric: what it consumes and produces, its parameters (types, defaults, ranges, rationale), and where its code and image are. Adding a model is adding a block. ([blocks/README.md](blocks/README.md)) |
| **Workflows** | `workflows/*.yaml` | A recipe: one source per input slot (plus transforms), one model with fixed parameters, a reference block, metrics, the grid resolution. No fire in it. Another version of a recipe is another workflow. |
| **Cases** | `cases/*.yaml` | A fire's facts: t0, weather window, CRS, domain (or derived from the reference perimeter), its ignition points, its reference perimeter (a file or a hosted URL). ([cases/README.md](cases/README.md)) |
| **Conventions** | `docs/conventions.md` | One answer to every question models disagree on (orientation, wind direction, units, time, grid). Sources convert into them, model adapters convert out of them, and every artifact is checked against them before it is accepted. |
| **Identity** | `src/bench/wstc.py` | Every artifact is stored under a hash of everything that determines it. Finished work is reused across runs and workflows, and changing anything that matters changes the hash, so nothing stale is ever served. |

## From workflow to results

```
workflows/elmfire_reported_ignition.yaml      cases/hughes.yaml, cases/...
        │  bench.workflows.load_workflow / load_cases   (one recipe? the case's facts?)
        ▼
bench.resolver.resolve(workflow, case)        (registry checks, defaults, auto reproject steps,
        │                                      required inputs; a recipe that can't run is an error)
        ▼
bench.wstc.compile_plan / build_workflow_file (per fire: identities, artifact paths; images)
        ▼
<me>_<workflow>.py ──▶ d.upload()  (deploy/wstc/dagster_client.py)
        │
        ▼  on the WSTC Dagster: one pod per step, each started from its block's image
<me>_<workflow>_job, one run per fire ──▶ <me>_<workflow>_report_job (the table)
```

What a workflow becomes in Dagster (all keys under `<me>/<workflow>/`; everything but
the report is **partitioned by fire**, so Dagster charts any number across fires):

| Asset | Is |
|---|---|
| `<slot>/<step>` | one per input step: the source, any automatic reproject, then the slot's transforms |
| `reference/<block>` | the fire's observed perimeter, rasterized onto its grid |
| `prediction` | the model run on the fire |
| `score/<metric>` | the prediction vs the reference |
| `report` | the workflow's table: one row per fire (done, or unavailable and why), also written as CSV |

A fire that lacks what a block needs (no NGFS detection in the case, no weather
station in range, no reference) is **unavailable** for that workflow, with the
reason, instead of failing.

## Repository

```
blocks/              block manifests (+ README: the manifest fields)
workflows/           recipes: elmfire_reported_ignition, elmfire_ngfs_ignition
cases/               fires (+ README: the case fields); hughes.yaml, sandia.yaml
references/          observed perimeters the cases point at (GeoJSON), until they are hosted
images/core/         the Python-only blocks: LANDFIRE, roads override, station wind,
                     moisture, the case's ignition points, reproject, reference, IoU, area bias
images/elmfire/      the ELMFIRE adapter (pyretechnics)
images/wstc/         one image recipe per block family (core, elmfire), FROM the
                     server's Dagster version; pushed to Docker Hub
src/bench/
  registry.py        loads blocks/
  workflows.py       loads and checks workflows and cases; a fire's domain
  resolver.py        (workflow, case) -> plan
  conventions.py     the case grid and the machine-checkable conventions
  previews.py        the prediction preview image (same look for every model)
  wstc.py            workflow x fires -> one uploadable file for the WSTC Dagster; identities; images
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
3. **A fire's reference perimeter** is named in its case: a local GeoJSON in
   `references/` (WGS84) or a hosted `url`, with `observed_utc`: the prediction is
   scored at that time. Without it the runs still run; the scores say there is no reference.

## Adding things

| You want to add | You do |
|---|---|
| a model | a manifest with `execution` (code + image family), an adapter that follows the model contract (docs/block_contract.md), an image recipe in `images/wstc/<family>/` |
| a data source, transform or metric | a manifest, and a function in `images/core/core_blocks.py` (or its own image if it needs other packages) |
| a fire | a case file in `cases/` (and its reference perimeter) |
| another version of a recipe (a parameter value, a different source) | another workflow file |

## Tests

```bash
pip install -e ".[blocks,test]"
pytest -q
```

`test_blocks.py` needs the blocks' packages (rasterio, pyretechnics with NumPy 1.26)
and is skipped without them.

## Not yet

See [docs/TEMPORARY.md](docs/TEMPORARY.md). In short: finished artifacts move to the
S3 bucket; cases are generated from the lab's benchmark geopackage; FARSITE, WindNinja and WRF-Fire get
implementations.
