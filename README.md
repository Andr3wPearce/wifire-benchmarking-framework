# Benchmarking framework (Dagster prototype)

A composable framework for benchmarking wildfire spread models: you pick
**building blocks** in a config file, and the orchestrator handles the rest.
**ELMFIRE (pyretechnics) is the first real block**: it runs in its own
container on Kubernetes (minikube locally). Every other block is still a
**dry run** that logs what it would do and writes its provenance record;
until the input blocks are real, ELMFIRE runs on a synthetic test landscape
and says so in its output.

## Run it

**macOS:** double-click **`Start Dagster.command`** (first time: right-click > Open, since it is an unsigned script). **Windows:** double-click **`Start Dagster.bat`** (runs inside WSL). Either one sets up Python + Dagster
(first run: a few minutes), submits a first run, and opens
<http://localhost:3070> (set `BENCH_PORT` to change it). Keep the window open; closing it stops Dagster.

Submit more runs from a terminal (WSL on Windows) in this folder:

```bash
scripts/submit.sh --set farsite.spread_adjustment=1.0 --note "lower spread"
scripts/submit.sh --set farsite.spread_adjustment="[0.8, 1.0]"   # a list = a sweep
```

## Real blocks on minikube

Needs `minikube` running (`minikube start`). The launcher then:

1. builds `bench/elmfire:0.1` into minikube if it isn't there
   (`scripts/build_blocks.sh`; the first build compiles pyretechnics)
2. mounts `artifacts/` into minikube at `/bench-artifacts`
3. starts Dagster with `BENCH_BACKEND=k8s`

Each ELMFIRE partition then runs as a pod (`kubectl get pods -l bench/block=elmfire`).
Its outputs land in `artifacts/<workflow>/runs/<run>/model/prediction/` and its
summary shows up as metadata on the `prediction` asset.

Without Kubernetes: `BENCH_BACKEND=local BENCH_BLOCK_PYTHON=/path/to/python`
(a Python with `images/elmfire/requirements.txt` installed) runs the same
block as a subprocess.

What a block receives and must produce: [`docs/block_contract.md`](docs/block_contract.md).

## How it fits together

```
blocks/*.yaml         one manifest per block: kind, requires/produces, params
                      (defaults, ranges, rationale), native CRS/resolution, image
workflows/*.yaml      a workflow: case, input slots + sources + transforms,
                      models (lists = sweeps), reference, metrics
submissions/          API stand-in: a run request = workflow + param overrides
images/<block>/       real blocks: Dockerfile + entrypoint (the model adapter)
docs/                 block contract
src/bench/
  registry.py         loads blocks/ (blocks are data, not code)
  resolver.py         spec -> plan: params, auto reproject/regrid, sweeps,
                      comparison axes, skips with reasons
  executor.py         dry-run stand-in for blocks that aren't real yet
  containers.py       runs real blocks via Dagster Pipes (k8s pod or subprocess)
  definitions.py      builds the Dagster graph from workflows/ + blocks/
  submit.py           validates and drops a submission (like POST /runs)
```

What Dagster shows for each workflow:

| Group | Assets |
|---|---|
| `<wf>__landscape`, `__weather`, `__moisture`, `__ignition` | one asset per input step (fetch -> auto reproject/regrid -> transforms); WindNinja depends on the LANDFIRE fetch because its manifest says it needs elevation |
| `<wf>__reference` | the observed reference |
| `<wf>__runs` | `prediction` and one `score/<metric>` per metric, **partitioned by run** (model x params x input choice), plus `report` |

**Submitting a run:** the sensor resolves the submission, adds new run
partitions, materializes only the inputs that are missing, and lets
automation run each new partition once its inputs exist. Runs with the
same identity are reused, not recomputed. Bad params are rejected; params
outside their usual range are a warning; models whose requirements aren't
met are skipped, and the report says why.

## Extending it

| You want to add | You do | Redeploy? |
|---|---|---|
| a model, source, transform or metric | add `blocks/<name>.yaml` (+ image) and use it in a workflow | no code changes |
| a new fire / case | add a workflow file | reload code location |
| a different param value or sweep | submit with `--set` | no |
| a new block *kind* or input *slot* | framework change | yes |

## Tests

```bash
~/.bench-framework/venv/bin/pytest -q
```

## Not yet

- Real versions of the other blocks (LANDFIRE, weather, ignition, FARSITE, metrics)
- NRP: same pods, with an NRP kube context, namespace and object storage instead of minikube mount
- Object storage for artifacts (per-user private buckets)
- The real API + CILogon; per-user isolation (open-source Dagster has no RBAC)
- Bring-your-own data/model registration through the API
