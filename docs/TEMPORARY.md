# Temporary code (remove before deployment)

This is the deployment repo. Anything that exists only for local testing or
as a stand-in is tagged in the code so it can't be forgotten:

```bash
scripts/list_temporary.sh     # lists every tag; exits non-zero while any remain
```

Tag new temporary code the same way: a comment containing `TEMP(<tag>)` on
the line (or the docstring's first line). Use an existing tag if one fits.

| Tag | Means | Goes away when |
|---|---|---|
| `TEMP(dry-run)` | a block that only writes a provenance receipt, or code that works around such blocks (e.g. ELMFIRE's synthetic test landscape) | every block in the workflow has a real image; then delete `executor.py` and the dry-run branches |
| `TEMP(local-dev)` | laptop-only plumbing: `dagster dev`, minikube (mount, in-cluster image builds, `imagePullPolicy: Never`, resource caps), the local subprocess backend, double-click launchers | deployment on NRP: Dagster via Helm, images from a registry, artifacts in object storage / a PVC, manifest resources as-is |
| `TEMP(api-stand-in)` | `submissions/` folder + sensor + `bench.submit` standing in for the API | the API creates partitions and launches runs directly |
| `TEMP(wstc-minimal)` | running on the WSTC Dagster as uploaded single files (`deploy/wstc/`, `src/bench/wstc.py`, `scripts/build_wstc_minimal.py`, `wheels/`): a shared Python environment on the volume (built by `<me>_bench_env_job` from an embedded pyretechnics wheel) stands in for block images; probes; the vendored `dagster_client.py` | steps can use the block's own image (or the server image has our package), and the framework's real assets run on WSTC |
| placeholder (`registry.example/...`) | made-up image names in block manifests | each block gets a real image in the lab's registry |

Not temporary, even though they help with testing: `docs/conventions.md`
checks, tests under `tests/`, the cache/identity logic, `docs/block_contract.md`.
