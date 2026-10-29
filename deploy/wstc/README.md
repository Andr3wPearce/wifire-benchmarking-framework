# Running on the WSTC Dagster (NRP)

The WSTC Dagster (<https://wstc-dagster.nrp-nautilus.io>) is a shared Dagster
server run by Kai Lin's team on NRP Nautilus. Users upload Python files through
a system job; every step runs in its own Kubernetes pod.

What matters for us:

| | |
|---|---|
| Uploads | one `.py` per upload (max 5 MB); same name replaces; prefix everything with your short name (`apearce`) |
| Step pods | the server's standard image unless a step asks for its own via `op_tags` (`dagster-k8s/config` → `container_config.image`), which is what we do |
| Images | must carry dagster 1.13.26 / dagster-k8s 0.29.26; NRP pulls from public Docker Hub repos |
| Storage | shared volume at `/opt/dagster/shared/storage/<me>/` (CephFS, 10 GiB for everyone, not backed up); S3 bucket `wstc-dagster` with credentials in every pod |
| Limits | 4 runs at once across all users, 8 step pods per run, resource limits within 20% of requests |
| Network | step pods reach PyPI, GitHub, Docker Hub, LANDFIRE (lfps.usgs.gov) and the Iowa Mesonet |

## Files here

| File | |
|---|---|
| `template_workflow.py` | what a compiled workflow looks like; `bench.wstc.build_workflow_file` fills it in |
| `dagster_client.py` | the WSTC team's client (upload, run, results, logs, remove), vendored unchanged |

Compiled files go to `deploy/wstc/build/` (not committed). The notebook
(`notebooks/red_sky.ipynb`) compiles, uploads and runs; from plain Python:

```python
import getpass, sys
sys.path[:0] = ["src", "deploy/wstc"]
from bench import wstc
from dagster_client import Dagster

d = Dagster(password=getpass.getpass())
path, cp = wstc.build_workflow_file("workflows/hughes_red_sky.yaml", "apearce", "deploy/wstc/build",
                                    wstc.image_refs("a3pearce"))
d.upload(str(path))
jobs = wstc.job_names("apearce", cp["workflow"])
d.run(jobs["inputs"], tags={"bench/lfps_email": "you@ucsd.edu"})
for key in cp["runs"]:
    d.run(jobs["runs"], tags={"dagster/partition": key})
d.run(jobs["report"])
```

If a run fails: `d.errors(run_id)`, `d.logs(run_id)`, or the run's page
(`d.url_for(run_id)`).
