# Minimal run on the WSTC Dagster (NRP)  (TEMP(wstc-minimal))

One job, two steps (each its own pod):

| Step | Does |
|---|---|
| `<me>/env_check` | reports the pod: Python, arch, compiler, network to PyPI/GitHub |
| `<me>/elmfire_synthetic` | builds a Python 3.12 env in the pod (uv + embedded pyretechnics wheel), runs the real ELMFIRE block on the synthetic landscape, records its summary + a preview image |

## Build the file (only after changing the block or previews)
```bash
python scripts/build_wstc_minimal.py --me apearce \
  --wheel wheels/pyretechnics-2026.10.20-cp312-cp312-linux_x86_64.slim.whl
# -> deploy/wstc/apearce_bench_minimal.py  (generated, not committed)
```

## Run it from NRP JupyterHub
Put `apearce_bench_minimal.py` next to `dagster_client.py`, then:
```python
import getpass
from dagster_client import Dagster
d = Dagster(password=getpass.getpass("Dagster password: "))

d.upload("apearce_bench_minimal.py")          # -> job list incl. apearce_bench_minimal_job
run_id, status, results = d.run(
    "apearce_bench_minimal_job",
    run_config={"ops": {"apearce__elmfire_synthetic": {"config": {"duration_min": 180.0}}}})
print(status, d.url_for(run_id))
results                                       # env_check + ELMFIRE summary
```
If it fails: `d.errors(run_id)`, `d.logs(run_id)`.

First run is slow (~2-4 min): pods start, uv installs Python 3.12 and the block's
packages. The preview shows in the dashboard: Assets > apearce/elmfire_synthetic.
