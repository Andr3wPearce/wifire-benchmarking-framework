# What is still temporary

Anything that exists as a stand-in is tagged in the code with `TEMP(<tag>)`, so it
can't be forgotten:

```bash
scripts/list_temporary.sh     # lists every tag; exits non-zero while any remain
```

Tag new temporary code the same way, on the line (or the comment above it).

| Tag | What | Goes away when |
|---|---|---|
| `TEMP(placeholder-data)` | the NGFS ignition in `hughes_red_sky.yaml` is the CAL FIRE reported origin again, so the two ELMFIRE runs are identical | we have the NGFS first-detection point and time |
| `TEMP(shared-volume)` | artifacts live on the WSTC shared volume (`/opt/dagster/shared/storage/<me>/bench`): not backed up, 10 GiB shared by all users, anyone with the dashboard password can overwrite it | finished artifacts are written to the S3 bucket (`wstc-dagster`, credentials already in every step pod) under the same identity paths |

## Known limits (not tagged, by design for now)

| Limit | Why | Next |
|---|---|---|
| One fire per workflow | the case is part of the workflow | a case catalog, with the fire as a second partition dimension |
| `prediction` is one asset, so one image | Dagster sets a step's image per asset, not per partition | one prediction asset per model once there are two real models (the compiler refuses to mix images today) |
| Runs are launched from the notebook | there is no API yet | the API creates partitions and launches runs |
| FARSITE, WindNinja, WRF-Fire have manifests only | not implemented | the resolver skips such models with the reason, and refuses such inputs |
| Station wind is uniform over the domain | no WindNinja | gridded wind (the ELMFIRE adapter already reads it) |
