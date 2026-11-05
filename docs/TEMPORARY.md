# What is still temporary

Anything that exists as a stand-in is tagged in the code with `TEMP(<tag>)`, so it
can't be forgotten:

```bash
scripts/list_temporary.sh     # lists every tag; exits non-zero while any remain
```

Tag new temporary code the same way, on the line (or the comment above it).

| Tag | What | Goes away when |
|---|---|---|
| `TEMP(shared-volume)` | artifacts live on the WSTC shared volume (`/opt/dagster/shared/storage/<me>/bench`): not backed up, 10 GiB shared by all users, anyone with the dashboard password can overwrite it | reads and reuse come from S3 too. Finished artifacts are already written through to the S3 bucket under the same identity paths (`blocks/lib/s3_store.py`); the volume would then be scratch only |

## Known limits (not tagged, by design for now)

| Limit | Why | Next |
|---|---|---|
| The fires are fixed when a workflow is compiled | static partitions | dynamic partitions once there is an API, so adding a fire doesn't mean re-uploading |
| Cases are written by hand | two fires so far | generate them from the lab's benchmark geopackage (Centers, NGFS, IRWIN, Perimeters layers) |
| The model's `duration_min` is set by hand in the workflow | one value per recipe | default it from each case's `reference.observed_utc` (scoring already happens at that time; a run that ends too early is unavailable) |
| One Dagster run per fire | fine for a few fires; WSTC allows 4 runs at once for everyone | batch many fires into one run |
| Runs are launched from the notebook | there is no API yet | the API takes "workflow X on fires A, B, C" |
| FARSITE, WindNinja, WRF-Fire have manifests only | not implemented | a workflow naming them is rejected by the resolver with the reason |
| Prediction and ignition identities hash more of the case than they read | the prediction hashes the whole case (incl. `reference` and every `ignitions` entry); the ignition slot's `CASE_KEYS` include all `ignitions` | hash only what each step reads (the chosen ignition; no reference in the prediction), so fixing `observed_utc` or adding an ignition point doesn't rerun the model |
| Station wind is uniform over the domain | no WindNinja | gridded wind (the ELMFIRE adapter already reads it) |
