# What is still temporary

Anything that exists as a stand-in is tagged in the code with `TEMP(<tag>)`, so it
can't be forgotten:

```bash
scripts/list_temporary.sh     # lists every tag; exits non-zero while any remain
```

Tag new temporary code the same way, on the line (or the comment above it).

| Tag | What | Goes away when |
|---|---|---|
| `TEMP(shared-volume)` | artifacts live on the WSTC shared volume (`/opt/dagster/shared/storage/<me>/bench`): not backed up, 10 GiB shared by all users, anyone with the dashboard password can overwrite it | finished artifacts are written to the S3 bucket (`wstc-dagster`, credentials already in every step pod) under the same identity paths |

## Known limits (not tagged, by design for now)

| Limit | Why | Next |
|---|---|---|
| The fires are fixed when a workflow is compiled | static partitions | dynamic partitions once there is an API, so adding a fire doesn't mean re-uploading |
| Cases are written by hand | one fire so far | generate them from the lab's benchmark geopackage (Centers, NGFS, IRWIN, Perimeters layers) |
| Metrics score everything the model burned | the reference has no observation time yet | score at the perimeter's observation time; default the duration and weather window to it |
| One Dagster run per fire | fine for a few fires; WSTC allows 4 runs at once for everyone | batch many fires into one run |
| Runs are launched from the notebook | there is no API yet | the API takes "workflow X on fires A, B, C" |
| FARSITE, WindNinja, WRF-Fire have manifests only | not implemented | a workflow naming them is rejected by the resolver with the reason |
| Station wind is uniform over the domain | no WindNinja | gridded wind (the ELMFIRE adapter already reads it) |
