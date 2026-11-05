# TODO: tabled decisions and deferred work

Things we decided to come back to. Each item says where it stands and where the
detail lives. Temporary code is tracked separately (`docs/TEMPORARY.md`,
`scripts/list_temporary.sh`).

## Open decisions

### Fire modeling and benchmarking as two workflows
Detail: `docs/next_steps.md` §1.

- [ ] **Split** today's single workflow into a fire modeling workflow (to fire rasters)
      and a benchmarking workflow (fire rasters vs references to a scoring matrix),
      with incident sets as their own versioned lists.
- [ ] **What a benchmarking workflow compares (prescribed inputs).** Model
      intercomparison (every input fixed), system benchmark (only incidents and
      grading fixed), or tracks (`prescribed:` per track). Leaning: tracks.
- [ ] **The benchmarking workflow fixes the scoring grid.** Leaning: yes (today
      scores at different resolutions aren't comparable: the reference is rasterized
      on each workflow's grid).
- [ ] **The domain leak.** A case's default domain is the reference perimeter's
      bounding box plus 5 km, so modeling sees where the fire went. Derive it from
      the ignition instead, and never pass observation facts to modeling.
- [ ] **Freezing.** Leaning: drafts mutable, `published: true` freezes, fixes mean a
      new version.
- [ ] **Who publishes.** A lab decision, not an engineering one.

### Public S3 links
- [ ] Ask the WSTC operators for anonymous read on one prefix (or a public bucket),
      then mirror only what is ours to publish (our predictions, scores) there and
      show permanent links (`public_url`) instead of presigned ones. Not references
      or submissions (others' data).

### Metrics
Detail: `docs/metrics_contract.md`.

- [ ] **Ensembles.** Agreed on paper: `toa` may gain a leading member axis later. Not built.
- [ ] **Temporal metrics** (arrival-time error, spread rate). The contract already
      allows them; blocked on references with more than one perimeter per fire. The
      observed arrival-time builder goes in the reference block.

## Deferred work

### Metrics contract (agreed, not implemented)
- [ ] `MatchedPair` adapter and the single scoring step (ours).
- [ ] Hand the contract to Luis; metrics package with tests against synthetic pairs.
- [ ] Migrate `blocks/metrics/` (see the migration table in the contract).

### Identity hashes more of the case than steps read
Detail: `docs/TEMPORARY.md`, Known limits.

- [ ] Prediction hashes the whole case (incl. `reference` and every `ignitions`
      entry); the ignition slot's `CASE_KEYS` include all `ignitions`. Fixing
      `observed_utc` or adding an ignition point reruns the model.

### Scaling on WSTC
Detail: `docs/next_steps.md` §6. Not needed for two fires.

- [ ] Launch only steps whose artifacts are missing; merge cheap steps that always run together.
- [ ] Ask the WSTC operators: per-user run limits instead of 4 shared; can `max_concurrent` (8 step pods per run) be raised per job.
- [ ] Many fires: fan out within one run, or Dagster Pipes from one step.

### Toward a framework others can use
Detail: `docs/next_steps.md` §2 to §5.

- [ ] Replay block: score submitted predictions without model code.
- [ ] Generate the case catalog from the benchmark geopackage.
- [ ] Slot dependencies into source manifests (out of `CASE_KEYS`).
- [ ] Methods: wire `consumes` (fan-in from other slots, plan-time check, identity, Dagster deps, slot order); `weights`, `stochastic` + `seed`; no observation access. Open: methods on outputs.
- [ ] Local backend.

## Raised, not discussed
Noted so they aren't lost; drop any that don't matter.

- [ ] **Ignition comparison confounds location and time.** ELMFIRE starts each
      ignition at its own `time_min` after t0 (Hughes: reported ~3 min, NGFS ~4,
      CAL FIRE ~16), so a later ignition burns less before the observation for
      reasons unrelated to where it started. An `ignition_time: case_t0 | source`
      option would isolate location (relevant to testing ignition-location bias).
- [ ] **Recipe name vs identity.** Editing a workflow file in place keeps its name but
      changes its results; score tables should key on `prediction_id`, not the name.
