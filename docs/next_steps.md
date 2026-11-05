# Next steps

Status: proposals, not implemented.

What we have today runs and scores spread models end to end: blocks, a resolver that
rejects bad recipes before anything runs, conventions enforced at every boundary,
identity-keyed reuse and provenance, `unavailable` as a result, and replay of
predictions from models we don't run. The next step is to split it along the line it
already has.

## 1. Two workflows: fire modeling and benchmarking

| | Fire modeling workflow | Benchmarking workflow |
|---|---|---|
| **Takes** | an incident's facts (domain, t0, ignitions) | a modeling workflow's fire rasters + the incident's observations |
| **Does** | sources, then methods that alter them, then a model | reference, alignment (`MatchedPair`), metrics |
| **Produces** | fire rasters (`time_of_arrival.tif`, ...) | score rows, viewed as a **scoring matrix** |
| **Cost** | minutes per fire | seconds |

Today both live in one workflow file and one job: everything up to `prediction` is
modeling, `reference`, `score/*` and `report` are benchmarking.

**The seam is the prediction artifact.** Benchmarking needs fire rasters that follow
the conventions and nothing else, so it doesn't care where they came from: our
modeling workflows, someone else's open model, or a black box that only sends
perimeters (`perimeter_replay`, `submissions/`). Every one is scored the same way;
openness buys an explanation (lineage, attribution of differences to one method or
input), not a better score.

### Incidents

Incidents (cases) are the shared axis: both workflows are partitioned by incident. A
case is a menu of facts and each workflow picks from it: modeling picks from the
inputs facts (which ignition), benchmarking from the observation facts (which
reference counts: the final FIRIS perimeter, the FIRIS series, later VIIRS). Which
incidents is neither workflow's business: a named, versioned **incident set**
(e.g. `socal_2025_v1: [hughes, kenneth, madre, sandia]`). A published result pins
three things: a benchmarking workflow version, an incident set version, and the
modeling workflows scored.

### The scoring matrix

Score rows have four dimensions: modeling workflow x incident x metric x observation.
The default view is modeling workflows x incidents for one metric, at the latest
observation; every empty cell says why (`unavailable`, `undefined`). It is for
reading across fires, inputs and models, not a ranking. Two workflows that differ in
one thing (the ignition source) isolate what that one thing did.

### What the split fixes

* **The domain leaks the answer.** A case without `domain_wgs84` gets the reference
  perimeter's bounding box plus 5 km, so the landscape is cut around where the fire
  actually went. Rule: a modeling workflow never reads observation facts. The domain
  comes from something known at ignition (the ignition point plus a radius, or a fixed
  extent), and methods never receive the case's `reference`. Harmless for a
  retrospective demo; it matters the moment anything is called predictive.
* **Scores at different resolutions aren't comparable today.** The reference is
  rasterized on the workflow's grid, so a 30 m and a 90 m workflow are scored against
  different reference rasters, and coarse grids inflate IoU. The scoring grid belongs
  to the benchmarking workflow; alignment regrids every prediction onto it.
* **Duration stops being a guess.** Scoring already cuts arrival time at each
  observation, so a run longer than needed serves every benchmarking workflow. The
  modeling side runs each fire at least to the latest observation any benchmarking
  workflow scores it against, and the prediction's identity hashes that resolved
  number (not "benchmarking workflow X's rule"), so predictions stay shared.
* **New versions of the grading are cheap.** Grading isn't in a prediction's
  identity, so a metric version bump, an added perimeter, or a new benchmarking
  workflow version reuses every prediction and reruns only scoring.

### In Dagster

| Job | File | Partitioned | Does |
|---|---|---|---|
| `<me>_<modeling wf>_predict_job` | modeling workflow | by incident, one run each | inputs, methods, model: fire rasters |
| `<me>_<benchmarking wf>_score_job` | benchmarking workflow | by incident, **one run for all** (`BackfillPolicy.single_run`) | reference on its grid, then score rows per modeling workflow |
| `<me>_<benchmarking wf>_results_job` | benchmarking workflow | no | the scoring matrix (Dagster table + CSV in S3) |

Score assets in the benchmarking file depend on prediction assets in the modeling
files; WSTC loads every file into one code location, so cross-file dependencies
should work (to be probed first). Every file uses one partition set (the whole case
catalog). Input assets stay keyed per modeling workflow: two files defining the same
asset key is an error, and sharing already happens in the identity store. Chaining
predict, score and results is done by the notebook until automation is available.

### Open decisions

1. **What a benchmarking workflow compares.** Model intercomparison (every input
   prescribed), system benchmark (only incidents and grading fixed), or tracks
   (`prescribed:` per track). Leaning: tracks; the provenance pitch needs inputs that
   can vary.
2. **The scoring grid belongs to the benchmarking workflow.** Leaning: yes.
3. **Freezing.** Leaning: drafts mutable, `published: true` freezes; a fix is a new version.
4. **Who publishes.** A lab decision: whoever owns what counts as scored signs off.

## 2. Submissions (replay): done, and what's next

`submitted_perimeters` reads a submission (timed perimeters, a black box) from S3 and
`perimeter_replay` turns it into a prediction (`submissions/README.md`). Next: accept submitted arrival-time rasters (no time
quantization), interpolate between perimeters, and let submitters upload directly
(credentials) instead of through the submit job.

## 3. The case catalog

In any benchmark, case curation (references, ignitions, observation times) is most of
the value. Cases are written by hand (two fires). Generate them from the lab's
benchmark geopackage (Centers, NGFS, IRWIN, Perimeters layers).

## 4. Slots and cross-slot inputs

* Slot names are fixed in code (`CASE_KEYS` in `src/bench/wstc.py`). Move what each
  source depends on into its manifest, so a new kind of input is a new block, not a
  framework change.
* **Methods** (what used to be called transforms) alter a slot's data by any means: a
  rule (clip PIG values, roads to a burnable fuel), an ML model (imputation,
  generation), anything. To make "anything" real, the resolver must use what a method
  already declares, `consumes`: for each consumed variable, wire in the step that
  provides it (the latest producer in its own chain, else another slot's tail), reject
  the recipe at plan time if nothing does, put it in the identity and the Dagster deps,
  and order the slots (rejecting cycles). `needs_from_slots` then goes away. Today
  `consumes` is declared but never checked, and a step only gets `{"in": <previous
  artifact>}`, so WindNinja (wind from terrain) can't be wired.
* Methods also need: `weights: {uri, sha256}` in the identity (ML methods),
  `stochastic: true` with a required `seed` param, and no access to observation facts
  (the case's `reference`), enforced by the resolver.
* Open: methods on outputs (e.g. an ML correction of a model's arrival times), as a
  method on the prediction.

## 5. A local backend

The plan is portable data; everything after it is WSTC-specific (template, shared
volume, upload limit, code shipped as strings, fires fixed at compile time). A local
backend lets others develop and try the framework without WSTC credentials.

## 6. Scaling on WSTC (deferred)

Limits: 4 runs at once across all users, 8 step pods per run, limits within 20% of
requests (an NRP rule; just request honestly). In order:

1. Launch only steps whose artifacts are missing (a reused step still costs a pod
   start), and merge cheap steps that always run together into one pod.
2. Ask the WSTC operators for per-user run limits and whether `max_concurrent` can be raised per job.
3. Many fires: fan out within one run, or Dagster Pipes from one step if the step's
   service account may create pods. Longer term, the lab's own NRP namespace.
