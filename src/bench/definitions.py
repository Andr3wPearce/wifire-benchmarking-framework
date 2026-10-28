"""Dagster definitions, generated from data.

For every workflow in workflows/ we build, at load time:
  * one asset per input step (source -> auto reproject/regrid -> transforms),
    grouped by slot, with cross-slot deps taken from block declarations
  * one reference asset
  * a `prediction` asset partitioned by run (dynamic partitions: one key per
    valid model x param sweep x input choice, added at runtime)
  * one `score` asset per metric, same partitions
  * a `report` asset over all runs
  * a sensor that turns submissions (the API stand-in) into partitions + runs

Graph *shape* is fixed when code loads (new block kinds or slots = redeploy).
Everything users do day to day (new data, models, params, fires) arrives at
runtime as manifests, submissions and partitions.
"""
from __future__ import annotations

import hashlib
import json
import warnings
from pathlib import Path

import yaml
from dagster import (
    AssetKey,
    AssetSelection,
    AutomationCondition,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    DynamicPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    asset,
    sensor,
)

from dagster._core.definitions.assets.graph.asset_graph_subset import AssetGraphSubset
from dagster._core.definitions.events import AssetKeyPartitionKey

from .containers import is_container_block, run_container_block
from .executor import ARTIFACT_ROOT, run_block
from .overrides import apply_overrides
from .registry import BLOCKS, ROOT
from .resolver import resolve
from .workflows import load_spec

WORKFLOWS_DIR = ROOT / "workflows"
SUBMISSIONS_DIR = ROOT / "submissions"
# Only the report refreshes itself (when scores change). Model runs and scores run exactly
# when asked: Materialize in the UI, or a submission (which starts a backfill).
# It covers whatever runs have finished: planned-but-never-run partitions don't hold it back.
ON_INPUTS_UPDATED = (
    AutomationCondition.any_deps_updated().since_last_handled()
    & ~AutomationCondition.any_deps_in_progress()
    & ~AutomationCondition.in_progress()
)


def _plans_dir(wf: str) -> Path:
    return ARTIFACT_ROOT / wf / "_plans"


def build_workflow(spec_path: Path):
    spec = load_spec(spec_path)
    plan = resolve(spec)
    wf = plan["workflow"]
    runs_part = DynamicPartitionsDefinition(name=f"{wf}__runs")
    assets, input_keys = [], []
    tails: dict[tuple[str, str], AssetKey] = {}      # (slot, branch) -> last step's asset
    tail_steps: dict[tuple[str, str], str] = {}      # (slot, branch) -> last step's id
    first_fetch: dict[str, AssetKey] = {}

    # ---- inputs: one asset per step, chained per source branch ----
    for slot, branches in plan["slots"].items():
        for br in branches:
            prev = None
            for step in br["steps"]:
                key = AssetKey([wf, slot, step["step"].split(".", 1)[1].replace(".", "__")])
                deps = [prev] if prev else []
                for needed_slot in step.get("needs_from_slots", {}).values():
                    deps.append(first_fetch[needed_slot])
                assets.append(_step_asset(wf, slot, step, key, deps))
                input_keys.append(key)
                if step["kind"] == "source":
                    first_fetch.setdefault(slot, key)
                prev = key
            tails[(slot, br["branch"])] = prev
            tail_steps[(slot, br["branch"])] = br["steps"][-1]["step"]

    # ---- reference ----
    ref_block = spec["reference"]["block"]
    ref_key = AssetKey([wf, "reference", ref_block])
    assets.append(_step_asset(wf, "reference", {
        "step": f"reference.{ref_block}", "kind": "reference", "block": ref_block,
        "image": BLOCKS[ref_block]["image"], "params": {}}, ref_key, []))
    input_keys.append(ref_key)

    # ---- per-run assets ----
    pred_key = AssetKey([wf, "prediction"])

    @asset(key=pred_key, deps=list(tails.values()), partitions_def=runs_part,
           group_name=f"{wf}__runs", kinds={"model"},
           description="One partition per valid run (model x params x input choices). "
                       "prepare -> execute -> collect through the model's adapter.")
    def prediction(context):
        run = json.loads((_plans_dir(wf) / f"{context.partition_key}.json").read_text())
        # the input branch this run uses in each slot ("main" when a slot has one source)
        picked = [tails[(s, run["inputs"].get(s, "main"))].to_user_string() for s in plan["slots"]]
        manifest = BLOCKS[run["model"]]
        if run.get("block_version") != manifest.get("version"):
            # the plan was made for an older version of this block (e.g. Materialize clicked
            # in the UI without resubmitting): bring its params up to the current manifest.
            # Params the plan already has (user choices) win; new params get their defaults.
            defaults = {k: v["default"] for k, v in manifest.get("params", {}).items()}
            added = sorted(set(defaults) - set(run["params"]))
            run["params"] = {**defaults, **{k: v for k, v in run["params"].items() if k in defaults}}
            context.log.warning(f"plan for {context.partition_key} was made for {run['model']} "
                                f"{run.get('block_version', '(unknown version)')}, now {manifest.get('version')}; "
                                f"using current defaults for {added or 'no new params'}. "
                                "Resubmit to re-plan properly.")
            run["block_version"] = manifest.get("version")
        common = {"run_id": run["run_id"], "model": run["model"],
                  "inputs": run["inputs"] or {"(single source per slot)": "-"},
                  "submission": run.get("submission", "-")}
        if is_container_block(manifest):
            # real block: hand it the artifact dir of the last step in each input slot
            slot_dirs = {s: f"{wf}/" + tail_steps[(s, run["inputs"].get(s, "main"))].replace(".", "/")
                         for s in plan["slots"]}
            result = run_container_block(context, workflow=wf, run_key=context.partition_key,
                                         block=run["model"], manifest=manifest, run=run,
                                         case=plan["case"], inputs=slot_dirs)
            return MaterializeResult(metadata={**result.metadata, **common})
        # TEMP(dry-run): models without an execution section only write receipts
        payload = {**run, "block": run["model"]}
        base = f"runs/{context.partition_key}"
        run_block(context, wf, f"{base}/prepare", payload, picked)
        run_block(context, wf, f"{base}/execute", payload, [f"{base}/prepare"])
        md = run_block(context, wf, f"{base}/collect", payload, [f"{base}/execute"])
        return MaterializeResult(metadata={
            **md, **common, "resources": run.get("resources") or {},
            "uses_optional": ", ".join(run.get("uses_optional", [])) or "-",
        })
    assets.append(prediction)

    score_keys = []
    for metric in [m["block"] for m in spec["metrics"]]:
        score_keys.append(AssetKey([wf, "score", metric]))
        assets.append(_score_asset(wf, metric, pred_key, ref_key, runs_part))

    @asset(key=AssetKey([wf, "report"]), deps=score_keys, group_name=f"{wf}__runs",
           kinds={"report"}, automation_condition=ON_INPUTS_UPDATED,
           description="All scored runs plus every skipped combination and why.")
    def report(context):
        pd = _plans_dir(wf)
        planned = [json.loads(p.read_text()) for p in sorted(pd.glob("*.json")) if not p.name.startswith("_")]
        scored = context.instance.get_materialized_partitions(score_keys[0]) if score_keys else set()
        runs = [r for r in planned if r["run_key"] in scored]
        skipped = json.loads((pd / "_skipped.json").read_text()) if (pd / "_skipped.json").exists() else []
        rows = ["| run | model | params | inputs |", "|---|---|---|---|"]
        rows += [f"| {r['run_key']} | {r['model']} | {r['params']} | {r['inputs']} |" for r in runs]
        sk = [f"- **{s['model']}** {s['inputs'] or ''}: {s['reason']}" for s in skipped]
        md = run_block(context, wf, "report", {"block": "report", "params": {},
                       "rationale": f"{len(runs)} runs scored, {len(skipped)} skipped"},
                       [r["run_key"] for r in runs])
        return MaterializeResult(metadata={
            **md, "runs_scored": len(runs), "planned_not_run": len(planned) - len(runs),
            "skipped": len(skipped),
            "summary": MetadataValue.md("\n".join(rows) + "\n\n**Skipped**\n" + ("\n".join(sk) or "none"))})
    assets.append(report)

    return assets, [_submission_sensor(wf, spec_path, runs_part, input_keys, [pred_key, *score_keys])]


def _step_asset(wf, slot, step, key, deps):  # TEMP(dry-run): input/reference blocks are receipts only
    doc = step.get("rationale") or step.get("reason") or BLOCKS.get(step["block"], {}).get("rationale")
    kind = {"auto": "transform"}.get(step["kind"], step["kind"])

    @asset(key=key, deps=deps, group_name=f"{wf}__{slot}", kinds={kind},
           description=doc or f"{step['kind']} block `{step['block']}`",
           metadata={"block": step["block"], "image": str(step.get("image") or "builtin"),
                     "params": step.get("params") or {}})
    def _a(context):
        return MaterializeResult(metadata=run_block(context, wf, step["step"], step,
                                                    [d.to_user_string() for d in deps]))
    return _a


def _score_asset(wf, metric, pred_key, ref_key, runs_part):
    @asset(key=AssetKey([wf, "score", metric]), deps=[pred_key, ref_key], partitions_def=runs_part,
           group_name=f"{wf}__runs", kinds={"metric"},
           description=f"Metric block `{metric}`: prediction vs reference.")
    def _s(context):
        md = run_block(context, wf, f"runs/{context.partition_key}/score/{metric}",
                       {"block": metric, "image": BLOCKS[metric]["image"], "params": {}},
                       [pred_key.to_user_string(), ref_key.to_user_string()])
        return MaterializeResult(metadata={**md, "value": MetadataValue.text("(dry run)")})  # TEMP(dry-run)
    return _s


def _submission_sensor(wf, spec_path, runs_part, input_keys, per_run_keys):
    # TEMP(api-stand-in): the real API will create partitions and runs directly;
    # watching a submissions/ folder is only for the prototype
    @sensor(name=f"submissions__{wf}", minimum_interval_seconds=5,
            asset_selection=AssetSelection.assets(*input_keys, *per_run_keys),
            default_status=DefaultSensorStatus.RUNNING,
            description="Plans the workflow as written (runs appear, ready to Materialize), and "
                        "turns each YAML dropped in submissions/ (API stand-in) into runs.")
    def _sensor(context: SensorEvaluationContext):
        done = set(json.loads(context.cursor or "[]"))
        run_requests, add_keys, requested = [], [], set()
        existing = set(context.instance.get_dynamic_partitions(runs_part.name))

        # the workflow file as written, planned once per version of the file: its runs show
        # up as partitions so they can be materialized straight from the UI; nothing starts
        default_id = "default:" + hashlib.sha1(spec_path.read_bytes()).hexdigest()[:12]
        if default_id not in done:
            done.add(default_id)
            try:
                plan = resolve(load_spec(spec_path))
            except ValueError as e:
                context.log.error(f"workflows/{spec_path.name}: can't plan: {e}")
                plan = None
            if plan:
                pd = _plans_dir(wf)
                pd.mkdir(parents=True, exist_ok=True)
                fresh = [r for r in plan["runs"] if r["run_key"] not in existing]
                for r in fresh:
                    (pd / f"{r['run_key']}.json").write_text(
                        json.dumps({**r, "submission": f"workflows/{spec_path.name}"}, indent=2))
                    existing.add(r["run_key"])
                add_keys += [r["run_key"] for r in fresh]
                (pd / "_skipped.json").write_text(json.dumps(plan["skipped"], indent=2))
                context.log.info(f"workflows/{spec_path.name}: planned {len(plan['runs'])} runs "
                                 f"({len(fresh)} new), {len(plan['skipped'])} skipped. "
                                 "Materialize them in the UI.")

        new = [p for p in sorted(SUBMISSIONS_DIR.glob("*.yaml")) if p.name not in done]
        for p in new:
            done.add(p.name)
            sub = yaml.safe_load(p.read_text()) or {}
            if sub.get("workflow") != wf:
                continue
            spec = apply_overrides(load_spec(spec_path), sub.get("overrides") or {})
            try:
                plan = resolve(spec)
            except ValueError as e:     # bad submission: reject it, don't crash-loop the sensor
                context.log.error(f"{p.name}: REJECTED: {e}")
                continue
            pd = _plans_dir(wf)
            pd.mkdir(parents=True, exist_ok=True)
            new_here, changed = [], []
            done_parts = context.instance.get_materialized_partitions(per_run_keys[0])
            for r in plan["runs"]:
                f = pd / f"{r['run_key']}.json"
                if r["run_key"] in existing:
                    old = json.loads(f.read_text()) if f.exists() else {}
                    if old.get("run_id") == r["run_id"] and r["run_key"] in done_parts:
                        continue                  # same identity, and Dagster has it: reuse (cache)
                    # new identity (e.g. new block version), or Dagster lost it (wiped/failed): redo
                    changed.append(r["run_key"])
                else:
                    new_here.append(r["run_key"])
                    existing.add(r["run_key"])
                f.write_text(json.dumps({**r, "submission": p.name}, indent=2))
            add_keys += new_here
            # one backfill per submission: missing inputs (cached ones are reused) and the
            # new/changed runs; Dagster orders it (inputs first), so nothing races
            missing = [k for k in input_keys if k not in requested
                       and context.instance.get_latest_materialization_event(k) is None]
            requested.update(missing)
            todo = {AssetKeyPartitionKey(k, None) for k in missing} | {
                AssetKeyPartitionKey(k, part) for part in new_here + changed for k in per_run_keys}
            if todo:
                run_requests.append(RunRequest.for_asset_graph_subset(
                    AssetGraphSubset.from_asset_partition_set(todo, context.repository_def.asset_graph),
                    tags={"submission": p.name}))
            (pd / "_skipped.json").write_text(json.dumps(plan["skipped"], indent=2))
            for w in plan["warnings"]:
                context.log.warning(f"{p.name}: {w}")
            for s in plan["skipped"]:
                context.log.info(f"{p.name}: SKIPPED {s['model']} {s['inputs']}: {s['reason']}")
            context.log.info(f"{p.name}: {len(plan['runs'])} runs, {len(new_here)} new, "
                             f"{len(changed)} re-run (changed or not materialized), "
                             f"{len(plan['runs']) - len(new_here) - len(changed)} reused, "
                             f"{len(missing)} input steps to materialize")
        return SensorResult(
            run_requests=run_requests,
            dynamic_partitions_requests=[runs_part.build_add_request(add_keys)] if add_keys else [],
            cursor=json.dumps(sorted(done)),
        )
    return _sensor


_assets, _sensors, BROKEN_WORKFLOWS = [], [], {}
for _spec in sorted(WORKFLOWS_DIR.glob("*.yaml")):
    try:
        a, s = build_workflow(_spec)
    except Exception as e:   # one bad workflow must not take every other one down
        BROKEN_WORKFLOWS[_spec.name] = f"{type(e).__name__}: {e}"
        warnings.warn(f"workflows/{_spec.name} skipped: {BROKEN_WORKFLOWS[_spec.name]}")
        continue
    _assets += a
    _sensors += s

defs = Definitions(
    assets=_assets,
    sensors=_sensors + [AutomationConditionSensorDefinition(
        "run_when_inputs_ready", target=AssetSelection.all(),
        default_status=DefaultSensorStatus.RUNNING, minimum_interval_seconds=5)],
)
