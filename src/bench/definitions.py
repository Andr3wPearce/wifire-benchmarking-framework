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

import json
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

from .executor import ARTIFACT_ROOT, run_block
from .overrides import apply_overrides
from .registry import BLOCKS, ROOT
from .resolver import resolve

WORKFLOWS_DIR = ROOT / "workflows"
SUBMISSIONS_DIR = ROOT / "submissions"
EAGER = AutomationCondition.eager()


def _plans_dir(wf: str) -> Path:
    return ARTIFACT_ROOT / wf / "_plans"


def build_workflow(spec_path: Path):
    spec = yaml.safe_load(spec_path.read_text())
    plan = resolve(spec)
    wf = plan["workflow"]
    runs_part = DynamicPartitionsDefinition(name=f"{wf}__runs")
    assets, input_keys = [], []
    tails: dict[tuple[str, str], AssetKey] = {}      # (slot, branch) -> last step
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
           group_name=f"{wf}__runs", kinds={"model"}, automation_condition=EAGER,
           description="One partition per valid run (model x params x input choices). "
                       "prepare -> execute -> collect through the model's adapter.")
    def prediction(context):
        run = json.loads((_plans_dir(wf) / f"{context.partition_key}.json").read_text())
        # the input branch this run uses in each slot ("main" when a slot has one source)
        picked = [tails[(s, run["inputs"].get(s, "main"))].to_user_string() for s in plan["slots"]]
        payload = {**run, "block": run["model"]}
        base = f"runs/{context.partition_key}"
        run_block(context, wf, f"{base}/prepare", payload, picked)
        run_block(context, wf, f"{base}/execute", payload, [f"{base}/prepare"])
        md = run_block(context, wf, f"{base}/collect", payload, [f"{base}/execute"])
        return MaterializeResult(metadata={
            **md, "run_id": run["run_id"], "model": run["model"],
            "inputs": run["inputs"] or {"(single source per slot)": "-"},
            "resources": run.get("resources") or {},
            "uses_optional": ", ".join(run.get("uses_optional", [])) or "-",
            "submission": run.get("submission", "-"),
        })
    assets.append(prediction)

    score_keys = []
    for metric in [m["block"] for m in spec["metrics"]]:
        score_keys.append(AssetKey([wf, "score", metric]))
        assets.append(_score_asset(wf, metric, pred_key, ref_key, runs_part))

    @asset(key=AssetKey([wf, "report"]), deps=score_keys, group_name=f"{wf}__runs",
           kinds={"report"}, automation_condition=EAGER,
           description="All scored runs plus every skipped combination and why.")
    def report(context):
        pd = _plans_dir(wf)
        runs = [json.loads(p.read_text()) for p in sorted(pd.glob("*.json")) if not p.name.startswith("_")]
        skipped = json.loads((pd / "_skipped.json").read_text()) if (pd / "_skipped.json").exists() else []
        rows = ["| run | model | params | inputs |", "|---|---|---|---|"]
        rows += [f"| {r['run_key']} | {r['model']} | {r['params']} | {r['inputs']} |" for r in runs]
        sk = [f"- **{s['model']}** {s['inputs'] or ''}: {s['reason']}" for s in skipped]
        md = run_block(context, wf, "report", {"block": "report", "params": {},
                       "rationale": f"{len(runs)} runs scored, {len(skipped)} skipped"},
                       [r["run_key"] for r in runs])
        return MaterializeResult(metadata={
            **md, "runs_scored": len(runs), "skipped": len(skipped),
            "summary": MetadataValue.md("\n".join(rows) + "\n\n**Skipped**\n" + ("\n".join(sk) or "none"))})
    assets.append(report)

    return assets, [_submission_sensor(wf, spec_path, runs_part, input_keys)]


def _step_asset(wf, slot, step, key, deps):
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
           group_name=f"{wf}__runs", kinds={"metric"}, automation_condition=EAGER,
           description=f"Metric block `{metric}`: prediction vs reference.")
    def _s(context):
        md = run_block(context, wf, f"runs/{context.partition_key}/score/{metric}",
                       {"block": metric, "image": BLOCKS[metric]["image"], "params": {}},
                       [pred_key.to_user_string(), ref_key.to_user_string()])
        return MaterializeResult(metadata={**md, "value": MetadataValue.text("(dry run)")})
    return _s


def _submission_sensor(wf, spec_path, runs_part, input_keys):
    @sensor(name=f"submissions__{wf}", minimum_interval_seconds=5,
            target=AssetSelection.assets(*input_keys),
            default_status=DefaultSensorStatus.RUNNING,
            description="Stand-in for the API: each YAML dropped in submissions/ "
                        "is resolved into runs (partitions); missing inputs are materialized.")
    def _sensor(context: SensorEvaluationContext):
        done = set(json.loads(context.cursor or "[]"))
        new = [p for p in sorted(SUBMISSIONS_DIR.glob("*.yaml")) if p.name not in done]
        run_requests, add_keys, requested = [], [], set()
        existing = set(context.instance.get_dynamic_partitions(runs_part.name))
        for p in new:
            done.add(p.name)
            sub = yaml.safe_load(p.read_text()) or {}
            if sub.get("workflow") != wf:
                continue
            spec = apply_overrides(yaml.safe_load(spec_path.read_text()), sub.get("overrides") or {})
            try:
                plan = resolve(spec)
            except ValueError as e:     # bad submission: reject it, don't crash-loop the sensor
                context.log.error(f"{p.name}: REJECTED: {e}")
                continue
            pd = _plans_dir(wf)
            pd.mkdir(parents=True, exist_ok=True)
            new_here = []
            for r in plan["runs"]:
                if r["run_key"] in existing:      # same identity already planned: reuse it (cache)
                    continue
                (pd / f"{r['run_key']}.json").write_text(json.dumps({**r, "submission": p.name}, indent=2))
                new_here.append(r["run_key"])
                existing.add(r["run_key"])
            add_keys += new_here
            (pd / "_skipped.json").write_text(json.dumps(plan["skipped"], indent=2))
            for w in plan["warnings"]:
                context.log.warning(f"{p.name}: {w}")
            for s in plan["skipped"]:
                context.log.info(f"{p.name}: SKIPPED {s['model']} {s['inputs']}: {s['reason']}")
            missing = [k for k in input_keys if k not in requested
                       and context.instance.get_latest_materialization_event(k) is None]
            if missing:   # cached inputs are reused; only missing ones run
                requested.update(missing)
                run_requests.append(RunRequest(run_key=f"{p.name}:inputs", asset_selection=missing,
                                               tags={"submission": p.name}))
            context.log.info(f"{p.name}: {len(plan['runs'])} runs, {len(new_here)} new "
                             f"({len(plan['runs']) - len(new_here)} reused), "
                             f"{len(missing)} input steps to materialize")
        return SensorResult(
            run_requests=run_requests,
            dynamic_partitions_requests=[runs_part.build_add_request(add_keys)] if add_keys else [],
            cursor=json.dumps(sorted(done)),
        )
    return _sensor


_assets, _sensors = [], []
for _spec in sorted(WORKFLOWS_DIR.glob("*.yaml")):
    a, s = build_workflow(_spec)
    _assets += a
    _sensors += s

defs = Definitions(
    assets=_assets,
    sensors=_sensors + [AutomationConditionSensorDefinition(
        "run_when_inputs_ready", target=AssetSelection.all(),
        default_status=DefaultSensorStatus.RUNNING, minimum_interval_seconds=5)],
)
