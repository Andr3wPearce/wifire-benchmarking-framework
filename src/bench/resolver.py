"""The resolver: turns a workflow (a YAML spec) into a plan, before anything runs.

A workflow says *what* to compare: a case, one or more sources per input slot,
models, a reference and metrics. The plan says exactly *which steps run*:

* every block named exists in the registry, and every parameter has its declared
  type (out-of-range values only warn: someone may mean it);
* parameters are merged with each manifest's defaults, so the plan is explicit;
* automatic steps are inserted where a source's native CRS differs from the case
  (ignition points come in WGS84 and get a reproject step to the case CRS);
* sweeps (a list of values for a model parameter) and comparison axes (an input
  slot with several sources) expand into individual runs;
* runs that cannot work are skipped with the reason, not dropped silently: a model
  whose required inputs no source provides, or a model that has no implementation.

The plan is plain data (JSON-serialisable). `bench.wstc` compiles it for the WSTC
Dagster; nothing here depends on how or where the plan is executed.
"""
from __future__ import annotations

import hashlib
import itertools
import json

from .registry import BLOCKS

PY_TYPES = {"float": (int, float), "int": (int,), "bool": (bool,), "str": (str,)}
# steps the resolver inserts itself; the framework implements them (images/core/core_blocks.py)
AUTO_BLOCKS = {"reproject"}


def _block(name: str) -> dict:
    if name not in BLOCKS:
        raise ValueError(f"unknown block '{name}' (no blocks/{name}.yaml)")
    return BLOCKS[name]


def implemented(name: str) -> bool:
    """A block runs for real only if its manifest says where its code is (`execution`)."""
    return name in AUTO_BLOCKS or "execution" in _block(name)


def _params(block_name: str, overrides: dict | None) -> tuple[dict, list[str]]:
    """Manifest defaults merged with the workflow's values.

    A wrong type is an error (it would fail later, inside a pod, for a worse reason).
    A value outside the manifest's usual `range` is only a warning. A list value is
    kept as a list here; `_sweep` expands it into one run per value.
    """
    schema = _block(block_name).get("params", {})
    merged = {k: v["default"] for k, v in schema.items()}
    warnings = []
    for key, value in (overrides or {}).items():
        if key not in schema:
            raise ValueError(f"{block_name}: unknown param '{key}' (known: {sorted(schema)})")
        for v in (value if isinstance(value, list) else [value]):
            if not isinstance(v, PY_TYPES[schema[key]["type"]]):
                raise ValueError(f"{block_name}.{key}: expected {schema[key]['type']}, got {v!r}")
            lo, hi = schema[key].get("range", (None, None))
            if lo is not None and not lo <= v <= hi:
                warnings.append(f"{block_name}.{key}={v} is outside the usual range [{lo}, {hi}]")
        merged[key] = value
    return merged, warnings


def _sweep(params: dict) -> list[dict]:
    """A list-valued parameter becomes one parameter set per value (all combinations)."""
    keys = [k for k, v in params.items() if isinstance(v, list)]
    if not keys:
        return [params]
    out = []
    for combo in itertools.product(*(params[k] for k in keys)):
        p = dict(params)
        p.update(dict(zip(keys, combo)))
        out.append(p)
    return out


def _auto_steps(native: dict, case: dict, prefix: str) -> list[dict]:
    """Steps that bring a source's output into the case's CRS."""
    steps = []
    if native.get("crs") and native["crs"] != case["crs"]:
        steps.append({"step": f"{prefix}.auto_reproject", "kind": "auto", "block": "reproject",
                      "params": {"to": case["crs"]},
                      "reason": f"native CRS {native['crs']} differs from case CRS {case['crs']}"})
    return steps


def resolve(spec: dict) -> dict:
    """Workflow spec -> plan: {"workflow", "case", "slots", "axes", "runs", "skipped", "warnings",
    "reference", "metrics"}. Raises ValueError for anything that can't be planned."""
    case = spec["case"]
    warnings: list[str] = []
    slots: dict[str, list[dict]] = {}
    available: set[str] = set()       # every variable some input provides

    # ---- inputs: one branch per source; a branch is the source, its auto steps, its transforms ----
    for slot, cfg in spec["inputs"].items():
        sources = cfg.get("sources") or [cfg["source"]]
        branches = []
        for src in sources:
            name = src["block"]
            b = _block(name)
            if b.get("kind") != "source" or b.get("slot") != slot:
                raise ValueError(f"{name} is a {b.get('kind')} for slot {b.get('slot')!r}, not a source for {slot!r}")
            params, w = _params(name, src.get("params"))
            warnings += w
            # with several sources the branch is named after its source: that name is the axis value
            branch_id = name if len(sources) > 1 else "main"
            steps = [{"step": f"{slot}.{name}.fetch", "kind": "source", "block": name,
                      "version": src.get("version", b.get("version")),
                      "selection": {k: v for k, v in src.items() if k not in ("block", "params", "version")},
                      "params": params}]
            steps += _auto_steps(b.get("native", {}), case, f"{slot}.{name}")
            produced = set(b["produces"])
            for t in cfg.get("transforms", []):
                tb = _block(t["block"])
                tparams, w = _params(t["block"], t.get("params"))
                warnings += w
                steps.append({"step": f"{slot}.{name}.{t['block']}", "kind": "transform",
                              "block": t["block"], "params": tparams,
                              "needs_from_slots": tb.get("needs_from_slots", {})})
                produced |= set(tb["produces"])
            missing_code = [s["block"] for s in steps if not implemented(s["block"])]
            if missing_code:
                # an input can't be skipped: every run downstream would be missing it
                raise ValueError(f"input slot {slot!r} uses {missing_code}, which have no implementation "
                                 f"yet (no `execution` in their manifests)")
            available |= produced
            branches.append({"branch": branch_id, "source": name, "steps": steps,
                             "produces": sorted(produced)})
        slots[slot] = branches

    for part, names in [("reference", [spec["reference"]["block"]]),
                        ("metrics", [m["block"] for m in spec["metrics"]])]:
        for n in names:
            if not implemented(n):
                raise ValueError(f"{part} block {n!r} has no implementation yet")

    # ---- runs: models x parameter sweeps x one source per comparison axis ----
    axes = {slot: [br["branch"] for br in brs] for slot, brs in slots.items() if len(brs) > 1}
    runs, skipped = [], []
    for m in spec["models"]:
        mb = _block(m["block"])
        params, w = _params(m["block"], m.get("params"))
        warnings += w
        missing = sorted(set(mb.get("requires", [])) - available)
        for p in _sweep(params):
            for choice in itertools.product(*axes.values()) if axes else [()]:
                picks = dict(zip(axes.keys(), choice))
                desc = {"model": m["block"], "params": p, "inputs": picks}
                if missing:
                    skipped.append({**desc, "reason": f"{m['block']} requires {missing}, which no input provides"})
                    continue
                if not implemented(m["block"]):
                    skipped.append({**desc, "reason": f"{m['block']} has no implementation yet "
                                                      f"(no `execution` in blocks/{m['block']}.yaml)"})
                    continue
                # run_id: what the run *is*, independent of the workflow's name or file
                identity = json.dumps({"case": case, "model": m["block"], "version": mb.get("version"),
                                       "params": p, "inputs": picks}, sort_keys=True)
                run_id = hashlib.sha1(identity.encode()).hexdigest()[:10]
                # run_key: readable name for the UI: model, non-default params, input choices
                defaults = {k: v["default"] for k, v in mb.get("params", {}).items()}
                parts = [m["block"]] + [f"{k}-{v}" for k, v in sorted(p.items()) if v != defaults.get(k)]
                parts += [picks[s] for s in sorted(picks)]
                runs.append({**desc, "run_id": run_id, "run_key": "__".join(str(x) for x in parts),
                             "block_version": mb.get("version"), "resources": mb.get("resources", {}),
                             "uses_optional": sorted(set(mb.get("uses_if_present", [])) & available)})

    return {"workflow": spec["workflow"], "case": case, "slots": slots, "axes": axes,
            "runs": runs, "skipped": skipped, "warnings": warnings,
            "reference": spec["reference"], "metrics": [m["block"] for m in spec["metrics"]]}
