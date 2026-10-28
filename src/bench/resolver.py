"""Resolver: turns a run spec into a plan (the run graph).

It checks declarations, merges params with defaults, inserts automatic
transforms (reproject / regrid), expands sweeps and comparison axes into
runs, and skips combinations that cannot work, with the reason recorded.
"""
from __future__ import annotations

import hashlib
import itertools
import json
from copy import deepcopy

from .registry import BLOCKS

PY_TYPES = {"float": (int, float), "int": (int,), "bool": (bool,), "str": (str,)}


def _block(name: str) -> dict:
    if name not in BLOCKS:
        raise ValueError(f"unknown block '{name}' (not in registry)")
    return BLOCKS[name]


def _params(block_name: str, overrides: dict | None) -> tuple[dict, list[str]]:
    """Defaults merged with user overrides. Type errors fail; out-of-range only warns."""
    schema = _block(block_name).get("params", {})
    merged = {k: v["default"] for k, v in schema.items()}
    warnings = []
    for key, value in (overrides or {}).items():
        if key not in schema:
            raise ValueError(f"{block_name}: unknown param '{key}'")
        values = value if isinstance(value, list) else [value]
        for v in values:
            if not isinstance(v, PY_TYPES[schema[key]["type"]]):
                raise ValueError(f"{block_name}.{key}: expected {schema[key]['type']}, got {v!r}")
            lo, hi = schema[key].get("range", (None, None))
            if lo is not None and not lo <= v <= hi:
                warnings.append(f"{block_name}.{key}={v} is outside the usual range [{lo}, {hi}]")
        merged[key] = value
    return merged, warnings


def _sweep(params: dict) -> list[dict]:
    """A list-valued param becomes one run per value."""
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
    steps = []
    if native.get("crs") and native["crs"] != case["crs"]:
        steps.append({"step": f"{prefix}.auto_reproject", "kind": "auto",
                      "block": "reproject", "params": {"to": case["crs"]},
                      "reason": f"native CRS {native['crs']} differs from case CRS {case['crs']}"})
    res = native.get("resolution_m")
    if res and res != case["resolution_m"]:
        steps.append({"step": f"{prefix}.auto_regrid", "kind": "auto",
                      "block": "regrid", "params": {"to_m": case["resolution_m"]},
                      "reason": f"native resolution {res} m differs from case grid {case['resolution_m']} m"})
    return steps


def resolve(spec: dict) -> dict:
    case = spec["case"]
    warnings: list[str] = []
    slots: dict[str, list[dict]] = {}
    available: set[str] = set()

    # ---- inputs: one branch per source; each branch is a chain of steps ----
    for slot, cfg in spec["inputs"].items():
        sources = cfg.get("sources") or [cfg["source"]]
        branches = []
        for src in sources:
            name = src["block"]
            b = _block(name)
            params, w = _params(name, src.get("params"))
            warnings += w
            branch_id = name if len(sources) > 1 else "main"
            steps = [{"step": f"{slot}.{name}.fetch", "kind": "source", "block": name,
                      "version": src.get("version", b.get("version")),
                      "selection": {k: v for k, v in src.items() if k not in ("block", "params", "version")},
                      "params": params, "image": b["image"]}]
            steps += _auto_steps(b.get("native", {}), case, f"{slot}.{name}")
            produced = set(b["produces"])
            for t in cfg.get("transforms", []):
                tb = _block(t["block"])
                tparams, w = _params(t["block"], t.get("params"))
                warnings += w
                steps.append({"step": f"{slot}.{name}.{t['block']}", "kind": "transform",
                              "block": t["block"], "params": tparams, "image": tb["image"],
                              "needs_from_slots": tb.get("needs_from_slots", {}),
                              "rationale": tb.get("rationale")})
                produced |= set(tb["produces"])
            available |= produced
            branches.append({"branch": branch_id, "source": name, "steps": steps,
                             "produces": sorted(produced)})
        slots[slot] = branches

    # ---- runs: models x param sweeps x branch choices (comparison axes) ----
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
                identity = json.dumps({"case": case, "model": m["block"], "version": mb.get("version"),
                                       "params": p, "inputs": picks}, sort_keys=True)
                run_id = hashlib.sha1(identity.encode()).hexdigest()[:10]
                # readable key for the UI: model, non-default params, input choices
                defaults = {k: v["default"] for k, v in mb.get("params", {}).items()}
                parts = [m["block"]] + [f"{k}-{v}" for k, v in sorted(p.items()) if v != defaults.get(k)]
                parts += [picks[s] for s in sorted(picks)]
                run_key = "__".join(str(x) for x in parts)
                adapter_notes = []
                nres = mb.get("native", {}).get("resolution_m")
                if nres and nres != case["resolution_m"]:
                    adapter_notes.append(f"adapter regrids inputs {case['resolution_m']} m -> {nres} m "
                                         "(fuel models by majority, continuous by mean)")
                used_optional = sorted(set(mb.get("uses_if_present", [])) & available)
                runs.append({**desc, "run_id": run_id, "run_key": run_key, "image": mb["image"],
                             "block_version": mb.get("version"),
                             "resources": mb.get("resources", {}), "adapter_notes": adapter_notes,
                             "uses_optional": used_optional,
                             "reference": spec["reference"]["block"],
                             "metrics": [x["block"] for x in spec["metrics"]]})

    return {"workflow": spec["workflow"], "case": case, "slots": slots, "axes": axes,
            "runs": runs, "skipped": skipped, "warnings": warnings}


if __name__ == "__main__":  # quick look: python -m benchmark_framework.resolver spec.yaml
    import sys, yaml
    print(json.dumps(resolve(yaml.safe_load(open(sys.argv[1]))), indent=2))
