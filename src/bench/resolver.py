"""The resolver: a workflow (recipe) applied to a case (fire) -> a plan, before anything runs.

* every block named exists in the registry and is the right kind for where it is used;
* every parameter has its declared type (out-of-range values only warn: someone may
  mean it), and is merged with the manifest's defaults, so the plan is explicit;
* automatic steps are inserted where a source's native CRS differs from the case's
  (lat/lon ignition points get a reproject step to the case CRS);
* the model's required inputs must be produced by the workflow's sources, and every
  block it uses must have an implementation. A recipe that can't run is an error here,
  for every fire, rather than a failure inside a pod.

What *can* differ per fire (a fire with no NGFS detection, no weather station nearby)
is found out when the step runs, and recorded as "unavailable", with the reason.

The plan is plain data. `bench.wstc` compiles plans for the WSTC Dagster.
"""
from __future__ import annotations

from .registry import BLOCKS

PY_TYPES = {"float": (int, float), "int": (int,), "bool": (bool,), "str": (str,)}
# steps the resolver inserts itself; their code is in blocks/<kind>/ with no manifest (blocks/methods/reproject.py)
AUTO_BLOCKS = {"reproject"}


def _block(name: str) -> dict:
    if name not in BLOCKS:
        raise ValueError(f"unknown block '{name}' (no blocks/{name}.yaml)")
    return BLOCKS[name]


def implemented(name: str) -> bool:
    """A block runs for real only if its manifest says where its code is (`execution`)."""
    return name in AUTO_BLOCKS or "execution" in _block(name)


def _require(name: str, kind: str, where: str) -> dict:
    b = _block(name)
    if b.get("kind") != kind:
        raise ValueError(f"{where}: {name} is a {b.get('kind')}, not a {kind}")
    if not implemented(name):
        raise ValueError(f"{where}: {name} has no implementation yet (no `execution` in blocks/{name}.yaml)")
    return b


def params(block_name: str, given: dict | None) -> tuple[dict, list[str]]:
    """Manifest defaults merged with the workflow's values. A wrong type is an error (it
    would fail later, inside a pod, for a worse reason); out of the usual range warns."""
    schema = _block(block_name).get("params", {})
    merged = {k: v["default"] for k, v in schema.items()}
    warnings = []
    for key, value in (given or {}).items():
        if key not in schema:
            raise ValueError(f"{block_name}: unknown param '{key}' (known: {sorted(schema)})")
        if not isinstance(value, PY_TYPES[schema[key]["type"]]):
            raise ValueError(f"{block_name}.{key}: expected {schema[key]['type']}, got {value!r}")
        lo, hi = schema[key].get("range", (None, None))
        if lo is not None and not lo <= value <= hi:
            warnings.append(f"{block_name}.{key}={value} is outside the usual range [{lo}, {hi}]")
        merged[key] = value
    return merged, warnings


def resolve(workflow: dict, case: dict) -> dict:
    """(workflow, case as from workflows.case_for_workflow) -> plan:
    {"workflow", "case", "slots": {slot: [steps]}, "model", "reference", "metrics", "warnings"}."""
    warnings: list[str] = []
    slots: dict[str, list[dict]] = {}
    available: set[str] = set()       # every variable the inputs provide

    for slot, cfg in workflow["inputs"].items():
        src = cfg["source"]
        b = _require(src["block"], "source", f"inputs.{slot}")
        if b.get("slot") != slot:
            raise ValueError(f"inputs.{slot}: {src['block']} is a source for slot {b.get('slot')!r}")
        p, w = params(src["block"], src.get("params"))
        warnings += w
        steps = [{"step": f"{slot}.{src['block']}", "kind": "source", "block": src["block"],
                  "version": src.get("version", b.get("version")),
                  "selection": {k: v for k, v in src.items() if k not in ("block", "params", "version")},
                  "params": p}]
        native = b.get("native", {})
        if native.get("crs") and native["crs"] != case["crs"]:
            steps.append({"step": f"{slot}.auto_reproject", "kind": "auto", "block": "reproject",
                          "params": {"to": case["crs"]},
                          "reason": f"native CRS {native['crs']} differs from case CRS {case['crs']}"})
        produced = set(b["produces"])
        for t in cfg.get("methods", []):
            tb = _require(t["block"], "method", f"inputs.{slot}")
            tp, w = params(t["block"], t.get("params"))
            warnings += w
            steps.append({"step": f"{slot}.{t['block']}", "kind": "method", "block": t["block"],
                          "params": tp, "needs_from_slots": tb.get("needs_from_slots", {})})
            produced |= set(tb["produces"])
        available |= produced
        slots[slot] = steps

    m = workflow["model"]
    mb = _block(m["block"])
    if mb.get("kind") != "model":
        raise ValueError(f"model: {m['block']} is a {mb.get('kind')}, not a model")
    # what it needs first: that explains more than "not implemented" when both are true
    missing = sorted(set(mb.get("requires", [])) - available)
    if missing:
        raise ValueError(f"{m['block']} requires {missing}, which no input of this workflow provides")
    _require(m["block"], "model", "model")
    mp, w = params(m["block"], m.get("params"))
    warnings += w
    _require(workflow["reference"]["block"], "reference", "reference")
    for x in workflow["metrics"]:
        _require(x["block"], "metric", "metrics")

    return {"workflow": workflow["workflow"], "case": case, "slots": slots,
            "model": {"block": m["block"], "params": mp, "version": mb.get("version"),
                      "uses_optional": sorted(set(mb.get("uses_if_present", [])) & available)},
            "reference": workflow["reference"], "metrics": [x["block"] for x in workflow["metrics"]],
            "warnings": warnings}
