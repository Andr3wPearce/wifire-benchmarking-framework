"""Compile a workflow, for a set of fires, into the one file the WSTC Dagster takes per upload.

The WSTC server loads uploaded .py files into a shared code location, and every step
runs in its own Kubernetes pod. A workflow therefore becomes one file,
`<me>_<workflow>.py` (from deploy/wstc/template_workflow.py), holding:

  * the plan for every fire (the resolver's output plus what WSTC needs: asset keys,
    identities, artifact paths, step payloads, the image each step runs in);
  * the code of every block the workflow uses, plus the framework's conventions
    checks and preview renderer, which each step writes to its pod's /tmp and runs;
  * each fire's reference perimeter, when the case has it as a local file.

Every asset is partitioned by fire, so "run this workflow on Hughes" is one Dagster
run, and the workflow's `report` is a table with one row per fire.

Each step's pod starts from its block's image (images/wstc/<family>/, pushed to Docker
Hub by scripts/build_wstc_image.sh). The code is not baked into the images, so
changing a block never needs a rebuild.

Identity and reuse
------------------
Every artifact is stored on the shared volume under an *identity*: a hash of everything
that determines its content, namely the block's code, its version, params and data
selection, the facts of the fire it depends on, and its inputs' identities. A step
whose identity already holds a finished artifact reuses it. So the same LANDFIRE is
downloaded once for every workflow that needs it, and changing anything that matters
(a parameter, the domain, the block's code) changes the identity: nothing stale is
ever served.
"""
from __future__ import annotations

import ast
import base64
import hashlib
import json
import re
from pathlib import Path

from .registry import BLOCKS, ROOT
from .resolver import resolve
from .workflows import case_for_workflow, load_cases, load_workflow, reference_text

UPLOAD_LIMIT = 5_000_000                     # bytes; the WSTC server's limit per uploaded file
TEMPLATE = ROOT / "deploy" / "wstc" / "template_workflow.py"
IMAGE_DIR = ROOT / "images" / "wstc"
IMAGE_FAMILIES = ("core", "elmfire")         # images/wstc/<family>/ -> docker.io/<user>/bench-<family>
# framework code every step needs next to the blocks' code
FRAMEWORK_CODE = [ROOT / "src" / "bench" / "conventions.py", ROOT / "src" / "bench" / "previews.py"]

# Which facts of the fire each slot's artifacts depend on. LANDFIRE depends only on the
# grid, so every workflow on the same fire and grid shares one download; changing the
# fire's ignition time re-fetches the wind but not the fuels. Models and metrics
# depend on everything their inputs do (through their inputs' identities).
CASE_KEYS = {
    "landscape": ["crs", "resolution_m", "domain_wgs84"],
    "weather": ["crs", "resolution_m", "domain_wgs84", "t0", "window_h"],
    "moisture": [],
    "ignition": ["crs", "t0", "ignitions"],
    "reference": ["crs", "resolution_m", "domain_wgs84", "reference"],
}


def _h(obj) -> str:
    """Short, stable hash of any JSON-able value."""
    return hashlib.sha1(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:10]


# --------------------------------------------------------------------------- #
# images
# --------------------------------------------------------------------------- #
def image_tag(family: str) -> str:
    """The tag scripts/build_wstc_image.sh pushes: a hash of the image's Dockerfile +
    requirements, so a recipe change is a new tag and a workflow file always names
    the image it was compiled for."""
    d = IMAGE_DIR / family
    return hashlib.sha1((d / "Dockerfile").read_bytes() + (d / "requirements.txt").read_bytes()).hexdigest()[:10]


def image_refs(user: str = "a3pearce") -> dict[str, str]:
    """{family: image reference} for every image family."""
    return {f: f"docker.io/{user}/bench-{f}:{image_tag(f)}" for f in IMAGE_FAMILIES}


def image_published(ref: str) -> bool | None:
    """Is this tag on Docker Hub (where NRP pulls it from)? None if Docker Hub can't be reached."""
    import requests
    repo, tag = ref.split("docker.io/", 1)[1].rsplit(":", 1)
    try:
        r = requests.get(f"https://hub.docker.com/v2/repositories/{repo}/tags/{tag}", timeout=20)
    except requests.RequestException:
        return None
    return r.status_code == 200


def block_image(block: str) -> str:
    """The image family a block runs in (its manifest's execution.image). The resolver's
    automatic steps (reproject) run in the core image."""
    return BLOCKS.get(block, {}).get("execution", {}).get("image", "core")


# what a step asks Kubernetes for unless its manifest says otherwise: the non-model blocks
# move a few MB of rasters, so this is plenty (and keeps us under the shared quota)
DEFAULT_RESOURCES = {"cpu": 0.5, "memory_gb": 1}


def k8s_resources(block: str) -> dict:
    """A block's manifest `resources` as Kubernetes requests/limits. NRP requires limits
    within 20% of requests, so limits are requests x 1.2."""
    r = {**DEFAULT_RESOURCES, **BLOCKS.get(block, {}).get("resources", {})}
    cpu, mem_mi = float(r["cpu"]), int(float(r["memory_gb"]) * 1024)
    return {"requests": {"cpu": f"{cpu:g}", "memory": f"{mem_mi}Mi"},
            "limits": {"cpu": f"{cpu * 1.2:g}", "memory": f"{int(mem_mi * 1.2)}Mi"}}


# --------------------------------------------------------------------------- #
# block code: what ships with a workflow, and what goes into identities
# --------------------------------------------------------------------------- #
BLOCKS_DIR = ROOT / "blocks"
# steps the resolver inserts itself: code next to the other blocks of their kind, no manifest
AUTO_STEP_CODE = {"reproject": BLOCKS_DIR / "methods" / "reproject.py"}
# the step runner for every non-model block (python run_block.py <step.json>)
RUNNER = BLOCKS_DIR / "lib" / "run_block.py"
# the write-through copy of finished artifacts to S3, run by the template after every step
S3_STORE = BLOCKS_DIR / "lib" / "s3_store.py"
# shared modules that don't shape any artifact's content, so changing them rebuilds nothing:
# the HTTP session (retries) and the runner (provenance)
NOT_IN_IDENTITY = {"http_session", "run_block", "s3_store"}


def block_code(block: str) -> Path:
    """The file holding a block's code: blocks/<kind>/<name>.py (the registry fills it in
    as the manifest's execution.code), or the automatic step's file."""
    if block in AUTO_STEP_CODE:
        return AUTO_STEP_CODE[block]
    code = BLOCKS.get(block, {}).get("execution", {}).get("code")
    if not code:
        raise ValueError(f"block '{block}' has no code (no execution: in its manifest)")
    return ROOT / code


def _shared_modules() -> dict[str, Path]:
    """Modules a block may import that aren't blocks themselves: blocks/lib/*.py, helpers
    kept with the blocks that use them (blocks/metrics/scoring.py), and the framework's
    conventions. {module name: file}."""
    blocks = {block_code(b) for b in BLOCKS if BLOCKS[b].get("execution")} | set(AUTO_STEP_CODE.values())
    mods = {f.stem: f for f in sorted(BLOCKS_DIR.glob("*/*.py")) if f not in blocks}
    mods.update({f.stem: f for f in FRAMEWORK_CODE})
    return mods


def _imports(path: Path) -> set[str]:
    """Top-level module names a file imports (read with ast; nothing is imported)."""
    names = set()
    for n in ast.walk(ast.parse(path.read_text())):
        if isinstance(n, ast.Import):
            names |= {a.name.split(".")[0] for a in n.names}
        elif isinstance(n, ast.ImportFrom) and n.module and not n.level:
            names.add(n.module.split(".")[0])
    return names


def code_closure(path: Path) -> list[Path]:
    """A code file plus every shared module it imports, directly or not."""
    shared, seen, todo = _shared_modules(), {}, [path]
    while todo:
        f = todo.pop()
        if f.stem in seen:
            continue
        seen[f.stem] = f
        todo += [shared[m] for m in _imports(f) if m in shared]
    return sorted(seen.values())


def block_code_hash(block: str) -> str:
    """Hash of the code that produces a block's artifacts, so changing it invalidates them:
    the block's own file plus every shared module it imports (blockkit, scoring,
    conventions), except the ones that don't shape the data (NOT_IN_IDENTITY). Fixing
    the wind block therefore doesn't re-download LANDFIRE, and adding retries to the
    HTTP session rebuilds nothing."""
    files = [f for f in code_closure(block_code(block)) if f.stem not in NOT_IN_IDENTITY]
    # by file name and content, not path: moving a block between folders changes nothing it builds
    # (file names are unique across blocks/, which the compiler enforces)
    return _h([[f.name, f.read_text()] for f in files])


def _case_for(slot: str, case: dict) -> dict:
    keys = CASE_KEYS.get(slot)
    return {k: case[k] for k in keys} if keys is not None else case


# --------------------------------------------------------------------------- #
# plan -> WSTC plan
# --------------------------------------------------------------------------- #
def _plan_one_fire(workflow: dict, case: dict, me: str, ref_text: str | None) -> dict:
    """One fire's steps, run and scores, with their identities and artifact paths."""
    plan = resolve(workflow, case)
    inc, wf = case["name"], plan["workflow"]
    steps, prev_by_slot = {}, {}
    for slot, slot_steps in plan["slots"].items():
        prev = None
        for st in slot_steps:
            block = st["block"]
            identity = _h({"case": _case_for(slot, case), "block": block, "version": st.get("version"),
                           "params": st.get("params", {}), "selection": st.get("selection", {}),
                           "upstream": prev and prev["identity"], "code": block_code_hash(block)})
            steps[st["step"]] = {
                "name": st["step"], "slot": slot, "block": block, "kind": st["kind"], "reason": st.get("reason"),
                "identity": identity, "path": f"{inc}/{slot}/{block}-{identity}",
                "payload": {"block": block, "kind": st["kind"], "case": case, "version": st.get("version"),
                            "params": st.get("params", {}), "selection": st.get("selection", {})},
                "after": prev and prev["name"], "input_paths": {"in": prev["path"]} if prev else {}}
            prev = steps[st["step"]]
        prev_by_slot[slot] = prev                 # the slot's final artifact feeds the model

    ref_block = plan["reference"]["block"]
    rid = _h({"case": _case_for("reference", case), "code": block_code_hash(ref_block),
              "file": ref_text and hashlib.sha1(ref_text.encode()).hexdigest()})
    reference = {"identity": rid, "path": f"{inc}/reference/{ref_block}-{rid}",
                 "payload": {"block": ref_block, "kind": "reference", "case": case, "params": {}}}

    m = plan["model"]
    pid = _h({"model": m["block"], "version": m["version"], "params": m["params"], "case": case,
              "inputs": {s: t["identity"] for s, t in prev_by_slot.items()}, "code": block_code_hash(m["block"])})
    inputs = {s: t["path"] for s, t in prev_by_slot.items()}
    run = {"identity": pid, "path": f"{inc}/runs/{m['block']}-{pid}", "input_paths": inputs,
           "input_steps": {s: t["name"] for s, t in prev_by_slot.items()},
           "run_json": {"model": m["block"], "block_version": m["version"], "run_id": pid, "case": case,
                        "params": m["params"], "inputs": inputs}}
    scores = {}
    for metric in plan["metrics"]:
        sid = _h({"prediction": pid, "reference": rid, "code": block_code_hash(metric)})
        scores[metric] = f"{inc}/scores/{metric}-{sid}"
    return {"plan": plan, "steps": steps, "reference": reference, "run": run, "scores": scores}


def compile_plan(workflow: dict, cases: dict[str, dict], me: str) -> dict:
    """The workflow planned for every fire, plus what WSTC needs to build one asset per step
    (keys, images, resources), with one partition per fire."""
    wf = workflow["workflow"]
    fires, references, step_order = {}, {}, None
    for name, raw in cases.items():
        case = case_for_workflow(raw, workflow)
        text = reference_text(raw)
        fires[name] = _plan_one_fire(workflow, case, me, text)
        if text is not None:
            references[name] = text
        order = list(fires[name]["steps"])
        if step_order is None:
            step_order = order
        elif order != step_order:
            # e.g. a fire in a CRS where a source needs no reproject step: one asset graph can't
            # have a step for some partitions only (yet)
            raise ValueError(f"case {name!r} plans steps {order}, others {step_order}; "
                             "run fires that need different steps as separate workflows")
    any_fire = next(iter(fires.values()))
    assets = []
    for step_name in step_order:
        st = any_fire["steps"][step_name]
        assets.append({"name": step_name, "slot": st["slot"], "block": st["block"], "kind": st["kind"],
                       "reason": st["reason"], "key": [me, wf, *step_name.split(".")],
                       "after": st["after"] and [me, wf, *st["after"].split(".")],
                       "image": block_image(st["block"]), "code": block_code(st["block"]).name,
                       "resources": k8s_resources(st["block"])})
    plan = any_fire["plan"]
    model, ref_block = plan["model"]["block"], plan["reference"]["block"]
    return {
        "me": me, "workflow": wf, "fires": {n: {k: v for k, v in f.items() if k != "plan"} for n, f in fires.items()},
        "references": references, "assets": assets,
        "model": {"block": model, "params": plan["model"]["params"], "image": block_image(model),
                  "code": block_code(model).name, "resources": k8s_resources(model)},
        "reference": {"block": ref_block, "key": [me, wf, "reference", ref_block], "image": block_image(ref_block),
                      "code": block_code(ref_block).name, "resources": k8s_resources(ref_block)},
        "metrics": {m: {"image": block_image(m), "code": block_code(m).name, "resources": k8s_resources(m)}
                    for m in plan["metrics"]},
        "warnings": {n: f["plan"]["warnings"] for n, f in fires.items() if f["plan"]["warnings"]},
        "image_families": sorted({a["image"] for a in assets} | {block_image(model), block_image(ref_block)}
                                 | {block_image(m) for m in plan["metrics"]}),
    }


def build_workflow_file(workflow_path: Path, cases: list[str] | None, me: str, out_dir: Path,
                        images: dict[str, str]) -> tuple[Path, dict]:
    """Write <me>_<workflow>.py (ready for d.upload()) for these fires (None = every case in
    cases/), and return (path, compiled plan). images: {family: image} (image_refs())."""
    workflow = load_workflow(Path(workflow_path))
    if not re.fullmatch(r"[a-z][a-z0-9_]*", workflow["workflow"]):
        raise ValueError(f"workflow name {workflow['workflow']!r}: use lowercase letters, digits, underscores")
    cp = compile_plan(workflow, load_cases(cases), me)
    missing = [f for f in cp["image_families"] if f not in images]
    if missing:
        raise ValueError(f"no image given for families {missing}")
    used = {a["block"] for a in cp["assets"]} | {cp["model"]["block"], cp["reference"]["block"]} | set(cp["metrics"])
    code_files = {f for b in used for f in code_closure(block_code(b))}
    code_files |= set(code_closure(RUNNER)) | {S3_STORE} | set(FRAMEWORK_CODE)
    names = [f.name for f in code_files]
    clash = sorted({n for n in names if names.count(n) > 1})
    if clash:   # every file lands in one directory in the step's pod
        raise ValueError(f"two code files share a name: {clash}")
    code = {f.name: f.read_text() for f in sorted(code_files, key=lambda f: f.name)}
    src = TEMPLATE.read_text()
    for k, v in {"__ME__": me, "__WF__": cp["workflow"],
                 "__IMAGES__": repr({f: images[f] for f in cp["image_families"]}),
                 "__PLAN__": repr(json.dumps(cp)), "__CODE__": repr(code)}.items():
        src = src.replace(k, v)
    out = Path(out_dir) / f"{me}_{cp['workflow']}.py"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(src)
    if out.stat().st_size > UPLOAD_LIMIT:
        raise ValueError(f"{out.name} is {out.stat().st_size:,} bytes; the upload limit is {UPLOAD_LIMIT:,}")
    return out, cp


def job_names(me: str, wf: str) -> dict:
    """The jobs a compiled workflow defines: `run` (one Dagster run per fire: inputs, model,
    scores) and `report` (the table across fires)."""
    return {"run": f"{me}_{wf}_job", "report": f"{me}_{wf}_report_job"}


def decode_preview(md: str) -> bytes | None:
    """The PNG inside a preview's markdown metadata (a data: URI)."""
    m = re.search(r"base64,([A-Za-z0-9+/=]+)", md or "")
    return base64.b64decode(m.group(1)) if m else None
