"""Compile a workflow for the WSTC Dagster (NRP) into the one file it takes per upload.

The WSTC server loads uploaded .py files into a shared code location, and every
step runs in its own Kubernetes pod. A workflow therefore becomes one file,
`<me>_<workflow>.py` (from deploy/wstc/template_workflow.py), holding:

  * the plan (the resolver's output plus what WSTC needs: asset keys, identities,
    artifact paths, step payloads, the image each step runs in);
  * the code of every block the plan uses, plus the framework's conventions checks
    and preview renderer, which each step writes to its pod's /tmp and runs;
  * the reference perimeter, if the workflow names one.

Each step's pod starts from its block's image (images/wstc/<family>/, pushed to
Docker Hub by scripts/build_wstc_image.sh), which has the block's packages. The
code is not baked into the images, so changing a block never needs a rebuild.

Identity and reuse
------------------
Every artifact is stored on the shared volume under an *identity*: a hash of
everything that determines its content, namely the block's code, its version,
params and data selection, the parts of the case it depends on, and its inputs'
identities. A step whose identity already holds a finished artifact reuses it.
So the same LANDFIRE is downloaded once for every workflow and run that needs it,
and changing anything that matters (a parameter, the domain, the block's code)
changes the identity, so nothing stale is ever served.
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
from .workflows import load_spec

UPLOAD_LIMIT = 5_000_000                     # bytes; the WSTC server's limit per uploaded file
TEMPLATE = ROOT / "deploy" / "wstc" / "template_workflow.py"
IMAGE_DIR = ROOT / "images" / "wstc"
IMAGE_FAMILIES = ("core", "elmfire")         # images/wstc/<family>/ -> docker.io/<user>/bench-<family>
# framework code every step needs next to the blocks' code
FRAMEWORK_CODE = [ROOT / "src" / "bench" / "conventions.py", ROOT / "src" / "bench" / "previews.py"]

# Which parts of the case each kind of artifact depends on. LANDFIRE and the
# reference depend only on the grid, so workflows that differ in, say, ignition
# time share one LANDFIRE download. Everything else depends on the whole case.
CASE_KEYS = {
    "landscape": ["crs", "resolution_m", "domain_wgs84"],
    "reference": ["crs", "resolution_m", "domain_wgs84"],
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
    automatic steps (reproject) are core blocks."""
    return BLOCKS.get(block, {}).get("execution", {}).get("image", "core")


# what a step asks Kubernetes for unless its manifest says otherwise: the core blocks
# move a few MB of rasters, so this is plenty (and keeps us under the shared quota)
DEFAULT_RESOURCES = {"cpu": 0.5, "memory_gb": 1}


def k8s_resources(block: str) -> dict:
    """A block's manifest `resources` as Kubernetes requests/limits. NRP requires limits
    within 20% of requests, so limits are requests x 1.2."""
    r = {**DEFAULT_RESOURCES, **BLOCKS.get(block, {}).get("resources", {})}
    cpu, mem_mi = float(r["cpu"]), int(float(r["memory_gb"]) * 1024)
    return {"requests": {"cpu": f"{cpu:g}", "memory": f"{mem_mi}Mi"},
            "limits": {"cpu": f"{cpu * 1.2:g}", "memory": f"{int(mem_mi * 1.2)}Mi"}}


def block_code(block: str) -> Path:
    """The file holding a block's code (its manifest's execution.code)."""
    return ROOT / BLOCKS.get(block, {}).get("execution", {}).get("code", "images/core/core_blocks.py")


# --------------------------------------------------------------------------- #
# identity
# --------------------------------------------------------------------------- #
def _function_sources(path: Path) -> dict[str, str]:
    """Top-level function name -> its source text (read with ast; nothing is imported)."""
    text = path.read_text()
    return {n.name: ast.get_source_segment(text, n) for n in ast.parse(text).body
            if isinstance(n, ast.FunctionDef)}


# core blocks whose function name differs from the block name
CORE_FUNCTION = {"triangulated_ignition": "ignition_point", "ngfs_ignition": "ignition_point"}
# helpers in core_blocks.py / conventions.py that shape every core block's output
CORE_HELPERS = ["t0", "write_tif", "_read", "_masks", "_write_score", "_copy_tree", "grid_convergence_deg"]


def block_code_hash(block: str) -> str:
    """Hash of the code that produces a block's artifacts, so changing it invalidates them.

    Models hash their whole adapter file. Core blocks hash only their own function
    plus the shared helpers and the case grid, so fixing the wind block does not
    re-download LANDFIRE."""
    path = block_code(block)
    if path.name != "core_blocks.py":
        return _h(path.read_text())
    fns = _function_sources(path)
    grid = _function_sources(ROOT / "src" / "bench" / "conventions.py")["case_grid"]
    return _h([fns[CORE_FUNCTION.get(block, block)], *(fns[k] for k in CORE_HELPERS if k in fns), grid])


def _case_for(slot: str, case: dict) -> dict:
    keys = CASE_KEYS.get(slot)
    return {k: case[k] for k in keys} if keys else case


# --------------------------------------------------------------------------- #
# plan -> WSTC plan
# --------------------------------------------------------------------------- #
def compile_plan(spec: dict, me: str, reference_text: str | None) -> dict:
    """The resolver's plan plus what WSTC needs: asset keys, identities, artifact paths
    (relative to the artifact store), step payloads and each step's image family."""
    plan = resolve(spec)
    wf, case = plan["workflow"], plan["case"]
    inc = case["incident"]

    # input steps: one asset each, chained within a branch
    inputs, tails = [], {}
    for slot, branches in plan["slots"].items():
        for br in branches:
            prev = None
            for st in br["steps"]:
                block = st["block"]
                identity = _h({"case": _case_for(slot, case), "block": block, "version": st.get("version"),
                               "params": st.get("params", {}), "selection": st.get("selection", {}),
                               "upstream": prev and prev["identity"], "code": block_code_hash(block)})
                entry = {
                    "name": st["step"], "slot": slot, "branch": br["branch"], "block": block,
                    "kind": st["kind"], "reason": st.get("reason"),
                    "key": [me, wf, *st["step"].split(".")],
                    "identity": identity, "path": f"{inc}/{slot}/{block}-{identity}",
                    "image": block_image(block), "code": block_code(block).name,
                    "resources": k8s_resources(block),
                    "step": {"block": block, "kind": st["kind"], "case": case, "version": st.get("version"),
                             "params": st.get("params", {}), "selection": st.get("selection", {})},
                    "deps": [prev["key"]] if prev else [],
                    "input_paths": {"in": prev["path"]} if prev else {},
                }
                inputs.append(entry)
                prev = entry
            tails[(slot, br["branch"])] = prev      # the branch's final artifact feeds the models

    # the reference: identity = the file's content + the grid it is rasterized onto
    ref_block = plan["reference"]["block"]
    ref = None
    if reference_text is not None:
        rid = _h({"case": _case_for("reference", case), "code": block_code_hash(ref_block),
                  "file": hashlib.sha1(reference_text.encode()).hexdigest()})
        ref = {"key": [me, wf, "reference", ref_block], "block": ref_block, "identity": rid,
               "path": f"{inc}/reference/{ref_block}-{rid}", "image": block_image(ref_block),
               "code": block_code(ref_block).name, "resources": k8s_resources(ref_block),
               "step": {"block": ref_block, "kind": "reference", "case": case, "params": {}}}

    # runs: one partition of `prediction` each; identity = the run + its inputs' identities
    runs = {}
    for r in plan["runs"]:
        picked = {slot: tails[(slot, r["inputs"].get(slot, "main"))] for slot in plan["slots"]}
        pid = _h({"run_id": r["run_id"], "inputs": {s: t["identity"] for s, t in picked.items()},
                  "code": block_code_hash(r["model"]), "version": r.get("block_version")})
        runs[r["run_key"]] = {
            "model": r["model"], "run_id": r["run_id"], "identity": pid,
            "path": f"{inc}/runs/{r['model']}-{pid}", "image": block_image(r["model"]),
            "code": block_code(r["model"]).name, "params": r["params"], "inputs": r["inputs"],
            "input_paths": {s: t["path"] for s, t in picked.items()},
            "resources": k8s_resources(r["model"]),
            "run_json": {"model": r["model"], "block_version": r.get("block_version"), "run_id": r["run_id"],
                         "case": case, "params": r["params"],
                         "inputs": {s: t["path"] for s, t in picked.items()}},
            "scores": {}}

    # scores: one per metric per run; identity = prediction + reference + the metric's code
    metrics = {}
    for m in plan["metrics"]:
        metrics[m] = {"image": block_image(m), "code": block_code(m).name, "resources": k8s_resources(m)}
        for run in runs.values():
            sid = _h({"prediction": run["identity"], "reference": ref and ref["identity"],
                      "code": block_code_hash(m)})
            run["scores"][m] = f"{inc}/scores/{m}-{sid}"

    images = sorted({e["image"] for e in inputs} | {r["image"] for r in runs.values()}
                    | {m["image"] for m in metrics.values()} | ({ref["image"]} if ref else set()))
    return {"me": me, "workflow": wf, "case": case, "inputs": inputs, "reference": ref, "runs": runs,
            "metrics": metrics, "skipped": plan["skipped"], "warnings": plan["warnings"],
            "axes": plan["axes"], "image_families": images}


def build_workflow_file(spec_path: Path, me: str, out_dir: Path, images: dict[str, str],
                        reference: Path | None = None) -> tuple[Path, dict]:
    """Write <me>_<workflow>.py (ready for d.upload()) and return (path, compiled plan).

    images: {family: image reference} (image_refs()); every family the plan uses must be there.
    reference: the reference perimeter GeoJSON; defaults to the workflow's reference.file.
    """
    spec = load_spec(Path(spec_path))
    ref_path = reference or (ROOT / spec["reference"]["file"] if spec["reference"].get("file") else None)
    ref_text = Path(ref_path).read_text() if ref_path and Path(ref_path).exists() else None
    cp = compile_plan(spec, me, ref_text)
    if not re.fullmatch(r"[a-z][a-z0-9_]*", cp["workflow"]):
        raise ValueError(f"workflow name {cp['workflow']!r}: use lowercase letters, digits, underscores")
    missing = [f for f in cp["image_families"] if f not in images]
    if missing:
        raise ValueError(f"no image given for families {missing}")
    # ship the code of every block the plan uses, plus the framework's checks and previews
    used = ({e["block"] for e in cp["inputs"]} | {r["model"] for r in cp["runs"].values()}
            | set(cp["metrics"]) | ({cp["reference"]["block"]} if cp["reference"] else set()))
    code_files = {block_code(b) for b in used}
    code = {f.name: f.read_text() for f in sorted(code_files | set(FRAMEWORK_CODE))}
    src = TEMPLATE.read_text()
    for k, v in {"__ME__": me, "__WF__": cp["workflow"],
                 "__IMAGES__": repr({f: images[f] for f in cp["image_families"]}),
                 "__PLAN__": repr(json.dumps(cp)), "__REFERENCE__": repr(ref_text),
                 "__CODE__": repr(code)}.items():
        src = src.replace(k, v)
    out = Path(out_dir) / f"{me}_{cp['workflow']}.py"
    out.write_text(src)
    if out.stat().st_size > UPLOAD_LIMIT:
        raise ValueError(f"{out.name} is {out.stat().st_size:,} bytes; the upload limit is {UPLOAD_LIMIT:,}")
    return out, cp


def job_names(me: str, wf: str) -> dict:
    """The three jobs a compiled workflow defines."""
    return {"inputs": f"{me}_{wf}_inputs_job", "runs": f"{me}_{wf}_runs_job", "report": f"{me}_{wf}_report_job"}


def decode_preview(md: str) -> bytes | None:
    """The PNG inside a preview's markdown metadata (a data: URI)."""
    m = re.search(r"base64,([A-Za-z0-9+/=]+)", md or "")
    return base64.b64decode(m.group(1)) if m else None
