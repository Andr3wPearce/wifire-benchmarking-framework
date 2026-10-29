"""TEMP(wstc-minimal): compile a workflow into the single file the WSTC Dagster takes.

The WSTC server loads one uploaded .py file per pipeline (max 1,000,000 bytes) and
only ships dagster/pandas/numpy/requests/boto3. So instead of images, we upload:

  <me>_bench_env.py        once: builds the blocks' Python environment on the shared
                           volume (CPython 3.12, rasterio, NumPy 1.26, compiled
                           pyretechnics). ~0.9 MB, nearly all of it pyretechnics.
  <me>_<workflow>.py       per workflow: the resolved plan + the blocks' code. Every
                           step runs its block in that environment, in its own pod.

Artifacts live on the shared volume under an *identity* path: a hash of everything
that determines the artifact (the block's code, version, params, data selection,
the parts of the case it depends on, and its inputs' identities). A step whose
identity path already holds a finished artifact is reused, not recomputed, across
runs and across workflows.
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

UPLOAD_LIMIT = 1_000_000
CORE = ROOT / "images" / "core" / "core_blocks.py"
ELMFIRE = ROOT / "images" / "elmfire" / "elmfire_block.py"
PREVIEWS = ROOT / "src" / "bench" / "previews.py"
TEMPLATE = ROOT / "deploy" / "wstc" / "template_workflow.py"
ENV_TEMPLATE = ROOT / "deploy" / "wstc" / "template_env.py"
WHEEL = ROOT / "wheels" / "pyretechnics-2026.10.20-cp312-cp312-linux_x86_64.slim.whl"
# what each kind of artifact depends on in the case (so e.g. LANDFIRE for the same domain
# is shared by workflows that differ only in ignition time)
CASE_KEYS = {
    "landscape": ["crs", "resolution_m", "domain_wgs84"],
    "reference": ["crs", "resolution_m", "domain_wgs84"],
}
IMAGE_DIR = ROOT / "images" / "wstc"
EXTRA_REQUIREMENTS = ["scipy~=1.12.0", "sortedcontainers~=2.4.0", "matplotlib<3.10", "requests~=2.32"]


def _h(obj) -> str:
    return hashlib.sha1(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:10]


def _function_sources(path: Path) -> dict[str, str]:
    """Top-level function name -> its source text (no import needed)."""
    text = path.read_text()
    return {n.name: ast.get_source_segment(text, n) for n in ast.parse(text).body
            if isinstance(n, ast.FunctionDef)}


def _block_code_hash(block: str) -> str:
    """Hash of the code that implements `block`, so changing it invalidates its artifacts."""
    if block == "elmfire":
        return _h(ELMFIRE.read_text())
    fns = _function_sources(CORE)
    impl = {"triangulated_ignition": "ignition_point", "ngfs_ignition": "ignition_point"}.get(block, block)
    if impl not in fns:
        return "dry-run"
    # the block's function plus the module's shared helpers (case grid, readers, writers)
    helpers = [fns[k] for k in ("case_grid", "t0", "write_tif", "_read", "_masks", "_write_score",
                                "_copy_tree", "grid_convergence_deg") if k in fns]
    return _h([fns[impl], *helpers])


def is_real(block: str) -> bool:
    m = BLOCKS[block]
    return "python" in m.get("execution", {}) or block == "elmfire"


def _case_for(slot: str, case: dict) -> dict:
    keys = CASE_KEYS.get(slot)
    return {k: case[k] for k in keys} if keys else case


def compile_plan(spec: dict, me: str, reference_text: str | None) -> dict:
    """The resolver's plan, plus what WSTC needs: asset keys, identity paths, step payloads."""
    plan = resolve(spec)
    wf, case = plan["workflow"], plan["case"]
    inc = case["incident"]
    inputs, tails = [], {}
    for slot, branches in plan["slots"].items():
        for br in branches:
            prev = None
            for st in br["steps"]:
                block = st["block"]
                name = st["step"]
                payload = {"block": block, "kind": st["kind"], "case": case,
                           "params": st.get("params", {}), "selection": st.get("selection", {}),
                           "version": st.get("version")}
                identity = _h({"case": _case_for(slot, case), "block": block, "version": st.get("version"),
                               "params": st.get("params", {}), "selection": st.get("selection", {}),
                               "upstream": prev and prev["identity"], "code": _block_code_hash(block)})
                real = block == "reproject" or is_real(block)
                entry = {"name": name, "slot": slot, "branch": br["branch"], "block": block,
                         "key": [me, wf, *name.split(".")], "identity": identity,
                         "path": f"{inc}/{slot}/{block}-{identity}", "real": real,
                         "step": payload, "deps": [prev["key"]] if prev else [],
                         "input_paths": {"in": prev["path"]} if prev else {},
                         "reason": st.get("reason"), "image_family": block_image_family(block)}
                inputs.append(entry)
                prev = entry
            tails[(slot, br["branch"])] = prev

    ref_block = spec["reference"]["block"]
    ref = None
    if reference_text is not None:
        rid = _h({"case": _case_for("reference", case), "file": hashlib.sha1(reference_text.encode()).hexdigest(),
                  "code": _block_code_hash(ref_block)})
        ref = {"key": [me, wf, "reference", ref_block], "block": ref_block, "identity": rid,
               "path": f"{inc}/reference/{ref_block}-{rid}", "real": is_real(ref_block),
               "step": {"block": ref_block, "kind": "reference", "case": case, "params": {}},
               "image_family": block_image_family(ref_block)}

    runs = {}
    for r in plan["runs"]:
        picked = {slot: tails[(slot, r["inputs"].get(slot, "main"))] for slot in plan["slots"]}
        pid = _h({"run_id": r["run_id"], "inputs": {s: t["identity"] for s, t in picked.items()},
                  "code": _block_code_hash(r["model"]), "version": r.get("block_version")})
        runs[r["run_key"]] = {
            "model": r["model"], "image_family": block_image_family(r["model"]), "run_id": r["run_id"], "identity": pid, "real": is_real(r["model"]),
            "path": f"{inc}/runs/{r['model']}-{pid}", "params": r["params"], "inputs": r["inputs"],
            "input_paths": {s: t["path"] for s, t in picked.items()},
            "input_real": {s: t["real"] for s, t in picked.items()},
            "block_version": r.get("block_version"), "resources": r.get("resources", {}),
            "run_json": {"model": r["model"], "block_version": r.get("block_version"),
                         "run_id": r["run_id"], "case": case, "params": r["params"],
                         "inputs": {s: t["path"] for s, t in picked.items()}}}
    metrics = {}
    for m in plan["runs"][0]["metrics"] if plan["runs"] else []:
        metrics[m] = {"real": is_real(m), "code": _block_code_hash(m), "image_family": block_image_family(m)}
        for key, run in runs.items():
            sid = _h({"prediction": run["identity"], "reference": ref and ref["identity"],
                      "code": metrics[m]["code"]})
            run.setdefault("scores", {})[m] = f"{inc}/scores/{m}-{sid}"
    return {"me": me, "workflow": wf, "case": case, "inputs": inputs, "reference": ref,
            "runs": runs, "metrics": metrics, "skipped": plan["skipped"], "warnings": plan["warnings"],
            "axes": plan["axes"], "resolved": plan}


IMAGE_FAMILIES = ("core", "elmfire")      # images/wstc/<family>/ -> docker.io/<user>/bench-<family>


def image_tag(family: str) -> str:
    """Same tag scripts/build_wstc_image.sh pushes: hash of that image's Dockerfile + requirements."""
    d = IMAGE_DIR / family
    return hashlib.sha1((d / "Dockerfile").read_bytes() + (d / "requirements.txt").read_bytes()).hexdigest()[:10]


def image_refs(user: str = "a3pearce") -> dict[str, str]:
    """{family: image}: every real block runs in its family's image (blocks/<name>.yaml, execution.wstc_image)."""
    return {f: f"docker.io/{user}/bench-{f}:{image_tag(f)}" for f in IMAGE_FAMILIES}


def block_image_family(block: str) -> str:
    return BLOCKS.get(block, {}).get("execution", {}).get("wstc_image", "core")   # auto steps (reproject): core


def image_published(ref: str) -> bool:
    """Is this tag on Docker Hub? (NRP pulls it from there.)"""
    import requests
    _, path = ref.split("docker.io/", 1)
    repo, tag = path.rsplit(":", 1)
    r = requests.get(f"https://hub.docker.com/v2/repositories/{repo}/tags/{tag}", timeout=20)
    return r.status_code == 200


def env_requirements() -> list[str]:
    reqs = [l.strip() for l in (ROOT / "images/elmfire/requirements.txt").read_text().splitlines()
            if l.strip() and not l.startswith("#") and not l.startswith(("pyretechnics", "dagster-pipes"))]
    return reqs + EXTRA_REQUIREMENTS


def env_id() -> str:
    return _h({"reqs": env_requirements(), "wheel": hashlib.sha1(WHEEL.read_bytes()).hexdigest(),
               "template": hashlib.sha1(ENV_TEMPLATE.read_bytes()).hexdigest()})


def build_env_file(me: str, out_dir: Path) -> Path:
    """<me>_bench_env.py: builds the shared environment once (job <me>_bench_env_job)."""
    import sys
    sys.path.insert(0, str(ROOT / "scripts"))
    from build_wstc_minimal import pack_pyretechnics   # same packing as the minimal run
    src = ENV_TEMPLATE.read_text()
    for k, v in {"__ME__": me, "__ENV_ID__": env_id(), "__REQUIREMENTS__": repr(env_requirements()),
                 "__PKG_B85__": pack_pyretechnics(WHEEL)}.items():
        src = src.replace(k, v)
    out = Path(out_dir) / f"{me}_bench_env.py"
    out.write_text(src)
    _check_size(out)
    return out


def build_workflow_file(spec_path: Path, me: str, out_dir: Path, reference: Path | None = None,
                        images: dict[str, str] | None = None) -> tuple[Path, dict]:
    """<me>_<workflow>.py: the compiled plan and the blocks' code, ready for d.upload().
    images: {family: image} from image_refs(): each step runs in its block's image (per-step
    images); None = every step uses the shared environment."""
    spec = load_spec(Path(spec_path))
    ref_path = reference or (ROOT / spec["reference"]["file"] if spec["reference"].get("file") else None)
    ref_text = Path(ref_path).read_text() if ref_path and Path(ref_path).exists() else None
    cp = compile_plan(spec, me, ref_text)
    if not re.fullmatch(r"[a-z][a-z0-9_]*", cp["workflow"]):
        raise ValueError(f"workflow name {cp['workflow']!r}: use lowercase letters, digits, underscores")
    src = TEMPLATE.read_text()
    for k, v in {"__ME__": me, "__WF__": cp["workflow"], "__ENV_ID__": env_id(), "__IMAGES__": repr(images),
                 "__PLAN__": repr(json.dumps({k: v for k, v in cp.items() if k != "resolved"})),
                 "__REFERENCE__": repr(ref_text),
                 "__CORE_SOURCE__": repr(CORE.read_text()),
                 "__ELMFIRE_SOURCE__": repr(ELMFIRE.read_text()),
                 "__PREVIEW_SOURCE__": repr(PREVIEWS.read_text())}.items():
        src = src.replace(k, v)
    out = Path(out_dir) / f"{me}_{cp['workflow']}.py"
    out.write_text(src)
    _check_size(out)
    return out, cp


def _check_size(path: Path) -> None:
    size = path.stat().st_size
    if size > UPLOAD_LIMIT:
        raise ValueError(f"{path.name} is {size:,} bytes; the WSTC upload limit is {UPLOAD_LIMIT:,}")


def job_names(me: str, wf: str) -> dict:
    return {"env": f"{me}_bench_env_job", "inputs": f"{me}_{wf}_inputs_job",
            "runs": f"{me}_{wf}_runs_job", "report": f"{me}_{wf}_report_job"}


def decode_preview(md: str) -> bytes | None:
    """The PNG inside a preview's markdown metadata (data URI)."""
    m = re.search(r"base64,([A-Za-z0-9+/=]+)", md or "")
    return base64.b64decode(m.group(1)) if m else None
