"""Running a real (containerized) block from Dagster, via Dagster Pipes.

A block whose manifest has an `execution:` section runs for real; every other
block is still a dry run (executor.run_block). Two backends, chosen with
BENCH_BACKEND:

  k8s    (default)  a Kubernetes pod running the block's image. Locally that's
                    minikube; on NRP it's the same code with a different
                    kube context, namespace and storage.
  local             a subprocess running the block's entrypoint with a local
                    Python (BENCH_BLOCK_PYTHON). For debugging without k8s.

Either way the block gets one thing, `run_dir` (relative to the artifact root),
reads `run.json` there, writes its outputs there, and reports metadata back
through Pipes so it shows up on the asset in the Dagster UI.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from dagster import MaterializeResult, MetadataValue

from .conventions import check_dir
from .executor import ARTIFACT_ROOT
from .previews import preview_markdown, render_prediction
from .registry import ROOT


class ConventionError(Exception):
    """An artifact crossing a block boundary breaks docs/conventions.md."""


def is_container_block(manifest: dict) -> bool:
    return "execution" in manifest


def _is_dry_run(d: Path) -> bool:  # TEMP(dry-run): goes away when every block is real
    prov = d / "provenance.json"
    return prov.exists() and json.loads(prov.read_text()).get("dry_run", False)


def _k8s_resources(manifest: dict) -> dict:
    """Requests == limits (NRP requires limits within 20% of requests).
    Capped for a laptop minikube via BENCH_K8S_MAX_CPU / BENCH_K8S_MAX_MEMORY_GB."""
    r = manifest.get("resources", {})
    # TEMP(local-dev): laptop caps; on NRP use the manifest values as-is
    cpu = min(float(r.get("cpu", 1)), float(os.environ.get("BENCH_K8S_MAX_CPU", 1)))
    mem = min(float(r.get("memory_gb", 2)), float(os.environ.get("BENCH_K8S_MAX_MEMORY_GB", 2)))
    q = {"cpu": str(cpu), "memory": f"{mem:g}Gi"}
    return {"requests": q, "limits": q}


def run_container_block(context, *, workflow: str, run_key: str, block: str, manifest: dict,
                        run: dict, case: dict, inputs: dict[str, str | None]) -> MaterializeResult:
    """Write run.json, launch the block, return its materialization (host + block metadata)."""
    run_dir_rel = f"{workflow}/runs/{run_key}/model"
    run_dir = ARTIFACT_ROOT / run_dir_rel
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run.json").write_text(json.dumps({
        "model": block, "block_version": manifest.get("version"), "run_id": run["run_id"],
        "case": case, "params": run["params"],
        "inputs": inputs,                      # paths relative to the artifact root, or null
    }, indent=2))

    # inputs that are real data (not dry-run receipts) must follow the conventions too
    for slot, rel in inputs.items():
        d = ARTIFACT_ROOT / rel if rel else None
        if d and d.is_dir() and not _is_dry_run(d):
            problems = check_dir(d, case)
            if problems:
                raise ConventionError(f"input slot '{slot}' violates docs/conventions.md:\n  "
                                      + "\n  ".join(problems))

    ex = manifest["execution"]
    backend = os.environ.get("BENCH_BACKEND", "k8s")
    extras = {"run_dir": run_dir_rel}
    context.log.info(f"{block}: launching on backend '{backend}', run dir {run_dir_rel}")

    if backend == "local":  # TEMP(local-dev): debugging without Kubernetes
        from dagster import PipesSubprocessClient
        python = os.environ.get("BENCH_BLOCK_PYTHON")
        if not python:
            raise RuntimeError("BENCH_BACKEND=local needs BENCH_BLOCK_PYTHON (a Python with the "
                               "block's requirements, e.g. images/elmfire/requirements.txt)")
        invocation = PipesSubprocessClient().run(
            context=context, extras=extras,
            command=[python, str(ROOT / ex["local_entrypoint"])],
            env={"BENCH_ARTIFACT_ROOT": str(ARTIFACT_ROOT)})
    elif backend == "k8s":
        from dagster_k8s import PipesK8sClient
        client = PipesK8sClient(load_incluster_config=False,
                                kube_context=os.environ.get("BENCH_KUBE_CONTEXT", "minikube"),  # TEMP(local-dev) default
                                poll_interval=2.0)
        try:
            invocation = client.run(
                context=context, extras=extras, image=ex["image"],
                namespace=os.environ.get("BENCH_K8S_NAMESPACE", "default"),
                env={"BENCH_ARTIFACT_ROOT": "/artifacts"},
                base_pod_meta={"labels": {"bench/block": block, "bench/run-key": run["run_id"]}},
                base_pod_spec={
                    "containers": [{
                        "name": "block",
                        # TEMP(local-dev): image built straight into minikube, never pulled;
                        # on NRP: a registry image with IfNotPresent
                        "imagePullPolicy": os.environ.get("BENCH_IMAGE_PULL_POLICY", "Never"),
                        "resources": _k8s_resources(manifest),
                        "volumeMounts": [{"name": "artifacts", "mountPath": "/artifacts"}],
                    }],
                    # TEMP(local-dev): repo/artifacts mounted into minikube by `minikube mount`;
                    # on NRP: object storage / a PVC instead of a hostPath
                    "volumes": [{"name": "artifacts", "hostPath": {
                        "path": os.environ.get("BENCH_K8S_ARTIFACT_HOSTPATH", "/bench-artifacts"),
                        "type": "Directory"}}],
                })
        except Exception as e:
            if "ErrImageNeverPull" in str(e) or "ErrImagePull" in str(e) or "ImagePullBackOff" in str(e):
                raise RuntimeError(  # TEMP(local-dev): on NRP images come from a registry
                    f"image {ex['image']} isn't in minikube. Build it with "
                    f"`scripts/build_blocks.sh {block}` (or relaunch, which builds missing images)") from e
            raise
    else:
        raise ValueError(f"unknown BENCH_BACKEND '{backend}' (use k8s or local)")

    block_md = dict(invocation.get_materialize_result().metadata or {})

    # docs/conventions.md: check what the block saw and what it produced
    checked = [run_dir / "prediction"] + [d for d in [run_dir / "synthetic_inputs"] if d.is_dir()]
    problems = [p for d in checked for p in check_dir(d, case)]
    if problems:
        raise ConventionError(f"{block} violates docs/conventions.md:\n  " + "\n  ".join(problems))
    context.log.info(f"conventions ok: {', '.join(d.name for d in checked)}")

    preview = {}
    try:   # a preview is a convenience: never fail a good run because a picture didn't render
        png, png_path = render_prediction(run_dir / "prediction", run["params"].get("duration_min"))
        preview = {"preview": MetadataValue.md(preview_markdown(png)), "preview_png": str(png_path)}
    except Exception as e:
        context.log.warning(f"no preview: {type(e).__name__}: {e}")

    return MaterializeResult(metadata={
        **preview,
        **block_md,
        "backend": backend,
        "image": ex["image"],
        "run_dir": str(run_dir),
        "prediction": str(run_dir / "prediction"),
        "dry_run": False,
    })
