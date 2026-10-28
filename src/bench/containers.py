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

from dagster import MaterializeResult

from .executor import ARTIFACT_ROOT
from .registry import ROOT


def is_container_block(manifest: dict) -> bool:
    return "execution" in manifest


def _k8s_resources(manifest: dict) -> dict:
    """Requests == limits (NRP requires limits within 20% of requests).
    Capped for a laptop minikube via BENCH_K8S_MAX_CPU / BENCH_K8S_MAX_MEMORY_GB."""
    r = manifest.get("resources", {})
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

    ex = manifest["execution"]
    backend = os.environ.get("BENCH_BACKEND", "k8s")
    extras = {"run_dir": run_dir_rel}
    context.log.info(f"{block}: launching on backend '{backend}', run dir {run_dir_rel}")

    if backend == "local":
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
                                kube_context=os.environ.get("BENCH_KUBE_CONTEXT", "minikube"),
                                poll_interval=2.0)
        invocation = client.run(
            context=context, extras=extras, image=ex["image"],
            namespace=os.environ.get("BENCH_K8S_NAMESPACE", "default"),
            env={"BENCH_ARTIFACT_ROOT": "/artifacts"},
            base_pod_meta={"labels": {"bench/block": block, "bench/run-key": run["run_id"]}},
            base_pod_spec={
                "containers": [{
                    "name": "block",
                    # image is built straight into minikube (scripts/build_blocks.sh), never pulled
                    "imagePullPolicy": os.environ.get("BENCH_IMAGE_PULL_POLICY", "Never"),
                    "resources": _k8s_resources(manifest),
                    "volumeMounts": [{"name": "artifacts", "mountPath": "/artifacts"}],
                }],
                # repo/artifacts on the host, mounted into the minikube node by
                # `minikube mount` (started by scripts/run_dagster.sh)
                "volumes": [{"name": "artifacts", "hostPath": {
                    "path": os.environ.get("BENCH_K8S_ARTIFACT_HOSTPATH", "/bench-artifacts"),
                    "type": "Directory"}}],
            })
    else:
        raise ValueError(f"unknown BENCH_BACKEND '{backend}' (use k8s or local)")

    block_md = dict(invocation.get_materialize_result().metadata or {})
    return MaterializeResult(metadata={
        **block_md,
        "backend": backend,
        "image": ex["image"],
        "run_dir": str(run_dir),
        "prediction": str(run_dir / "prediction"),
        "dry_run": False,
    })
