# TEMP(wstc-minimal): probes the WSTC step pods, to find out (without waiting on anyone):
#   apearce_probe_job           what a step pod is: image, versions, mounts, shared-volume space
#                               and writability, where the code-location loader lives
#   apearce_image_override_job  whether a step can run in a different image: it asks for
#                               docker.io/dagster/dagster-k8s:<same dagster version as the server>
#                               via op_tags (the same mechanism as the resources example)
# Reads only the pod's own environment. Never records secrets: no env dump, no tokens.
import os
import platform
import shutil
import sys
import time
from pathlib import Path

import dagster as dg

ME = "apearce"
GROUP = f"{ME}_probe"
OVERRIDE_IMAGE = f"docker.io/dagster/dagster-k8s:{dg.__version__}"
SAFE_ENV = ["DAGSTER_CURRENT_IMAGE", "DAGSTER_HOME", "HOSTNAME", "PYTHONPATH"]


def _describe(context) -> dict:
    info = {
        "python": sys.version.split()[0],
        "dagster": dg.__version__,
        "platform": platform.platform(),
        "argv": " ".join(sys.argv[:6])[:300],
        **{f"env_{k}": os.environ.get(k, "-") for k in SAFE_ENV},
        "in_kubernetes": "KUBERNETES_SERVICE_HOST" in os.environ,
        "service_account_mounted": Path("/var/run/secrets/kubernetes.io/serviceaccount").exists(),
    }
    for pkg in ["dagster_k8s", "dagster_postgres", "pandas", "boto3", "rasterio"]:
        try:
            m = __import__(pkg)
            info[f"has_{pkg}"] = getattr(m, "__version__", "yes")
        except Exception:
            info[f"has_{pkg}"] = "no"
    try:
        mounts = [l.split()[1] for l in open("/proc/mounts") if "/opt/dagster" in l.split()[1]]
        info["mounts_opt_dagster"] = ", ".join(mounts) or "none"
    except Exception as e:
        info["mounts_opt_dagster"] = f"? {e}"
    # where the code-location loader comes from: shared volume, or baked into the image?
    loaders = sorted({str(Path(m.__file__).parent) for m in list(sys.modules.values())
                      if getattr(m, "__file__", None) and m.__file__.startswith("/opt/dagster")
                      and "site-packages" not in m.__file__})
    info["code_modules_dirs"] = ", ".join(loaders)[:400] or "none"
    shared = Path("/opt/dagster/shared")
    if shared.exists():
        du = shutil.disk_usage(shared)
        info["shared_total_gb"] = round(du.total / 1e9, 1)
        info["shared_free_gb"] = round(du.free / 1e9, 1)
        probe_dir = shared / "storage" / ME / "_probe"
        try:
            probe_dir.mkdir(parents=True, exist_ok=True)
            f = probe_dir / f"write_test_{int(time.time())}.txt"
            f.write_text("ok")
            info["shared_writable"] = f.read_text() == "ok"
            f.unlink()
        except Exception as e:
            info["shared_writable"] = f"no ({type(e).__name__}: {e})"[:200]
    for k, v in info.items():
        context.log.info(f"{k}: {v}")
    context.add_output_metadata(info)
    return info


@dg.asset(key_prefix=ME, group_name=GROUP, description="Describes the default step pod.")
def probe_default_pod(context: dg.AssetExecutionContext) -> dict:
    return _describe(context)


@dg.asset(key_prefix=ME, group_name=f"{GROUP}_override",
          description=f"Same probe, in a step that asks for image {OVERRIDE_IMAGE}.",
          op_tags={"dagster-k8s/config": {"container_config": {"image": OVERRIDE_IMAGE}}})
def probe_override_pod(context: dg.AssetExecutionContext) -> dict:
    info = _describe(context)
    context.log.info(f"asked for {OVERRIDE_IMAGE}; running in {info.get('env_DAGSTER_CURRENT_IMAGE')}")
    return info


defs = dg.Definitions(
    assets=[probe_default_pod, probe_override_pod],
    jobs=[dg.define_asset_job(f"{ME}_probe_job", selection=dg.AssetSelection.groups(GROUP)),
          dg.define_asset_job(f"{ME}_image_override_job",
                              selection=dg.AssetSelection.groups(f"{GROUP}_override"))],
)
