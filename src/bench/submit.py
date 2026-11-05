"""Compile a submission (a prediction from a model we don't run) into the file the WSTC
Dagster takes, whose one job puts it in the S3 bucket.

A submission lives in the repository as submissions/<name>/<version>/<fire>.geojson
(timed perimeters, WGS84; see submissions/README.md). In S3 it lands at
<me>/bench/submissions/<name>/<version>/, which is what a workflow's
submitted_perimeters source names as its `prefix`.
"""
from __future__ import annotations

import base64
import re
from pathlib import Path

from .registry import ROOT

TEMPLATE = ROOT / "deploy" / "wstc" / "template_submit.py"
UPLOAD_LIMIT = 5_000_000


def s3_prefix(me: str, name: str, version: str) -> str:
    return f"{me}/bench/submissions/{name}/{version}"


def build_submit_file(name: str, version: str, me: str, out_dir: Path,
                      root: Path = ROOT / "submissions") -> tuple[Path, str]:
    """Write <me>_submit_<name>_<version>.py; return (path, its S3 prefix)."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", name) or not re.fullmatch(r"[a-z0-9_]+", version):
        raise ValueError("submission name and version: lowercase letters, digits, underscores")
    src = Path(root) / name / version
    files = sorted(src.glob("*.geojson"))
    if not files:
        raise ValueError(f"no submission files in {src}")
    tag, prefix = f"{name}_{version}", s3_prefix(me, name, version)
    text = TEMPLATE.read_text()
    for k, v in {"__ME__": me, "__NAME__": tag, "__PREFIX__": prefix,
                 "__FILES__": repr({f.name: base64.b64encode(f.read_bytes()).decode() for f in files})}.items():
        text = text.replace(k, v)
    out = Path(out_dir) / f"{me}_submit_{tag}.py"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    if out.stat().st_size > UPLOAD_LIMIT:
        raise ValueError(f"{out.name} is {out.stat().st_size:,} bytes; the upload limit is {UPLOAD_LIMIT:,}")
    return out, prefix


def job_name(me: str, name: str, version: str) -> str:
    return f"{me}_submit_{name}_{version}_job"
