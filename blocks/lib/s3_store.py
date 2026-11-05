"""Write-through copy of finished artifacts to the S3 bucket (the WSTC bucket on NRP).

    python s3_store.py <artifact_dir> <key_prefix> <result.json> [<file to presign> ...]

The shared volume stays the artifact store that steps read from and reuse; this keeps a
durable copy of every finished artifact in S3, under the same identity path, and makes
24-hour download links for chosen files (e.g. the prediction's GeoTIFFs).

* Already in S3 (its `_done.json` is there): nothing is uploaded; links are still made.
* Otherwise every file is uploaded, `_done.json` last, so an artifact whose `_done.json`
  is in S3 is complete there too.
* Never fails the step: S3 not configured, no credentials, or an error is reported in
  result.json (`status`), and the caller logs it.

Run with the image's block Python (it has boto3 in both images). Credentials and the
bucket come from the pod's environment (Kai's Kubernetes Secret): the standard AWS
variables, the bucket from DAGSTER_S3_BUCKET, the endpoint from AWS_ENDPOINT_URL_S3,
AWS_ENDPOINT_URL or S3_ENDPOINT_URL. Not part of any artifact's identity.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

DEFAULT_BUCKET = "wstc-dagster"      # deploy/wstc/README.md
LINK_SECONDS = 24 * 3600


def bucket() -> str:
    return os.environ.get("DAGSTER_S3_BUCKET") or DEFAULT_BUCKET


def client():
    import boto3
    from botocore.config import Config
    endpoint = next((os.environ[k] for k in ("AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "S3_ENDPOINT_URL")
                     if os.environ.get(k)), None)
    return boto3.client("s3", endpoint_url=endpoint, config=Config(
        connect_timeout=5, read_timeout=60, retries={"max_attempts": 2, "mode": "standard"}))


def exists(s3, bkt: str, key: str) -> bool:
    from botocore.exceptions import ClientError
    try:
        s3.head_object(Bucket=bkt, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def push(artifact: Path, prefix: str, presign: list[str]) -> dict:
    t = time.time()
    s3, bkt = client(), bucket()
    out = {"uri": f"s3://{bkt}/{prefix}/", "links": {}}
    if exists(s3, bkt, f"{prefix}/_done.json"):
        out.update(status="already in S3", files=0, bytes=0)
    else:
        files = sorted(p for p in artifact.rglob("*") if p.is_file())
        files.sort(key=lambda p: p.name == "_done.json")          # the marker goes last
        for p in files:
            s3.upload_file(str(p), bkt, f"{prefix}/{p.relative_to(artifact).as_posix()}")
        out.update(status="uploaded", files=len(files), bytes=sum(p.stat().st_size for p in files))
    for rel in presign:
        if (artifact / rel).exists():
            out["links"][rel] = s3.generate_presigned_url(
                "get_object", Params={"Bucket": bkt, "Key": f"{prefix}/{rel}"}, ExpiresIn=LINK_SECONDS)
    out["seconds"] = round(time.time() - t, 1)
    return out


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print("usage: python s3_store.py <artifact_dir> <key_prefix> <result.json> [<file> ...]")
        return 2
    artifact, prefix, result = Path(argv[0]), argv[1].strip("/"), Path(argv[2])
    try:
        out = push(artifact, prefix, argv[3:])
    except ModuleNotFoundError as e:
        out = {"status": f"not available: {e}"}
    except Exception as e:                    # no credentials, endpoint unreachable, denied, ...
        out = {"status": f"failed: {type(e).__name__}: {e}"[:500]}
    result.write_text(json.dumps(out))
    print(f"s3: {out['status']}" + (f" ({out.get('files', 0)} files, {out.get('bytes', 0):,} bytes)"
                                    if out["status"] == "uploaded" else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
