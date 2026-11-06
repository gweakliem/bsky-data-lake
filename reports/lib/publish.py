"""Build the site and publish it to S3, for the in-cluster CronJob.

    python lib/publish.py s3://<bucket>/reports

Uploads dist/ to <prefix>/builds/<build id>/, then writes the build id to
<prefix>/current. The web pod's sync sidecar (sync_site.py) serves whatever
`current` names, so a build that fails, or dies mid-upload, never replaces
the site. Keeps the newest KEEP_BUILDS builds and deletes older ones.
"""

from __future__ import annotations

import datetime as dt
import logging
import mimetypes
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KEEP_BUILDS = 3
log = logging.getLogger("publish")


def split(url: str) -> tuple[str, str]:
    bucket, _, prefix = url.removeprefix("s3://").partition("/")
    return bucket, prefix.strip("/")


def build_ids(s3, bucket: str, prefix: str) -> list[str]:
    """Build ids under <prefix>/builds/, oldest first (ids sort by time)."""
    ids = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/builds/", Delimiter="/"):
        ids.update(p["Prefix"].rstrip("/").rsplit("/", 1)[1] for p in page.get("CommonPrefixes", []))
    return sorted(ids)


def upload(s3, bucket: str, prefix: str, dist: Path, build_id: str) -> int:
    files = [f for f in sorted(dist.rglob("*")) if f.is_file()]
    for f in files:
        key = f"{prefix}/builds/{build_id}/{f.relative_to(dist).as_posix()}"
        content_type = mimetypes.guess_type(f.name)[0] or "application/octet-stream"
        s3.upload_file(str(f), bucket, key, ExtraArgs={"ContentType": content_type})
    return len(files)


def prune(s3, bucket: str, prefix: str, keep: int = KEEP_BUILDS) -> list[str]:
    """Delete all but the newest `keep` builds; returns the deleted ids."""
    old = build_ids(s3, bucket, prefix)[:-keep]
    for build_id in old:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/builds/{build_id}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
    return old


def publish(s3, url: str, dist: Path, now: dt.datetime | None = None) -> str:
    bucket, prefix = split(url)
    build_id = (now or dt.datetime.now(dt.timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    count = upload(s3, bucket, prefix, dist, build_id)
    # Last, so `current` only ever names a complete upload.
    s3.put_object(Bucket=bucket, Key=f"{prefix}/current", Body=build_id.encode(), ContentType="text/plain")
    log.info("published %d file(s) as build %s; pruned %s", count, build_id, prune(s3, bucket, prefix) or "none")
    return build_id


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    url = sys.argv[1]
    # check=True: a failed loader fails the build, and nothing is published.
    subprocess.run(["npm", "run", "build"], cwd=ROOT, check=True)
    import boto3

    publish(boto3.client("s3"), url, ROOT / "dist")


if __name__ == "__main__":
    main()
