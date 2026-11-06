"""Sidecar for the web pod: keep a local copy of the published site.

    python lib/sync_site.py s3://<bucket>/reports /site

Polls <prefix>/current. When it names a new build, downloads that build to
/site/builds/<id>/ and then repoints the /site/current symlink, which nginx
serves, in one rename. Until the first build is published there is nothing
to serve and nginx returns 404.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
import time
from pathlib import Path

from publish import split

POLL_SECONDS = 60
log = logging.getLogger("sync_site")


def current_build(s3, bucket: str, prefix: str) -> str | None:
    try:
        return s3.get_object(Bucket=bucket, Key=f"{prefix}/current")["Body"].read().decode().strip() or None
    except s3.exceptions.NoSuchKey:
        return None


def download(s3, bucket: str, prefix: str, build_id: str, dest: Path) -> int:
    count = 0
    source = f"{prefix}/builds/{build_id}/"
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=source):
        for obj in page.get("Contents", []):
            target = dest / obj["Key"].removeprefix(source)
            target.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, obj["Key"], str(target))
            count += 1
    return count


def switch(root: Path, build_id: str) -> None:
    """Point root/current at builds/<build_id> atomically, then drop other builds."""
    link = root / "current"
    tmp = root / "current.tmp"
    tmp.unlink(missing_ok=True)
    tmp.symlink_to(Path("builds") / build_id)
    os.replace(tmp, link)
    for old in (root / "builds").iterdir():
        if old.name != build_id:
            shutil.rmtree(old, ignore_errors=True)


def sync_once(s3, url: str, root: Path, served: str | None) -> str | None:
    bucket, prefix = split(url)
    build_id = current_build(s3, bucket, prefix)
    if build_id is None or build_id == served:
        return served
    dest = root / "builds" / build_id
    shutil.rmtree(dest, ignore_errors=True)
    count = download(s3, bucket, prefix, build_id, dest)
    switch(root, build_id)
    log.info("serving build %s (%d file(s))", build_id, count)
    return build_id


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    import boto3

    url, root = sys.argv[1], Path(sys.argv[2])
    (root / "builds").mkdir(parents=True, exist_ok=True)
    s3, served = boto3.client("s3"), None
    while True:
        try:
            served = sync_once(s3, url, root, served)
        except Exception:
            # Keep serving the last good copy; try again next poll.
            log.exception("sync failed")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
