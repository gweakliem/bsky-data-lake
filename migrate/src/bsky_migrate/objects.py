"""Copy every object under an S3 prefix to another bucket, server side, resumably.

boto3's managed copy uses CopyObject, or UploadPartCopy for large objects, so
no data passes through the pod. Objects already at the destination with the
same size are skipped, so a rerun picks up where one stopped. The same
principal needs to read the source and write the destination: in the member
account, the migration role, read access granted by the old bucket's policy.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from urllib.parse import urlparse

log = logging.getLogger("bsky_migrate.objects")


@dataclass(frozen=True)
class Result:
    source: str
    dest: str
    objects: int
    copied: int
    skipped: int
    bytes_copied: int
    seconds: float


def split(url: str) -> tuple[str, str]:
    u = urlparse(url)
    if u.scheme != "s3" or not u.netloc:
        raise ValueError(f"not an s3:// URL: {url!r}")
    return u.netloc, u.path.lstrip("/")


def listing(s3, bucket: str, prefix: str) -> dict[str, int]:
    """key -> size for every object under the prefix."""
    out: dict[str, int] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            out[o["Key"]] = o["Size"]
    return out


def copy_prefix(s3, source: str, dest: str, workers: int = 16) -> Result:
    t0 = time.monotonic()
    sb, sp = split(source)
    db, dp = split(dest)
    if not sp.endswith("/") or not dp.endswith("/"):
        raise ValueError("source and dest prefixes must end with '/'")
    src = listing(s3, sb, sp)
    have = listing(s3, db, dp)
    todo = {k: n for k, n in src.items() if have.get(dp + k[len(sp):]) != n}
    log.info("%s -> %s: %d objects, %d to copy (%.1f GB)", source, dest, len(src), len(todo), sum(todo.values()) / 1e9)
    copied = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(s3.copy, {"Bucket": sb, "Key": k}, db, dp + k[len(sp):]): k for k in todo}
        for i, f in enumerate(as_completed(futures), 1):
            f.result()
            copied += todo[futures[f]]
            if i % 500 == 0:
                log.info("%s: %d of %d copied", dest, i, len(todo))
    return Result(source, dest, len(src), len(todo), len(src) - len(todo), copied, round(time.monotonic() - t0, 1))
