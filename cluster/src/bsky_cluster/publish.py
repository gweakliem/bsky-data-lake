"""Upload the site snapshot (data/cluster/site/, written by crosstab.py) for the reports site.

    uv run python -m bsky_cluster.publish [s3://<warehouse bucket>/reports]

Copies stories.json and communities.json to <site prefix>/inputs/cluster/, where
the reports build reads them (reports/lib/cluster.py); the next hourly build
shows them. The default prefix is s3://$BSKY_WAREHOUSE_BUCKET/reports (repo .env).
Needs AWS credentials in the environment.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

import boto3

from bsky_cluster.crosstab import SITE

log = logging.getLogger("bsky_cluster.publish")
FILES = ("stories.json", "communities.json")
MAX_BYTES = 1_000_000  # the reports site's limit for a data file (reports/tests/test_site.py)


def check(name: str, body: bytes) -> None:
    """The snapshot ships to every visitor's browser: small, and no adult communities."""
    if len(body) >= MAX_BYTES:
        raise SystemExit(f"{name} is {len(body):,} bytes, over the site's {MAX_BYTES:,}")
    data = json.loads(body)
    if name == "communities.json" and any(c.get("adult") for c in data["communities"]):
        raise SystemExit("communities.json lists an adult community")
    if name == "stories.json" and data.get("adult") != "exclude":
        raise SystemExit("stories.json is from an --adult run other than exclude")


def dotenv() -> dict[str, str]:
    """The repo's git-ignored .env."""
    env = Path(__file__).resolve().parents[3] / ".env"
    pairs = (line.split("=", 1) for line in (env.read_text().splitlines() if env.exists() else [])
             if "=" in line and not line.lstrip().startswith("#"))
    return {k.strip(): v.strip().strip("'\"") for k, v in pairs}


def publish(url: str, s3=None) -> None:
    s3 = s3 or boto3.client("s3")
    bucket, _, prefix = url.removeprefix("s3://").partition("/")
    for name in FILES:
        body = (SITE / name).read_bytes()
        check(name, body)
        key = f"{prefix.strip('/')}/inputs/cluster/{name}"
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
        log.info("s3://%s/%s: %d bytes", bucket, key, len(body))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("url", nargs="?")
    a = p.parse_args()
    if a.url is None:
        bucket = os.environ.get("BSKY_WAREHOUSE_BUCKET") or dotenv().get("BSKY_WAREHOUSE_BUCKET")
        if not bucket:
            raise SystemExit("pass the site prefix, or set BSKY_WAREHOUSE_BUCKET (repo .env)")
        a.url = f"s3://{bucket}/reports"
    publish(a.url)


if __name__ == "__main__":
    main()
