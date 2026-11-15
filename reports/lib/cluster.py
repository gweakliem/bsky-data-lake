"""The clustering snapshot (stories and communities) for the Stories and Communities pages.

The clustering prototype (cluster/, `bsky_cluster.publish`) writes small aggregate
JSON files, stories.json and communities.json, to <site prefix>/inputs/cluster/
in the warehouse bucket; the report role can already read there. A loader prints
one of them. Where to find them:

- BSKY_CLUSTER_SNAPSHOT: a local directory or an s3:// prefix, for local builds
  (it wins over the fixture, so `npm run dev:fixture` can preview a real snapshot).
- BSKY_REPORTS_FIXTURE=1: a synthetic snapshot (no real posts), for tests.
- otherwise <BSKY_REPORTS_SITE_URL>/inputs/cluster, which publish.py sets in the
  in-cluster build.

A missing snapshot is {"available": false}, not an error: the pages say so, and
the rest of the site still builds.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

NAMES = ("stories", "communities")


def _location() -> str | None:
    if where := os.environ.get("BSKY_CLUSTER_SNAPSHOT"):
        return where.rstrip("/")
    if site := os.environ.get("BSKY_REPORTS_SITE_URL"):
        return f"{site.rstrip('/')}/inputs/cluster"
    return None


def _read(where: str, name: str, s3=None) -> dict | None:
    if not where.startswith("s3://"):
        path = Path(where) / f"{name}.json"
        return json.loads(path.read_text()) if path.exists() else None
    if s3 is None:
        import boto3

        s3 = boto3.client("s3")
    bucket, _, prefix = where.removeprefix("s3://").partition("/")
    try:
        body = s3.get_object(Bucket=bucket, Key=f"{prefix}/{name}.json")["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None
    return json.loads(body)


def snapshot(name: str, s3=None) -> dict:
    assert name in NAMES, name
    where = _location()
    if os.environ.get("BSKY_REPORTS_FIXTURE") and not os.environ.get("BSKY_CLUSTER_SNAPSHOT"):
        return {"available": True} | fixture(name)
    data = _read(where, name, s3) if where else None
    return {"available": True} | data if data else {"available": False}


def write(name: str) -> None:
    """Data loader body."""
    json.dump(snapshot(name), sys.stdout, ensure_ascii=False)


def fixture(name: str) -> dict:
    """A tiny synthetic snapshot with the real files' shape."""
    story = {"id": 1, "rank": 1, "words": ["outage", "email", "teams", "global"], "posts": 1200, "authors": 900,
             "excess": 800, "growth": 3.0, "burst": 2.5, "burst_hour": 8, "likes": 5000,
             "hourly": [10 * (1 + (h in (8, 9, 10)) * 9) for h in range(24)],
             "languages": [["en", 0.8], ["fr", 0.2]], "domains": [["example.com", 40]],
             "titles": [["A synthetic headline", 30]],
             "clusters": [{"lang": "en", "posts": 1000, "words": ["outage", "email"]},
                          {"lang": "fr", "posts": 200, "words": ["panne", "messagerie"]}],
             "examples": [{"text": "Synthetic example post", "url": "https://bsky.app/", "lang": "en"}],
             "communities": [{"id": 7, "words": ["tech", "protocol"], "lift": 3.0, "share": 0.05}]}
    topic = story | {"id": 2, "rank": None, "words": ["cats", "dogs"], "growth": 1.0, "excess": 0, "communities": []}
    if name == "stories":
        return {"day": "2026-11-13", "adult": "exclude", "embedded_posts": 3000, "clustered_posts": 1500,
                "left_out": {"replies": 1000, "adult": 50}, "clusters": 2, "assigned": 0.5,
                "stories": [story], "topics": [topic]}
    return {"start": "2026-11-07", "end": "2026-11-13", "accounts": 1000, "posts": 5000, "likes": 20000,
            "adult_left_out": 1,
            "communities": [{"id": 7, "rank": 1, "accounts": 400, "words": ["tech", "protocol", "blog"],
                             "adult": False, "adult_share": 0.0, "languages": [["en", 0.9]],
                             "examples": [{"text": "Synthetic liked post", "url": "https://bsky.app/"}],
                             "stories": [{"id": 1, "rank": 1, "words": ["outage", "email"], "lift": 3.0,
                                          "share": 0.05}]}]}
