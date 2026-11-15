"""What the prototype leaves out: adult content, high-volume accounts and copy-paste spam.

Adult: our data has authors' self-labels only (Bluesky's moderation labels aren't on
the firehose). A post is adult if it carries an adult self-label, or its author
self-labelled at least ADULT_MIN posts and ADULT_SHARE of their posts that week.
"""

from __future__ import annotations

import datetime as dt
import logging

import pyarrow.compute as pc
import pyarrow.parquet as pq

from bsky_cluster.fetch import DATA, days

log = logging.getLogger("bsky_cluster.filters")
ADULT_LABELS = ["porn", "sexual", "nudity"]
ADULT_MIN = 3
ADULT_SHARE = 0.2
MAX_POSTS_PER_DAY = 300  # accounts posting more than this in a day are treated as bots


def did_of(uri: str) -> str:
    return uri.split("/", 3)[2]


def adult_posts(start: dt.date, end: dt.date) -> set[str]:
    out: set[str] = set()
    for d in days(start, end):
        t = pq.read_table(DATA / f"labels-{d}.parquet", columns=["uri", "self_labels"])
        wanted = set(ADULT_LABELS)
        out.update(u for u, labels in zip(t["uri"].to_pylist(), t["self_labels"].to_pylist())
                   if wanted.intersection(labels or ()))
    return out


def adult_accounts(start: dt.date, end: dt.date, posts: set[str]) -> set[str]:
    labelled: dict[str, int] = {}
    for uri in posts:
        d = did_of(uri)
        labelled[d] = labelled.get(d, 0) + 1
    total: dict[str, int] = {}
    for d in days(start, end):
        t = pq.read_table(DATA / f"authors-{d}.parquet")
        for did, n in zip(t["did"].to_pylist(), t["posts"].to_pylist()):
            if did in labelled:
                total[did] = total.get(did, 0) + n
    out = {d for d, n in labelled.items() if n >= ADULT_MIN and n >= ADULT_SHARE * total.get(d, n)}
    log.info("adult: %d self-labelled posts by %d accounts; %d accounts flagged", len(posts), len(labelled), len(out))
    return out


def busy_accounts(day: dt.date, limit: int = MAX_POSTS_PER_DAY) -> set[str]:
    t = pq.read_table(DATA / f"authors-{day}.parquet")
    return set(pc.filter(t["did"], pc.greater(t["posts"], limit)).to_pylist())
