"""What the prototype leaves out (or keeps only): adult content, high-volume accounts and copy-paste spam.

Adult has two sources: authors' self-labels (on the firehose, so in silver) and
Bluesky's moderation labels from mod.bsky.app (modlabels.py). A post is adult if
either labels it. An account is adult if moderation labelled the account itself,
or if at least ADULT_MIN of its posts that week, and ADULT_SHARE of them, are
labelled by either source.

Labels are used as they stand now, not as of the day: this is a look back, and a
moderator's label from later in the week still says what the post was.

Modes (--adult on stories and communities), as on spacecowboy's For You feed:
exclude (the default) leaves adult out, only keeps nothing else, include keeps all.
"""

from __future__ import annotations

import datetime as dt
import logging
from functools import cache

import pyarrow.compute as pc
import pyarrow.parquet as pq

from bsky_cluster import modlabels
from bsky_cluster.fetch import DATA, days

log = logging.getLogger("bsky_cluster.filters")
ADULT_LABELS = ["porn", "sexual", "nudity", "sexual-figurative"]
ADULT_MIN = 3
ADULT_SHARE = 0.2
MAX_POSTS_PER_DAY = 300  # accounts posting more than this in a day are treated as bots
MODES = ("exclude", "only", "include")
TID_CHARS = modlabels.TID_CHARS
LIKED_LOOKBACK = dt.timedelta(days=30)  # likes in a week are mostly on posts from the month before


def did_of(uri: str) -> str:
    return uri.split("/", 3)[2] if uri.startswith("at://") else uri


def tid_time(rkey: str) -> dt.datetime | None:
    """A record key that is a TID encodes its creation time (microseconds, above 10 clock-id bits)."""
    if len(rkey) != 13 or any(c not in TID_CHARS for c in rkey):
        return None
    v = 0
    for c in rkey:
        v = v * 32 + TID_CHARS.index(c)
    return dt.datetime.fromtimestamp((v >> 10) / 1e6, dt.UTC)


def suffix(mode: str) -> str:
    """Output-file suffix: the default mode keeps the plain names."""
    return "" if mode == "exclude" else f"-adult-{mode}"


def window(start: dt.date, end: dt.date) -> tuple[dt.datetime, dt.datetime]:
    """The posts a week's stories and likes can touch: created up to LIKED_LOOKBACK before it, through its end."""
    lo = dt.datetime.combine(start, dt.time(), dt.UTC)
    return lo - LIKED_LOOKBACK, lo + dt.timedelta(days=(end - start).days + 1)


@cache
def moderated(start: dt.date, end: dt.date) -> tuple[frozenset[str], frozenset[str]]:
    """Posts (created in window()) and accounts that mod.bsky.app labels adult; an account label is on
    the did or its profile record."""
    if not modlabels.OUT.exists() or not any(modlabels.OUT.glob("*.parquet")):
        log.warning("no moderation labels in %s: adult filter uses self-labels only", modlabels.OUT)
        return frozenset(), frozenset()
    uris = modlabels.active(ADULT_LABELS, *window(start, end))["uri"].to_pylist()
    posts = frozenset(u for u in uris if "/app.bsky.feed.post/" in u)
    accounts = frozenset(did_of(u) for u in uris if u.startswith("did:") or "/app.bsky.actor.profile/" in u)
    log.info("moderation: %d adult-labelled posts, %d adult-labelled accounts", len(posts), len(accounts))
    return posts, accounts


def adult_posts(start: dt.date, end: dt.date) -> set[str]:
    out: set[str] = set()
    wanted = set(ADULT_LABELS)
    for d in days(start, end):
        t = pq.read_table(DATA / f"labels-{d}.parquet", columns=["uri", "self_labels"])
        out.update(u for u, labels in zip(t["uri"].to_pylist(), t["self_labels"].to_pylist())
                   if wanted.intersection(labels or ()))
    self_labelled = len(out)
    out |= moderated(start, end)[0]
    log.info("adult posts: %d self-labelled this week, %d with moderation labels", self_labelled, len(out))
    return out


def adult_accounts(start: dt.date, end: dt.date, posts: set[str]) -> set[str]:
    lo = dt.datetime.combine(start, dt.time(), dt.UTC)
    hi = lo + dt.timedelta(days=(end - start).days + 1)
    labelled: dict[str, int] = {}
    for uri in posts:  # count the week's labelled posts per author; the rkey dates moderation-labelled ones
        when = tid_time(uri.rsplit("/", 1)[-1])
        if when is None or lo <= when < hi:
            d = did_of(uri)
            labelled[d] = labelled.get(d, 0) + 1
    total: dict[str, int] = {}
    for d in days(start, end):
        t = pq.read_table(DATA / f"authors-{d}.parquet")
        for did, n in zip(t["did"].to_pylist(), t["posts"].to_pylist()):
            if did in labelled:
                total[did] = total.get(did, 0) + n
    by_share = {d for d, n in labelled.items() if d in total and n >= ADULT_MIN and n >= ADULT_SHARE * total[d]}
    out = by_share | moderated(start, end)[1]
    log.info("adult accounts: %d by share of labelled posts, %d in all with moderation account labels",
             len(by_share), len(out))
    return out


def busy_accounts(day: dt.date, limit: int = MAX_POSTS_PER_DAY) -> set[str]:
    t = pq.read_table(DATA / f"authors-{day}.parquet")
    return set(pc.filter(t["did"], pc.greater(t["posts"], limit)).to_pylist())
