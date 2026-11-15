"""Pull the prototype's inputs from silver into local Parquet (data/cluster/, git-ignored).

    uv run python -m bsky_cluster.fetch posts 2026-11-13        # one day's embedded posts: vector + text
    uv run python -m bsky_cluster.fetch likes 2026-11-07 2026-11-13   # likes per day: did, subject_uri
    uv run python -m bsky_cluster.fetch texts 2026-11-07 2026-11-13   # post text per day, for labels

Each output is written per day and skipped if it exists, so a rerun resumes.
Needs AWS credentials in the environment (see the repo CLAUDE.md).
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.expressions import And, EqualTo

from bsky_embed import spec
from bsky_enrich import incremental as inc

log = logging.getLogger("bsky_cluster.fetch")
DATA = Path(__file__).resolve().parents[3] / "data" / "cluster"
POST_COLUMNS = ("uri", "cid", "did", "created_at", "langs", "text", "image_alts", "external_uri", "external_title",
                "reply_root_uri", "quote_uri", "embed_type")


def days(start: dt.date, end: dt.date) -> list[dt.date]:
    return [start + dt.timedelta(d) for d in range((end - start).days + 1)]


def scan(cat, table: str, day: dt.date, columns: tuple[str, ...], extra=None) -> pa.Table:
    f = EqualTo("day", day)
    t = cat.load_table((inc.SILVER, table))
    return t.scan(row_filter=And(f, extra) if extra is not None else f, selected_fields=columns).to_arrow()


def latest_versions(posts: pa.Table) -> pa.Table:
    """One row per (uri, cid): posts holds a row per create/update event."""
    keys = pc.binary_join_element_wise(posts["uri"], posts["cid"], "|")
    idx = pa.table({"k": keys, "i": pa.array(range(len(keys)))}).group_by("k").aggregate([("i", "max")])["i_max"]
    return posts.take(idx)


def fetch_posts(cat, day: dt.date) -> None:
    out = DATA / f"posts-{day}.parquet"
    if out.exists():
        return
    t0 = time.monotonic()
    emb = scan(cat, "post_embeddings", day, ("uri", "cid", "vector", "spec_version"),
               EqualTo("spec_version", spec.SPEC_VERSION)).drop_columns(["spec_version"])
    posts = latest_versions(scan(cat, "posts", day, POST_COLUMNS))
    # Arrow joins can't carry list columns (langs, image_alts): join keys and row numbers, then take.
    ek = emb.select(["uri", "cid"]).append_column("ei", pa.array(range(len(emb))))
    pk = posts.select(["uri", "cid"]).append_column("pi", pa.array(range(len(posts))))
    m = ek.join(pk, ["uri", "cid"], join_type="inner")
    rows = posts.take(m["pi"])
    rows = rows.append_column("vector", emb["vector"].cast(pa.large_binary()).take(m["ei"]))
    pq.write_table(rows, out, compression="zstd")
    log.info("%s: %d embeddings, %d post rows, %d joined in %.0f s", day, len(emb), len(posts), len(rows),
             time.monotonic() - t0)


def fetch_likes(cat, day: dt.date) -> None:
    out = DATA / f"likes-{day}.parquet"
    if out.exists():
        return
    t0 = time.monotonic()
    rows = scan(cat, "likes", day, ("did", "subject_uri"))
    rows = rows.filter(pc.starts_with(rows["subject_uri"], "at://")).filter(
        pc.match_substring(rows["subject_uri"], "/app.bsky.feed.post/"))
    pq.write_table(rows, out, compression="zstd")
    log.info("%s: %d likes in %.0f s", day, len(rows), time.monotonic() - t0)


def fetch_texts(cat, day: dt.date) -> None:
    out = DATA / f"texts-{day}.parquet"
    if out.exists():
        return
    t0 = time.monotonic()
    rows = scan(cat, "posts", day, ("uri", "text", "langs", "external_title"))
    pq.write_table(rows, out, compression="zstd")
    log.info("%s: %d post texts in %.0f s", day, len(rows), time.monotonic() - t0)


def fetch_labels(cat, day: dt.date) -> None:
    """Self-labelled posts (uri, did, self_labels) and every author's post count, for the adult filter."""
    out = DATA / f"labels-{day}.parquet"
    if out.exists():
        return
    t0 = time.monotonic()
    rows = scan(cat, "posts", day, ("uri", "did", "self_labels"))
    authors = rows.group_by("did").aggregate([("uri", "count")]).rename_columns(["did", "posts"])
    labelled = rows.filter(pc.greater(pc.list_value_length(rows["self_labels"]), 0))
    pq.write_table(labelled, out, compression="zstd")
    pq.write_table(authors, DATA / f"authors-{day}.parquet", compression="zstd")
    log.info("%s: %d posts, %d self-labelled, %d authors in %.0f s", day, len(rows), len(labelled), len(authors),
             time.monotonic() - t0)


def fetch_sample(cat, day: dt.date, fraction: float = 0.1) -> None:
    """A random fraction of a day's embedded posts, with what the stories step needs to place them."""
    out = DATA / f"sample-{day}.parquet"
    if out.exists():
        return
    t0 = time.monotonic()
    emb = scan(cat, "post_embeddings", day, ("uri", "cid", "vector", "spec_version"),
               EqualTo("spec_version", spec.SPEC_VERSION))
    keep = pa.array(np.random.default_rng(0).random(len(emb)) < fraction)
    emb = emb.filter(keep).select(["uri", "cid", "vector"])
    posts = latest_versions(scan(cat, "posts", day, ("uri", "cid", "did", "created_at", "reply_root_uri")))
    rows = emb.join(posts, ["uri", "cid"], join_type="inner")
    pq.write_table(rows, out, compression="zstd")
    log.info("%s: sampled %d embedded posts in %.0f s", day, len(rows), time.monotonic() - t0)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("what", choices=["posts", "likes", "texts", "labels", "sample"])
    p.add_argument("start", type=dt.date.fromisoformat)
    p.add_argument("end", type=dt.date.fromisoformat, nargs="?")
    a = p.parse_args()
    DATA.mkdir(parents=True, exist_ok=True)
    cat = inc.catalog()
    fn = {"posts": fetch_posts, "likes": fetch_likes, "texts": fetch_texts, "labels": fetch_labels,
          "sample": fetch_sample}[a.what]
    for day in days(a.start, a.end or a.start):
        fn(cat, day)


if __name__ == "__main__":
    main()
