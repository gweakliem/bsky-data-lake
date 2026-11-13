"""bsky-index: keep the search index in step with post_embeddings (WP E3).

Each run reads what post_embeddings has committed since the index's own state,
one span of silver windows at a time (about an hour, `--span-minutes`):

1. Join the span's embeddings (spec v1) to its posts for the text and filter
   columns, dropping posts deleted in the same span (bsky_enrich.incremental).
2. Delete the index's rows for posts the span updated (a new cid replaces the
   old row), append the new rows, then delete posts the span's `deletes` name.
3. Record the span in the runs table. A crash before this leaves rows newer
   than the state, which the next run deletes before it starts again.

Then it drops days older than the window, keeps the indexes current (full-text,
the scalar filter columns, IVF-PQ on the vectors once there are enough rows)
and compacts. A single writer is assumed: the CronJob forbids overlapping runs.

    uv run bsky-index --dry-run       # what a run would read; no writes
    uv run bsky-index                 # until caught up with post_embeddings
    uv run bsky-index --reindex       # also retrain the vector index (the window drifts)
    uv run bsky-index --rebuild --no-maintain   # initial load: drop and reload, no table deletes

Needs AWS credentials and BSKY_WAREHOUSE_BUCKET (or BSKY_LANCE_URI and
BSKY_SILVER_LOCATION's bucket).
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import math
import resource
import sys
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass

import lancedb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from lancedb.index import FTS, BTree, IvfPq, LabelList
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import And, EqualTo, GreaterThan, LessThanOrEqual

from bsky_embed import spec
from bsky_enrich import incremental as inc
from bsky_search import store

log = logging.getLogger("bsky_search.index")

EMBEDDINGS = "post_embeddings"
POST_COLUMNS = (*inc.POST_COLUMNS, "created_at")
MIN_VECTOR_INDEX_ROWS = 100_000  # below this a flat scan is fast and IVF-PQ trains poorly
SUB_VECTORS = 64  # 16 dims each: 64 bytes a vector in memory
TRAIN_SAMPLE_RATE = 64
DELETE_BATCH = 1000
# Memory is about 10 KB a row while a span is joined (dry run: 1.8 GB at 100k rows),
# so a span with more rows is split by time. An ingest catch-up after an outage puts
# hours of posts into one hour of bronze_ingested_at (2.5M on 2026-11-02 12:00).
MAX_SPAN_ROWS = 200_000
# icu splits Japanese and Chinese into words; no stemming or stop words, since posts come in every language.
TEXT_INDEX = FTS(base_tokenizer="icu", stem=False, remove_stop_words=False, lower_case=True, ascii_folding=True)
SCALAR = {"uri": BTree(), "bronze_ingested_at": BTree(), "day": BTree(), "langs": LabelList()}  # no Bitmap on dates


@dataclass
class Stats:
    spans: int = 0
    read: int = 0  # embedding rows read
    appended: int = 0
    replaced: int = 0  # rows removed because the post was updated
    deleted: int = 0  # rows removed because the post was deleted
    expired: int = 0
    rows: int = 0  # in the index at the end


def exists(db: lancedb.DBConnection, name: str) -> bool:
    return name in db.list_tables().tables


def watermark(db: lancedb.DBConnection) -> dt.datetime | None:
    if not exists(db, store.RUNS):
        return None
    through = db.open_table(store.RUNS).to_arrow()["through"]
    top = pc.max(through).as_py()
    return top.astimezone(dt.UTC) if top else None


def spans(windows: list[inc.Window], through: dt.datetime, minutes: int) -> Iterator[inc.Window]:
    """Consecutive windows merged into spans of at least `minutes`, none past `through`.

    The last span is cut at `through` (post_embeddings' watermark): embed
    commits never split a loader commit, so everything up to it is there.
    """
    after: dt.datetime | None = None
    end: dt.datetime | None = None
    for w in windows:
        if w.after is not None and w.after >= through:
            break
        if end is None:
            after = w.after
        end = min(w.through, through)
        if end >= through or after is None or end - after >= dt.timedelta(minutes=minutes):
            yield inc.Window(after, end)
            if end >= through:
                return
            end = None
    if end is not None:
        yield inc.Window(after, end)


def split(cat: Catalog, w: inc.Window, max_rows: int, namespace: str = inc.SILVER) -> list[inc.Window]:
    """The span cut by time into pieces of about `max_rows` embeddings at most.

    Reads only the timestamps. Any cut is safe: the span ends at or before
    post_embeddings' watermark, so everything in it is committed. Rows sharing a
    timestamp stay in one piece.
    """
    marks = cat.load_table(f"{namespace}.{EMBEDDINGS}").scan(
        row_filter=And(_between(w), EqualTo("spec_version", spec.SPEC_VERSION)),
        selected_fields=(inc.WATERMARK,),
    ).to_arrow()[inc.WATERMARK]
    if len(marks) <= max_rows:
        return [w]
    marks = pc.take(marks, pc.sort_indices(marks)).to_pylist()
    pieces, after, i = [], w.after, max_rows
    while i < len(marks):
        cut = marks[i - 1]
        while i < len(marks) and marks[i] == cut:  # keep equal timestamps together
            i += 1
        if i >= len(marks) or cut >= w.through:
            break
        pieces.append(inc.Window(after, cut))
        after, i = cut, i + max_rows
    pieces.append(inc.Window(after, w.through))
    return pieces


def uri_hashes(uris: list[str]) -> np.ndarray:
    """64-bit hashes of uris: 8 bytes each instead of about 75 as strings."""
    return np.fromiter((int.from_bytes(hashlib.blake2b(u.encode(), digest_size=8).digest(), "little") for u in uris),
                       np.uint64, count=len(uris))


@dataclass
class Superseded:
    """For --rebuild: every post deleted, and each edited post's newest version, up to a watermark.

    A post's delete or edit always comes after its creation, so dropping these
    rows before they are appended makes every append final: the load needs no
    table deletes, which scan the whole uri column until it is indexed.

    Deleted uris are kept as sorted 64-bit hashes (about 60 MB for 30 days of
    deletes at 2026-11 traffic, 7.5M) and read a batch at a time, so the strings
    are never all in memory. A collision drops a live post: about 1 in 10^7 for
    15M deletes against 175M posts.
    """

    deleted: np.ndarray  # sorted uri_hashes
    newest: dict[str, dt.datetime]  # uri -> bronze_ingested_at of its newest edit

    @classmethod
    def read(cls, cat: Catalog, w: inc.Window, namespace: str = inc.SILVER) -> Superseded:
        reader = cat.load_table(f"{namespace}.deletes").scan(
            row_filter=And(_between(w), EqualTo("collection", inc.POST)), selected_fields=("uri",)
        ).to_arrow_batch_reader()
        parts = [uri_hashes(batch.column("uri").to_pylist()) for batch in reader]
        deleted = np.unique(np.concatenate(parts)) if parts else np.empty(0, np.uint64)  # sorted
        edits = cat.load_table(f"{namespace}.posts").scan(
            row_filter=And(_between(w), EqualTo("operation", "update")), selected_fields=("uri", inc.WATERMARK)
        ).to_arrow().group_by("uri").aggregate([(inc.WATERMARK, "max")])
        newest = dict(zip(edits["uri"].to_pylist(), edits[f"{inc.WATERMARK}_max"].to_pylist(), strict=True))
        return cls(deleted, newest)

    def drop(self, rows: pa.Table) -> pa.Table:
        uris = rows["uri"].to_pylist()
        h = uri_hashes(uris)
        at = np.minimum(np.searchsorted(self.deleted, h), max(len(self.deleted) - 1, 0))
        keep = ~(self.deleted[at] == h) if len(self.deleted) else np.ones(len(uris), bool)
        if self.newest:
            marks = rows["bronze_ingested_at"].to_pylist()
            keep &= np.array([u not in self.newest or m >= self.newest[u] for u, m in zip(uris, marks, strict=True)],
                             bool)
        return rows.filter(pa.array(keep))


def _between(w: inc.Window):
    upper = LessThanOrEqual(inc.WATERMARK, w.through.isoformat())
    return upper if w.after is None else And(GreaterThan(inc.WATERMARK, w.after.isoformat()), upper)


def span_rows(cat: Catalog, w: inc.Window, first_day: dt.date, namespace: str = inc.SILVER) -> tuple[pa.Table, list[str], int]:
    """The index rows for a span (one per uri), the uris it updates, and how many embedding rows were read."""
    emb = cat.load_table(f"{namespace}.{EMBEDDINGS}").scan(
        row_filter=And(_between(w), EqualTo("spec_version", spec.SPEC_VERSION)),
        selected_fields=("uri", "cid", "bronze_ingested_at", "vector"),
    ).to_arrow()
    if not emb.num_rows:
        return store.SCHEMA.empty_table(), [], 0
    posts = inc.read_posts(cat, w, namespace, POST_COLUMNS)
    posts = posts.filter(pc.greater_equal(posts["day"], pa.scalar(first_day, pa.date32())))
    # Arrow joins can't carry list columns, so join on keys and row numbers, then take.
    keys = posts.select(["uri", "cid"]).append_column("_p", pa.array(range(posts.num_rows), pa.int64()))
    pairs = emb.select(["uri", "cid"]).append_column("_e", pa.array(range(emb.num_rows), pa.int64()))
    pairs = pairs.join(keys, keys=["uri", "cid"], join_type="inner")
    if not pairs.num_rows:
        return store.SCHEMA.empty_table(), [], emb.num_rows
    joined = posts.take(pairs["_p"]).append_column("vector", emb["vector"].take(pairs["_e"]))
    # One row per uri: an update in the same span, or an embed rerun's duplicate, keeps the newest.
    joined = joined.sort_by([(inc.WATERMARK, "descending")]).append_column("_i", pa.array(range(joined.num_rows)))
    keep = joined.group_by("uri", use_threads=False).aggregate([("_i", "min")])["_i_min"]
    joined = joined.take(pc.sort_indices(keep)).drop_columns(["_i"])
    updated = pc.unique(joined.filter(pc.equal(joined["operation"], "update"))["uri"]).to_pylist()
    texts = [spec.text(r) for r in joined.select(["text", "image_alts", "external_title"]).to_pylist()]
    rows = pa.table({
        "uri": joined["uri"], "cid": joined["cid"], "did": joined["did"], "day": joined["day"],
        "created_at": joined["created_at"].cast(store.TS), "langs": joined["langs"].cast(pa.list_(pa.string())),
        "text": pa.array(texts, pa.string()), "external_uri": joined["external_uri"],
        "bronze_ingested_at": joined[inc.WATERMARK].cast(store.TS), "vector": store.vectors(joined["vector"]),
    }, schema=store.SCHEMA)
    return rows, sorted(updated), emb.num_rows


def span_deletes(cat: Catalog, w: inc.Window, namespace: str = inc.SILVER) -> list[str]:
    deletes = cat.load_table(f"{namespace}.deletes").scan(
        row_filter=And(_between(w), EqualTo("collection", inc.POST)), selected_fields=("uri",)
    ).to_arrow()
    return sorted(set(deletes["uri"].to_pylist()))


def _quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def delete_uris(table, uris: list[str]) -> int:
    removed = 0
    for i in range(0, len(uris), DELETE_BATCH):
        r = table.delete(f"uri IN ({', '.join(_quote(u) for u in uris[i : i + DELETE_BATCH])})")
        removed += r.num_deleted_rows
    return removed


def run(
    cat: Catalog,
    db: lancedb.DBConnection,
    *,
    today: dt.date,
    namespace: str = inc.SILVER,
    span_minutes: int = 60,
    max_span_rows: int = MAX_SPAN_ROWS,
    max_spans: int | None = None,
    dry_run: bool = False,
    maintain_indexes: bool = True,
    reindex: bool = False,
    rebuild: bool = False,
) -> Stats:
    stats = Stats()
    first_day = store.cutoff(today)
    emb_through = inc.committed_through(cat.load_table(f"{namespace}.{EMBEDDINGS}"))
    if rebuild and not dry_run:
        for name in (store.TABLE, store.RUNS):
            if exists(db, name):
                db.drop_table(name)
        log.warning("rebuild: dropped the index and its state")
    after = None if rebuild else watermark(db)
    table = db.open_table(store.TABLE) if exists(db, store.TABLE) else None
    log.info("index through %s, post_embeddings through %s, window from %s",
             after.isoformat() if after else "none", emb_through.isoformat() if emb_through else "none", first_day)
    if table is not None and after is not None and not dry_run:
        stale = table.delete(f"bronze_ingested_at > timestamp '{after.strftime('%Y-%m-%d %H:%M:%S.%f')}'").num_deleted_rows
        if stale:
            log.warning("removed %d rows past the state from an interrupted run", stale)
    start = after or dt.datetime.combine(first_day - dt.timedelta(days=1), dt.time(), dt.UTC)
    superseded = None
    if rebuild and emb_through is not None:
        superseded = Superseded.read(cat, inc.Window(start, emb_through), namespace)
        log.info("rebuild: %d deleted posts and %d edited posts to leave out", len(superseded.deleted), len(superseded.newest))
    if emb_through is not None:
        for w in (piece for span in spans(inc.windows(cat, start, namespace), emb_through, span_minutes)
                  for piece in split(cat, span, max_span_rows, namespace)):
            rows, updated, read = span_rows(cat, w, first_day, namespace)
            if superseded is not None:
                rows, updated, gone = superseded.drop(rows), [], []
            else:
                gone = span_deletes(cat, w, namespace)
            stats.spans += 1
            stats.read += read
            if dry_run:
                stats.appended += rows.num_rows
                log.info("span (%s, %s]: %d rows, %d deletes", w.after, w.through, rows.num_rows, len(gone))
            else:
                if rows.num_rows:
                    if table is None:
                        table = db.create_table(store.TABLE, schema=store.SCHEMA)
                    stats.replaced += delete_uris(table, updated)
                    table.add(rows)
                    stats.appended += rows.num_rows
                if table is not None and gone:
                    stats.deleted += delete_uris(table, gone)
                record(db, w.through, rows.num_rows, len(gone))
                after = w.through
                log.info("span (%s, %s]: %d rows committed, %d so far", w.after, w.through, rows.num_rows, stats.appended)
            if max_spans is not None and stats.spans >= max_spans:
                break
    if table is not None and not dry_run:
        stats.expired = table.delete(f"day < date '{first_day.isoformat()}'").num_deleted_rows
        if maintain_indexes:
            maintain(table, reindex=reindex)
        stats.rows = table.count_rows()
    return stats


def record(db: lancedb.DBConnection, through: dt.datetime, appended: int, deleted: int) -> None:
    row = pa.table({"through": pa.array([through], store.TS), "indexed_at": pa.array([dt.datetime.now(dt.UTC)], store.TS),
                    "appended": [appended], "deleted": [deleted]}, schema=store.RUNS_SCHEMA)
    if exists(db, store.RUNS):
        db.open_table(store.RUNS).add(row)
    else:
        db.create_table(store.RUNS, row)


def maintain(table, reindex: bool = False) -> None:
    """Create missing indexes, fold new rows into existing ones, compact, and drop old versions."""
    names = {i.columns[0]: i.index_type for i in table.list_indices()}
    for column, config in [*SCALAR.items(), ("text", TEXT_INDEX)]:
        if column not in names:
            table.create_index(column, config=config)
    n = table.count_rows()
    if n >= MIN_VECTOR_INDEX_ROWS and ("vector" not in names or reindex):
        partitions = max(1, round(math.sqrt(n)))
        log.info("training IVF-PQ on %d rows: %d partitions, %d sub-vectors", n, partitions, SUB_VECTORS)
        # k-means trains on sample_rate rows per partition: 64 keeps it to about 0.8 GB of float16 at 37M rows (256 is 4x).
        table.create_index("vector", config=IvfPq(distance_type="cosine", num_partitions=partitions,
                                                  num_sub_vectors=SUB_VECTORS, sample_rate=TRAIN_SAMPLE_RATE))
    # Compaction, new rows into every index, and versions older than a day (a reader may still hold one).
    table.optimize(cleanup_older_than=dt.timedelta(days=1))


def _peak_rss_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 2**20 if sys.platform == "darwin" else peak / 2**10


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--span-minutes", type=int, default=60, help="silver windows merged per commit (default 60)")
    parser.add_argument("--max-spans", type=int, help="stop after this many spans")
    parser.add_argument("--namespace", default=inc.SILVER)
    parser.add_argument("--max-span-rows", type=int, default=MAX_SPAN_ROWS,
                        help="split a span with more embeddings than this, by time (bounds memory)")
    parser.add_argument("--rebuild", action="store_true",
                        help="drop the index and load it again from post_embeddings, leaving out deleted and "
                             "superseded posts up front (no table deletes); then run --reindex")
    parser.add_argument("--reindex", action="store_true", help="retrain the vector index on the current rows")
    parser.add_argument("--no-maintain", action="store_true", help="skip index upkeep and compaction (backfill passes)")
    parser.add_argument("--dry-run", action="store_true", help="read and count only: no writes")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)

    from dotenv import load_dotenv

    from bsky_enrich import lake

    load_dotenv(lake.REPO / ".env")
    start = time.monotonic()
    stats = run(inc.catalog(), store.connect(), today=dt.datetime.now(dt.UTC).date(), namespace=args.namespace,
                span_minutes=args.span_minutes, max_span_rows=args.max_span_rows, max_spans=args.max_spans, dry_run=args.dry_run,
                maintain_indexes=not args.no_maintain, reindex=args.reindex, rebuild=args.rebuild)
    r = resource.getrusage(resource.RUSAGE_SELF)
    out = asdict(stats) | {"dry_run": args.dry_run, "seconds": round(time.monotonic() - start, 1),
                           "cpu_seconds": round(r.ru_utime + r.ru_stime, 1), "peak_rss_mb": round(_peak_rss_mb())}
    log.info("run %s", json.dumps(out))  # one line per run, as bsky-embed's


if __name__ == "__main__":
    main()
