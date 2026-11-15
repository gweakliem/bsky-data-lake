"""The moderation-labels job: append mod.bsky.app's new labels to bsky_silver.moderation_labels.

    bsky-labels [--dry-run] [--chunk 100000]

Each run reads the table's max `seq` (from file stats, as the support jobs read
their watermarks), asks the stream for its newest seq, and replays everything
between, appending `--chunk` labels at a time. A run cut short resumes from its
last commit. At about 55k labels a day (2026-11) a daily run takes under a minute.

The table keeps every event, retractions (neg) and expiries (exp) included, as
the stream sends them: a label's current state is its latest event per (src, uri,
val). An empty table starts at seq 0, which replays the whole history (43M labels
since 2023-04, about 6 h on one connection); load that with bsky-labels-backfill
from a replay already on disk instead.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import resource
import time
from dataclasses import asdict, dataclass

import pyarrow.compute as pc
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import BooleanType, DateType, LongType, NestedField, StringType, TimestamptzType

from bsky_enrich import incremental as inc
from bsky_labels import stream

log = logging.getLogger("bsky_labels.job")
TABLE = "moderation_labels"
SCHEMA = Schema(
    NestedField(1, "seq", LongType(), required=False),
    NestedField(2, "src", StringType(), required=False),
    NestedField(3, "uri", StringType(), required=False),
    NestedField(4, "cid", StringType(), required=False),
    NestedField(5, "val", StringType(), required=False),
    NestedField(6, "neg", BooleanType(), required=False),
    NestedField(7, "cts", TimestamptzType(), required=False),
    NestedField(8, "exp", TimestamptzType(), required=False),
    NestedField(9, "day", DateType(), required=False),
    NestedField(10, "ingested_at", TimestamptzType(), required=False),
)


def output_table(cat: Catalog, namespace: str = inc.SILVER, location: str | None = None) -> Table:
    return inc.ensure_table(cat, f"{namespace}.{TABLE}", SCHEMA, location or inc.location(TABLE))


def committed_seq(table: Table) -> int:
    """The table's max seq (0 when empty), from file stats when every file has them."""
    if table.current_snapshot() is None:
        return 0
    metrics = table.inspect.files().column("readable_metrics").to_pylist()
    bounds = [(m or {}).get("seq", {}).get("upper_bound") for m in metrics]
    if bounds and all(b is not None for b in bounds):
        return int(max(bounds))
    top = pc.max(table.scan(selected_fields=("seq",)).to_arrow()["seq"]).as_py()
    return int(top or 0)


@dataclass
class Stats:
    after: int = 0
    head: int | None = None
    events: int = 0
    labels: int = 0
    commits: int = 0
    seconds: float = 0.0
    peak_rss_mb: float = 0.0


async def run(table: Table | None, after: int, chunk: int, connect=stream.websockets.connect,
              now=lambda: dt.datetime.now(dt.UTC)) -> Stats:
    """Replay (after, head] into `table` (None: a dry run that only counts)."""
    t0 = time.monotonic()
    stats = Stats(after=after, head=await stream.head(connect))
    if stats.head is None or stats.head <= after:
        log.info("nothing new after seq %d (head %s)", after, stats.head)
        return stats
    batch: list[dict] = []

    def commit(last: int) -> None:
        if table is not None and batch:
            inc.append(table, stream.table(batch))
            stats.commits += 1
        log.info("committed %d labels through seq %d%s", len(batch), last, " (dry run)" if table is None else "")
        batch.clear()

    seq = after
    async for seq, labels in stream.replay(after, stats.head, connect):
        stats.events += 1
        stats.labels += len(labels)
        batch.extend(stream.rows(seq, labels, now()))
        if len(batch) >= chunk:
            commit(seq)
    commit(seq)
    stats.seconds = round(time.monotonic() - t0, 1)
    return stats


def _peak_rss_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(peak / (1 << 20) if peak > 1 << 32 else peak / 1024, 1)  # bytes on macOS, KiB on Linux


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--chunk", type=int, default=100_000, help="labels per commit")
    p.add_argument("--dry-run", action="store_true", help="replay and count, without creating or writing the table")
    a = p.parse_args()
    cat = inc.catalog()
    if a.dry_run:
        identifier = f"{inc.SILVER}.{TABLE}"
        table, after = None, (committed_seq(cat.load_table(identifier)) if cat.table_exists(identifier) else 0)
    else:
        table = output_table(cat)
        after = committed_seq(table)
    stats = asyncio.run(run(table, after, a.chunk))
    stats.peak_rss_mb = _peak_rss_mb()
    log.info("run %s", json.dumps(asdict(stats)))


if __name__ == "__main__":
    main()
