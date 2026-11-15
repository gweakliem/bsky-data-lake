"""Load a label replay already on disk into bsky_silver.moderation_labels.

    bsky-labels-backfill <dir> [--batch 2000000] [--dry-run]

<dir> holds Parquet files with seq, src, uri, cid, val, neg, cts and exp (times as
the stream's ISO strings), as the clustering prototype's replay writes them
(data/cluster/modlabels/). Only labels after the table's max seq are loaded, in
seq order, `--batch` at a time, so a rerun resumes. Run it from a laptop with
AWS credentials before the CronJob is unsuspended: the job then picks up from
the backfill's last seq instead of replaying 43M labels from the stream.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from bsky_enrich import incremental as inc
from bsky_labels import job, stream

log = logging.getLogger("bsky_labels.backfill")


def times(values: pa.ChunkedArray | pa.Array) -> pa.Array:
    """ISO strings to UTC timestamps: Arrow's parser for the usual shapes, Python's for the rest."""
    try:
        return pc.cast(values, stream.TS)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return pa.array([stream.timestamp(v) for v in values.to_pylist()], type=stream.TS)


def convert(raw: pa.Table, ingested_at: dt.datetime) -> pa.Table:
    """Replay rows (string times) to the silver table's rows."""
    cts, exp = times(raw["cts"]), times(raw["exp"])
    day = pc.cast(pc.coalesce(cts, pa.scalar(ingested_at, stream.TS)), pa.date32())
    return pa.table({
        "seq": raw["seq"], "src": raw["src"], "uri": raw["uri"], "cid": raw["cid"], "val": raw["val"],
        "neg": raw["neg"], "cts": cts, "exp": exp, "day": day,
        "ingested_at": pa.array([ingested_at] * len(raw), type=stream.TS),
    }, schema=stream.ARROW)


def batches(directory: Path, after: int, size: int):
    """The replay's labels after `after`, in seq order, `size` at a time."""
    con = duckdb.connect()
    con.execute(f"""CREATE VIEW r AS SELECT seq, src, uri, cid, val, neg, cts, exp
                    FROM read_parquet('{directory}/*.parquet') WHERE seq > {after}""")
    top = con.execute("SELECT max(seq) FROM r").fetchone()[0]
    if top is None:
        return
    lo = after
    while lo < top:
        hi = con.execute(f"SELECT max(seq) FROM (SELECT seq FROM r WHERE seq > {lo} ORDER BY seq LIMIT {size})").fetchone()[0]
        yield con.execute(f"SELECT * FROM r WHERE seq > {lo} AND seq <= {hi} ORDER BY seq").fetch_arrow_table()
        lo = hi


def check_contiguous(directory: Path, after: int) -> None:
    """Refuse a replay that starts after the table's max seq (it would leave a gap)."""
    first = duckdb.sql(f"SELECT min(seq) FROM read_parquet('{directory}/*.parquet') WHERE seq > {after}").fetchone()[0]
    if first is not None and after and first > after + 1_000_000:
        raise SystemExit(f"replay starts at seq {first}, far after the table's {after}: missing labels between")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("directory", type=Path)
    p.add_argument("--batch", type=int, default=2_000_000)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    table = job.output_table(inc.catalog())
    after = job.committed_seq(table)
    check_contiguous(a.directory, after)
    now = dt.datetime.now(dt.UTC)
    total, t0 = 0, time.monotonic()
    for raw in batches(a.directory, after, a.batch):
        rows = convert(raw, now)
        if not a.dry_run:
            inc.append(table, rows)
        total += len(rows)
        log.info("%s %d labels through seq %d (%d so far, %.0f s)", "read" if a.dry_run else "committed", len(rows),
                 raw["seq"][-1].as_py(), total, time.monotonic() - t0)
    log.info("done: %d labels after seq %d", total, after)


if __name__ == "__main__":
    main()
