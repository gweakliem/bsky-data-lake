"""Process new bronze rows into the silver tables.

A run covers a window of bronze `ingested_at` after the last run's watermark,
holding at most `max_rows` rows (memory grows with a window's rows).
It parses every row once (Python, via mapInArrow) into a staging Parquet
directory, then appends each table's rows from there in one Iceberg commit per
table, then `post_edges` from the same staged rows, and finally records the
window in `silver_runs`. Sail's cache() doesn't materialize, which is why
staging is a real write.

Windows recorded before `post_edges` existed get their edges from
`backfill_edges`, which reads them back from the silver tables.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import shutil
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from functools import reduce
from pathlib import Path

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import normalize as N
from .tables import BACKFILL_DDL, EDGES, RUNS_DDL, SCHEMAS, STAGE_DDL, STAGE_SCHEMA, TABLES, ddl

log = logging.getLogger(__name__)

RUNS = "silver_runs"
# Windows of silver_runs whose edges backfill_edges has built.
BACKFILL = "post_edges_backfill"


def quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _utc(value: dt.datetime | None) -> dt.datetime | None:
    """collect() returns naive datetimes in the Python process's local time zone
    (not the session's), and astimezone reads a naive value as local."""
    return value.astimezone(dt.timezone.utc) if value is not None else None


def normalize_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    """mapInArrow body: bronze (payload, received_at, ingested_at, day) to STAGE_SCHEMA."""
    names = STAGE_SCHEMA.names
    for batch in batches:
        columns: dict[str, list] = {n: [] for n in names}
        payloads = batch.column("payload").to_pylist()
        for payload in payloads:
            target, fields = N.normalize(payload) if payload is not None else (N.REJECTS, {"reason": "null payload", "event_id": "reject:null"})
            if target == N.REJECTS:
                fields = fields | {"payload": payload}
            fields["target"] = target
            for n in names:
                columns[n].append(fields.get(n))
        # Carried over from bronze.
        columns["received_at"] = batch.column("received_at").to_pylist()
        columns["bronze_ingested_at"] = batch.column("ingested_at").to_pylist()
        columns["day"] = batch.column("day").to_pylist()
        yield pa.RecordBatch.from_arrays(
            [pa.array(columns[f.name], type=f.type) for f in STAGE_SCHEMA], schema=STAGE_SCHEMA
        )


def edges(posts: DataFrame, likes: DataFrame, reposts: DataFrame) -> DataFrame:
    """post_edges rows: replies and quotes from posts, likes and reposts of posts.

    Only edges to posts: likes, reposts and record embeds can also point at
    feed generators, lists or starter packs. A post's edit repeats its edges
    under the new version's event_id; post_edges_current keeps the latest.
    """

    def select(rows: DataFrame, dst: str, kind: str) -> DataFrame:
        return rows.where(F.col(dst).startswith("at://") & F.col(dst).contains(f"/{N.POST}/")).select(
            F.concat("event_id", F.lit(f"|{kind}")).alias("event_id"),
            F.col("uri").alias("src_uri"),
            F.col(dst).alias("dst_uri"),
            F.lit(kind).alias("type"),
            F.col("did").alias("src_did"),
            "rev", "created_at", "time_us", "received_at", "bronze_ingested_at", "day",
        )

    return reduce(
        DataFrame.unionByName,
        [
            select(posts, "reply_parent_uri", "reply_parent"),
            select(posts, "reply_root_uri", "reply_root"),
            select(posts, "quote_uri", "quote"),
            select(likes, "subject_uri", "like"),
            select(reposts, "subject_uri", "repost"),
        ],
    )


class SilverJob:
    def __init__(
        self,
        spark: SparkSession,
        bronze_table: str,
        namespace: str,
        location: str,
        staging_dir: str | Path,
        max_rows: int = 500_000,
        settle: dt.timedelta = dt.timedelta(minutes=5),
    ):
        self.spark = spark
        self.bronze_table = bronze_table
        self.namespace = namespace
        self.location = location.rstrip("/")
        self.staging_dir = Path(staging_dir)
        self.max_rows = max_rows
        self.settle = settle
        spark.conf.set("spark.sql.session.timeZone", "UTC")

    def table(self, name: str) -> str:
        return f"{self.namespace}.{name}"

    def ensure_tables(self) -> None:
        """Create missing tables. The namespace must exist (infra/ manages it)."""
        for name in SCHEMAS:
            self._create(name, ddl(name), "PARTITIONED BY (day)")
        self._create(RUNS, RUNS_DDL, "")
        self._create(BACKFILL, BACKFILL_DDL, "")

    def _create(self, name: str, columns: str, partitioning: str) -> None:
        # Sail's Glue catalog has no warehouse setting, so the location is explicit.
        self.spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {self.table(name)} ({columns}) USING iceberg {partitioning}
            LOCATION {quote(f"{self.location}/{name}")}"""
        )

    def watermark(self) -> dt.datetime | None:
        return _utc(self.spark.table(self.table(RUNS)).agg(F.max("bronze_through")).collect()[0][0])

    def next_window(self, now: dt.datetime) -> tuple[dt.datetime | None, dt.datetime] | None:
        """(after, through] of bronze ingested_at for the next run, or None when caught up.

        Stops `settle` short of now, so a loader commit still in flight (its
        ingested_at is taken before it commits) lands in a later window. The
        window ends at the last ingested_at that keeps it within max_rows. Every
        row of a loader commit shares one ingested_at, so a window always takes
        at least one whole commit (at most ~850k rows at the loader's 128 MB cap).
        Time doesn't bound rows: a backlog upload loads days of events in hours.
        """
        after = self.watermark()
        bronze = self.spark.table(self.bronze_table).where(F.col("ingested_at") <= F.lit(now - self.settle))
        if after is not None:
            bronze = bronze.where(F.col("ingested_at") > F.lit(after))
        commits = bronze.groupBy("ingested_at").count().orderBy("ingested_at").collect()
        if not commits:
            return None
        through, rows = None, 0
        for commit in commits:
            if through is not None and rows + commit["count"] > self.max_rows:
                break
            through, rows = commit.ingested_at, rows + commit["count"]
        return after, _utc(through)

    def run(self, now: dt.datetime | None = None) -> dict | None:
        """Process one window. Returns the run's counts, or None when caught up."""
        now = now or dt.datetime.now(dt.timezone.utc)
        window = self.next_window(now)
        if window is None:
            return None
        after, through = window
        started = dt.datetime.now(dt.timezone.utc)
        run_id = uuid.uuid4().hex
        t0 = time.monotonic()

        bronze = self.spark.table(self.bronze_table).where(F.col("ingested_at") <= F.lit(through))
        if after is not None:
            bronze = bronze.where(F.col("ingested_at") > F.lit(after))
        stage_path = self.staging_dir / run_id
        (
            bronze.select("payload", "received_at", "ingested_at", "day")
            .mapInArrow(normalize_batches, STAGE_DDL)
            .write.mode("overwrite")
            .parquet(stage_path.as_uri())
        )
        try:
            stage = self.spark.read.parquet(stage_path.as_uri())
            targets = Counter()
            ignored = Counter()
            for row in stage.groupBy("target", "collection").count().collect():
                targets[row.target] += row["count"]
                if row.target == N.IGNORED:
                    ignored[row.collection] += row["count"]
            log.info("staged %d bronze row(s) in %.1f s", sum(targets.values()), time.monotonic() - t0)

            appended = {}
            for name in TABLES:
                if targets.get(name):
                    appended[name] = self._append(name, stage.where(F.col("target") == name))
            # Always counted: backfill_edges takes runs without it as built before post_edges.
            appended[EDGES] = self._append(
                EDGES, edges(*(stage.where(F.col("target") == t) for t in ("posts", "likes", "reposts")))
            )
        finally:
            shutil.rmtree(stage_path, ignore_errors=True)

        counts = {
            "staged": dict(targets),
            "appended": appended,
            # Unknown collections and kinds, largest first; the long tail is summed.
            "ignored": dict(ignored.most_common(25)) | {"(other)": sum(n for _, n in ignored.most_common()[25:])},
        }
        self._record_run(run_id, started, after, through, sum(targets.values()), counts)
        log.info(
            "run %s: bronze (%s, %s], %d row(s), appended %s, %d ignored, %d rejected, in %.1f s",
            run_id, after, through, sum(targets.values()), appended, targets.get(N.IGNORED, 0),
            targets.get(N.REJECTS, 0), time.monotonic() - t0,
        )
        return counts

    def _append(self, name: str, rows: DataFrame) -> int:
        """Append rows not already in the table, deduplicated on event_id.

        Duplicates come from the ingestor's restart overlap (a replayed event
        has the same time_us but a later received_at) and from rerunning a
        window whose run row wasn't written. Existing rows are read only for
        the batch's days (plus the day before) and time_us range, which
        Iceberg's partition and column statistics prune.
        """
        columns = SCHEMAS[name]
        rows = rows.select(*columns).dropDuplicates(["event_id"])
        has_time = "time_us" in columns
        bounds = rows.agg(
            F.min("day"), F.max("day"), *([F.min("time_us"), F.max("time_us")] if has_time else [])
        ).collect()[0]
        first_day, last_day = bounds[0], bounds[1]
        if first_day is None:
            return 0
        existing = self.spark.table(self.table(name)).where(
            F.col("day").between(F.lit(first_day - dt.timedelta(days=1)), F.lit(last_day))
        )
        if has_time:
            existing = existing.where(F.col("time_us").between(bounds[2], bounds[3]))
        new = rows.join(existing.select("event_id"), "event_id", "left_anti")
        count = new.count()
        if count:
            # One task per day partition, so one data file per day per commit.
            new.repartition("day").writeTo(self.table(name)).append()
        return count

    def backfill_edges(self, max_windows: int | None = None) -> int:
        """Build post_edges for windows that runs recorded before the job built edges.

        Those runs have no post_edges count. Each window's posts, likes and
        reposts are read back from the silver tables by bronze_ingested_at
        (file statistics prune that well: each run writes its own files) and
        appended like a run's edges, so a window is the same memory as a run's
        edge step. A finished window is recorded in post_edges_backfill; one
        interrupted before that is redone, and the anti-join drops what it had
        already appended. Returns the number of windows processed.
        """
        done = {_utc(r.bronze_through) for r in self.spark.table(self.table(BACKFILL)).collect()}
        pending = sorted(
            (r for r in self.spark.table(self.table(RUNS)).collect()
             if EDGES not in json.loads(r.counts).get("appended", {}) and _utc(r.bronze_through) not in done),
            key=lambda r: _utc(r.bronze_through),
        )[:max_windows]
        if pending:
            log.info("backfilling post_edges for %d window(s)", len(pending))
        for run in pending:
            t0 = time.monotonic()
            after, through = _utc(run.bronze_after), _utc(run.bronze_through)

            def window(name: str) -> DataFrame:
                rows = self.spark.table(self.table(name)).where(F.col("bronze_ingested_at") <= F.lit(through))
                return rows.where(F.col("bronze_ingested_at") > F.lit(after)) if after is not None else rows

            count = self._append(EDGES, edges(window("posts"), window("likes"), window("reposts")))
            row = {
                "bronze_after": after,
                "bronze_through": through,
                "edges": count,
                "finished_at": dt.datetime.now(dt.timezone.utc),
            }
            self.spark.createDataFrame([row], BACKFILL_DDL).writeTo(self.table(BACKFILL)).append()
            log.info("backfill: bronze (%s, %s], %d edge(s), in %.1f s", after, through, count, time.monotonic() - t0)
        return len(pending)

    def _record_run(self, run_id, started, after, through, bronze_rows, counts) -> None:
        row = {
            "run_id": run_id,
            "started_at": started,
            "finished_at": dt.datetime.now(dt.timezone.utc),
            "bronze_after": after,
            "bronze_through": through,
            "bronze_rows": bronze_rows,
            "counts": json.dumps(counts, sort_keys=True),
        }
        self.spark.createDataFrame([row], RUNS_DDL).writeTo(self.table(RUNS)).append()

