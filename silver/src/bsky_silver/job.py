"""Process new bronze rows into the silver tables.

A run covers a window of bronze `ingested_at` after the last run's watermark.
It parses every row once (Python, via mapInArrow) into a staging Parquet
directory, then appends each table's rows from there in one Iceberg commit per
table, and finally records the window in `silver_runs`. Sail's cache() doesn't
materialize, which is why staging is a real write.
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
from pathlib import Path

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import normalize as N
from .tables import RUNS_DDL, STAGE_DDL, STAGE_SCHEMA, TABLES, ddl

log = logging.getLogger(__name__)

RUNS = "silver_runs"


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


class SilverJob:
    def __init__(
        self,
        spark: SparkSession,
        bronze_table: str,
        namespace: str,
        location: str,
        staging_dir: str | Path,
        window: dt.timedelta = dt.timedelta(hours=1),
        settle: dt.timedelta = dt.timedelta(minutes=5),
    ):
        self.spark = spark
        self.bronze_table = bronze_table
        self.namespace = namespace
        self.location = location.rstrip("/")
        self.staging_dir = Path(staging_dir)
        self.window = window
        self.settle = settle
        spark.conf.set("spark.sql.session.timeZone", "UTC")

    def table(self, name: str) -> str:
        return f"{self.namespace}.{name}"

    def ensure_tables(self) -> None:
        """Create missing tables. The namespace must exist (infra/ manages it)."""
        for name in TABLES:
            self._create(name, ddl(name), "PARTITIONED BY (day)")
        self._create(RUNS, RUNS_DDL, "")

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
        ingested_at is taken before it commits) lands in a later window.
        """
        after = self.watermark()
        bronze = self.spark.table(self.bronze_table)
        if after is not None:
            bronze = bronze.where(F.col("ingested_at") > F.lit(after))
        cutoff = now - self.settle
        first = _utc(bronze.where(F.col("ingested_at") <= F.lit(cutoff)).agg(F.min("ingested_at")).collect()[0][0])
        if first is None:
            return None
        through = _utc(
            bronze.where(F.col("ingested_at") <= F.lit(min(cutoff, first + self.window)))
            .agg(F.max("ingested_at"))
            .collect()[0][0]
        )
        return after, through

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
        columns = TABLES[name]
        rows = rows.select(*columns).dropDuplicates(["event_id"])
        has_time = "time_us" in columns
        bounds = rows.agg(
            F.min("day"), F.max("day"), *([F.min("time_us"), F.max("time_us")] if has_time else [])
        ).collect()[0]
        first_day, last_day = bounds[0], bounds[1]
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

