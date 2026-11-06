"""Synthetic lake tables for building the site without AWS.

Same table and column names as the lake (see silver/src/bsky_silver/tables.py
and the loader's bronze table), with only the columns the reports read
populated meaningfully. Times are relative to now, so the pages look current.
"""

from __future__ import annotations

import datetime as dt
import json
import random

import duckdb

TABLES = ["posts", "likes", "reposts", "follows", "blocks", "profiles", "identities", "accounts", "deletes", "post_edges"]


def build(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rng = random.Random(42)
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    con.execute("CREATE SCHEMA silver; CREATE SCHEMA bronze")
    con.execute(
        """CREATE TABLE silver.silver_runs (run_id VARCHAR, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
        bronze_after TIMESTAMPTZ, bronze_through TIMESTAMPTZ, bronze_rows BIGINT, counts VARCHAR)"""
    )
    con.execute(
        """CREATE TABLE silver.rejects (event_id VARCHAR, received_at TIMESTAMPTZ, bronze_ingested_at TIMESTAMPTZ,
        day DATE, payload VARCHAR, reason VARCHAR)"""
    )
    con.execute(
        """CREATE TABLE bronze.jetstream (received_at TIMESTAMPTZ, time_us BIGINT, payload VARCHAR,
        source_file VARCHAR, ingested_at TIMESTAMPTZ, day DATE, hour INTEGER)"""
    )

    # Hourly runs at :05 for 8 days, each two or three windows, the last one an hour behind.
    runs = []
    through = now - dt.timedelta(days=8, minutes=50)
    hour = (now - dt.timedelta(days=8)).replace(minute=5, second=0)
    while hour < now:
        started = hour
        # Each run catches up to five minutes before it started, in k windows.
        k = rng.choice([2, 3])
        step = (hour - dt.timedelta(minutes=5) - through) / k
        for _ in range(k):
            after, through = through, through + step
            rows = rng.randint(200_000, 500_000)
            appended = {t: int(rows * share) for t, share in zip(TABLES, [0.08, 0.72, 0.1, 0.04, 0.005, 0.01, 0.01, 0.005, 0.03, 0.9])}
            finished = started + dt.timedelta(seconds=rng.randint(60, 150))
            counts = {"appended": appended, "staged": appended, "ignored": {"app.bsky.feed.threadgate": rows // 100}}
            runs.append((f"run{len(runs)}", started, finished, after, through, rows, json.dumps(counts)))
            started = finished
        hour += dt.timedelta(hours=1)
    con.executemany("INSERT INTO silver.silver_runs VALUES (?, ?, ?, ?, ?, ?, ?)", runs)

    rejects = []
    for days_ago in range(0, 30, 3):
        day = (now - dt.timedelta(days=days_ago)).date()
        for i in range(rng.randint(1, 4)):
            reason = rng.choice(["missing time_us", "Expecting value: line 1 column 1 (char 0)"])
            at = dt.datetime.combine(day, dt.time(12), dt.timezone.utc)
            rejects.append((f"reject:{days_ago}-{i}", at, at, day, "{}", reason))
    con.executemany("INSERT INTO silver.rejects VALUES (?, ?, ?, ?, ?, ?)", rejects)

    for minutes_ago in (90, 30, 4):
        at = now - dt.timedelta(minutes=minutes_ago)
        con.execute(
            "INSERT INTO bronze.jetstream VALUES (?, ?, '{}', 'file:///fixture.parquet', ?, ?, ?)",
            [at, int(at.timestamp() * 1e6), at, at.date(), at.hour],
        )
    return {"silver": "silver", "bronze": "bronze"}
