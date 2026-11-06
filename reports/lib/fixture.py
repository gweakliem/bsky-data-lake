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

    _content(con)

    for minutes_ago in (90, 30, 4):
        at = now - dt.timedelta(minutes=minutes_ago)
        con.execute(
            "INSERT INTO bronze.jetstream VALUES (?, ?, '{}', 'file:///fixture.parquet', ?, ?, ?)",
            [at, int(at.timestamp() * 1e6), at, at.date(), at.hour],
        )
    return {"silver": "silver", "bronze": "bronze"}


# Posts in the fixture: 200 a day over 20 days, so some fall outside the recent window.
POSTS_PER_DAY = 200
POST_DAYS = 20
EDITED = 10  # posts 0-9 get a second version
DELETED = range(10, 20)  # posts 10-19 are deleted


def _content(con: duckdb.DuckDBPyConnection) -> None:
    """posts, deletes, likes and reposts, with every column posts_current selects."""
    common = """event_id VARCHAR, did VARCHAR, time_us BIGINT, received_at TIMESTAMPTZ,
        bronze_ingested_at TIMESTAMPTZ, day DATE, rkey VARCHAR, uri VARCHAR, rev VARCHAR"""
    con.execute(
        f"""CREATE TABLE silver.posts ({common}, cid VARCHAR, operation VARCHAR, created_at TIMESTAMPTZ,
        text VARCHAR, langs VARCHAR[], reply_root_uri VARCHAR, reply_parent_uri VARCHAR, embed_type VARCHAR,
        quote_uri VARCHAR, image_alts VARCHAR[], external_uri VARCHAR, external_title VARCHAR,
        mention_dids VARCHAR[], link_uris VARCHAR[], tags VARCHAR[], self_labels VARCHAR[], record VARCHAR)"""
    )
    con.execute(f"CREATE TABLE silver.deletes ({common}, collection VARCHAR)")
    for table in ("likes", "reposts"):
        con.execute(f"CREATE TABLE silver.{table} ({common}, cid VARCHAR, subject_uri VARCHAR, created_at TIMESTAMPTZ)")

    # Post i lands on day current_date - (i % POST_DAYS), with languages by (i // 20) % 10.
    post = f"""
        'did:plc:' || (i % 50) AS did, (epoch_us(t) + i) AS time_us, t AS received_at, t AS bronze_ingested_at,
        CAST(t AS DATE) AS day, CAST(i AS VARCHAR) AS rkey, 'at://did:plc:' || (i % 50) || '/app.bsky.feed.post/' || i AS uri"""
    days = f"current_date - CAST(i % {POST_DAYS} AS INTEGER) + INTERVAL 12 HOUR"
    con.execute(
        f"""INSERT INTO silver.posts
        SELECT 'post' || i || '#1', {post}, 'r1-' || lpad(CAST(i AS VARCHAR), 6, '0'), 'cid' || i, 'create', t,
            'post ' || i,
            CASE (i // 20) % 10 WHEN 0 THEN [] WHEN 1 THEN ['ja'] WHEN 2 THEN ['es', 'en'] WHEN 3 THEN ['de']
                WHEN 4 THEN ['pt-BR'] WHEN 5 THEN ['en-US', 'en'] ELSE ['en'] END,
            NULL, NULL, NULL, NULL, [], NULL, NULL, [], [], [], [], '{{}}'
        FROM (SELECT i, CAST({days} AS TIMESTAMPTZ) AS t FROM range({POSTS_PER_DAY * POST_DAYS}) r(i))"""
    )
    # A second version of the first posts (same uri, later rev), and deletes of the next ones.
    con.execute(
        f"""INSERT INTO silver.posts SELECT * REPLACE ('post' || rkey || '#2' AS event_id, 'r2-' || rkey AS rev,
            'update' AS operation) FROM silver.posts WHERE CAST(rkey AS INTEGER) < {EDITED}"""
    )
    con.execute(
        f"""INSERT INTO silver.deletes SELECT 'del' || rkey, did, time_us + 1, received_at, bronze_ingested_at, day,
            rkey, uri, 'r9-' || rkey, 'app.bsky.feed.post'
        FROM silver.posts WHERE CAST(rkey AS INTEGER) BETWEEN {DELETED.start} AND {DELETED.stop - 1}"""
    )
    # Likes and reposts: 1000 and 150 a day.
    for table, per_day in (("likes", 1000), ("reposts", 150)):
        con.execute(
            f"""INSERT INTO silver.{table}
            SELECT '{table}' || i, {post.replace("app.bsky.feed.post", f"app.bsky.feed.{table[:-1]}")},
                'r1-' || i, 'cid' || i, 'at://did:plc:1/app.bsky.feed.post/1', t
            FROM (SELECT i, CAST(current_date - CAST(i % {POST_DAYS} AS INTEGER) + INTERVAL 12 HOUR AS TIMESTAMPTZ) AS t
                  FROM range({per_day * POST_DAYS}) r(i))"""
        )
