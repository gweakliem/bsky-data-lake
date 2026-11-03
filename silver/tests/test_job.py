import datetime as dt
import json
from collections import Counter

import pytest
from conftest import fixtures, utc

from bsky_silver.job import RUNS, SilverJob
from bsky_silver.normalize import IGNORED, REJECTS, normalize
from bsky_silver.tables import TABLES

INGESTED = utc(2026, 11, 1, 12, 0)
NOW = utc(2026, 11, 1, 13, 0)

BRONZE_DDL = """received_at TIMESTAMP, time_us BIGINT, payload STRING, source_file STRING,
    ingested_at TIMESTAMP, day DATE, hour INT"""


@pytest.fixture
def job(spark, tmp_path, request):
    name = request.node.name.replace("[", "_").replace("]", "").replace("-", "_")
    namespace = f"silver_{name}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace}")
    bronze = f"{namespace}.bronze"
    spark.sql(
        f"CREATE TABLE {bronze} ({BRONZE_DDL}) USING iceberg PARTITIONED BY (day, hour) LOCATION '{(tmp_path / 'bronze').as_uri()}'"
    )
    job = SilverJob(spark, bronze, namespace, (tmp_path / "silver").as_uri(), tmp_path / "staging")
    job.ensure_tables()
    yield job
    for table in [*TABLES, RUNS, "bronze"]:
        spark.sql(f"DROP TABLE IF EXISTS {namespace}.{table}")


def add_bronze(job: SilverJob, payloads: list[str], ingested_at: dt.datetime, received_at: dt.datetime | None = None):
    received_at = received_at or ingested_at - dt.timedelta(seconds=30)
    rows = []
    for payload in payloads:
        try:
            time_us = json.loads(payload).get("time_us")
        except ValueError:
            time_us = None
        rows.append((received_at, time_us, payload, "file:///src.parquet", ingested_at, received_at.date(), received_at.hour))
    job.spark.createDataFrame(rows, BRONZE_DDL).writeTo(job.bronze_table).append()


def payloads() -> list[str]:
    return [json.dumps(event) for _, event in fixtures()]


def expected_counts(items: list[str]) -> Counter:
    """Rows per table: distinct events, since the S1 sample repeats some across cases."""
    rows = {(t, f["event_id"]) for t, f in map(normalize, items) if t != IGNORED}
    return Counter(t for t, _ in rows)


def table_counts(job: SilverJob) -> Counter:
    return Counter({name: job.spark.table(job.table(name)).count() for name in TABLES})


def test_run_fills_every_table(job):
    add_bronze(job, payloads(), INGESTED)

    counts = job.run(NOW)

    expected = expected_counts(payloads())
    assert counts["appended"] == dict(expected)
    assert table_counts(job) == expected
    # Ignored collections are counted by name.
    assert counts["ignored"]["app.bsky.feed.threadgate"] == 3
    assert job.run(NOW) is None


def test_replayed_events_are_not_appended_again(job):
    add_bronze(job, payloads(), INGESTED)
    job.run(NOW)
    before = table_counts(job)

    # The ingestor's restart overlap: the same events, received and loaded later.
    add_bronze(job, payloads(), INGESTED + dt.timedelta(minutes=10))
    counts = job.run(NOW + dt.timedelta(minutes=10))

    assert sum(counts["appended"].values()) == 0
    assert table_counts(job) == before


def test_rerun_after_lost_run_row_is_idempotent(job):
    add_bronze(job, payloads(), INGESTED)
    job.run(NOW)
    before = table_counts(job)

    # A crash after the table commits but before the run row: the window runs again.
    job.spark.sql(f"DELETE FROM {job.table(RUNS)}")
    counts = job.run(NOW)

    assert sum(counts["appended"].values()) == 0
    assert table_counts(job) == before


def test_duplicates_within_a_window_are_dropped(job):
    add_bronze(job, payloads()[:20] * 2, INGESTED)
    counts = job.run(NOW)
    assert counts["appended"] == dict(expected_counts(payloads()[:20]))


def test_extracted_post_fields(job):
    add_bronze(job, payloads(), INGESTED)
    job.run(NOW)
    posts = job.spark.table(job.table("posts"))

    def count(condition: str) -> int:
        return posts.where(condition).count()

    by_case = Counter(case for case, _ in fixtures())
    assert count("reply_root_uri IS NOT NULL AND reply_parent_uri IS NOT NULL") >= by_case["post_reply"]
    assert count("quote_uri LIKE 'at://%'") >= by_case["post_quote"] + by_case["post_quote_images"]
    assert count("size(mention_dids) > 0") >= by_case["post_mention"]
    assert count("size(link_uris) > 0") >= by_case["post_link"]
    assert count("size(tags) > 0") >= by_case["post_tag"]
    assert count("embed_type = 'app.bsky.embed.gallery'") == by_case["post_gallery"]
    assert count("record IS NULL") == 0
    assert count("operation = 'update'") == by_case["post_update"]


def test_deletes_record_the_deleted_uri(job):
    add_bronze(job, payloads(), INGESTED)
    job.run(NOW)

    rows = job.spark.table(job.table("deletes")).collect()
    deleted = {(case, e["commit"]["collection"]) for case, e in fixtures() if case.endswith("_delete")}
    assert {r.collection for r in rows} == {c for _, c in deleted}
    assert all(r.uri == f"at://{r.did}/{r.collection}/{r.rkey}" for r in rows)


def test_malformed_rows_go_to_rejects(job):
    add_bronze(job, payloads()[:5] + ["{broken", '{"did": "did:plc:x", "kind": "commit"}'], INGESTED)
    counts = job.run(NOW)

    assert counts["staged"][REJECTS] == 2
    rejects = {r.payload: r.reason for r in job.spark.table(job.table("rejects")).collect()}
    assert rejects == {"{broken": "invalid JSON", '{"did": "did:plc:x", "kind": "commit"}': "missing time_us"}


def test_recent_rows_wait_for_the_next_run(job):
    add_bronze(job, payloads()[:10], INGESTED)
    add_bronze(job, payloads()[10:20], NOW - dt.timedelta(minutes=2))

    first = job.run(NOW)
    assert sum(first["staged"].values()) == 10
    assert job.run(NOW) is None

    second = job.run(NOW + dt.timedelta(minutes=10))
    assert sum(second["staged"].values()) == 10


def test_windows_are_bounded_by_rows_in_whole_commits(job):
    # Three loader commits: 10, 10 and 10 rows.
    for i, ingested in enumerate([INGESTED - dt.timedelta(hours=5), INGESTED - dt.timedelta(minutes=30), INGESTED]):
        add_bronze(job, payloads()[i * 10 : (i + 1) * 10], ingested)
    job.max_rows = 25

    assert sum(job.run(NOW)["staged"].values()) == 20
    # A commit is never split, even when it alone is over the cap.
    job.max_rows = 5
    assert sum(job.run(NOW)["staged"].values()) == 10
    assert job.run(NOW) is None

    runs = job.spark.table(job.table(RUNS)).orderBy("bronze_through").collect()
    assert [r.bronze_through.astimezone(dt.timezone.utc) for r in runs] == [INGESTED - dt.timedelta(minutes=30), INGESTED]
    assert runs[1].bronze_after == runs[0].bronze_through



def test_tables_partition_by_day(job, tmp_path):
    add_bronze(job, payloads()[:40], INGESTED)
    job.run(NOW)
    dirs = {p.parent.name for p in (tmp_path / "silver").rglob("data/*/*.parquet")}
    assert dirs == {"day=2026-11-01"}
