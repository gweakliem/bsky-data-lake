import datetime as dt
import json
from collections import Counter

from conftest import add_bronze, fixtures, utc

from bsky_silver.job import BACKFILL, RUNS, SilverJob
from bsky_silver.normalize import IGNORED, POST, REJECTS, normalize
from bsky_silver.tables import EDGES, SCHEMAS

INGESTED = utc(2026, 11, 1, 12, 0)
NOW = utc(2026, 11, 1, 13, 0)


def payloads() -> list[str]:
    return [json.dumps(event) for _, event in fixtures()]


def expected_edges(items: list[str]) -> set[tuple[str, str, str]]:
    """(src_uri, dst_uri, type) of every edge to a post."""
    sources = {
        "posts": [("reply_parent_uri", "reply_parent"), ("reply_root_uri", "reply_root"), ("quote_uri", "quote")],
        "likes": [("subject_uri", "like")],
        "reposts": [("subject_uri", "repost")],
    }
    edges = set()
    for target, fields in map(normalize, items):
        for column, kind in sources.get(target, []):
            dst = fields.get(column)
            if dst and dst.startswith("at://") and f"/{POST}/" in dst:
                edges.add((fields["uri"], dst, kind))
    return edges


def expected_counts(items: list[str]) -> Counter:
    """Rows per table: distinct events, since the S1 sample repeats some across cases."""
    rows = {(t, f["event_id"]) for t, f in map(normalize, items) if t != IGNORED}
    return Counter(t for t, _ in rows) + Counter({EDGES: len(expected_edges(items))})


def table_counts(job: SilverJob) -> Counter:
    return Counter({name: job.spark.table(job.table(name)).count() for name in SCHEMAS})


def edge_set(job: SilverJob) -> set[tuple[str, str, str]]:
    return {(r.src_uri, r.dst_uri, r.type) for r in job.spark.table(job.table(EDGES)).collect()}


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


def test_edges_from_posts_likes_and_reposts(job):
    add_bronze(job, payloads(), INGESTED)
    job.run(NOW)

    edges = edge_set(job)
    assert edges == expected_edges(payloads())
    assert {kind for _, _, kind in edges} == {"reply_parent", "reply_root", "quote", "like", "repost"}
    rows = job.spark.table(job.table(EDGES)).collect()
    assert all(r.event_id.endswith("|" + r.type) and r.src_uri.startswith(f"at://{r.src_did}/") for r in rows)


def test_edges_only_point_at_posts(job):
    feed = "at://did:plc:creator/app.bsky.feed.generator/whats-hot"
    like = {
        "did": "did:plc:liker", "time_us": 1761998400000000, "kind": "commit",
        "commit": {
            "rev": "3m4aaaaaaaa22", "operation": "create", "collection": "app.bsky.feed.like", "rkey": "3m4aaaaaaaa22",
            "cid": "bafy", "record": {"subject": {"uri": feed, "cid": "bafy"}, "createdAt": "2026-11-01T11:59:00Z"},
        },
    }
    add_bronze(job, [json.dumps(like)], INGESTED)
    counts = job.run(NOW)
    assert counts["appended"] == {"likes": 1, EDGES: 0}


def forget_edges(job: SilverJob) -> None:
    """Make the recorded runs look like they ran before the job built edges."""
    runs = job.spark.table(job.table(RUNS)).collect()
    job.spark.sql(f"DELETE FROM {job.table(EDGES)}")
    job.spark.sql(f"DELETE FROM {job.table(RUNS)}")
    rows = []
    for r in runs:
        counts = json.loads(r.counts)
        del counts["appended"][EDGES]
        rows.append(r.asDict() | {"counts": json.dumps(counts)})
    job.spark.createDataFrame(rows, job.spark.table(job.table(RUNS)).schema).writeTo(job.table(RUNS)).append()


def test_backfill_builds_edges_for_earlier_windows(job):
    for i, ingested in enumerate([INGESTED - dt.timedelta(hours=2), INGESTED - dt.timedelta(hours=1), INGESTED]):
        add_bronze(job, payloads()[i * 70 : (i + 1) * 70], ingested)
    job.max_rows = 70
    while job.run(NOW):
        pass
    built = edge_set(job)
    forget_edges(job)
    assert edge_set(job) == set()

    # Interrupted after one window: the next backfill does the other two.
    assert job.backfill_edges(max_windows=1) == 1
    assert job.backfill_edges() == 2
    assert edge_set(job) == built == expected_edges(payloads())
    assert job.backfill_edges() == 0
    # Runs that built their own edges are never backfilled.
    add_bronze(job, payloads()[:10], NOW, received_at=NOW)
    job.run(NOW + dt.timedelta(minutes=10))
    assert job.backfill_edges() == 0


def test_backfill_rerun_of_a_window_is_idempotent(job):
    add_bronze(job, payloads(), INGESTED)
    job.run(NOW)
    before = edge_set(job)
    count = job.spark.table(job.table(EDGES)).count()
    forget_edges(job)
    job.backfill_edges()

    # Edges appended, but the backfill was killed before recording the window.
    job.spark.sql(f"DELETE FROM {job.table(BACKFILL)}")
    assert job.backfill_edges() == 1

    assert edge_set(job) == before
    assert job.spark.table(job.table(EDGES)).count() == count
