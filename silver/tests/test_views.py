"""The current-state views in silver/sql/, run on Sail and on DuckDB over the same rows."""

import datetime as dt
import json

import duckdb
import pytest
from conftest import add_bronze, utc

from bsky_silver.tables import TABLES
from bsky_silver.views import VIEWS, statements

INGESTED = utc(2026, 11, 1, 12, 0)
NOW = utc(2026, 11, 1, 13, 0)
POST = "app.bsky.feed.post"
LIKE = "app.bsky.feed.like"
ALICE, BOB = "did:plc:alice", "did:plc:bob"

# The support tables, as C2 and E2 plan them (vectors are float16 bytes).
SUPPORT_DDL = {
    "post_classifications": """uri STRING, cid STRING, day DATE, model STRING, spec_version STRING,
        classified_at TIMESTAMP, bronze_ingested_at TIMESTAMP, p_spam DOUBLE, p_adult DOUBLE,
        p_ai_generated DOUBLE, topic STRING, topic_confidence DOUBLE, topic_probs MAP<STRING, DOUBLE>""",
    "post_embeddings": """uri STRING, cid STRING, day DATE, model STRING, spec_version STRING,
        embedded_at TIMESTAMP, bronze_ingested_at TIMESTAMP, vector BINARY""",
}


def uri(did: str, collection: str, rkey: str) -> str:
    return f"at://{did}/{collection}/{rkey}"


DELETED = uri(ALICE, POST, "p1")  # quotes REPLY, then deleted
EDITED = uri(ALICE, POST, "p2")  # edited: cid c2a, then c2b
REPLY = uri(BOB, POST, "p3")  # replies to EDITED
UNLIKED = uri(BOB, LIKE, "l1")  # likes EDITED, then deleted
LIKED = uri(ALICE, LIKE, "l2")  # likes REPLY


def commit(did, collection, rkey, rev, operation, time_us, cid=None, record=None) -> str:
    body = {"rev": rev, "operation": operation, "collection": collection, "rkey": rkey}
    if operation != "delete":
        body |= {"cid": cid, "record": record | {"createdAt": "2026-11-01T11:00:00Z"}}
    return json.dumps({"did": did, "time_us": time_us, "kind": "commit", "commit": body})


def strong_ref(target: str) -> dict:
    return {"uri": target, "cid": "bafyref"}


def events() -> list[str]:
    t = 1761998400000000
    reply = {"root": strong_ref(EDITED), "parent": strong_ref(EDITED)}
    quote = {"$type": "app.bsky.embed.record", "record": strong_ref(REPLY)}
    # Revs are TIDs, which sort by time within a repo.
    return [
        commit(ALICE, POST, "p1", "3maaa1", "create", t + 1, "c1", {"text": "soon gone", "embed": quote}),
        commit(ALICE, POST, "p2", "3maaa2", "create", t + 2, "c2a", {"text": "first draft"}),
        commit(BOB, POST, "p3", "3mbbb1", "create", t + 3, "c3", {"text": "a reply", "reply": reply}),
        commit(BOB, LIKE, "l1", "3mbbb2", "create", t + 4, "cl1", {"subject": strong_ref(EDITED)}),
        commit(ALICE, LIKE, "l2", "3maaa3", "create", t + 5, "cl2", {"subject": strong_ref(REPLY)}),
        commit(ALICE, POST, "p2", "3maaa4", "update", t + 6, "c2b", {"text": "second draft"}),
        commit(ALICE, POST, "p1", "3maaa5", "delete", t + 7),
        commit(BOB, LIKE, "l1", "3mbbb3", "delete", t + 8),
    ]


def support_rows() -> dict[str, list[tuple]]:
    at = utc(2026, 11, 1, 12, 30)
    day = dt.date(2026, 11, 1)
    scores = (0.1, 0.2, 0.3, "news", 0.9, {"news": 0.9})
    return {
        "post_classifications": [
            (DELETED, "c1", day, "jev", "v1", at, at, *scores),
            # Classified before the edit only.
            (EDITED, "c2a", day, "jev", "v1", at, at, *scores),
            (REPLY, "c3", day, "jev", "v1", at, at, *scores),
            # An inactive spec version, newer than v1.
            (REPLY, "c3", day, "jev", "v0", at + dt.timedelta(hours=1), at, *scores),
        ],
        "post_embeddings": [
            (DELETED, "c1", day, "m", "v1", at, at, b"\x01\x02"),
            (EDITED, "c2a", day, "m", "v1", at, at, b"\x01\x02"),
            # Embedded again after the edit.
            (EDITED, "c2b", day, "m", "v1", at - dt.timedelta(hours=1), at, b"\x03\x04"),
            (REPLY, "c3", day, "m", "v1", at, at, b"\x05\x06"),
        ],
    }


@pytest.fixture
def silver(job, tmp_path):
    """The job's namespace, holding the events above and the support tables."""
    add_bronze(job, events(), INGESTED)
    job.run(NOW)
    for name, ddl in SUPPORT_DDL.items():
        job.spark.sql(
            f"CREATE TABLE {job.table(name)} ({ddl}) USING iceberg PARTITIONED BY (day) "
            f"LOCATION '{(tmp_path / name).as_uri()}'"
        )
        job.spark.createDataFrame(support_rows()[name], ddl).writeTo(job.table(name)).append()
    yield job
    for name in SUPPORT_DDL:
        job.spark.sql(f"DROP TABLE IF EXISTS {job.table(name)}")


def sail_query(job):
    for statement in statements(job.namespace):
        job.spark.sql(statement)
    return lambda sql: [r.asDict() for r in job.spark.sql(sql).collect()]


def duckdb_query(job):
    """Copy the namespace's tables into DuckDB and create the views there."""
    con = duckdb.connect()
    con.execute(f"CREATE SCHEMA {job.namespace}")
    for name in [*TABLES, "post_edges", *SUPPORT_DDL]:
        rows = job.spark.table(job.table(name)).toArrow()  # noqa: F841 (read by DuckDB below)
        con.execute(f"CREATE TABLE {job.namespace}.{name} AS SELECT * FROM rows")
    for statement in statements(job.namespace):
        con.execute(statement)

    def query(sql):
        cursor = con.execute(sql)
        names = [d[0] for d in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    return query


@pytest.fixture(params=["sail", "duckdb"])
def query(request, silver):
    return {"sail": sail_query, "duckdb": duckdb_query}[request.param](silver)


def test_posts_current_has_latest_versions_without_deletes(query):
    rows = query("SELECT uri, cid, text FROM posts_current")
    assert {(r["uri"], r["cid"], r["text"]) for r in rows} == {(EDITED, "c2b", "second draft"), (REPLY, "c3", "a reply")}


def test_posts_current_has_the_columns_of_posts(query, silver):
    row = query("SELECT * FROM posts_current WHERE uri = '" + REPLY + "'")[0]
    assert list(row) == TABLES["posts"]


def test_classifications_current(query):
    rows = query("SELECT uri, cid, current_cid, stale, spec_version FROM post_classifications_current")
    assert {r["uri"]: (r["cid"], r["current_cid"], r["stale"], r["spec_version"]) for r in rows} == {
        # The deleted post is gone; the edited one shows its old classification as stale.
        EDITED: ("c2a", "c2b", True, "v1"),
        REPLY: ("c3", "c3", False, "v1"),
    }


def test_embeddings_current(query):
    rows = query("SELECT uri, cid, stale FROM post_embeddings_current")
    # The current cid wins over a newer embedding of the old one.
    assert {r["uri"]: (r["cid"], r["stale"]) for r in rows} == {EDITED: ("c2b", False), REPLY: ("c3", False)}


def test_post_edges_current(query):
    rows = query("SELECT src_uri, dst_uri, type FROM post_edges_current")
    assert {(r["src_uri"], r["dst_uri"], r["type"]) for r in rows} == {
        (REPLY, EDITED, "reply_parent"),
        (REPLY, EDITED, "reply_root"),
        (LIKED, REPLY, "like"),
    }


def test_post_edges_keep_every_version(silver):
    edges = {(r.src_uri, r.type) for r in silver.spark.table(silver.table("post_edges")).collect()}
    # The deleted quote and the unliked like stay in the table; only the view drops them.
    assert (DELETED, "quote") in edges and (UNLIKED, "like") in edges


def test_statements_include_dependencies():
    created = [s.split("\n")[0].split()[-2] for s in statements("ns", ["post_embeddings_current"])]
    assert created == ["posts_current", "post_embeddings_current"]
    assert len(statements("ns")) == len(VIEWS)
