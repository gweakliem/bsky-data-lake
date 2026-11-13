"""A fake silver namespace on a local SQLite catalog, for the support jobs' tests (no AWS).

Needs the dev group's pyiceberg[sql-sqlite]. Tables carry only the columns the
support jobs read.
"""

import datetime as dt

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import DateType, ListType, NestedField, StringType, TimestamptzType

from bsky_enrich import incremental as inc

T0 = dt.datetime(2026, 11, 1, 12, 0, tzinfo=dt.UTC)
DAY = dt.date(2026, 11, 1)
TS = pa.timestamp("us", tz="UTC")


def t(minutes: int) -> dt.datetime:
    return T0 + dt.timedelta(minutes=minutes)


def opt(i: int, name: str, kind) -> NestedField:
    return NestedField(i, name, kind, required=False)


POSTS = Schema(
    opt(1, "uri", StringType()), opt(2, "cid", StringType()), opt(3, "did", StringType()), opt(4, "day", DateType()),
    opt(5, "operation", StringType()), opt(6, "langs", ListType(20, StringType(), element_required=False)),
    opt(7, "text", StringType()), opt(8, "image_alts", ListType(21, StringType(), element_required=False)),
    opt(9, "external_uri", StringType()), opt(10, "external_title", StringType()),
    opt(11, "bronze_ingested_at", TimestamptzType()), opt(12, "created_at", TimestamptzType()),
)
DELETES = Schema(
    opt(1, "uri", StringType()), opt(2, "collection", StringType()), opt(3, "day", DateType()),
    opt(4, "bronze_ingested_at", TimestamptzType()),
)
RUNS = Schema(opt(1, "bronze_after", TimestamptzType()), opt(2, "bronze_through", TimestamptzType()))
OUT = Schema(opt(1, "uri", StringType()), opt(2, "day", DateType()), opt(3, "bronze_ingested_at", TimestamptzType()))


def silver_catalog(tmp_path) -> SqlCatalog:
    c = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}/warehouse")
    c.create_namespace(inc.SILVER)
    for name, schema in [("posts", POSTS), ("deletes", DELETES), ("silver_runs", RUNS)]:
        c.create_table(f"{inc.SILVER}.{name}", schema=schema)
    return c


def add_posts(cat, rows: list[tuple[str, dt.datetime]], text: str = "hello world", *, cid: str = "c",
              operation: str = "create", day: dt.date = DAY) -> None:
    """Posts by (uri, bronze_ingested_at), all with the same text, cid, operation and day unless given."""
    n = len(rows)
    cat.load_table(f"{inc.SILVER}.posts").append(pa.table({
        "uri": [u for u, _ in rows], "cid": [cid] * n, "did": ["did:plc:a"] * n, "day": [day] * n,
        "operation": [operation] * n, "langs": pa.array([["en"]] * n, pa.list_(pa.string())), "text": [text] * n,
        "image_alts": pa.array([[]] * n, pa.list_(pa.string())), "external_uri": pa.array([None] * n, pa.string()),
        "external_title": pa.array([None] * n, pa.string()), "bronze_ingested_at": pa.array([m for _, m in rows], TS),
        "created_at": pa.array([m for _, m in rows], TS),
    }))


def add_runs(cat, *throughs: dt.datetime) -> None:
    cat.load_table(f"{inc.SILVER}.silver_runs").append(pa.table({
        "bronze_after": pa.array([None] * len(throughs), TS), "bronze_through": pa.array(list(throughs), TS),
    }))


def add_deletes(cat, rows: list[tuple[str, dt.datetime]], collection: str = inc.POST) -> None:
    n = len(rows)
    cat.load_table(f"{inc.SILVER}.deletes").append(pa.table({
        "uri": [u for u, _ in rows], "collection": [collection] * n, "day": [DAY] * n,
        "bronze_ingested_at": pa.array([m for _, m in rows], TS),
    }))
