import datetime as dt

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DateType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

from bsky_migrate import cli, tables

TS = pa.timestamp("us", tz="UTC")
SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=False),
    NestedField(2, "day", DateType(), required=False),
    NestedField(3, "hour", IntegerType(), required=False),
    NestedField(4, "received_at", TimestamptzType(), required=False),
)
ARROW = pa.schema([("event_id", pa.string()), ("day", pa.date32()), ("hour", pa.int32()), ("received_at", TS)])


def catalog(tmp_path, name: str) -> SqlCatalog:
    (tmp_path / name).mkdir()
    c = SqlCatalog(name, uri=f"sqlite:///{tmp_path}/{name}/catalog.db", warehouse=f"file://{tmp_path}/{name}/wh")
    c.create_namespace("bsky_silver")
    c.create_namespace("bsky_raw")
    return c


def rows(day: dt.date, n: int, start_hour: int = 0) -> pa.Table:
    times = [dt.datetime.combine(day, dt.time(start_hour + i % 2), dt.UTC) for i in range(n)]
    return pa.table({"event_id": [f"{day}-{start_hour}-{i}" for i in range(n)], "day": [day] * n,
                     "hour": [t.hour for t in times], "received_at": times}, schema=ARROW)


def source_table(cat, identifier: str, fields: list[str], days: int = 3, per_append: int = 5):
    spec = PartitionSpec(*[PartitionField(source_id=SCHEMA.find_field(f).field_id, field_id=1000 + i,
                                          transform=IdentityTransform(), name=f) for i, f in enumerate(fields)])
    t = cat.create_table(identifier, schema=SCHEMA, partition_spec=spec, properties={"write.parquet.compression-codec": "zstd"})
    for d in range(days):
        for _ in range(2):  # two appends per day: partitions span several files
            t.append(rows(dt.date(2026, 11, 1 + d), per_append))
    return cat.load_table(identifier)


def sorted_ids(t) -> list[str]:
    return sorted(t.scan().to_arrow()["event_id"].to_pylist())


@pytest.fixture
def cats(tmp_path):
    return catalog(tmp_path, "old"), catalog(tmp_path, "new")


def copy(src_cat_table, dest_cat, identifier, location, **kw):
    source = tables.open_source(src_cat_table.metadata_location)
    dest = tables.ensure_destination(source, dest_cat, identifier, location)
    return source, dest, tables.copy_table(source, dest, **kw)


def test_copies_every_row_with_the_same_partitioning_and_properties(cats, tmp_path):
    old, new = cats
    src = source_table(old, "bsky_silver.posts", ["day"])
    source, dest, r = copy(src, new, "bsky_silver.posts", f"file://{tmp_path}/new/wh/bsky_silver/posts")
    assert (r.partitions, r.copied, r.skipped, r.rows) == (3, 3, 0, 30)
    dest = new.load_table("bsky_silver.posts")
    assert sorted_ids(dest) == sorted_ids(src)
    assert [f.name for f in dest.spec().fields] == ["day"]
    assert dest.properties["write.parquet.compression-codec"] == "zstd"
    assert dest.location() == f"file://{tmp_path}/new/wh/bsky_silver/posts"
    assert tables.compare(source, dest)["ok"]


def test_a_rerun_skips_finished_partitions(cats, tmp_path):
    old, new = cats
    src = source_table(old, "bsky_silver.likes", ["day"])
    copy(src, new, "bsky_silver.likes", f"file://{tmp_path}/new/wh/likes")
    snaps = len(new.load_table("bsky_silver.likes").snapshots())
    _, _, r = copy(src, new, "bsky_silver.likes", f"file://{tmp_path}/new/wh/likes")
    assert (r.copied, r.skipped, r.rows) == (0, 3, 0)
    assert len(new.load_table("bsky_silver.likes").snapshots()) == snaps


def test_a_crash_mid_partition_leaves_no_duplicates(cats, tmp_path, monkeypatch):
    old, new = cats
    src = source_table(old, "bsky_silver.follows", ["day"], per_append=6)
    real = tables._append
    calls = {"n": 0}

    def flaky(table, chunk, props, **kw):
        calls["n"] += 1
        if calls["n"] == 4:  # partition 2's second commit: its first is in, unmarked
            raise RuntimeError("pod killed")
        real(table, chunk, props, **kw)

    monkeypatch.setattr(tables, "_append", flaky)
    with pytest.raises(RuntimeError):
        copy(src, new, "bsky_silver.follows", f"file://{tmp_path}/new/wh/follows", rows_per_commit=6)
    monkeypatch.setattr(tables, "_append", real)
    _, _dest, r = copy(src, new, "bsky_silver.follows", f"file://{tmp_path}/new/wh/follows", rows_per_commit=6)
    assert (r.copied, r.skipped) == (2, 1)
    assert sorted_ids(new.load_table("bsky_silver.follows")) == sorted_ids(src)


def test_bronze_keeps_rows_before_the_cutoff_and_hour_partitions(cats, tmp_path):
    old, new = cats
    src = source_table(old, "bsky_raw.jetstream", ["day", "hour"], days=2)
    cutoff = ("received_at", tables.utc_iso("2026-11-02T01:00:00Z"))  # day 2's hour-1 rows are left out
    source, _dest, _r = copy(src, new, "bsky_raw.jetstream", f"file://{tmp_path}/new/wh/jetstream", before=cutoff)
    got = new.load_table("bsky_raw.jetstream").scan().to_arrow()
    assert max(got["received_at"].to_pylist()) < dt.datetime(2026, 11, 2, 1, tzinfo=dt.UTC)
    assert len(got) == 10 + 6  # day 1 in full, day 2 hour 0 (rows alternate hours 0,1,0,1,0 per append)
    report = tables.compare(source, new.load_table("bsky_raw.jetstream"), cutoff)
    assert report["ok"] and report["fewer_by_filter"] == 1  # day 2 hour 1, emptied by the cutoff


def test_unpartitioned_tables_copy_in_one_go(cats, tmp_path):
    old, new = cats
    t = old.create_table("bsky_silver.silver_runs", schema=SCHEMA)
    t.append(rows(dt.date(2026, 11, 1), 4))
    _, _, r = copy(old.load_table("bsky_silver.silver_runs"), new, "bsky_silver.silver_runs", f"file://{tmp_path}/new/wh/runs")
    assert (r.partitions, r.rows) == (1, 4)
    assert len(new.load_table("bsky_silver.silver_runs").scan().to_arrow()) == 4


def test_verify_flags_a_missing_partition(cats, tmp_path):
    old, new = cats
    src = source_table(old, "bsky_silver.posts", ["day"])
    source = tables.open_source(src.metadata_location)
    dest = tables.ensure_destination(source, new, "bsky_silver.posts", f"file://{tmp_path}/new/wh/posts")
    dest.append(rows(dt.date(2026, 11, 1), 10))
    report = tables.compare(source, new.load_table("bsky_silver.posts"))
    assert not report["ok"] and len(report["missing"]) == 2


def test_plan_lists_both_namespaces_and_needs_a_bronze_cutoff(cats):
    old, _ = cats
    source_table(old, "bsky_raw.jetstream", ["day", "hour"], days=1)
    source_table(old, "bsky_silver.posts", ["day"], days=1)
    source_table(old, "bsky_silver.post_edges_backfill", [], days=1)
    with pytest.raises(SystemExit):
        cli.build_plan(old, "new-bucket", None, ["lance/"], set())
    plan = cli.build_plan(old, "new-bucket", "2026-11-20T12:00:00Z", ["lance/"], {"bsky_silver.post_edges_backfill"})
    assert [e["identifier"] for e in plan["tables"]] == ["bsky_raw.jetstream", "bsky_silver.posts"]
    assert plan["tables"][0]["before"] == ["received_at", "2026-11-20T12:00:00+00:00"]
    assert plan["tables"][1]["before"] is None
    assert plan["dest_iceberg"] == "s3://new-bucket/iceberg"
    assert plan["objects"][0]["dest"] == "s3://new-bucket/lance/"
