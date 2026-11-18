"""Copy an Iceberg table into another catalog, one partition at a time, resumably.

The source is opened read-only from its metadata file (StaticTable), so only
S3 read access to the old warehouse is needed, not the old account's Glue
catalog. Copying the objects wouldn't work: Iceberg metadata names every file
by its absolute s3:// path, and the new warehouse bucket has a different name.

The destination gets the same schema, partition spec and properties. Rows are
read with a scan per partition and appended in commits of `rows_per_commit`.
The last commit of each partition carries MARK in its snapshot summary, so a
rerun skips finished partitions; a partition left half-copied by a crash has
no mark, and its rows are deleted before it is copied again.

`live` is for a destination that something else also writes: M5's bronze,
which the new account's loader has appended to since T0. There an unmarked
partition holds the loader's rows, not a crashed copy's, so nothing is
deleted. A crash mid-partition then leaves duplicates, which silver's
anti-join on `event_id` drops. (Without this, a run deleted the new loader's
rows from every hour after T0, 2026-11-18.)

`before` (column, ISO timestamp) keeps only earlier rows: bronze up to the
moment the new account's ingestor started.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import AlwaysTrue, And, BooleanExpression, EqualTo, LessThan
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import StaticTable, Table
from pyiceberg.transforms import IdentityTransform

log = logging.getLogger("bsky_migrate.tables")
MARK = "bsky-migrate.partition"
ROWS_PER_COMMIT = 2_000_000
# Set by the catalog or the writer itself, not part of the table's definition.
SKIP_PROPERTIES = {"metadata_location", "previous_metadata_location", "table_type"}


@dataclass(frozen=True)
class Result:
    identifier: str
    partitions: int
    copied: int
    skipped: int
    rows: int
    seconds: float


def open_source(metadata_location: str, properties: dict[str, str] | None = None) -> Table:
    return StaticTable.from_metadata(metadata_location, properties=properties or {})


def partition_fields(table: Table) -> list[str]:
    """The source column of each partition field. Only identity partitions are supported, which is all the lake has."""
    spec, schema = table.spec(), table.schema()
    names = []
    for f in spec.fields:
        if not isinstance(f.transform, IdentityTransform):
            raise ValueError(f"{table.name()}: partition field {f.name} isn't an identity transform")  # noqa: TRY004
        names.append(schema.find_column_name(f.source_id))
    return names


def partitions(table: Table) -> list[dict]:
    """Every partition's values ({column: value}), in a stable order; [{}] for an unpartitioned table."""
    names = partition_fields(table)
    if not names:
        return [{}]
    if table.current_snapshot() is None:
        return []
    rows = table.inspect.partitions().column("partition").to_pylist()
    spec_names = [f.name for f in table.spec().fields]
    out = [{col: row[pname] for col, pname in zip(names, spec_names)} for row in rows]
    return sorted(out, key=key)


def key(partition: dict) -> str:
    return json.dumps(partition, sort_keys=True, default=str)


def row_filter(partition: dict, before: tuple[str, str] | None = None) -> BooleanExpression:
    terms: list[BooleanExpression] = [EqualTo(col, value) for col, value in partition.items()]
    if before:
        terms.append(LessThan(before[0], before[1]))
    if not terms:
        return AlwaysTrue()
    return terms[0] if len(terms) == 1 else And(*terms)


def finished(table: Table) -> set[str]:
    """Partitions whose last commit is in the table (MARK in a snapshot summary)."""
    out = set()
    for snap in table.snapshots():
        props = snap.summary.additional_properties if snap.summary else {}
        if MARK in props:
            out.add(props[MARK])
    return out


def ensure_destination(source: Table, dest_cat: Catalog, identifier: str, location: str) -> Table:
    namespace = identifier.rsplit(".", 1)[0]
    if not dest_cat.namespace_exists(namespace):
        dest_cat.create_namespace(namespace)
    props = {k: v for k, v in source.properties.items() if k not in SKIP_PROPERTIES}
    return dest_cat.create_table_if_not_exists(
        identifier, schema=source.schema(), partition_spec=source.spec(), sort_order=source.sort_order(),
        properties=props, location=location,
    )


def _append(table: Table, rows: pa.Table, props: dict[str, str], attempts: int = 4,
            sleep: Callable[[float], None] = time.sleep) -> None:
    for attempt in range(attempts):
        try:
            table.append(rows, snapshot_properties=props)
            return
        except CommitFailedException:
            if attempt == attempts - 1:
                raise
            table.refresh()
            sleep(5 * (attempt + 1))


def _commits(batches: Iterator[pa.RecordBatch], rows_per_commit: int) -> Iterator[tuple[pa.Table, bool]]:
    """Group batches into tables of about rows_per_commit rows; the flag marks the last one."""
    pending: list[pa.RecordBatch] = []
    n = 0
    ready: pa.Table | None = None
    for b in batches:
        if not b.num_rows:
            continue
        pending.append(b)
        n += b.num_rows
        if n >= rows_per_commit:
            if ready is not None:
                yield ready, False
            ready, pending, n = pa.Table.from_batches(pending), [], 0
    if pending:
        if ready is not None:
            yield ready, False
        ready = pa.Table.from_batches(pending)
    if ready is not None:
        yield ready, True


def read(source: Table, row_filter: BooleanExpression) -> Iterator[pa.RecordBatch]:
    """The rows matching row_filter, one data file at a time.

    Not `scan().to_arrow_batch_reader()`: pyiceberg 0.12 hands every file of the scan to its
    thread pool at once and each task reads its whole file, so finished files pile up ahead of
    a slower writer. A 44M-row day of likes (21 files) went past 16 Gi that way (2026-11-18).
    """
    scan = source.scan(row_filter=row_filter)
    reader = ArrowScan(source.metadata, source.io, scan.projection(), scan.row_filter, scan.case_sensitive)
    for task in scan.plan_files():
        yield from reader.to_record_batches([task])


def copy_table(source: Table, dest: Table, before: tuple[str, str] | None = None,
               rows_per_commit: int = ROWS_PER_COMMIT, live: bool = False) -> Result:
    t0 = time.monotonic()
    done = finished(dest)
    parts = partitions(source)
    copied = skipped = rows = 0
    for p in parts:
        k = key(p)
        if k in done:
            skipped += 1
            continue
        f = row_filter(p, before)
        # Rows from a crashed attempt at this partition (no mark): drop them, then copy again.
        # Not in a live table, where they are another writer's rows.
        if p and not live and dest.current_snapshot() is not None and dest.scan(row_filter=row_filter(p)).plan_files():
            log.info("%s %s: clearing a partial copy", dest.name(), k)
            dest.delete(row_filter(p))
        n = 0
        for chunk, last in _commits(read(source, f), rows_per_commit):
            _append(dest, chunk, {MARK: k} if last else {})
            n += chunk.num_rows
        rows += n
        copied += 1
        log.info("%s %s: %d rows", dest.name(), k, n)
    return Result(".".join(dest.name()), len(parts), copied, skipped, rows, round(time.monotonic() - t0, 1))


def counts(table: Table) -> dict[str, int]:
    """Rows per partition, from the table's metadata (no data read)."""
    if table.current_snapshot() is None:
        return {}
    names = partition_fields(table)
    if not names:
        s = table.current_snapshot().summary
        return {key({}): int(s.additional_properties.get("total-records", 0)) if s else 0}
    t = table.inspect.partitions()
    spec_names = [f.name for f in table.spec().fields]
    return {key({c: row[p] for c, p in zip(names, spec_names)}): n
            for row, n in zip(t.column("partition").to_pylist(), t.column("record_count").to_pylist())}


def compare(source: Table, dest: Table, before: tuple[str, str] | None = None, live: bool = False) -> dict:
    """Per-partition row counts. With `before`, partitions may hold fewer rows in the destination;
    in a `live` destination (another writer's rows, duplicates from a crash), more, never fewer."""
    src, dst = counts(source), counts(dest)
    missing = sorted(k for k in src if src[k] and k not in dst)
    differ = {k: (src[k], dst[k]) for k in src if k in dst and src[k] != dst[k]}
    if before:
        # The filter can empty a partition (missing) or shorten it, never lengthen it.
        bad = {k: v for k, v in differ.items() if v[1] > v[0]}
        fewer = len(differ) - len(bad) + len(missing)
        missing = []
    elif live:
        bad = {k: v for k, v in differ.items() if v[1] < v[0]}
        fewer = 0
    else:
        bad, fewer = differ, 0
    return {"source_rows": sum(src.values()), "dest_rows": sum(dst.values()), "partitions": len(src),
            "missing": missing, "mismatched": bad, "fewer_by_filter": fewer,
            "ok": not missing and not bad}


def utc_iso(when: str) -> str:
    """Normalise an ISO timestamp to UTC with an explicit offset, as the filter literal."""
    t = dt.datetime.fromisoformat(when)  # accepts a trailing Z
    if t.tzinfo is None:
        raise ValueError(f"{when!r} needs a time zone")
    return t.astimezone(dt.UTC).isoformat()
