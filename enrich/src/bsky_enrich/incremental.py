"""Incremental reads of silver posts and chunked appends, shared by the support jobs (E2, C2).

pyiceberg over the Glue catalog, no Spark. The watermark is `bronze_ingested_at`
(Shared conventions in docs/plans/data-pipeline.md):

- A support table's own max `bronze_ingested_at` is how far it has got; there is
  no other state. It is read from the data files' column stats, not a scan.
- Posts are read one silver window at a time, up to the newest `bronze_through`
  in `silver_runs`. Silver writes that row after its appends, so a window read
  here is complete.
- The loader stamps a whole commit with one `ingested_at`, so thousands of posts
  share a value. `chunks` ends a chunk only where it changes: a chunk boundary
  inside a group would leave the rest of the group behind the watermark if the
  run died between the two commits.
- Posts deleted in the same window are dropped. Later deletes are the views' job
  (`posts_current`), as for every support table.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, BooleanExpression, EqualTo, GreaterThan, LessThanOrEqual
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform

log = logging.getLogger(__name__)

SILVER = "bsky_silver"
WATERMARK = "bronze_ingested_at"
POST = "app.bsky.feed.post"
# What the support jobs read from posts; the eligibility rule and text_spec need the content columns.
POST_COLUMNS = (
    "uri", "cid", "did", "day", "operation", "langs", "text", "image_alts", "external_uri", "external_title",
    WATERMARK,
)


def catalog() -> Catalog:
    """The Glue catalog, with credentials from the environment (Pod Identity in the cluster)."""
    region = os.environ.get("AWS_REGION", "us-west-2")
    return load_catalog("glue", type="glue", **{"client.region": region})


def location(table: str) -> str:
    """Explicit table location, as silver's (Sail's Glue catalog has no warehouse setting)."""
    return f"s3://{os.environ['BSKY_WAREHOUSE_BUCKET']}/iceberg/{SILVER}/{table}"


def ensure_table(cat: Catalog, identifier: str, schema: Schema, table_location: str) -> Table:
    """Create the table, partitioned by `day`, unless it exists."""
    day = schema.find_field("day")
    spec = PartitionSpec(PartitionField(source_id=day.field_id, field_id=1000, transform=IdentityTransform(), name="day"))
    return cat.create_table_if_not_exists(identifier, schema=schema, location=table_location, partition_spec=spec)


def committed_through(table: Table) -> dt.datetime | None:
    """The table's max `bronze_ingested_at`, or None when it is empty."""
    if table.current_snapshot() is None:
        return None
    metrics = table.inspect.files().column("readable_metrics").to_pylist()
    bounds = [(m or {}).get(WATERMARK, {}).get("upper_bound") for m in metrics]
    if any(b is None for b in bounds):  # a file without stats: fall back to a scan
        column = table.scan(selected_fields=(WATERMARK,)).to_arrow()[WATERMARK]
        top = pc.max(column).as_py()
    else:
        top = max(bounds, default=None)
    return _utc(top) if top is not None else None


@dataclass(frozen=True)
class Window:
    """Posts with `bronze_ingested_at` in (after, through]; after is None for the first."""

    after: dt.datetime | None
    through: dt.datetime


def windows(cat: Catalog, after: dt.datetime | None, namespace: str = SILVER) -> list[Window]:
    """Silver's finished windows after the watermark, oldest first."""
    runs = cat.load_table(f"{namespace}.silver_runs").scan(selected_fields=("bronze_through",)).to_arrow()
    throughs = sorted({_utc(t) for t in runs["bronze_through"].to_pylist() if t is not None})
    out, prev = [], after
    for t in throughs:
        if after is None or t > after:
            out.append(Window(prev, t))
            prev = t
    return out


def _between(w: Window) -> BooleanExpression:
    upper = LessThanOrEqual(WATERMARK, w.through.isoformat())
    return upper if w.after is None else And(GreaterThan(WATERMARK, w.after.isoformat()), upper)


def read_posts(cat: Catalog, w: Window, namespace: str = SILVER, columns: tuple[str, ...] = POST_COLUMNS) -> pa.Table:
    """The window's posts, minus any deleted in the same window, sorted by watermark."""
    posts = cat.load_table(f"{namespace}.posts").scan(row_filter=_between(w), selected_fields=columns).to_arrow()
    deletes = cat.load_table(f"{namespace}.deletes").scan(
        row_filter=And(_between(w), EqualTo("collection", POST)), selected_fields=("uri",)
    ).to_arrow()
    if deletes.num_rows:
        posts = posts.filter(pc.invert(pc.is_in(posts["uri"], value_set=deletes["uri"])))
    return posts.sort_by(WATERMARK)


def chunks(rows: pa.Table, size: int) -> Iterator[pa.Table]:
    """Slices of at least `size` rows (fewer at the end), each ending where the watermark changes.

    `rows` must be sorted by watermark. A group larger than `size` is one chunk.
    """
    marks = rows[WATERMARK].to_pylist()
    start, n = 0, rows.num_rows
    while start < n:
        end = min(start + size, n)
        while end < n and marks[end] == marks[end - 1]:
            end += 1
        yield rows.slice(start, end - start)
        start = end


def append(table: Table, rows: pa.Table, attempts: int = 4, sleep: Callable[[float], None] = time.sleep) -> None:
    """Append, retrying a lost commit race (Glue's compactor commits to the same tables)."""
    for i in range(attempts):
        try:
            table.append(rows)
            return
        except CommitFailedException:
            if i == attempts - 1:
                raise
            wait = 5 * 3**i
            log.warning("%s: commit conflict, retrying in %d s", table.name(), wait)
            sleep(wait)
            table.refresh()


def _utc(t: dt.datetime) -> dt.datetime:
    return t.replace(tzinfo=dt.UTC) if t.tzinfo is None else t.astimezone(dt.UTC)
