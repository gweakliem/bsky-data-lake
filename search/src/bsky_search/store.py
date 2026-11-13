"""The search index: a Lance dataset next to the Iceberg tables, and its row shape.

LanceDB is a library, not a server: the index job writes the dataset and the
query service opens it read-only, both straight from S3. The index is a derived
copy of `post_embeddings` (the full archive) joined to `posts`, holding only the
last WINDOW_DAYS days of posts; see WP E3 in docs/plans/data-pipeline.md.

One table per embedding spec version (`posts_v1`), since vectors are only
comparable within one. A rebuild writes a new table and the service switches.
"""

from __future__ import annotations

import datetime as dt
import os

import lancedb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from bsky_embed import spec

WINDOW_DAYS = 30
TABLE = f"posts_{spec.SPEC_VERSION}"
RUNS = f"index_runs_{spec.SPEC_VERSION}"  # the index job's state: one row per committed span
TS = pa.timestamp("us", tz="UTC")
VECTOR = pa.list_(pa.float16(), spec.DIMS)

SCHEMA = pa.schema([
    ("uri", pa.string()),
    ("cid", pa.string()),
    ("did", pa.string()),
    ("day", pa.date32()),
    ("created_at", TS),
    ("langs", pa.list_(pa.string())),
    ("text", pa.string()),  # text_spec v1, what was embedded: post text, alt texts, link title
    ("external_uri", pa.string()),
    ("bronze_ingested_at", TS),
    ("vector", VECTOR),
])
RUNS_SCHEMA = pa.schema([
    ("through", TS),  # everything with bronze_ingested_at <= through is indexed
    ("indexed_at", TS),
    ("appended", pa.int64()),
    ("deleted", pa.int64()),
])


def uri() -> str:
    """Where the datasets live: BSKY_LANCE_URI, else the warehouse bucket's `lance/` prefix."""
    return os.environ.get("BSKY_LANCE_URI") or f"s3://{os.environ['BSKY_WAREHOUSE_BUCKET']}/lance"


def connect(location: str | None = None, read_consistency: dt.timedelta | None = None) -> lancedb.DBConnection:
    """Open the database. read_consistency makes a reader pick up new versions that often."""
    location = location or uri()
    options = {"region": os.environ.get("AWS_REGION", "us-west-2")} if location.startswith("s3://") else {}
    return lancedb.connect(location, storage_options=options, read_consistency_interval=read_consistency)


def cutoff(today: dt.date) -> dt.date:
    """The first day the index keeps."""
    return today - dt.timedelta(days=WINDOW_DAYS - 1)


def vectors(stored: pa.Array | pa.ChunkedArray) -> pa.FixedSizeListArray:
    """post_embeddings' float16 bytes to a fixed-size list column, without a Python loop."""
    if isinstance(stored, pa.ChunkedArray):
        stored = stored.combine_chunks()
    if stored.null_count or len(stored) and not pc.all(pc.equal(pc.binary_length(stored), spec.DIMS * 2)).as_py():
        raise ValueError(f"every vector must be {spec.DIMS * 2} bytes")
    offsets = np.frombuffer(stored.buffers()[1], dtype=np.int32)[stored.offset : stored.offset + len(stored) + 1]
    data = np.frombuffer(stored.buffers()[2], dtype=np.uint8)[offsets[0] : offsets[-1]]
    values = pa.array(data.view(spec.DTYPE).astype(np.float16, copy=False), pa.float16())
    return pa.FixedSizeListArray.from_arrays(values, spec.DIMS)


def query_vector(v: np.ndarray) -> np.ndarray:
    """A query's API output, normalized the way stored vectors are."""
    v = np.asarray(v, dtype=np.float32).reshape(-1)
    if v.shape[0] != spec.DIMS:
        raise ValueError(f"expected a {spec.DIMS}-dim query vector, got {v.shape[0]}")
    return spec.normalize(v[None, :])[0]
