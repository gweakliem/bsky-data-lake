"""Silver table schemas. The staging schema and every table's DDL derive from
COLUMNS, so a new column is added in one place."""

from __future__ import annotations

import pyarrow as pa

# Every column used by any table, with its Spark SQL and Arrow types.
COLUMNS: dict[str, tuple[str, pa.DataType]] = {
    "event_id": ("STRING", pa.string()),
    "did": ("STRING", pa.string()),
    "collection": ("STRING", pa.string()),
    "operation": ("STRING", pa.string()),
    "rkey": ("STRING", pa.string()),
    "uri": ("STRING", pa.string()),
    "rev": ("STRING", pa.string()),
    "cid": ("STRING", pa.string()),
    "time_us": ("BIGINT", pa.int64()),
    "created_at": ("TIMESTAMP", pa.timestamp("us", tz="UTC")),
    "text": ("STRING", pa.string()),
    "langs": ("ARRAY<STRING>", pa.list_(pa.string())),
    "reply_root_uri": ("STRING", pa.string()),
    "reply_parent_uri": ("STRING", pa.string()),
    "embed_type": ("STRING", pa.string()),
    "quote_uri": ("STRING", pa.string()),
    "image_alts": ("ARRAY<STRING>", pa.list_(pa.string())),
    "external_uri": ("STRING", pa.string()),
    "external_title": ("STRING", pa.string()),
    "mention_dids": ("ARRAY<STRING>", pa.list_(pa.string())),
    "link_uris": ("ARRAY<STRING>", pa.list_(pa.string())),
    "tags": ("ARRAY<STRING>", pa.list_(pa.string())),
    "self_labels": ("ARRAY<STRING>", pa.list_(pa.string())),
    "record": ("STRING", pa.string()),
    "subject_uri": ("STRING", pa.string()),
    "subject_did": ("STRING", pa.string()),
    "display_name": ("STRING", pa.string()),
    "description": ("STRING", pa.string()),
    "handle": ("STRING", pa.string()),
    "seq": ("BIGINT", pa.int64()),
    "active": ("BOOLEAN", pa.bool_()),
    "status": ("STRING", pa.string()),
    "payload": ("STRING", pa.string()),
    "reason": ("STRING", pa.string()),
    # From bronze. day is the UTC date of received_at, as in bronze.
    "received_at": ("TIMESTAMP", pa.timestamp("us", tz="UTC")),
    "bronze_ingested_at": ("TIMESTAMP", pa.timestamp("us", tz="UTC")),
    "day": ("DATE", pa.date32()),
}

COMMON = ["event_id", "did", "time_us", "received_at", "bronze_ingested_at", "day"]
RECORD = ["rkey", "uri", "rev"]

TABLES: dict[str, list[str]] = {
    "posts": COMMON + RECORD + [
        "cid", "operation", "created_at", "text", "langs", "reply_root_uri", "reply_parent_uri", "embed_type",
        "quote_uri", "image_alts", "external_uri", "external_title", "mention_dids", "link_uris", "tags",
        "self_labels", "record",
    ],
    "likes": COMMON + RECORD + ["cid", "subject_uri", "created_at"],
    "reposts": COMMON + RECORD + ["cid", "subject_uri", "created_at"],
    "follows": COMMON + RECORD + ["cid", "subject_did", "created_at"],
    "blocks": COMMON + RECORD + ["cid", "subject_did", "created_at"],
    "profiles": COMMON + RECORD + ["cid", "operation", "display_name", "description", "self_labels", "record"],
    "identities": COMMON + ["handle", "seq"],
    "accounts": COMMON + ["active", "status", "seq"],
    "deletes": COMMON + RECORD + ["collection"],
    "rejects": ["event_id", "received_at", "bronze_ingested_at", "day", "payload", "reason"],
}

# What the normalize pass writes: every column plus where the row goes.
STAGE_SCHEMA = pa.schema([pa.field("target", pa.string())] + [pa.field(c, t) for c, (_, t) in COLUMNS.items()])
STAGE_DDL = ", ".join(["target STRING"] + [f"{c} {s}" for c, (s, _) in COLUMNS.items()])

RUNS_DDL = """
    run_id STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    bronze_after TIMESTAMP,
    bronze_through TIMESTAMP,
    bronze_rows BIGINT,
    counts STRING
"""


def ddl(table: str) -> str:
    return ", ".join(f"{c} {COLUMNS[c][0]}" for c in TABLES[table])
