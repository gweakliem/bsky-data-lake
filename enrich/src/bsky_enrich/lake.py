"""DuckDB over the lake's silver tables, as reports/lib/lake.py does it.

Needs AWS credentials in the environment (DuckDB's credential_chain doesn't read
SSO profiles) and BSKY_AWS_ACCOUNT_ID, from the repo's .env.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

REPO = Path(__file__).resolve().parents[3]


def connect() -> duckdb.DuckDBPyConnection:
    load_dotenv(REPO / ".env")
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    if limit := os.environ.get("BSKY_DUCKDB_MEMORY_LIMIT"):
        con.execute(f"SET memory_limit = '{limit}'")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg")
    region = os.environ.get("AWS_REGION", "us-west-2")
    con.execute(f"CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION '{region}')")
    con.execute(f"ATTACH '{os.environ['BSKY_AWS_ACCOUNT_ID']}' AS glue (TYPE iceberg, ENDPOINT_TYPE glue)")
    return con


SILVER = "glue.bsky_silver"
