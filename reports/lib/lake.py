"""DuckDB over the lake, or over a synthetic fixture, for the report data loaders.

The lake is read through the catalog's Iceberg REST endpoint (Glue today), so
moving clouds changes `_attach_lake`, not the SQL in `sql/`. Queries name
tables as `{silver}.<table>` and `{bronze}.<table>`.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"


def _load_dotenv() -> None:
    """The repo's git-ignored .env, without overriding the environment."""
    env = ROOT.parent / ".env"
    if not env.exists():
        return
    for line in env.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def _attach_lake(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    account = os.environ["BSKY_AWS_ACCOUNT_ID"]
    region = os.environ.get("AWS_REGION", "us-west-2")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg")
    # credential_chain reads env credentials locally and EKS Pod Identity in the cluster.
    con.execute(f"CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION '{region}')")
    con.execute(f"ATTACH '{account}' AS glue (TYPE iceberg, ENDPOINT_TYPE glue)")
    return {"silver": "glue.bsky_silver", "bronze": "glue.bsky_raw"}


def connect() -> tuple[duckdb.DuckDBPyConnection, dict[str, str]]:
    """A connection and the namespaces to fill into the SQL.

    BSKY_REPORTS_FIXTURE=1 uses an in-memory fixture with the same tables and
    columns, so the site builds without AWS.
    """
    _load_dotenv()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    if os.environ.get("BSKY_REPORTS_FIXTURE"):
        import fixture

        return con, fixture.build(con)
    return con, _attach_lake(con)


def query(name: str) -> pa.Table:
    con, namespaces = connect()
    return con.sql((SQL / f"{name}.sql").read_text().format(**namespaces)).fetch_arrow_table()


def write_csv(name: str) -> None:
    """Data loader body: the query's result as CSV on stdout."""
    import sys

    import pyarrow.csv as csv

    csv.write_csv(query(name), sys.stdout.buffer)


def write_json(name: str) -> None:
    """Data loader body: the query's single row as a JSON object on stdout."""
    import json
    import sys

    (row,) = query(name).to_pylist()
    json.dump(row, sys.stdout)
