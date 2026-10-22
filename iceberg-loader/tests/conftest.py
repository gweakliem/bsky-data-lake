import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import SparkSession

from bsky_iceberg_loader.loader import RawTableLoader

# Matches the schema written by the Rust ingestor.
RAW_SCHEMA = pa.schema(
    [
        pa.field("received_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("time_us", pa.int64()),
        pa.field("payload", pa.string(), nullable=False),
    ]
)


def utc(*args) -> dt.datetime:
    return dt.datetime(*args, tzinfo=dt.timezone.utc)


def write_raw_file(path: Path, received_at: list[dt.datetime]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "received_at": received_at,
            "time_us": [int(t.timestamp() * 1_000_000) for t in received_at],
            "payload": [f'{{"n":{i}}}' for i in range(len(received_at))],
        },
        schema=RAW_SCHEMA,
    )
    pq.write_table(table, path)
    return path.as_uri()


@pytest.fixture(scope="session")
def spark():
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    session = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    yield session
    session.stop()
    server.stop()


@pytest.fixture
def loader(spark, tmp_path, request):
    # A fresh table per test in Sail's default in-memory catalog.
    name = f"raw_{request.node.name.replace('[', '_').replace(']', '').replace('-', '_')}"
    loader = RawTableLoader(spark, name, (tmp_path / "table").as_uri())
    loader.ensure_table()
    yield loader
    spark.sql(f"DROP TABLE IF EXISTS {name}")
