import datetime as dt
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from bsky_silver.job import BACKFILL, RUNS, SilverJob
from bsky_silver.tables import SCHEMAS

FIXTURES = Path(__file__).with_name("fixtures") / "jetstream.jsonl"


def fixtures() -> list[tuple[str, dict]]:
    """(case, raw Jetstream event) from the S1 sample."""
    lines = [json.loads(line) for line in FIXTURES.read_text().splitlines()]
    return [(line["case"], line["event"]) for line in lines]


def utc(*args) -> dt.datetime:
    return dt.datetime(*args, tzinfo=dt.timezone.utc)


@pytest.fixture(scope="session")
def spark():
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    session = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    # Iceberg timestamps must be UTC; the job sets this too, but the test's bronze table comes first.
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()
    server.stop()


BRONZE_DDL = """received_at TIMESTAMP, time_us BIGINT, payload STRING, source_file STRING,
    ingested_at TIMESTAMP, day DATE, hour INT"""


@pytest.fixture
def job(spark, tmp_path, request):
    name = request.node.name.replace("[", "_").replace("]", "").replace("-", "_")
    namespace = f"silver_{name}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace}")
    bronze = f"{namespace}.bronze"
    spark.sql(
        f"CREATE TABLE {bronze} ({BRONZE_DDL}) USING iceberg PARTITIONED BY (day, hour) LOCATION '{(tmp_path / 'bronze').as_uri()}'"
    )
    job = SilverJob(spark, bronze, namespace, (tmp_path / "silver").as_uri(), tmp_path / "staging")
    job.ensure_tables()
    yield job
    for table in [*SCHEMAS, RUNS, BACKFILL, "bronze"]:
        spark.sql(f"DROP TABLE IF EXISTS {namespace}.{table}")


def add_bronze(job: SilverJob, payloads: list[str], ingested_at: dt.datetime, received_at: dt.datetime | None = None):
    received_at = received_at or ingested_at - dt.timedelta(seconds=30)
    rows = []
    for payload in payloads:
        try:
            time_us = json.loads(payload).get("time_us")
        except ValueError:
            time_us = None
        rows.append((received_at, time_us, payload, "file:///src.parquet", ingested_at, received_at.date(), received_at.hour))
    job.spark.createDataFrame(rows, BRONZE_DDL).writeTo(job.bronze_table).append()
