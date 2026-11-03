import datetime as dt
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

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
