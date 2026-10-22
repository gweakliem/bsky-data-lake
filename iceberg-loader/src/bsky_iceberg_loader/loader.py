"""Append raw Jetstream Parquet files to the Iceberg raw table."""

from __future__ import annotations

import logging
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

# Columns written by the Rust ingestor, plus lineage and partition columns.
# day/hour are the UTC date and hour of received_at, matching the
# date=/hour= layout of the source files.
TABLE_COLUMNS = """
    received_at TIMESTAMP,
    time_us BIGINT,
    payload STRING,
    source_file STRING,
    ingested_at TIMESTAMP,
    day DATE,
    hour INT
"""


def quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


class RawTableLoader:
    def __init__(self, spark: SparkSession, table: str, location: str):
        self.spark = spark
        self.table = table
        self.location = location
        # day/hour must be derived in UTC regardless of the server's zone.
        spark.conf.set("spark.sql.session.timeZone", "UTC")

    def ensure_table(self) -> None:
        """Create the table if it doesn't exist. The database must already
        exist (it's managed by the Pulumi stack in infra/)."""
        # Sail's Glue catalog has no warehouse setting, so the location is explicit.
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} ({TABLE_COLUMNS})
            USING iceberg
            PARTITIONED BY (day, hour)
            LOCATION {quote(self.location)}
            """
        )

    def load(self, uris: list[str]) -> list[str]:
        """Append the given Parquet files, skipping any already in the table.

        Returns the URIs that were appended. All files are committed in one
        Iceberg snapshot, so a failure leaves the table unchanged.
        """
        if not uris:
            return []

        df = self._read(uris)
        days = [row.day for row in df.select("day").distinct().collect()]
        loaded = self._already_loaded(uris, days)
        if loaded:
            log.info("skipping %d file(s) already in %s", len(loaded), self.table)
            df = df.where(~F.col("source_file").isin(sorted(loaded)))

        new = [u for u in uris if u not in loaded]
        if new:
            df.writeTo(self.table).append()
        return new

    def _read(self, uris: list[str]) -> DataFrame:
        # Sail doesn't support input_file_name(), so tag each file's rows
        # with a literal and union them.
        ingested_at = F.current_timestamp()
        frames = [
            self.spark.read.parquet(uri)
            .select("received_at", "time_us", "payload")
            .withColumn("source_file", F.lit(uri))
            for uri in uris
        ]
        return (
            reduce(DataFrame.unionByName, frames)
            .withColumn("ingested_at", ingested_at)
            .withColumn("day", F.to_date("received_at"))
            .withColumn("hour", F.hour("received_at"))
        )

    def _already_loaded(self, uris: list[str], days: list) -> set[str]:
        if not days:
            return set()
        day_list = ", ".join(f"DATE {quote(d.isoformat())}" for d in days)
        uri_list = ", ".join(quote(u) for u in uris)
        rows = self.spark.sql(
            f"""
            SELECT DISTINCT source_file FROM {self.table}
            WHERE day IN ({day_list}) AND source_file IN ({uri_list})
            """
        ).collect()
        return {row.source_file for row in rows}
