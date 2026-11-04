"""Process bronze Jetstream events into the silver Iceberg tables."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
import tempfile

import boto3
from dotenv import find_dotenv, load_dotenv
from pyspark.sql import SparkSession

from .job import SilverJob

log = logging.getLogger("bsky_silver")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env = os.environ.get
    warehouse = env("BSKY_WAREHOUSE_BUCKET")
    default_location = f"s3://{warehouse}/iceberg/bsky_silver" if warehouse else None

    p = argparse.ArgumentParser(description="Process new bronze Jetstream rows into the silver Iceberg tables.")
    p.add_argument(
        "--bronze-table",
        default=env("BSKY_BRONZE_TABLE", "glue.bsky_raw.jetstream"),
        help="Bronze Iceberg table [env: BSKY_BRONZE_TABLE] (default: %(default)s)",
    )
    p.add_argument(
        "--namespace",
        default=env("BSKY_SILVER_NAMESPACE", "glue.bsky_silver"),
        help="Catalog and database of the silver tables [env: BSKY_SILVER_NAMESPACE] (default: %(default)s)",
    )
    p.add_argument(
        "--location",
        default=env("BSKY_SILVER_LOCATION", default_location),
        required=not (env("BSKY_SILVER_LOCATION") or default_location),
        help="Base location for new silver tables [env: BSKY_SILVER_LOCATION] "
        "(default: s3://$BSKY_WAREHOUSE_BUCKET/iceberg/bsky_silver)",
    )
    p.add_argument(
        "--staging-dir",
        default=env("BSKY_SILVER_STAGING_DIR"),
        help="Local directory for the parsed batch [env: BSKY_SILVER_STAGING_DIR] (default: a temp dir)",
    )
    p.add_argument(
        "--max-rows",
        type=int,
        default=500_000,
        help="Max bronze rows per window; bounds memory, about 1 GB + 2 GB per million rows (default: %(default)s)",
    )
    p.add_argument(
        "--settle-minutes",
        type=float,
        default=5,
        help="Leave bronze rows this recent for the next run (default: %(default)s)",
    )
    p.add_argument("--once", action="store_true", help="Process a single window and exit")
    p.add_argument(
        "--max-runs", type=int, default=None, help="Stop after this many windows (default: until caught up)"
    )
    p.add_argument(
        "--backfill-edges",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Once caught up, build post_edges for windows processed before the job built edges (default: %(default)s)",
    )
    p.add_argument(
        "--spark-remote",
        default=env("SPARK_REMOTE"),
        help="Spark Connect URL of a running Sail server [env: SPARK_REMOTE]. If unset, an embedded server is started.",
    )
    p.add_argument("--catalog", default="glue", help="Name of the Glue catalog on the embedded server (default: %(default)s)")
    return p.parse_args(argv)


def aws_region() -> str | None:
    # boto3 reads AWS_DEFAULT_REGION and ~/.aws/config but not AWS_REGION.
    return os.environ.get("AWS_REGION") or boto3.session.Session().region_name


def connect_spark(args: argparse.Namespace) -> tuple[SparkSession, object | None]:
    if args.spark_remote:
        log.info("connecting to Sail at %s", args.spark_remote)
        return SparkSession.builder.remote(args.spark_remote).getOrCreate(), None

    from pysail.spark import SparkConnectServer

    if "SAIL_CATALOG__LIST" not in os.environ:
        os.environ["SAIL_CATALOG__LIST"] = f'[{{type="glue", name="{args.catalog}", region="{aws_region()}"}}]'
    # One session for the whole process; don't let Sail expire it between runs.
    os.environ.setdefault("SAIL_SPARK__SESSION_TIMEOUT_SECS", str(24 * 3600))
    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    log.info("started embedded Sail server on port %d", port)
    return SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate(), server


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)
    max_runs = 1 if args.once else args.max_runs

    spark, server = connect_spark(args)
    staging = tempfile.TemporaryDirectory(prefix="bsky-silver-") if not args.staging_dir else None
    try:
        job = SilverJob(
            spark,
            args.bronze_table,
            args.namespace,
            args.location,
            args.staging_dir or staging.name,
            max_rows=args.max_rows,
            settle=dt.timedelta(minutes=args.settle_minutes),
        )
        job.ensure_tables()
        runs, caught_up = 0, False
        while max_runs is None or runs < max_runs:
            if job.run() is None:
                log.info("caught up")
                caught_up = True
                break
            runs += 1
        log.info("%d run(s)", runs)
        # New windows first: the backfill only uses time left over, and resumes in the next run.
        if caught_up and args.backfill_edges:
            job.backfill_edges()
        return 0
    finally:
        spark.stop()
        if server is not None:
            server.stop()
        if staging is not None:
            staging.cleanup()


if __name__ == "__main__":
    sys.exit(main())
