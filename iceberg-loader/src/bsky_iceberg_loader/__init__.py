"""Load raw Bluesky Jetstream Parquet files from S3 into an Iceberg table."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading

import boto3
from botocore.exceptions import ClientError
from dotenv import find_dotenv, load_dotenv
from pyspark.sql import SparkSession

from .consumer import QueueConsumer
from .events import S3Object
from .loader import RawTableLoader

log = logging.getLogger("bsky_iceberg_loader")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env = os.environ.get
    warehouse = env("BSKY_WAREHOUSE_BUCKET")
    default_location = f"s3://{warehouse}/iceberg/bsky_raw/jetstream" if warehouse else None

    p = argparse.ArgumentParser(
        description="Append raw Jetstream Parquet files to an Iceberg table as S3 notifications arrive on SQS."
    )
    p.add_argument(
        "--queue-url",
        default=env("BSKY_LOADER_QUEUE_URL"),
        required=not env("BSKY_LOADER_QUEUE_URL"),
        help="SQS queue receiving S3 ObjectCreated notifications [env: BSKY_LOADER_QUEUE_URL]",
    )
    p.add_argument(
        "--table",
        default=env("BSKY_ICEBERG_TABLE", "glue.bsky_raw.jetstream"),
        help="Fully qualified Iceberg table [env: BSKY_ICEBERG_TABLE] (default: %(default)s)",
    )
    p.add_argument(
        "--table-location",
        default=env("BSKY_ICEBERG_LOCATION", default_location),
        required=not (env("BSKY_ICEBERG_LOCATION") or default_location),
        help="S3 location for a newly created table [env: BSKY_ICEBERG_LOCATION] "
        "(default: s3://$BSKY_WAREHOUSE_BUCKET/iceberg/bsky_raw/jetstream)",
    )
    p.add_argument(
        "--source-prefix",
        default=env("BSKY_PREFIX", "jetstream").strip("/") + "/",
        help="Only load keys under this prefix [env: BSKY_PREFIX] (default: %(default)s)",
    )
    p.add_argument(
        "--spark-remote",
        default=env("SPARK_REMOTE"),
        help="Spark Connect URL of a running Sail server, e.g. sc://localhost:50051 "
        "[env: SPARK_REMOTE]. If unset, an embedded Sail server is started.",
    )
    p.add_argument(
        "--catalog",
        default="glue",
        help="Name of the Glue catalog to configure on the embedded server (default: %(default)s)",
    )
    p.add_argument("--max-messages", type=int, default=100, help="Max SQS messages per batch (default: %(default)s)")
    p.add_argument(
        "--batch-window",
        type=float,
        default=30.0,
        help="Max seconds to keep collecting messages after the first arrives (default: %(default)s)",
    )
    p.add_argument("--once", action="store_true", help="Process a single batch and exit")
    return p.parse_args(argv)


def connect_spark(args: argparse.Namespace) -> tuple[SparkSession, object | None]:
    if args.spark_remote:
        log.info("connecting to Sail at %s", args.spark_remote)
        return SparkSession.builder.remote(args.spark_remote).getOrCreate(), None

    from pysail.spark import SparkConnectServer

    # The embedded server reads its catalog config from the environment.
    if "SAIL_CATALOG__LIST" not in os.environ:
        region = os.environ.get("AWS_REGION") or boto3.session.Session().region_name
        os.environ["SAIL_CATALOG__LIST"] = f'[{{type="glue", name="{args.catalog}", region="{region}"}}]'
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

    spark, server = connect_spark(args)
    try:
        loader = RawTableLoader(spark, args.table, args.table_location)
        loader.ensure_table()
        log.info("loading into %s at %s", args.table, args.table_location)

        s3 = boto3.client("s3")

        def object_exists(obj: S3Object) -> bool:
            try:
                s3.head_object(Bucket=obj.bucket, Key=obj.key)
                return True
            except ClientError as e:
                if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                    return False
                raise

        consumer = QueueConsumer(
            boto3.client("sqs"),
            args.queue_url,
            loader,
            object_exists=object_exists,
            source_prefix=args.source_prefix,
            max_messages=args.max_messages,
            batch_window=args.batch_window,
        )

        if args.once:
            messages = consumer.receive_batch()
            log.info("received %d message(s)", len(messages))
            return 0 if not messages or consumer.process(messages) else 1

        stop = threading.Event()

        def request_stop(signum, _frame):
            log.info("received %s; finishing current batch", signal.Signals(signum).name)
            stop.set()

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)
        log.info("polling %s", args.queue_url)
        consumer.run(stop)
        return 0
    finally:
        spark.stop()
        if server is not None:
            server.stop()


if __name__ == "__main__":
    sys.exit(main())
