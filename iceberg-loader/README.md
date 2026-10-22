# iceberg-loader

Appends the raw Jetstream Parquet files written by `bsky-ingest` to an Apache Iceberg table registered in AWS Glue. This table is the raw (bronze) layer of a medallion architecture. The job is written in PySpark and runs on [Sail](https://docs.lakesail.com/sail/latest/introduction/) over Spark Connect.

```
bsky-ingest ──PUT──▶ s3://bucket/jetstream/date=…/hour=…/*.parquet
                         │ s3:ObjectCreated (prefix jetstream/, suffix .parquet)
                         ▼
                     SQS queue ──(after 5 failed receives)──▶ dead-letter queue
                         │ long poll, batch
                         ▼
                     iceberg-loader ──Spark Connect──▶ Sail ──▶ glue.bsky_raw.jetstream (Iceberg)
```

## How it works

- Long-polls SQS and collects up to `--max-messages` (default 100). It stops collecting once `--batch-window` seconds (default 30) have passed since the first message arrived.
- Parses S3 notifications sent directly to SQS, wrapped in SNS, or delivered via EventBridge. S3 test events, non-create events, and keys outside `--source-prefix` or not ending in `.parquet` are ignored.
- Reads the batch's files and appends them to the table in **one Iceberg commit**.
- **Deletes the SQS messages only after the commit succeeds.** On failure the messages reappear after the visibility timeout and go to the dead-letter queue after repeated failures. Malformed messages are never deleted, so they also end up in the dead-letter queue.
- **Redelivery doesn't duplicate data.** Files whose `source_file` is already in the table are skipped, so a crash between commit and delete is harmless.
- Objects that no longer exist are logged and skipped.

## Table

`glue.bsky_raw.jetstream`, Iceberg, `PARTITIONED BY (day, hour)`:

| Column | Type | |
|---|---|---|
| `received_at` | `timestamp` | From the source file |
| `time_us` | `bigint` | From the source file (Jetstream cursor) |
| `payload` | `string` | Raw Jetstream JSON |
| `source_file` | `string` | `s3://` URI of the file the row came from |
| `ingested_at` | `timestamp` | When the loader appended the row |
| `day` | `date` | UTC date of `received_at` (partition) |
| `hour` | `int` | UTC hour of `received_at` (partition) |

The table is created on startup if it doesn't exist (the Glue database must already exist), at `--table-location` (default `s3://$BSKY_WAREHOUSE_BUCKET/iceberg/bsky_raw/jetstream`). The location is explicit because Sail's Glue catalog has no warehouse setting ([lakehq/sail#2315](https://github.com/lakehq/sail/issues/2315)). Keep it out of the raw bucket's notified prefix: Iceberg's data files are also `.parquet` and must not trigger the queue.

## Setup

The queue, dead-letter queue, bucket notification, Glue database and an IAM policy with the loader's permissions are managed by the Pulumi stack in [`../infra`](../infra). Deploy it, then add its `queue_url` output to the repo's `.env` as `BSKY_LOADER_QUEUE_URL`. Attach the `loader_policy_arn` policy to the role or user the loader runs as.

## Running

```bash
uv sync
uv run bsky-iceberg-loader            # runs until SIGINT/SIGTERM, finishing the current batch
uv run bsky-iceberg-loader --once     # process one batch and exit (non-zero on failure)
```

Settings come from flags, environment variables, or the repo's `.env`: `BSKY_LOADER_QUEUE_URL`, `BSKY_WAREHOUSE_BUCKET`, `BSKY_PREFIX`, `BSKY_ICEBERG_TABLE`, `BSKY_ICEBERG_LOCATION`, `SPARK_REMOTE`. See `--help`.

**Sail server.** By default the loader starts an embedded Sail server, configured with a Glue catalog named `glue` (`--catalog`) in `AWS_REGION`. To use a separately run Sail server instead, start it with the Glue catalog configured and point the loader at it:

```bash
SAIL_CATALOG__LIST='[{type="glue", name="glue", region="us-west-2"}]' sail spark server --port 50051
SPARK_REMOTE=sc://localhost:50051 uv run bsky-iceberg-loader
```

Both the Sail server (S3 + Glue) and the loader (SQS + S3) use the standard AWS credential chain, including `AWS_PROFILE`.

## Development

```bash
uv run pytest                          # runs a local Sail server; no AWS needed
uv run pytest tests/test_loader.py -k skips
```

The loader tests use Sail's in-memory catalog with Iceberg tables on local disk. Consumer tests use a fake SQS client and map `s3://bucket/key` onto local files.

## Sail notes

- **No `input_file_name()`:** Sail doesn't support it, so each file is read separately, tagged with a literal `source_file`, and the files are unioned.
- **Session time zone:** set to UTC so `day`/`hour` don't depend on the server's time zone.
- **No streaming:** Sail doesn't support Iceberg streaming writes, hence the SQS-driven batch loop rather than Structured Streaming.
