# bsky-ingest

Subscribes to the Bluesky [Jetstream](https://github.com/bluesky-social/jetstream) firehose and writes the raw messages to Parquet files.

[`iceberg-loader/`](iceberg-loader/) is a PySpark job (run on Sail) that appends those files to an Iceberg table in AWS Glue as S3 notifications arrive on SQS. [`infra/`](infra/) is the Pulumi stack for the AWS resources it uses (SQS, bucket notification, Glue database, IAM policy).

## Usage

```bash
cargo run --release -- data/ --batch-size 10000 --flush-interval 60
```

The output can be a local directory or an S3 location, given either as a URL or as `--bucket` (plus optional `--prefix`, default `jetstream`):

```bash
cargo run --release -- s3://my-bucket/raw/jetstream
cargo run --release -- --bucket my-bucket --prefix raw/jetstream
```

Settings can also be kept in a `.env` file in the working directory (git-ignored; see `.env.example`). Real environment variables and command-line options take precedence:

```bash
cp .env.example .env   # then set BSKY_BUCKET, AWS_REGION, ...
cargo run --release    # writes to s3://$BSKY_BUCKET/jetstream
```

A positional output path overrides `--bucket`, so you can still write locally with `cargo run -- data/`.

S3 settings come from the standard environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`/`AWS_DEFAULT_REGION`, and `AWS_ENDPOINT` (with `AWS_ALLOW_HTTP=true` for plain-HTTP endpoints such as MinIO). Instance/ECS/web-identity credentials are also picked up. Named profiles and SSO in `~/.aws/config` are **not** read directly; export them first:

```bash
eval "$(aws configure export-credentials --profile my-profile --format env)"
```

A batch is written when it reaches `--batch-size` messages or when `--flush-interval` seconds have passed, whichever comes first. On SIGINT/SIGTERM, buffered messages are flushed before exit.

| Option | Env | Default | |
|---|---|---|---|
| `<output>` | `BSKY_OUTPUT` | | Local directory or `s3://bucket/prefix`; overrides `--bucket` |
| `--bucket` | `BSKY_BUCKET` | | S3 bucket, used when no `<output>` is given |
| `--prefix` | `BSKY_PREFIX` | `jetstream` | Key prefix within `--bucket` |
| `--batch-size` | `BSKY_BATCH_SIZE` | 10000 | Messages per file |
| `--flush-interval` | `BSKY_FLUSH_INTERVAL` | 60 | Max seconds between writes |
| `--url` | `BSKY_JETSTREAM_URL` | `wss://jetstream2.us-east.bsky.network/subscribe` | Jetstream endpoint |
| `--collection` | | all | Filter to a collection (repeatable) |
| `--cursor` | | live | Start from a `time_us` cursor |
| `--compression` | | `zstd` | `zstd`, `snappy`, or `none` |

Logging is controlled with `RUST_LOG` (default `info`).

On disconnect, the ingestor reconnects with exponential backoff and resumes from the last `time_us` it received.

## Output

Each batch is written as one object at `<output>/date=YYYY-MM-DD/hour=HH/jetstream-<timestamp>-<id>.parquet`, partitioned by the UTC hour of `received_at`. A batch that spans an hour boundary is split into one object per hour, so every row is in the correct partition. Writes are atomic: a single S3 PUT, or a staging file renamed into place locally. Schema:

| Column | Type | |
|---|---|---|
| `received_at` | `TIMESTAMP(us, UTC)` | When the message was received |
| `time_us` | `INT64` (nullable) | Jetstream's event time / cursor |
| `payload` | `STRING` | Raw JSON message |

Query with DuckDB:

```sql
SELECT payload::JSON->>'kind' AS kind, count(*)
FROM read_parquet('data/**/*.parquet', hive_partitioning = true)  -- or 's3://my-bucket/raw/jetstream/**/*.parquet'
GROUP BY 1;

-- partition pruning: only reads the matching hour's files
SELECT count(*)
FROM read_parquet('data/**/*.parquet', hive_partitioning = true)
WHERE date = '2026-10-22' AND hour = 6;
```
