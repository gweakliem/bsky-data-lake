# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                                   # build
cargo test                                    # all tests (no network/AWS needed)
cargo test splits_batch_at_hour_boundaries    # single test by name substring
cargo clippy --all-targets                    # lint (kept warning-free)

cargo run -- [<output>] [--bucket B] [--prefix P] [--batch-size N] [--flush-interval SECS]
```

`<output>` is a local dir or `s3://bucket/prefix` and overrides `--bucket`. The real bucket name and region live in a git-ignored `.env` (`BSKY_BUCKET`, `AWS_REGION`; template in `.env.example`), loaded at startup via `dotenvy`. Keep bucket names and account IDs out of committed files, since the repo is public.

Running against S3 with an AWS profile (object_store does not read `~/.aws/config` profiles/SSO, so export creds first):

```bash
eval "$(aws configure export-credentials --profile <profile> --format env)"
cargo run -- --prefix jetstream-test   # bucket from .env; jetstream-test/ is the scratch prefix for manual runs
```

## iceberg-loader (Python subproject)

`iceberg-loader/` is a separate uv project. Run its commands from that directory:

```bash
uv run pytest                                   # starts a local Sail server; no AWS needed
uv run pytest tests/test_loader.py -k skips     # single test
uv run bsky-iceberg-loader --once               # one SQS batch against real AWS
```

It long-polls the SQS queue from `infra/` of S3 ObjectCreated notifications and appends the referenced files to the Iceberg table `glue.bsky_raw.jetstream`, partitioned by identity columns `day`/`hour` (UTC of `received_at`), via PySpark over Spark Connect to Sail.
- **Delivery:** messages are deleted only after the Iceberg commit. Redelivered files are skipped by checking `source_file` in the table.
- **Notification filter:** the S3 notification must filter on the ingestor's prefix, because Iceberg data files are also `.parquet` in the same bucket.
- **Sail constraints:** no `input_file_name()` (files are tagged with `F.lit`), Glue tables need an explicit `LOCATION`, and no streaming.
- **Tests:** they use Sail's in-memory catalog with `file://` table locations.

## infra (Pulumi subproject)

`infra/` is a Pulumi Python stack (uv toolchain) for the SQS queue + DLQ, the S3 bucket notification, the Glue database `bsky_raw` and the loader's IAM policy. The raw and warehouse buckets are pre-existing and not managed. Infrastructure changes go here, not into ad-hoc scripts.
- **Run commands from `infra/`:** `uv run pytest` checks resources with Pulumi mocks; `pulumi preview` / `pulumi up` need `AWS_PROFILE`.
- **Bucket names** come from `BSKY_BUCKET` / `BSKY_WAREHOUSE_BUCKET` in `.env`, so they aren't committed. Iceberg tables live in the warehouse bucket.
- **`BucketNotification` owns the bucket's entire notification configuration.**
- **State and passphrase:** state is in a dedicated S3 state bucket, not Pulumi Cloud; the CLI's default login is an ephemeral agent account. Before any `pulumi` command, set `AWS_PROFILE`, `PULUMI_BACKEND_URL=s3://<state-bucket>?region=us-west-2` and `PULUMI_CONFIG_PASSPHRASE_FILE=~/.config/bsky-data-lake/pulumi-passphrase`. The `pulumi` binary is at `~/.pulumi/bin/pulumi` and isn't on PATH.
- **IAM is off in `dev`:** `manageIam` is `false` there because the SSO `PowerUserAccess` role can't create IAM policies. The policy JSON is exported as `loader_policy_document` for an admin to create.

## Architecture (Rust ingestor)

Rust port of a Python Jetstream→DuckDB ingestor (`~/git/bluesky-jetstream-ingest`), writing Parquet instead. Three modules:

- `main.rs` — CLI (clap, with `BSKY_*` env fallbacks, `.env` loaded first; `resolve_output` turns output/bucket/prefix into one location), installs the rustls crypto provider, wires up the tasks, and turns SIGINT/SIGTERM into a `watch` shutdown signal.
- `ingest.rs` — two tokio tasks joined by a bounded `mpsc` channel:
  - `run_ingest`: websocket reader. Reconnects with exponential backoff (1s→60s) and resumes using the last `time_us` as Jetstream's `?cursor=`. Parses only `time_us` from each message; the payload is kept as the raw string.
  - `run_writer`: buffers `Record`s and flushes when `batch_size` is reached or `flush_interval` ticks.
- `writer.rs` — `ParquetWriter` over `Arc<dyn ObjectStore>`, so local disk and S3 go through the same path. Parquet encoding runs in `spawn_blocking`, then a single async `put` uploads the file.

Behaviors that span modules:

- **Shutdown/flush chain:** on a signal, `run_ingest` returns and drops the channel sender. `run_writer` then sees the channel close, flushes the remainder, and exits. `main` awaits both tasks. Any shutdown change has to keep this chain intact or buffered data is lost.
- **Fail-fast on write errors:** a failed write makes `run_writer` return `Err`. The next `tx.send` in `run_ingest` then fails, ingest stops, and the process exits non-zero. Nothing is retried beyond object_store's built-in HTTP retries.
- **Output layout:** `<prefix>/date=YYYY-MM-DD/hour=HH/jetstream-<first received_at>-<8 hex>.parquet`, partitioned by the UTC hour of `received_at`. `write_batch` splits a batch at hour boundaries, so one flush can produce several files. The random suffix (rather than an existence check) prevents overwrites on S3.
- **Schema:** `received_at` (timestamp µs UTC), `time_us` (nullable int64), `payload` (raw JSON string). It is defined in `ParquetWriter::new` and built in `encode`, which must stay in sync.

## Gotchas

- **TLS:** both tokio-tungstenite and object_store's `aws` feature use rustls. object_store pulls in `aws-lc-rs`, so `main` explicitly installs `rustls::crypto::aws_lc_rs::default_provider()`. Enabling another provider (e.g. `ring`) without that call makes rustls panic at the first TLS connection.
- **S3 setup:** `object_store::parse_url_opts` does not read `AWS_*` env vars, which is why S3 stores are built with `AmazonS3Builder::from_env().with_url(...)` in `ParquetWriter::from_location`.
- **Tests use `InMemory`:** writer/ingest tests run against object_store's `InMemory` store, so they need no S3. Only `writes_to_local_directory` touches the filesystem.
