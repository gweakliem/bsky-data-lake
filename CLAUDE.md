# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                                   # build
cargo test                                    # all tests (no network/AWS needed)
cargo test splits_batch_at_hour_boundaries    # single test by name substring
cargo clippy --all-targets                    # lint (kept warning-free)

cargo run -- <output> [--batch-size N] [--flush-interval SECS]   # <output> = local dir or s3://bucket/prefix
```

Running against S3 with an AWS profile (object_store does not read `~/.aws/config` profiles/SSO, so export creds first):

```bash
eval "$(aws configure export-credentials --profile gweakliem --format env)"
AWS_REGION=us-west-2 cargo run -- s3://bsky-data-drop-525238890881-us-west-2-an/jetstream-test
```

The bucket name really ends in `-an` (an S3 account-regional bucket). `jetstream-test/` is the scratch prefix used for manual test runs.

## Architecture

Rust port of a Python Jetstream→DuckDB ingestor (`~/git/bluesky-jetstream-ingest`), writing Parquet instead. Three modules:

- `main.rs` — CLI (clap, with `BSKY_*` env fallbacks), installs the rustls crypto provider, wires up the tasks, and turns SIGINT/SIGTERM into a `watch` shutdown signal.
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
