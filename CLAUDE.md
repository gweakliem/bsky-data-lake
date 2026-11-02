# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo clippy --all-targets                    # lint (kept warning-free)
```

The positional output (a local dir or `s3://bucket/prefix`) overrides `--bucket`. The real bucket name and region live in a git-ignored `.env` (`BSKY_BUCKET`, `AWS_REGION`; template in `.env.example`), loaded at startup via `dotenvy`. Keep bucket names and account IDs out of committed files, since the repo is public.

Running against S3 with an AWS profile (object_store does not read `~/.aws/config` profiles/SSO, so export creds first):

```bash
eval "$(aws configure export-credentials --profile <profile> --format env)"
cargo run -- --prefix jetstream-test --no-resume   # bucket from .env; jetstream-test/ is the scratch prefix for manual runs
```

Without `--no-resume`, a run replays from the prefix's newest file, which for a stale scratch prefix means catching up on everything since then.

Files under `jetstream-test/` never reach the Iceberg loader. The S3 notification (in `infra/`) only fires for the raw prefix, `jetstream/` by default. To exercise the loader, write to `jetstream/`.

## Subprojects

`iceberg-loader/` (uv, PySpark on Sail) and `infra/` (Pulumi) each have their own CLAUDE.md. Infrastructure changes go in `infra/`, not into ad-hoc scripts.

## Architecture (Rust ingestor)

Rust port of a Python Jetstream→DuckDB ingestor (`~/git/bluesky-jetstream-ingest`), writing Parquet instead.

Behaviors that span modules:

- **Shutdown/flush chain:** on a signal, `run_ingest` returns and drops the channel sender. `run_writer` then sees the channel close, flushes the remainder, and exits. `main` awaits both tasks. Any shutdown change has to keep this chain intact or buffered data is lost.
- **Fail-fast on write errors:** a failed write makes `run_writer` return `Err`. The next `tx.send` in `run_ingest` then fails, ingest stops, and the process exits non-zero. Nothing is retried beyond object_store's built-in HTTP retries.
- **Resume:** without `--cursor`, startup reads the max `time_us` from the newest output file's footer (`ParquetWriter::latest_time_us`) and subscribes 5 s (`--resume-margin`) before it, so restarts overlap rather than leave gaps. Reconnects reuse the in-memory cursor, and a connection silent for `--idle-timeout` is reconnected. Downstream dedups the overlap.
- **Output layout:** `<prefix>/date=YYYY-MM-DD/hour=HH/jetstream-<first received_at>-<8 hex>.parquet`, partitioned by the UTC hour of `received_at`. `write_batch` splits a batch at hour boundaries, so one flush can produce several files. The random suffix (rather than an existence check) prevents overwrites on S3.
- **Schema:** `received_at` (timestamp µs UTC), `time_us` (nullable int64), `payload` (raw JSON string). It is defined in `ParquetWriter::new` and built in `encode`, which must stay in sync.

## Gotchas

- **TLS:** both tokio-tungstenite and object_store's `aws` feature use rustls. object_store pulls in `aws-lc-rs`, so `main` explicitly installs `rustls::crypto::aws_lc_rs::default_provider()`. Enabling another provider (e.g. `ring`) without that call makes rustls panic at the first TLS connection.
- **S3 setup:** `object_store::parse_url_opts` does not read `AWS_*` env vars, which is why S3 stores are built with `AmazonS3Builder::from_env().with_url(...)` in `ParquetWriter::from_location`.
