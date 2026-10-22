mod ingest;
mod writer;

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tokio::sync::{mpsc, watch};
use tracing::info;
use tracing_subscriber::EnvFilter;

use writer::{Codec, ParquetWriter};

/// Subscribe to the Bluesky Jetstream firehose and write raw messages to Parquet files.
#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Where to write Parquet files: a local directory or an s3://bucket/prefix URL.
    /// Overrides --bucket when both are given.
    #[arg(env = "BSKY_OUTPUT")]
    output: Option<String>,

    /// S3 bucket to write to (as s3://<bucket>/<prefix>) when no output is given.
    /// S3 uses the standard AWS_* environment variables for credentials and region.
    #[arg(long, env = "BSKY_BUCKET")]
    bucket: Option<String>,

    /// Key prefix within --bucket.
    #[arg(long, env = "BSKY_PREFIX", default_value = "jetstream")]
    prefix: String,

    /// Number of messages per Parquet file; a batch is written as soon as it fills.
    #[arg(long, env = "BSKY_BATCH_SIZE", default_value_t = 10_000)]
    batch_size: usize,

    /// Maximum seconds between writes; partial batches are flushed on this interval.
    #[arg(long, env = "BSKY_FLUSH_INTERVAL", default_value_t = 60)]
    flush_interval: u64,

    /// Jetstream subscribe endpoint.
    #[arg(
        long,
        env = "BSKY_JETSTREAM_URL",
        default_value = "wss://jetstream2.us-east.bsky.network/subscribe"
    )]
    url: String,

    /// Only receive these collections (repeatable), e.g. app.bsky.feed.post.
    #[arg(long = "collection")]
    collections: Vec<String>,

    /// Start from this Jetstream cursor (unix microseconds) instead of live.
    #[arg(long)]
    cursor: Option<i64>,

    /// Parquet compression codec.
    #[arg(long, value_enum, default_value_t = Codec::Zstd)]
    compression: Codec,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load settings such as BSKY_BUCKET from .env; real env vars take precedence.
    let dotenv = dotenvy::dotenv();

    // Both the websocket and S3 clients use rustls; pick its crypto backend once.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    if let Ok(path) = &dotenv {
        info!(path = %path.display(), "loaded .env");
    }

    let args = Args::parse();
    let output = resolve_output(args.output, args.bucket, &args.prefix)?;
    anyhow::ensure!(args.batch_size > 0, "--batch-size must be greater than 0");
    anyhow::ensure!(args.flush_interval > 0, "--flush-interval must be greater than 0");

    let writer = ParquetWriter::from_location(&output, args.compression)?;
    info!(%output, "writing parquet");
    let (tx, rx) = mpsc::channel(args.batch_size.max(1024));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let writer_task = tokio::spawn(ingest::run_writer(
        writer,
        rx,
        args.batch_size,
        Duration::from_secs(args.flush_interval),
    ));
    let ingest_task = tokio::spawn(ingest::run_ingest(
        args.url,
        args.collections,
        args.cursor,
        tx,
        shutdown_rx,
    ));

    tokio::spawn(async move {
        wait_for_signal().await;
        info!("shutdown requested; flushing remaining messages");
        let _ = shutdown_tx.send(true);
    });

    // The ingest task drops its sender when it ends, which lets the writer
    // flush what's buffered and exit.
    ingest_task.await?;
    writer_task.await??;
    info!("shutdown complete");
    Ok(())
}

/// Pick the output location: an explicit output wins, otherwise build an S3
/// URL from the bucket and prefix.
fn resolve_output(output: Option<String>, bucket: Option<String>, prefix: &str) -> Result<String> {
    if let Some(output) = output {
        return Ok(output);
    }
    let Some(bucket) = bucket.filter(|b| !b.is_empty()) else {
        anyhow::bail!("no output given: pass a directory or s3:// URL, or set --bucket / BSKY_BUCKET");
    };
    let bucket = bucket.trim_start_matches("s3://").trim_end_matches('/');
    let prefix = prefix.trim_matches('/');
    Ok(if prefix.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{prefix}")
    })
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_output_overrides_bucket() {
        let out = resolve_output(Some("data/".into()), Some("b".into()), "p").unwrap();
        assert_eq!(out, "data/");
    }

    #[test]
    fn builds_s3_url_from_bucket_and_prefix() {
        assert_eq!(resolve_output(None, Some("b".into()), "raw/js").unwrap(), "s3://b/raw/js");
        assert_eq!(resolve_output(None, Some("s3://b/".into()), "/p/").unwrap(), "s3://b/p");
        assert_eq!(resolve_output(None, Some("b".into()), "").unwrap(), "s3://b");
    }

    #[test]
    fn requires_output_or_bucket() {
        assert!(resolve_output(None, None, "p").is_err());
        assert!(resolve_output(None, Some(String::new()), "p").is_err());
    }
}
