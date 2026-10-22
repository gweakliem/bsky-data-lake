use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::writer::{ParquetWriter, Record};

const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Only the fields we need from each message; everything else is kept raw.
#[derive(Deserialize)]
struct Envelope {
    time_us: Option<i64>,
}

/// Build the subscribe URL, adding collection filters and a resume cursor.
pub fn subscribe_url(base: &str, collections: &[String], cursor: Option<i64>) -> String {
    let mut params: Vec<String> = collections
        .iter()
        .map(|c| format!("wantedCollections={c}"))
        .collect();
    if let Some(cursor) = cursor {
        params.push(format!("cursor={cursor}"));
    }
    if params.is_empty() {
        return base.to_string();
    }
    let sep = if base.contains('?') { '&' } else { '?' };
    format!("{base}{sep}{}", params.join("&"))
}

/// Read from Jetstream until `shutdown` fires, reconnecting on failure and
/// resuming from the last seen `time_us`. Records are sent to `tx`.
pub async fn run_ingest(
    base_url: String,
    collections: Vec<String>,
    mut cursor: Option<i64>,
    tx: mpsc::Sender<Record>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = MIN_BACKOFF;

    while !*shutdown.borrow() {
        let url = subscribe_url(&base_url, &collections, cursor);
        info!(%url, "connecting");

        let ws = tokio::select! {
            res = connect_async(url.as_str()) => res,
            _ = shutdown.changed() => break,
        };
        let mut ws = match ws {
            Ok((ws, _)) => {
                info!("connected");
                ws
            }
            Err(e) => {
                error!(error = %e, ?backoff, "connection failed");
                if sleep_or_shutdown(backoff, &mut shutdown).await {
                    break;
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        loop {
            let msg = tokio::select! {
                msg = ws.next() => msg,
                _ = shutdown.changed() => {
                    let _ = ws.close(None).await;
                    return;
                }
            };
            let text = match msg {
                Some(Ok(Message::Text(text))) => text.to_string(),
                Some(Ok(Message::Binary(bytes))) => match String::from_utf8(bytes.to_vec()) {
                    Ok(text) => text,
                    Err(_) => {
                        warn!("skipping non-UTF-8 binary message");
                        continue;
                    }
                },
                Some(Ok(Message::Close(frame))) => {
                    warn!(?frame, "server closed connection");
                    break;
                }
                Some(Ok(_)) => continue, // ping/pong handled by tungstenite
                Some(Err(e)) => {
                    error!(error = %e, "websocket error");
                    break;
                }
                None => {
                    warn!("websocket stream ended");
                    break;
                }
            };

            backoff = MIN_BACKOFF;
            let time_us = serde_json::from_str::<Envelope>(&text).ok().and_then(|e| e.time_us);
            if time_us.is_some() {
                cursor = time_us;
            }
            let record = Record { received_at: Utc::now(), time_us, payload: text };
            if tx.send(record).await.is_err() {
                error!("writer stopped; ending ingest");
                return;
            }
        }

        if sleep_or_shutdown(backoff, &mut shutdown).await {
            break;
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Sleep for `dur`; returns true if shutdown was requested meanwhile.
async fn sleep_or_shutdown(dur: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(dur) => *shutdown.borrow(),
        _ = shutdown.changed() => true,
    }
}

/// Buffer records from `rx` and write them to Parquet whenever `batch_size`
/// records accumulate or `flush_interval` elapses. Flushes any remainder once
/// the channel closes.
pub async fn run_writer(
    writer: ParquetWriter,
    mut rx: mpsc::Receiver<Record>,
    batch_size: usize,
    flush_interval: Duration,
) -> Result<()> {
    let mut buffer: Vec<Record> = Vec::with_capacity(batch_size);
    let mut ticker = tokio::time::interval(flush_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await; // first tick fires immediately

    loop {
        tokio::select! {
            rec = rx.recv() => match rec {
                Some(rec) => {
                    buffer.push(rec);
                    if buffer.len() >= batch_size {
                        flush(&writer, &mut buffer).await?;
                        ticker.reset();
                    }
                }
                None => break,
            },
            _ = ticker.tick() => flush(&writer, &mut buffer).await?,
        }
    }

    flush(&writer, &mut buffer).await
}

async fn flush(writer: &ParquetWriter, buffer: &mut Vec<Record>) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let batch = std::mem::replace(buffer, Vec::with_capacity(buffer.capacity()));
    for (location, count) in writer.write_batch(batch).await? {
        info!(count, %location, "wrote batch");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::Codec;
    use futures_util::TryStreamExt;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[test]
    fn url_without_params() {
        assert_eq!(subscribe_url("wss://x/subscribe", &[], None), "wss://x/subscribe");
    }

    #[test]
    fn url_with_collections_and_cursor() {
        let cols = vec!["app.bsky.feed.post".to_string(), "app.bsky.feed.like".to_string()];
        assert_eq!(
            subscribe_url("wss://x/subscribe", &cols, Some(123)),
            "wss://x/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like&cursor=123"
        );
    }

    #[test]
    fn url_appends_to_existing_query() {
        assert_eq!(
            subscribe_url("wss://x/subscribe?compress=false", &[], Some(5)),
            "wss://x/subscribe?compress=false&cursor=5"
        );
    }

    fn rec(n: i64) -> Record {
        Record { received_at: Utc::now(), time_us: Some(n), payload: "{}".into() }
    }

    fn memory_writer() -> (Arc<InMemory>, ParquetWriter) {
        let store = Arc::new(InMemory::new());
        let writer = ParquetWriter::new(store.clone(), Default::default(), "mem://".into(), Codec::None);
        (store, writer)
    }

    async fn object_count(store: &InMemory) -> usize {
        store.list(None).try_collect::<Vec<_>>().await.unwrap().len()
    }

    #[tokio::test]
    async fn flushes_on_batch_size_and_on_close() {
        let (store, writer) = memory_writer();
        let (tx, rx) = mpsc::channel(16);
        let handle = tokio::spawn(run_writer(writer, rx, 2, Duration::from_secs(3600)));

        for n in 0..5 {
            tx.send(rec(n)).await.unwrap();
        }
        drop(tx);
        handle.await.unwrap().unwrap();

        // 2 + 2 by batch size, 1 on close
        assert_eq!(object_count(&store).await, 3);
    }

    #[tokio::test]
    async fn flushes_on_interval() {
        let (store, writer) = memory_writer();
        let (tx, rx) = mpsc::channel(16);
        let handle = tokio::spawn(run_writer(writer, rx, 1000, Duration::from_millis(100)));

        tx.send(rec(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        // let the spawned blocking write finish
        tokio::task::yield_now().await;
        drop(tx);
        handle.await.unwrap().unwrap();

        assert_eq!(object_count(&store).await, 1);
    }
}
