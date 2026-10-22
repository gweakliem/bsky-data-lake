use std::path::Path as FsPath;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use url::Url;

/// A single message received from Jetstream.
#[derive(Debug, Clone)]
pub struct Record {
    /// When this process received the message.
    pub received_at: DateTime<Utc>,
    /// Jetstream's `time_us` field, if present; usable as a resume cursor.
    pub time_us: Option<i64>,
    /// The raw JSON message.
    pub payload: String,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum Codec {
    Zstd,
    Snappy,
    None,
}

impl From<Codec> for Compression {
    fn from(codec: Codec) -> Self {
        match codec {
            Codec::Zstd => Compression::ZSTD(ZstdLevel::default()),
            Codec::Snappy => Compression::SNAPPY,
            Codec::None => Compression::UNCOMPRESSED,
        }
    }
}

/// Writes batches of records as individual Parquet objects, partitioned by
/// date and hour: `<prefix>/date=YYYY-MM-DD/hour=HH/jetstream-<timestamp>-<id>.parquet`.
///
/// Each object is uploaded in a single put, so readers never observe a
/// partially written file (the local store writes to a staging file and
/// renames it into place).
pub struct ParquetWriter {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    /// Human-readable root of `prefix`, used for logging.
    base: String,
    schema: SchemaRef,
    props: WriterProperties,
}

impl ParquetWriter {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path, base: String, codec: Codec) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "received_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("time_us", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let props = WriterProperties::builder()
            .set_compression(codec.into())
            .build();

        Self { store, prefix, base, schema, props }
    }

    /// Create a writer for `location`, which is either a local directory or
    /// an `s3://bucket/prefix` URL. S3 credentials and region are read from
    /// the standard `AWS_*` environment variables.
    pub fn from_location(location: &str, codec: Codec) -> Result<Self> {
        if location.starts_with("s3://") || location.starts_with("s3a://") {
            let url = Url::parse(location).with_context(|| format!("parsing {location}"))?;
            let store = AmazonS3Builder::from_env()
                .with_url(location)
                .build()
                .with_context(|| format!("configuring S3 for {location}"))?;
            let prefix = Path::from(url.path().trim_matches('/'));
            Ok(Self::new(
                Arc::new(store),
                prefix,
                location.trim_end_matches('/').to_string(),
                codec,
            ))
        } else {
            let dir = FsPath::new(location);
            std::fs::create_dir_all(dir)
                .with_context(|| format!("creating output directory {}", dir.display()))?;
            let dir = dir.canonicalize()?;
            let store = LocalFileSystem::new_with_prefix(&dir)?;
            Ok(Self::new(
                Arc::new(store),
                Path::default(),
                dir.display().to_string(),
                codec,
            ))
        }
    }

    /// Write `records` to Parquet and return each location written along
    /// with its row count.
    ///
    /// Records are partitioned by the hour of `received_at`, so a batch that
    /// spans an hour boundary is split into one object per hour.
    pub async fn write_batch(&self, mut records: Vec<Record>) -> Result<Vec<(String, usize)>> {
        anyhow::ensure!(!records.is_empty(), "refusing to write an empty batch");

        let mut locations = Vec::new();
        while !records.is_empty() {
            let hour = hour_partition(&records[0]);
            let split = records
                .iter()
                .position(|r| hour_partition(r) != hour)
                .unwrap_or(records.len());
            let rest = records.split_off(split);
            let count = records.len();
            locations.push((self.write_object(records).await?, count));
            records = rest;
        }
        Ok(locations)
    }

    /// Write `records`, which must all share an hour partition, to one object.
    async fn write_object(&self, records: Vec<Record>) -> Result<String> {
        let first = records[0].received_at;
        let key = self
            .prefix
            .clone()
            .join(format!("date={}", first.format("%Y-%m-%d")))
            .join(format!("hour={}", first.format("%H")))
            .join(format!(
                "jetstream-{}-{}.parquet",
                first.format("%Y%m%dT%H%M%S%.6fZ"),
                &uuid::Uuid::new_v4().simple().to_string()[..8],
            ));

        // Encoding is CPU-bound, so keep it off the async runtime.
        let (schema, props) = (self.schema.clone(), self.props.clone());
        let bytes = tokio::task::spawn_blocking(move || encode(schema, props, &records)).await??;

        self.store
            .put(&key, PutPayload::from_bytes(bytes))
            .await
            .with_context(|| format!("uploading {key}"))?;

        let relative = key.prefix_match(&self.prefix).map(|parts| parts.map(|p| p.as_ref().to_string()));
        Ok(match relative {
            Some(parts) => format!("{}/{}", self.base, parts.collect::<Vec<_>>().join("/")),
            None => key.to_string(),
        })
    }
}

/// The (date, hour) partition a record belongs to.
fn hour_partition(record: &Record) -> (chrono::NaiveDate, u32) {
    use chrono::Timelike;
    (record.received_at.date_naive(), record.received_at.hour())
}

fn encode(schema: SchemaRef, props: WriterProperties, records: &[Record]) -> Result<Bytes> {
    let received_at = TimestampMicrosecondArray::from_iter_values(
        records.iter().map(|r| r.received_at.timestamp_micros()),
    )
    .with_timezone("UTC");
    let time_us = Int64Array::from_iter(records.iter().map(|r| r.time_us));
    let payload = StringArray::from_iter_values(records.iter().map(|r| r.payload.as_str()));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(received_at), Arc::new(time_us), Arc::new(payload)],
    )?;

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(Bytes::from(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use futures_util::TryStreamExt;
    use object_store::memory::InMemory;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn record(payload: &str, time_us: Option<i64>) -> Record {
        Record { received_at: Utc::now(), time_us, payload: payload.to_string() }
    }

    fn memory_writer(prefix: &str) -> (Arc<InMemory>, ParquetWriter) {
        let store = Arc::new(InMemory::new());
        let writer =
            ParquetWriter::new(store.clone(), Path::from(prefix), "mem://".into(), Codec::Zstd);
        (store, writer)
    }

    #[tokio::test]
    async fn writes_readable_parquet() {
        let (store, writer) = memory_writer("raw/jetstream");
        let records = vec![record(r#"{"a":1}"#, Some(42)), record(r#"{"b":2}"#, None)];

        let locations = writer.write_batch(records).await.unwrap();
        assert_eq!(locations.len(), 1);
        let (location, count) = &locations[0];
        assert_eq!(*count, 2);
        assert!(location.starts_with("mem:///date="), "{location}");
        assert!(location.ends_with(".parquet"));

        let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert_eq!(objects.len(), 1);
        let key = objects[0].location.as_ref();
        assert!(key.starts_with("raw/jetstream/date="), "{key}");
        assert!(key.contains("/hour="), "{key}");

        let bytes = store.get(&objects[0].location).await.unwrap().bytes().await.unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap().build().unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let payload = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(payload.value(0), r#"{"a":1}"#);
        assert_eq!(payload.value(1), r#"{"b":2}"#);
        let time_us = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(time_us.value(0), 42);
        assert!(time_us.is_null(1));
    }

    #[tokio::test]
    async fn does_not_overwrite_existing_objects() {
        let (store, writer) = memory_writer("");
        let rec = record("{}", None);

        // Same received_at, so only the random suffix distinguishes them.
        writer.write_batch(vec![rec.clone()]).await.unwrap();
        writer.write_batch(vec![rec]).await.unwrap();
        let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert_eq!(objects.len(), 2);
    }

    #[tokio::test]
    async fn splits_batch_at_hour_boundaries() {
        let (store, writer) = memory_writer("p");
        let at = |s: &str| Record {
            received_at: DateTime::parse_from_rfc3339(s).unwrap().into(),
            time_us: None,
            payload: "{}".into(),
        };
        let records = vec![
            at("2026-10-22T10:59:58Z"),
            at("2026-10-22T10:59:59Z"),
            at("2026-10-22T11:00:00Z"),
            at("2026-10-22T23:59:59Z"),
            at("2026-10-23T00:00:01Z"),
        ];

        let locations = writer.write_batch(records).await.unwrap();
        let mut reported: Vec<usize> = locations.iter().map(|(_, n)| *n).collect();
        reported.sort();
        assert_eq!(reported, vec![1, 1, 1, 2]);

        let mut counts = Vec::new();
        for meta in store.list(None).try_collect::<Vec<_>>().await.unwrap() {
            let bytes = store.get(&meta.location).await.unwrap().bytes().await.unwrap();
            let rows: usize = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .unwrap()
                .build()
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum();
            let partition = meta.location.as_ref().rsplit_once('/').unwrap().0.to_string();
            counts.push((partition, rows));
        }
        counts.sort();
        assert_eq!(
            counts,
            vec![
                ("p/date=2026-10-22/hour=10".to_string(), 2),
                ("p/date=2026-10-22/hour=11".to_string(), 1),
                ("p/date=2026-10-22/hour=23".to_string(), 1),
                ("p/date=2026-10-23/hour=00".to_string(), 1),
            ]
        );
    }

    #[tokio::test]
    async fn rejects_empty_batch() {
        let (_, writer) = memory_writer("");
        assert!(writer.write_batch(vec![]).await.is_err());
    }

    #[tokio::test]
    async fn writes_to_local_directory() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("nested/out");
        let writer = ParquetWriter::from_location(out.to_str().unwrap(), Codec::None).unwrap();

        let locations = writer.write_batch(vec![record("{}", None)]).await.unwrap();
        let path = std::path::PathBuf::from(&locations[0].0);
        assert!(path.is_file(), "{}", locations[0].0);
        assert!(path.starts_with(out.canonicalize().unwrap()));
        let hour_dir = path.parent().unwrap();
        let date_dir = hour_dir.parent().unwrap();
        assert!(hour_dir.file_name().unwrap().to_str().unwrap().starts_with("hour="));
        assert!(date_dir.file_name().unwrap().to_str().unwrap().starts_with("date="));
    }

    #[test]
    fn parses_s3_location() {
        let writer = ParquetWriter::from_location("s3://my-bucket/raw/jetstream/", Codec::Zstd)
            .unwrap();
        assert_eq!(writer.prefix.as_ref(), "raw/jetstream");
        assert_eq!(writer.base, "s3://my-bucket/raw/jetstream");
    }
}
