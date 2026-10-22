# iceberg-loader

A separate uv project; run its commands from this directory:

```bash
uv run pytest                                   # starts a local Sail server; no AWS needed
uv run bsky-iceberg-loader --once               # one SQS batch against real AWS
```

It long-polls the SQS queue from `infra/` of S3 ObjectCreated notifications and appends the referenced files to the Iceberg table `glue.bsky_raw.jetstream`, partitioned by identity columns `day`/`hour` (UTC of `received_at`), via PySpark over Spark Connect to Sail.
- **Delivery:** messages are deleted only after the Iceberg commit. Redelivered files are skipped by checking `source_file` in the table.
- **Notification filter:** the S3 notification filters on the ingestor's prefix and `.parquet`. Iceberg tables live in the separate warehouse bucket, so the loader's own writes never trigger it. Keep it that way.
- **Sail constraints:** no `input_file_name()` (files are tagged with `F.lit`), Glue tables need an explicit `LOCATION`, and no streaming.
- **Tests:** they use Sail's in-memory catalog with `file://` table locations.
