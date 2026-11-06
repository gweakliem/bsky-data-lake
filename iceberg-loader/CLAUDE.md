# iceberg-loader

A separate uv project; run its commands from this directory:

```bash
uv run pytest                                   # starts a local Sail server; no AWS needed
uv run bsky-iceberg-loader --once               # one SQS batch against real AWS
```

It long-polls the SQS queue from `infra/` of S3 ObjectCreated notifications and appends the referenced files to the Iceberg table `glue.bsky_raw.jetstream`, partitioned by identity columns `day`/`hour` (UTC of `received_at`), via PySpark over Spark Connect to Sail.
- **Delivery:** each commit's messages are deleted right after it succeeds. Redelivered files are skipped by checking `source_file` in the table. On SIGTERM the loader finishes the current commit and releases the batch's remaining messages (visibility 0), so a deploy waits at most one commit.
- **Commit size:** each commit is repartitioned by `day`/`hour`, so it writes one data file per hour partition. A batch is split into commits of at most `--max-load-mb` (128) of source files, by the sizes in the notifications, because Sail's write path peaks at about 7× the compressed input (measured: 313 MB in, 2.3 GB RSS). Raise the pod's memory limit before raising the cap.
- **Concurrent writers:** Glue's compactor commits to the same table, so a loader commit can fail with `conflict: Cannot commit catalog-managed table`. The consumer retries it after 5, 15 and 45 s, waiting for Sail's catalog cache to see the new metadata (Sail has no `REFRESH TABLE`), then falls back to the visibility timeout.
- **Notification filter:** the S3 notification filters on the ingestor's prefix and `.parquet`. Iceberg tables live in the separate warehouse bucket, so the loader's own writes never trigger it. Keep it that way.
- **Sail 0.7.3:** 0.7.2 wrote manifest lists that Athena can't read (no `format-version`/`snapshot-id` header, partition summary record named `field_summary`), so every snapshot the loader committed failed in Athena with `GENERIC_INTERNAL_ERROR` while Glue compactor snapshots worked (2026-11-05). `test_manifest_list_is_readable_by_athena` guards it; check it, and query bronze in Athena, when changing the Sail version.
- **Sail constraints:** no `input_file_name()` (files are tagged with `F.lit`), Glue tables need an explicit `LOCATION`, and no streaming.
- **Session idle timeout:** Sail removes a session after `spark.session_timeout_secs` (default 900) without requests, and the loader holds a single session. The embedded server defaults `SAIL_SPARK__SESSION_TIMEOUT_SECS` to a year. With `--spark-remote`, the remote server's setting applies.
- **Tests:** they use Sail's in-memory catalog with `file://` table locations.
