# silver

A separate uv project; run its commands from this directory:

```bash
uv run pytest                     # local Sail server, in-memory catalog; no AWS needed
uv run bsky-silver --once         # one window against real AWS (export credentials first, as in the root CLAUDE.md)
uv run bsky-silver                # windows until caught up
```

It reads new rows of the bronze table `glue.bsky_raw.jetstream` and appends them to one Iceberg table per record type in `glue.bsky_silver`: posts, likes, reposts, follows, blocks, profiles, identities, accounts, deletes and rejects, partitioned by `day` (UTC date of `received_at`, as in bronze). Run state is in `silver_runs`.

- **Windows:** a run covers bronze `ingested_at` after the last run's `bronze_through`, at most `--max-rows` (500k) rows in whole loader commits, and stops `--settle-minutes` (5) short of now. Rows, not time: the Nov 2 backlog upload put 54M rows in 6 hours of `ingested_at`. Peak memory is about 1 GB + 2 GB per million rows (measured 2.1 GB at 560k, 4.2 GB at 1.66M). The loader takes `ingested_at` before its commit lands, so a row stamped just before the cutoff can still be committing.
- **One parse:** `normalize.py` (pure Python, tested without Spark) maps each payload to a table. `job.py` runs it once per row through `mapInArrow` into a local staging Parquet directory, then appends each table from there. Sail's `cache()` doesn't materialize (measured: the UDF reran on every action), so the staging write is what keeps it to one parse.
- **Idempotent appends:** every row has an `event_id`: `did/collection/rkey#rev` for commits, the payload's sha256 for rejects. New rows are anti-joined against existing ones in the same days (plus the day before) and `time_us` range. That covers the ingestor's restart overlap (same `time_us`, later `received_at`) and a window rerun after a crash before the `silver_runs` row. Sail does support `MERGE INTO` and `DELETE FROM` on Iceberg; append + anti-join keeps the scan bounded and explicit.
- **Rejects, not failures:** malformed structure (bad JSON, missing `did`/`time_us`/commit fields, wrong types, likes without `subject.uri`) goes to `rejects` with the payload and a reason. An unparseable `createdAt` is client data, so it becomes null rather than a reject.
- **Ignored:** collections without a table (threadgate, postgate, listitem and about 1,000 non-`app.bsky` ones) and unknown kinds are counted per collection in the run's `counts`, not stored. Deletes are kept only for collections that have a table.
- **Sail 0.7.3:** 0.7.2 can't create Glue tables with array columns (colliding field ids in the CreateTable request; the in-memory catalog doesn't check), so silver pins 0.7.3 while the loader stays on 0.7.2. Probe Glue itself, not only the tests, when changing the Sail version.
- **Time zones:** Iceberg timestamps must be UTC, so the job sets the session zone. `collect()` returns naive datetimes in the Python process's local zone (not the session's); `job._utc` converts them.
- **Survey and fixtures:** `scripts/` holds the S1 survey; `tests/fixtures/jetstream.jsonl` is its sample (see `docs/plans/notes/bronze-survey.md`).
