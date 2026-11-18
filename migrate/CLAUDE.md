# migrate

One-off: copy the lake into the member account (WP M5 in `docs/plans/account-management.md`, phase E). A uv project that depends on `../enrich` (editable) for the Glue catalog. Delete it after phase F.

```bash
uv sync
uv run pytest                                  # no AWS needed: local SQLite catalogs and a fake S3
# At the freeze, on the laptop with the OLD account's credentials:
eval "$(aws configure export-credentials --profile <old profile> --format env)"; export AWS_REGION=us-west-2
uv run bsky-migrate plan --dest-warehouse <new warehouse bucket> --exclude bsky_silver.post_edges_backfill \
  > ~/.config/bsky-data-lake/migration-plan.json
docker buildx build --build-context enrich=../enrich -t bsky-migrate .
```

Then, in the new account (apps stack with `migrationPlanFile` / `BSKY_MIGRATION_PLAN_FILE` and `migrationImageTag` set), start each step by hand and let it finish:

```bash
kubectl --context bsky-main -n bsky create job --from=cronjob/migrate-tables  migrate-tables-$(date +%s)
kubectl --context bsky-main -n bsky create job --from=cronjob/migrate-objects migrate-objects-$(date +%s)
kubectl --context bsky-main -n bsky create job --from=cronjob/migrate-verify  migrate-verify-$(date +%s)
```

- **The plan file names the old buckets and metadata files,** so it lives in `~/.config/bsky-data-lake/`, never in the repo. The apps stack mounts it as a ConfigMap.
- **Tables (`tables.py`) are re-written, not copied:** Iceberg metadata names every file by its absolute `s3://` path, and the new bucket's name differs.
  - The source is opened from its metadata file (`StaticTable`), so the job needs only S3 read access to the old warehouse (the old data stack's `migrationReadPolicy`), not the old Glue catalog.
  - The destination gets the same schema, identity partition spec and properties, at `<dest_iceberg>/<namespace>/<table>`.
  - Each partition is scanned and appended in commits of `--rows-per-commit` (2M). Only identity partitions are supported, which is all the lake has. On 2026-11-16 no table had delete files, one spec and one schema each.
  - **Memory:** `read` hands pyiceberg one data file at a time. Its own batch reader submits every file of a scan to the thread pool at once, and finished files pile up ahead of the writer: likes' 44M-row 2026-11-04 (twice a normal day) went past the pod's 16 Gi that way, OOM after OOM (2026-11-18).
  - **Commit size is in rows, not bytes:** at the default 2M rows a commit of post_embeddings (a vector per row) is about 4 GB of Arrow before pyiceberg's copies, and the pod hit 16 Gi on 2026-11-02's day. M5's run finished with `--rows-per-commit 250000` (2.3-2.7 GB throughout). A byte cap in `_commits` would make the default safe.
  - **Resume:** each partition's last commit carries `bsky-migrate.partition` in its snapshot summary. A rerun skips marked partitions, and deletes then redoes a partition that has rows but no mark (a crash mid-partition).
  - **Except in bronze, which is live:** the new account's loader has appended to it since T0, so an unmarked partition holds the loader's rows. The copy never deletes there (`_live` in `cli.py`); a crash mid-partition leaves duplicates, which silver drops by `event_id`. Verify then allows a bronze partition more rows than the source, never fewer. On 2026-11-18 the first run, without this, deleted the new loader's rows from 2026-11-16 17:00 to 2026-11-17 11:00 (replaced by the old loader's copies of the same events, all before W, so silver was unaffected) before it was stopped.
  - **Bronze is copied in full, as it stands when the plan is written,** right after the old silver's last run. Silver's position is a bronze `ingested_at`, and the two accounts' loaders stamp the same event minutes apart. Cutting bronze at T0 would lose the events between the copied silver's position and the new loader's. With everything copied, the new silver reads both copies of each event after its position, and its anti-join on `event_id` keeps one. Bronze then holds the T0-to-freeze day twice. `--bronze-before` remains for a cut-off by `received_at`, unused for M5.
- **Objects (`objects.py`)** under `lance/` and `reports/inputs/` are copied server side (`s3.copy`: CopyObject, or UploadPartCopy for large objects), skipping objects already at the destination with the same size. **Lance datasets survive a plain copy:** `lance-test/` copied this way opened in the new bucket with the same rows, version, indexes and query results (2026-11-16).
- **Verify** compares row counts per partition from both tables' metadata. With a cut-off (`before`), a destination partition may hold fewer rows or be absent, never more.
- **Each command ends with one `run {json}` line.** `verify` exits 1 on a mismatch.
