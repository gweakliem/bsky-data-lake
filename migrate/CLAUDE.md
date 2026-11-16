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
  - **Resume:** each partition's last commit carries `bsky-migrate.partition` in its snapshot summary. A rerun skips marked partitions, and deletes then redoes a partition that has rows but no mark (a crash mid-partition).
  - **Bronze is copied in full, as it stands when the plan is written,** right after the old silver's last run. Silver's position is a bronze `ingested_at`, and the two accounts' loaders stamp the same event minutes apart. Cutting bronze at T0 would lose the events between the copied silver's position and the new loader's. With everything copied, the new silver reads both copies of each event after its position, and its anti-join on `event_id` keeps one. Bronze then holds the T0-to-freeze day twice. `--bronze-before` remains for a cut-off by `received_at`, unused for M5.
- **Objects (`objects.py`)** under `lance/` and `reports/inputs/` are copied server side (`s3.copy`: CopyObject, or UploadPartCopy for large objects), skipping objects already at the destination with the same size. **Lance datasets survive a plain copy:** `lance-test/` copied this way opened in the new bucket with the same rows, version, indexes and query results (2026-11-16).
- **Verify** compares row counts per partition from both tables' metadata. With a cut-off (`before`), a destination partition may hold fewer rows or be absent, never more.
- **Each command ends with one `run {json}` line.** `verify` exits 1 on a mismatch.
