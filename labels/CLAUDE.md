# labels

Bluesky's moderation labels into silver: `bsky_silver.moderation_labels`, appended by a daily CronJob from mod.bsky.app's public label stream (`com.atproto.label.subscribeLabels`). Our firehose data only has authors' self-labels; this is what Bluesky's own moderation says (porn, sexual, nudity, spam, !takedown, ...), on posts and on whole accounts. A uv project that depends on `../enrich` (editable) for the Glue catalog, table creation and retried appends. Run commands from this directory.

```bash
uv sync
uv run pytest                                  # no AWS or network needed
eval "$(aws configure export-credentials --profile <profile> --format env)"; export AWS_REGION=us-west-2
uv run bsky-labels --dry-run                   # what a run would replay (also needs BSKY_WAREHOUSE_BUCKET)
uv run bsky-labels-backfill <replay dir>       # load a replay on disk (the cluster prototype's data/cluster/modlabels/)
docker buildx build --build-context enrich=../enrich -t bsky-labels .   # the job's image
```

- **Stream (`stream.py`):** a subscription with `cursor=N` replays every event after N, then stays open for live ones; frames are two DAG-CBOR objects (header, body). One connection gets about 2,000 labels/s. `head()` is the first event of a cursor-less subscription (None if nothing arrives in 2 min).
- **Job (`job.py`, `bsky-labels`):** reads the table's max `seq` from file stats (a scan only if a file lacks them), replays (max seq, head] and appends `--chunk` (100k) labels per commit, so a cut-short run resumes. Ends with one `run {json}` log line, which the data stack's stalled alarm counts. About 55k labels a day (2026-11).
- **Table:** one row per label event, as sent: `seq`, `src`, `uri` (a post's at:// URI, or a bare DID for an account label; a profile record's URI is also an account), `cid`, `val`, `neg` (a retraction), `cts`, `exp` (expiry, mostly null), `day` (partition: the day of `cts`, or of ingest if `cts` is unreadable) and `ingested_at`. Signatures are dropped. A label's current state is its latest event per (`src`, `uri`, `val`), unless that event retracts it or `exp` has passed.
- **Backfill (`backfill.py`):** the full history is 43M labels since 2023-04 (2026-11-15), about 6 h to replay on one connection, so it's loaded from a replay already on disk instead: labels after the table's max seq, in seq order, 2M per commit. It refuses a replay that starts well after the table's max seq (a gap). Run it before unsuspending the CronJob, or the job's first run replays the whole history itself.
