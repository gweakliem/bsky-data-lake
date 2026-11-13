# search

The search index over post embeddings and its query API (WP E3). A uv project that depends on `../embed` (embedding spec v1 and the OpenRouter client) and `../enrich` (incremental silver reads), both editable. Run commands from this directory.

```bash
uv sync
uv run pytest                                  # no AWS or API access needed
eval "$(aws configure export-credentials --profile <profile> --format env)"; export AWS_REGION=us-west-2
uv run bsky-index --dry-run                    # what a run would index (needs BSKY_WAREHOUSE_BUCKET)
uv run bsky-search query "midterm results" -k 5 --lang en   # hybrid; --mode vector|text
uv run bsky-search serve                       # the API on :8000 (GET /search, /healthz)
uv run python -m bsky_search.evaluate          # acceptance eval on E1's corpus; then judge + report in ../embed
docker buildx build --build-context embed=../embed --build-context enrich=../enrich -t bsky-search .
```

Set `BSKY_LANCE_URI` (for example `s3://<warehouse>/lance-test`) to work against a scratch dataset instead of `s3://<warehouse>/lance`. `OPENROUTER_API_KEY` comes from the repo's `.env`.

- **LanceDB is a library, not a server.** The index is a Lance dataset on S3 (`store.py`). The index job writes it, and the API opens it read-only, picking up new versions within a minute (`read_consistency_interval`). One table per embedding spec version (`posts_v1`), plus the job's state table `index_runs_v1`.
- **Window:** only posts whose `day` is in the last 30 days (`store.WINDOW_DAYS`). `post_embeddings` stays the full archive, so a longer window is a rebuild.
- **Index job (`index.py`, `bsky-index`):**
  - **Watermark:** the max `through` in `index_runs_v1`, not the table's own rows, since deleted and expired rows leave no trace there. Each run reads `post_embeddings` (spec v1) from that watermark up to `post_embeddings`' own watermark, in spans of silver windows of about an hour, and joins them to `posts` for text and filter columns. It drops posts deleted in the same span, and keeps one row per `uri`.
  - **Per span, in order:** delete the rows of posts the span updated (a new `cid`), append, delete posts named in the span's `deletes`, then record the span.
  - **Crash recovery:** a run that died before recording leaves rows newer than the state, and the next run deletes them first.
  - **Delete lag:** deletes are applied only up to `post_embeddings`' watermark, so they wait for embed.
  - **Single writer:** the CronJob forbids overlapping runs. There's no commit coordination beyond that.
  - **Upkeep after the spans:** expire old days, create missing indexes, fold new rows into existing ones and compact (`optimize`, which also drops versions over a day old). The indexes are BTree on `uri`, `bronze_ingested_at` and `day` (Bitmap can't index dates), LabelList on `langs`, full-text on `text`, and IVF-PQ on `vector`.
  - **Span size:** a span with more than 200k embeddings (`--max-span-rows`) is split by time, since an ingest catch-up can put hours of posts into one hour of `bronze_ingested_at`. Rows sharing a timestamp stay together.
  - **`--rebuild`** (initial build, or a full reload) drops the index and its state, reads every post delete (as sorted 64-bit uri hashes) and each edit's newest version up front, and leaves those rows out before appending. It does no table deletes, which scan the whole `uri` column until it's indexed. Run it with `--no-maintain`, then `--reindex`. If it crashes, run it again.
  - **IVF-PQ** starts once there are 100k rows: `sqrt(rows)` partitions, 64 sub-vectors (64 bytes a vector in memory), trained on 64 rows per partition. It isn't retrained as the window moves. `--reindex` retrains it.
- **Query (`query.py`):**
  - The query is embedded with `spec.QUERY_PROMPT`.
  - **Vector search** probes about 6% of the partitions and re-ranks the top 10×k on full vectors (refine factor 10). Without that re-ranking, IVF-PQ lost 0.03 nDCG overall and 0.20 on cross-language queries.
  - **Hybrid** is our own weighted RRF (0.9 vector, 0.1 BM25, 50 candidates each), not LanceDB's built-in hybrid, which scored 0.79 against vector-only's 0.91. Numbers are in `docs/plans/notes/search-eval.md`.
  - **Filters:** language (`array_has_any` on the LabelList index), `since` (a day), and an optional recency half-life. Language tags are validated, since they go into the SQL filter.
- **Full-text tokenizer:** `icu`, without stemming or stop words, because posts come in every language. It splits Japanese and Chinese into words, which plain whitespace splitting can't.
- **Tests** use enrich's fake silver catalog (`bsky_enrich.testing`) plus a `post_embeddings` table and a local Lance directory, and a fake embedder that maps "seed N" to post N's vector.
- **Gotchas:**
  - Arrow joins can't carry list columns, so `span_rows` joins on keys and row numbers, then takes.
  - LanceDB's `create_scalar_index`, `create_fts_index` and `table_names` are deprecated. Use `create_index(config=...)` and `list_tables().tables`.
  - Writing or optimizing a large index from a laptop is slow (it's network-bound). Build the real index in the cluster, where S3 traffic goes through the VPC's gateway endpoint.
