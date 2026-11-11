# Data pipeline plan: silver layer, classification, embeddings

## Goal

Turn the raw Jetstream events in `glue.bsky_raw.jetstream` (bronze) into:

1. **Silver tables:** flattened, deduplicated records per event type, plus an edge table for threads, quotes and reposts.
2. **Silver support tables**, produced by two independent streams that read silver `posts`:
   - **Classification** with Jev: spam, adult content, AI-generated, broad topic.
   - **Embeddings** for search.

The two support streams do not depend on each other. They meet only at query time, where search uses the classifications as filters.

Out of scope for now: topic clustering, the thread explorer UI, sentiment, and self-hosting vLLM-SR `d3`. `d3` serves the same `/v1/systemone` API, so the classifier should only need a different base URL if it is adopted later.

## Layout

```
glue.bsky_raw.jetstream              bronze (exists)
        │  silver job (Sail)
        ▼
glue.bsky_silver.{posts, likes, reposts, follows, blocks, profiles, identities, accounts, deletes, post_edges}
        │
        ├── classify job (Jev) ──► glue.bsky_silver.post_classifications
        └── embed job ───────────► glue.bsky_silver.post_embeddings ──► search index (rebuildable)
```

New code:

| Directory | Contents | Engine |
|---|---|---|
| `silver/` | uv project, the bronze → silver job | PySpark on Sail, as in `iceberg-loader/` |
| `enrich/` | uv project with two entry points, `bsky-classify` and `bsky-embed`, plus shared incremental-read and write code | pyiceberg (Glue catalog) + pyarrow |

**Decide (E2/C2):** the support jobs are I/O-bound (API calls) or GPU-bound, not shuffle-heavy. pyiceberg reading and appending to Glue tables avoids running a Sail server for them. The default is pyiceberg; switch to Sail only if pyiceberg can't read or append what's needed.

## Shared conventions

These apply to every silver and support table.

- **Locations:** all tables in the warehouse bucket under `iceberg/bsky_silver/<table>`, with an explicit `LOCATION` (Sail's Glue catalog has no warehouse setting). The Glue database `bsky_silver` is added in the data stack (D5 in the deployment plan, or earlier as a small infra change).
- **Incremental processing by watermark:**
  - Each output table carries the watermark column of its input: `bronze_ingested_at` in silver, `silver_processed_at` in support tables.
  - A run processes input rows with watermark in `(max watermark already in output, now - 5 min]`. The 5-minute lag avoids racing an in-flight commit.
  - No separate state store; the output table is the state. Runs must be safe to repeat: writes are append, and readers dedupe as described below.
- **Dedup key:** silver commit-derived records are keyed by `(did, collection, rkey)`. Keep the row with the highest `rev`, then the highest `time_us`. Jetstream cursor replay (P1) produces exact duplicates, so this is required, not optional.
- **Deletes:**
  - `commit.operation = delete` rows go to `deletes`. Current-state views exclude any record with a later delete.
  - Support tables are filtered through the same view, never served directly.
- **Versioning:** support tables carry `model` and `spec_version`. The spec version names the question set or the text-preparation scheme. A new version is backfilled alongside the old one, and views select the active version.
- **Partitioning:** `day` (date) on everything, derived from the event time (`received_at`) for silver and from the post's `day` for support tables, so they line up with `posts`.

## WP P1: ingestor cursor resume

**Status: done (2026-11-02).** `ParquetWriter::latest_time_us` reads the newest file's footer statistics. Flags: `--resume-margin` (5 s) and `--no-resume`. Results:
- A restart against `jetstream-test/` overlapped by exactly 5.0 s, with no gap.
- A `kill -9` with nothing written yet replayed everything from the last persisted file, at about 30k messages/s.
- Reconnects already used the in-memory cursor. A read idle timeout (`--idle-timeout`, 60 s) now also reconnects a connection that goes silent without erroring.

Prerequisite for unattended running and for D4.

- **Goal:** after a restart, the ingestor resumes from just before the last event it persisted, instead of from live.
- **Approach:**
  - On startup with no `--cursor`, list the newest object under the output prefix (date/hour partitions sort lexically, so list only the latest hour that exists).
  - Read the max `time_us` from that file's Parquet footer statistics.
  - Start from that value minus a safety margin, set by a `--resume-margin` flag (default 5 s).
  - A `--no-resume` flag keeps today's live behavior.
- **Keep intact:** the shutdown/flush chain and fail-fast behavior described in `CLAUDE.md`.
- **Also:** check how the ingestor handles a dropped WebSocket today. If it exits, keep that (Kubernetes restarts it and resume covers the gap). If it reconnects, it must reconnect with a cursor.
- **Acceptance:**
  - Unit test for finding the resume point against a local directory with several hours of files.
  - A manual run against `jetstream-test/` shows overlap, not a gap, across a restart.
  - `cargo clippy --all-targets` is clean.

## WP S1: bronze survey and fixtures

**Status: done (2026-11-02).** Numbers in `notes/bronze-survey.md`; 184 fixtures in `silver/tests/fixtures/jetstream.jsonl`; scripts in `silver/scripts/`. About 3.36M posts/day. It adds `app.bsky.embed.gallery` (0.3% of posts) to S2's embed types; see the note's implications section.

- **Goal:** ground the silver schema in real data.
- **Tasks:**
  - Query bronze (Sail, or DuckDB over the table) for the mix of `kind`, `commit.collection` and `commit.operation`.
  - For `app.bsky.feed.post`, measure how often each optional field appears: `reply`, each `embed.$type` (`images`, `external`, `record`, `recordWithMedia`, `video`), `facets` features (`#mention`, `#link`, `#tag`), `langs` and self-`labels`.
  - Report posts per day. This drives the Jev cost estimate in C2.
  - Extract ~200 representative raw payloads, covering every collection and edge case, as JSON-lines test fixtures in `silver/tests/fixtures/`.
  - Remove anything sensitive from fixtures. They are public Bluesky data, but prefer posts without personal details.
- **Deliverable:** `docs/plans/notes/bronze-survey.md` with the numbers, plus the fixtures.
- **Acceptance:** fixtures cover every collection with a share above 0.1% plus identity/account events, and the survey numbers are recorded.

## WP S2: silver job

**Status: done (2026-11-02).** `silver/` (see its CLAUDE.md); 215 tests over the S1 fixtures; the `bsky_silver` Glue database is in the data stack. Additions from S1: `app.bsky.embed.gallery` alts, an `event_id` column on every table for dedup, `operation` on profiles, and `seq` on identities and accounts.
- **Against AWS (from a laptop):** four windows, 3.0M bronze rows, every table's row count equal to its distinct `event_id`s and to the run log, no rejects. A full-rate window (1.7M rows, about an hour) took 191 s, 115 s of it the parse/staging pass, with bronze read over the internet. The remaining backlog (about 80M rows) is left for the in-cluster job in D5.
- **Sail 0.7.3, not 0.7.2:** 0.7.2's Glue catalog sends a CreateTable request whose array element field ids collide with top-level ids, which Glue rejects (`Multiple entries with same key`); the in-memory catalog the tests use doesn't check. 0.7.3 fixes it. The loader stays on 0.7.2 (no array columns).

- **Depends on:** S1.
- **Goal:** `silver/` uv project with entry point `bsky-silver`. One run processes new bronze rows into all silver tables in one pass.
- **Event shapes (Jetstream):**
  - Every event has `did`, `time_us` and `kind`.
  - `kind=commit` has `commit.{rev, operation, collection, rkey, cid, record}`.
  - `kind=identity` has `identity.{did, handle, seq, time}`.
  - `kind=account` has `account.{active, status, did, seq, time}`.
  - Record URIs are `at://<did>/<collection>/<rkey>`.
- **Tables (minimum columns, all with `did`, `rkey`, `uri`, `rev`, `time_us`, `received_at`, `bronze_ingested_at`, `day`):**

  | Table | From | Extra columns |
  |---|---|---|
  | `posts` | `app.bsky.feed.post` create/update | `cid`, `created_at`, `text`, `langs`, `reply_root_uri`, `reply_parent_uri`, `embed_type`, `quote_uri`, `image_alts`, `external_uri`, `external_title`, `mention_dids`, `link_uris`, `tags`, `self_labels`, `operation` |
  | `likes` | `app.bsky.feed.like` | `subject_uri`, `created_at` |
  | `reposts` | `app.bsky.feed.repost` | `subject_uri`, `created_at` |
  | `follows` | `app.bsky.graph.follow` | `subject_did`, `created_at` |
  | `blocks` | `app.bsky.graph.block` | `subject_did`, `created_at` |
  | `profiles` | `app.bsky.actor.profile` | `display_name`, `description`, `self_labels` |
  | `identities` | `kind=identity` | `handle` |
  | `accounts` | `kind=account` | `active`, `status` |
  | `deletes` | any `operation=delete` | `collection` |

  Keep the record JSON for posts and profiles in a `record` column, so new fields can be extracted later without re-reading bronze.
- **Rules:**
  - Malformed records go to a `rejects` table (raw payload + reason). Never drop them silently, and never fail the run on them.
  - Unknown collections are ignored, but counted in the run log.
  - One Iceberg commit per output table per run.
- **Decided (2026-11-02):** Sail 0.7.2 supports `MERGE INTO` (and `DELETE FROM`) on Iceberg. It isn't used: runs append rows whose `event_id` isn't already in the batch's days and `time_us` range, which keeps them idempotent with a bounded scan.
- **Acceptance:**
  - `uv run pytest` runs the job over the S1 fixtures on Sail's in-memory catalog, with `file://` locations as in `iceberg-loader/tests`, and checks:
    - row counts per table;
    - dedup when the same events are processed twice;
    - delete handling;
    - reply/quote/facet extraction;
    - rejects.
  - A `--once` run against AWS processes the existing bronze data.

## WP S3: edges and current-state views

**Status: done (2026-11-03), not yet deployed or backfilled.** See `silver/CLAUDE.md`; 231 tests (16 new).
- **`post_edges`** is built in the silver job, in each window, from the same staged rows as the tables, and appended with the same `event_id` anti-join (the edge's `event_id` is its source's plus `|<type>`). Beyond the columns below it carries the source's `rev`, `time_us`, `received_at` and `bronze_ingested_at`, which the anti-join and `post_edges_current` need. Partitioned by `day`, like its source.
- **Only edges to posts:** likes, reposts and record embeds can also point at feed generators, lists and starter packs; those are left out.
- **Backfill:** windows recorded before edges existed (no `post_edges` key in `silver_runs.counts.appended`) get their edges after the job has caught up, one window at a time, read back from the silver tables by `bronze_ingested_at`. Finished windows go to `post_edges_backfill`, so a run killed at its deadline resumes there. It is the same memory as the per-window edge step, and needs no infra change.
- **Decided: views are portable SQL, not Glue views.** Glue/Athena views can only be read by Athena, while reporting will use DuckDB and the jobs Sail. `silver/sql/*.sql` are plain SELECTs with `{silver}` (namespace) and spec-version placeholders; `bsky_silver.views.statements` makes them `CREATE OR REPLACE TEMPORARY VIEW`s. The tests run each view on Sail and on DuckDB over the same rows, so no dialect differences remain to note (no `QUALIFY`, `* EXCEPT` or anti-join syntax).
- **Changed from the plan:** the support views join `posts_current` on `uri` only and add `current_cid` and `stale` (`cid <> current_cid`), one row per post, preferring the current `cid`. An inner join on `uri` and `cid` would drop an edited post's old classification instead of showing it as stale, which the acceptance asks for; consumers wanting fresh rows filter `NOT stale`. Also added `post_edges_current` (latest version of each source, no deleted sources: unliked likes, deleted replies), since the plan's delete rule applies to edges too.
- **"Later delete"** compares `rev` (TIDs, which sort as strings within a repo), matching the dedup rule; the dedup key `(did, collection, rkey)` is `uri`.
- **Deferred:** the views scan whole tables (the latest-version window function can't be pruned by `day`). C2/E2 should read new posts by watermark from `posts` and check them against `posts_current` only for the URIs they process, or materialize it, if a full scan is too slow. The embedding view filters on `spec_version` only, not `model`; E2 should fix the model inside a spec version.

- **Depends on:** S2.
- **`post_edges`:** `(src_uri, dst_uri, type, src_did, created_at, day)`, where `type` is one of:
  - `reply_parent` and `reply_root` (both from `posts`);
  - `quote` (from `posts.quote_uri`);
  - `repost` and `like` (from `reposts`/`likes`).

  It is built in the same run as S2, or as a step right after it.
- **Views** (Glue views if Sail/Athena can create them; otherwise documented SQL in `silver/sql/` that downstream jobs use):
  - `posts_current`: latest version per `uri`, with deleted posts excluded.
  - `post_classifications_current` and `post_embeddings_current`: joined to `posts_current` on `uri` and `cid`, for the active `spec_version`. Defined here, populated once C2/E2 exist.
- **Acceptance:** tests show that a deleted post disappears from `posts_current` and from both support views, and that an edited post (new `cid`) shows its old classification as stale.

## WP S4: control the growth of likes (backlog)

- **Status: not started (raised 2026-11-10).**
- **Why:** on 2026-11-10, `likes` was 50 GB of silver's 136 GB, the largest table, and `post_edges` (32 GB) also holds a `like` row for every like, so likes are stored twice. The lake keeps every Iceberg table indefinitely as the archive. Intelligent-Tiering (data stack, 2026-11-10) cuts the cost per GB but not the growth.
- **Look at first:**
  - Rows and bytes per like, and which columns take the space (`event_id`, `uri`, subject URI, `did`, `rev`, `cid`). Check whether the bulk is simply the volume.
  - Whether like edges in `post_edges` are used by anything (S3's views, reports), or whether `likes` alone is enough.
  - Compression: sort order within files (by subject URI, so its dictionary encoding pays off), codec and level.
- **Options to weigh:**
  - Aggregate: per-post like counts by hour or day (`post_like_counts`), keeping raw likes for a limited window or only in bronze.
  - Drop columns that can be derived, or the duplicate edge rows.
  - Keep everything, and accept the growth at tiered prices.
- **Constraint:** unlikes arrive as deletes, so any aggregate has to subtract them or be rebuilt from the raw rows.

## WP C1: classification spec and evaluation set

- **Depends on:** S2 (needs real posts).
- **Status: in progress (2026-11-10).** `enrich/` has the spec, a 300-post sample of 2026-11-09 and the labeling and eval tools; the hand labels are next. See `enrich/CLAUDE.md`.
- **Decided so far:**
  - **Topics:** `politics`, `social_issues`, `news` (current events no other topic covers), `crime_justice`, `sports`, `tech`, `science`, `entertainment`, `business`, `personal`. The no-match case is a companion Noul `topic_fits`, not an `other` option, per TypeSafe's guidance. Posts can fairly have two topics, so a label may name a second acceptable one, and the eval accepts either.
  - **Humor:** a fifth question, a Noul for joke, sarcasm or snark. Much of Bluesky is, especially in threads, and it's a tone across topics rather than a topic.
  - **Thread context:** replies carry the thread root's text as well as the parent's. Without it, a reply in a thread about a trial couldn't be placed at all.
  - **Short posts aren't classified:** no link, and text, alt texts and link title under 21 characters together (about 17% of new posts). The sample is drawn from eligible posts only, so the thresholds fit what C2 will classify.
  - **No handles in the state:** Jetstream's identity events mostly omit them, so silver has almost none. Display names (from `profiles`) and reply-parent and quoted text are included where silver has them.
  - **No Bluesky moderation labels:** labeler labels aren't in Jetstream, so the `adult` comparison column is the poster's self-labels.
  - **English only in v1's eval set:** it's hand-labeled. Other languages are unmeasured.
  - **Candidates for v2 (noted, not added):**
    - Topics `art` (art and creative work, now under `entertainment`) and `gaming` (video and board games, also under `entertainment`).
    - Game result shares (Wordle, Connections, Strands grids) aren't spam: the poster isn't promoting anything, and the virality is the game's share button. Label them not spam (topic `entertainment`, or none). They're templated, so code can recognize them by pattern (`game_share`) rather than Jev, and they could be skipped to save cost. Check in the eval whether Jev's `spam` leans yes on them ("repetitive automated content" is in its criteria).
  - **Cost:** about 1,700 input tokens a request with the eval's order check, about $70 per million posts at $0.042/Mtok. All eligible posts would be about $200 a day, so C2 classifies a sample (see C2).
- **Open: what classification is for (raised 2026-11-10, after hand-labeling ~220 posts).** Undecided; settle this before finishing C1's thresholds.
  - **What labeling showed:**
    - Topics overlap in practice (politics and social issues often both apply).
    - Some lines, such as art versus entertainment, are hard to draw.
    - Labeling mistakes get noticed too late. The labeler now has a back key.
    - Non-English posts (about 40%) can't be hand-labeled, so Jev's accuracy on them can't be measured.
  - **Two kinds of question, which may want different tools:**
    - **Per-post judgments that something acts on**, such as keeping spam or adult posts out of reports and search. Precision on each post matters, there are only a few questions, and Jev fits well.
    - **Aggregate trends**, such as which topics are rising. Errors on single posts average out, so a fixed taxonomy may be the wrong tool. Clustering post embeddings finds topics without anyone defining or labeling them, covers every language, and costs much less per post. Jev could name the clusters, or answer narrow questions about them.
  - **The option on the table:** shrink C1 to spam, adult and maybe humor, and move topics to embedding clusters. That would bring topic clustering into scope (see Goal), and C3's daily topic shares would come from clusters instead.
  - **If topics stay in Jev:**
    - Overlap argues for one Noul per topic instead of a single Choice, which is TypeSafe's guidance when several labels can apply. That costs one question per topic.
    - A category line only needs to exist where something downstream treats the two sides differently, and that use defines where it goes.
  - **Label noise:** re-label about 30 posts later without seeing the first answers. How often the two passes agree caps the accuracy the eval can show for Jev.
  - **First eval run (2026-11-10, 293 labeled posts, $0.02):**
    - **Adult** works: 90% precision and 78% recall at 0.5, against 73% for both using self-labels.
    - **Spam** has too few labeled positives (4) to measure. Jev flags 14 posts at 0.7, 6 of them from the adult-signal group, and the hand labels didn't count engagement bait. Review with `evaluate --disagreements spam`.
    - **Humor:** 13 labeled yes, against 79 Jev answers above 0.5. The definition is ambiguous.
    - **Topic:** 76% correct. The confusions are mostly among politics, social issues and news, where the hand labels were also inconsistent (commentary on a political issue was often labeled politics).
  - **Decided: drop `ai_generated` in the next spec version.** The labels judged images and video, which Jev doesn't see, so the 2 positives measure something else. On Bluesky, AI content tends to arrive as a link (a generated blog post, say) rather than as long AI-written post text, so a text question over the post has little to find.
- **Goal:** a versioned question set and a labeled evaluation set, before any bulk spend.
- **Questions (one System One request per post):**

  | id | Primitive | Meaning |
  |---|---|---|
  | `spam` | Noul | Unsolicited promotion, scams, engagement bait or repetitive automated content |
  | `adult` | Noul | Sexual or explicit content (text, or alt text describing images) |
  | `ai_generated` | Noul | Text appears machine-generated rather than written by a person |
  | `topic` | Choice | `politics`, `social_policy`, `news`, `sports`, `tech`, `science`, `entertainment`, `business`, `personal`, `other`. Exact list to be fixed in this WP. Must include a no-match option |

- **State per post:**
  - `post.text`, `post.langs` and `post.image_alts`;
  - `post.external` (link title and domain), plus the quoted post's text when available;
  - `author.handle` and `author.display_name`, which help spam detection;
  - for replies, `parent.text` when it's in silver.

  Use named JSON fields and reference them with backticked paths in the instructions, per TypeSafe's state guidance.
- **Read first:** TypeSafe's live docs (`https://docs.typesafe.ai/llms.txt`): primitives, state, confidence, Python SDK (`typesafe-sdk`, `AsyncTypeSafeClient`, `TYPESAFE_API_KEY`), and the hierarchical-classification and moderation-related cookbooks. Get the Jev model name from `GET /v1/models` or the docs; don't hard-code a guess.
- **Evaluation set:**
  - ~300 posts sampled from silver, stratified so spam/adult/likely-AI cases are present. Purely random sampling would yield almost none.
  - Hand-labeled, stored as JSON lines in `enrich/eval/` with only post URIs and labels. Fetch the text at evaluation time, since the repo is public.
  - Include Bluesky's own moderation and self-labels as a comparison column for `adult`.
- **Deliverables:**
  - `enrich/src/.../spec.py` defining `spec_version = "v1"` and the questions;
  - an eval script reporting per-question precision/recall at candidate thresholds and topic accuracy and confusion;
  - a short write-up of the chosen thresholds in `docs/plans/notes/classification-eval.md`.
- **Notes:**
  - `ai_generated` on single short posts is expected to be weak, with probabilities near 0.5. Report how its probabilities are distributed, and treat near-0.5 as "unknown", not "no".
  - An account-level version, aggregating over many posts, is a candidate follow-up, not part of v1.
- **Acceptance:** the eval runs end to end against the API, and the thresholds and their measured performance are recorded.

## WP C2: classification job

- **Depends on:** C1, S3.
- **Goal:** `bsky-classify` reads new rows from `posts_current` and writes `post_classifications`.
- **Columns:**
  - `uri`, `cid`, `day`, `model`, `spec_version`, `classified_at`, `silver_processed_at`;
  - `p_spam`, `p_adult`, `p_ai_generated`;
  - `topic`, `topic_confidence`, and `topic_probs` (map<string,double>).

  Store raw probabilities only. Thresholds live in views (C3).
- **Behavior:**
  - **Prefilter:** skip posts with no text and no alt text, or fewer than N characters (default 10, configurable). Record skip counts in the run log.
  - **Sampling and budget:**
    - `--sample-rate` (default 1.0) and `--max-posts` per run.
    - Estimated input tokens are tallied before calls are made. The run stops cleanly when `--max-input-tokens` is reached.
    - The C2 PR must include a cost estimate based on S1's posts per day.
  - **Concurrency:** async client with bounded concurrency (configurable). Retry with backoff on transient errors.
    - A post that keeps failing is written to `classification_failures` with the error. It never blocks the run.
    - A run fails only when systemic errors pass a threshold (auth failure, or an error rate above a set percentage).
  - **Commits:** write in chunks, committing to Iceberg every N thousand posts, so a crash loses at most one chunk. Rerunning is safe because of the watermark plus dedup on `(uri, cid, spec_version)`.
  - **Base URL:** configurable, so a `d3` endpoint can be swapped in later.
- **Acceptance:**
  - Tests use a fake System One client (no network) to cover state building, prefilter, budget stop, partial failure, chunked commits and rerun idempotency.
  - A real run over one day of posts at a small sample rate succeeds, with the logged cost matching the estimate.

## WP C3: classification views and report

- **Depends on:** C2.
- **Tasks:**
  - Add view(s) applying C1's thresholds: `is_spam`, `is_adult`, `ai_generated` as `likely`/`unknown`/`unlikely`, and `topic` with a confidence floor.
  - Write a small report (script or notebook) giving daily topic shares, spam/adult rates, and agreement with Bluesky's labels.
- **Acceptance:** changing a threshold changes the views without re-running inference.

## WP E1: embedding model selection

- **Depends on:** S2.
- **Status: in progress (2026-11-10).** `embed/` has the harness (see `embed/CLAUDE.md`) and a first run: 100k posts of 2026-11-09 in every language, 9 models plus BM25 and truncated variants, 48 queries, relevance judged by Jev over the pooled top 10s ($0.05), and a k-means clustering check, which the plan didn't originally include. First results: Qwen3-8B via OpenRouter scores best everywhere (nDCG@10 0.93; 0.92 truncated to 1,024 dims; about $1.30 a day for all eligible posts). nomic-embed-text-v2-moe is the strongest local model (0.87, 77 posts/s on 4 CPU threads) and barely clusters by language, while harrier and e5 group many non-English posts by language. Next: hand-judge 150 pairs to check Jev, then the write-up and the choice.
- **Goal:** pick a model and a text-preparation scheme using measured search quality, not defaults.
- **Candidates:**
  - small open multilingual embedding models that run well on CPU or a single small GPU;
  - at least one hosted embedding API for comparison.

  Bluesky has a lot of non-English content, so multilingual support is a requirement.
- **Text to embed (`text_spec` v1):** post text, plus image alt texts, plus the external link title. Never include reply parent text, which pulls vectors toward the parent's topic.
- **Open: replies that only make sense in context (raised 2026-11-10, while hand-judging).** E1 judges relevance from the post's own text, which is all the models embed, so a reply like "exactly, he should be impeached" counts as not relevant to an impeachment query. That is right for comparing models, but search users would expect to find it. Candidates for `text_spec` v2 or E3: add the thread root's text (C1 found it often needed to place a reply), or index threads rather than single posts. The hand sample's `u` answers (relevance needed the thread or link) show how often this happens.
- **Evaluation:**
  - ~50 search queries written by hand, with relevant posts judged from silver.
  - Include non-English queries and judged posts. If every query is English, the model's quality on the roughly 40% of posts that aren't English goes unmeasured, which is the same gap C1's eval has. Judging them needs translation or a reader of the language.
  - Measure recall@10 and nDCG@10 per model, plus throughput (posts/sec) on the target hardware, and storage per vector.
- **Deliverable:** `docs/plans/notes/embedding-eval.md` with the choice, its dimension, and the hardware it needs. That hardware sizes the node pool in D5.
- **Acceptance:** the chosen model and `text_spec` are recorded with their measurements.

## WP E2: embedding job

- **Depends on:** E1, S3.
- **Goal:** `bsky-embed` reads new rows from `posts_current` and writes `post_embeddings`.
- **Columns:** `uri`, `cid`, `day`, `model`, `spec_version`, `embedded_at`, `silver_processed_at`, and `vector` (fixed-size list<float32>).
- **Behavior:** the same prefilter, chunked commits, watermark and idempotency rules as C2. Batch inference, on GPU when available, falling back to CPU.
- **Acceptance:** tests use a tiny or fake model to check batching, idempotency and the vector shape. A real run over one day of posts succeeds.

## WP E3: search index and query API

- **Depends on:** E2, and C2 for filters (the index can ship without them and add filters later).
- **Goal:** hybrid search over posts.
- **Index:**
  - LanceDB on S3 (default), holding the vectors plus a full-text index over `text`, with filter columns `day`, `langs`, `topic`, `is_spam` and `is_adult` from C3.
  - Built incrementally from `post_embeddings_current` joined to `posts_current` and `post_classifications_current`.
  - It is a derived copy: a full rebuild must be a supported command.
  - **Decide:** LanceDB or OpenSearch. LanceDB is the default because it fits the lake and has no server. Choose OpenSearch only if concurrent query load or keyword-ranking needs call for it.
- **Query:**
  - Combine BM25 and vector scores (reciprocal rank fusion).
  - Exclude spam and adult content by default.
  - Add a recency boost from `created_at`.
  - Optionally have Jev rerank the top 50 (a follow-up, behind a flag).
- **Interface:** a small HTTP service (FastAPI) plus a CLI. A UI is out of scope.
- **Deletes:** each index update removes `uri`s that appear in `deletes` since the last build.
- **Acceptance:**
  - On the E1 query set, hybrid search matches or beats vector-only search.
  - A post deleted in silver disappears from results after the next index update.
