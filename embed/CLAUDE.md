# embed

Embedding model selection (WP E1) and the embedding job (E2, in progress). A uv project that depends on `../enrich` (editable) for the lake connection, the eligibility rule and the terminal key reader. Run commands from this directory. The E1 harness's heavy dependencies (torch, sentence-transformers, scikit-learn, bm25s) are in the `eval` group, which `dev` includes, so `uv sync --no-dev` (the job's image) leaves them out.

```bash
uv sync
uv run pytest                                  # no AWS or API access needed
eval "$(aws configure export-credentials --profile <profile> --format env)"; export AWS_REGION=us-west-2
uv run python -m bsky_embed.corpus             # draw the corpus once (about 2 min)
uv run python -m bsky_embed.embed              # embed it with every model not yet cached (local ones take minutes each on MPS)
uv run python -m bsky_embed.embed --bench      # CPU throughput, 4 threads
uv run python -m bsky_embed.search             # top 10 per query per system, plus BM25
uv run python -m bsky_embed.judge              # Jev judges pooled pairs not yet judged
uv run python -m bsky_embed.judge --hand       # hand-judge 150 English pairs, in a real terminal
uv run python -m bsky_embed.report             # the numbers; --clusters MODEL to read one model's clusters
uv run bsky-embed --dry-run                    # E2: what a run would read and embed (also needs BSKY_WAREHOUSE_BUCKET)
```

`OPENROUTER_API_KEY` (hosted models) and `TYPESAFE_API_KEY` (Jev) come from the repo's `.env`.

- **Corpus:** 100,000 eligible posts of 2026-11-09 in every language, drawn by hash order with a fixed seed, deleted posts left out, plus the 300 C1 eval posts so C1's hand topics can score the clusters. Text is `text_spec` v1: post text, image alt texts and link title, one per line, never the reply parent. It lives in the git-ignored `eval/.cache/` with the vectors, query vectors, results and clusters.
- **Models (`models.py`):** local sentence-transformers models and hosted ones through OpenRouter's `/api/v1/embeddings`, each with the query and document prompts its card asks for. Posts are embedded once, as documents; that one vector serves search and clustering, as in production. Inputs are capped at 256 tokens. Vectors are stored L2-normalized as float16.
- **Queries (`eval/queries.jsonl`, committed):** 48, grouped by `kind`: English news, everyday, entertainment and commerce; `native` (Japanese, Spanish, Portuguese, German, French); and `cross` (an English query whose matches are mostly in another language).
- **Judging:** the pool is every system's top 10 per query. Jev judges each pair with one Noul (`judge.RELEVANT`), in any language, and `p >= 0.5` counts as relevant; the hand sample (English pairs only, spread over Jev's probabilities, shown without them) measures how far to trust that. `eval/judgments-jev.jsonl` and `eval/judgments-hand.jsonl` are committed with query ids, URIs and answers only. A pair is re-judged when its query's text changes. Recall is against the pool, so adding a system can lower the others' recall.
- **Clustering:** k-means, k=40, on each model's post vectors. Topic AMI against C1's hand topics is a rough signal (about 140 English posts). The language-split share is the share of non-English posts in clusters at least 80% one non-English language: high means the model groups by language rather than subject.
- **E2 job (`job.py`, `bsky-embed`):** reads silver posts window by window after `post_embeddings`' own max `bronze_ingested_at` (via `bsky_enrich.incremental`), skips posts the length rule rejects, embeds each distinct text once per chunk, and appends a chunk (`--chunk-posts`, 5,000 at least, never splitting one loader commit) at a time. `--dry-run` reads and counts without API calls or writes, and never creates the table. Each run ends with one `run {json}` log line: counts, wall and CPU seconds, CPU-seconds per 100k posts, peak RSS. On an empty table it starts at silver's first window, so the first real run is the backfill; bound it with `--max-posts`.
- **E2 so far:** `spec.py` is embedding spec v1 (text_spec v1, `qwen/qwen3-embedding-8b`, the API's 1,024-dim output via `dimensions`, not truncation, L2-normalized, then stored as little-endian float16 bytes). `client.py` requests base64 by default and is the OpenRouter client the job and E1 share: it retries 5xx, dropped connections and 429s (honoring `Retry-After`), treats 400/401/402/403 as fatal, and refuses `:free` model ids. Tests use `httpx.MockTransport`.
- **Gotchas:** DuckDB reading a whole day of silver over S3 fails to connect at the default thread count; `corpus` sets `threads = 8`. nomic-embed-text-v2-moe needs `trust_remote_code` and `einops`.
