# E1: embedding model selection

**Choice (2026-11-10): `qwen/qwen3-embedding-8b` via OpenRouter, truncated to 1,024 dims, `text_spec` v1.** Search is the core feature and cross-language search (an English query finding non-English posts) is where models differ most; Qwen3-8B leads there by a wide margin. nomic-embed-text-v2-moe stays a candidate for topic clustering (below).

The harness is `embed/` (see `embed/CLAUDE.md`); every number here is reproducible with `uv run python -m bsky_embed.report`.

## Setup

- **Corpus:** 100,000 live eligible posts of 2026-11-09, hash-sampled, in every language, plus C1's 300 posts (100,300).
- **Text (`text_spec` v1):** post text, image alt texts, external link title; at most 256 tokens.
- **Queries:** 48 by hand (`embed/eval/queries.jsonl`):
  - English: news, everyday, entertainment, commerce.
  - `native`: non-English queries.
  - `cross`: English queries whose relevant posts are mostly non-English.
- **Systems:** 9 models plus BM25 (bm25s), and Matryoshka truncations of qwen3-8b (1,024, 256), openai-3-small (512) and nomic (256).
- **Relevance:** the pool is every system's top 10 (2,602 pairs). Jev (`jev-1.14.0`) judges each pair from the same text the models embed ($0.05 in all); relevant is p ≥ 0.5. Recall is against the pool.
- **Hand check:** 150 English pairs, 50 from each of Jev's low (<0.2), middle and high (≥0.8) bands, judged blind to Jev's answer.

## Is Jev a good enough judge?

| Threshold | Agree with hand | Precision | Recall |
|---|---|---|---|
| 0.5 (used) | 85% | 94% | 80% |
| 0.3 | 87% | 89% | 90% |
| 0.2 | 89% | 88% | 96% |

- **Jev is strict.** Its confident answers are right (4 of 50 low-band pairs were hand-relevant, 48 of 50 high-band), but 40 of 50 middle-band pairs were hand-relevant.
- **The ranking doesn't depend on the threshold.** Recomputed at 0.3 and 0.2, scores rise everywhere and the tiers stay the same. For E3's acceptance comparisons, 0.3 matches hand judgment best.
- **Thread context didn't matter.** No hand pair needed the thread or link to judge (`u` was never used). The open question on context-dependent replies in the plan stands for search UX, not for this comparison.

## Search quality

nDCG@10 at p ≥ 0.5. `en` is English queries, `native` non-English queries, `cross` English queries over mostly non-English posts.

| System | all | en | native | cross |
|---|---|---|---|---|
| qwen3-8b | 0.93 | 0.91 | 1.00 | 0.91 |
| **qwen3-8b@1024** | 0.92 | 0.89 | 1.00 | 0.95 |
| qwen3-8b@256 | 0.89 | 0.88 | 0.96 | 0.83 |
| openai-3-small | 0.88 | 0.87 | 0.93 | 0.86 |
| nomic-v2-moe | 0.87 | 0.88 | 0.93 | 0.70 |
| qwen3-0.6b | 0.87 | 0.88 | 0.92 | 0.71 |
| harrier-0.6b | 0.87 | 0.87 | 0.99 | 0.67 |
| openai-3-small@512 | 0.87 | 0.86 | 0.93 | 0.82 |
| harrier-270m | 0.86 | 0.86 | 0.97 | 0.68 |
| gemini-2 | 0.86 | 0.84 | 0.91 | 0.87 |
| nomic-v2-moe@256 | 0.85 | 0.87 | 0.90 | 0.65 |
| granite-97m | 0.76 | 0.74 | 0.87 | 0.66 |
| e5-small | 0.73 | 0.76 | 0.79 | 0.30 |
| bm25 | 0.58 | 0.66 | 0.43 | 0.18 |

- **Monolingual search is nearly solved** by any of the mid-size models (0.86–0.88 on English). The gap is cross-language: the hosted models score 0.86–0.95, the local ones about 0.70.
- **Truncating Qwen3-8B to 1,024 dims costs 0.01 overall** and scored higher on cross queries (likely noise on few queries). 256 dims costs more (0.83 cross).
- **BM25 alone is weak,** especially across languages, as expected; E3 still fuses it in for exact names and terms.

## Cost, speed and storage

| Model | Dims | Posts/s (MPS or API) | Posts/s (4 CPU threads) | $/day at 2.9M posts |
|---|---|---|---|---|
| qwen3-8b | 4,096 | 60 (concurrency 6) | — | 1.31 |
| openai-3-small | 1,536 | 341 | — | 2.89 |
| gemini-2 | 3,072 | 205 | — | 24.21 |
| nomic-v2-moe | 768 | 166 | 77 | — |
| harrier-0.6b / qwen3-0.6b | 1,024 | 57–62 | 1.4 | — |
| e5-small | 384 | 842 | 278 | — |

- **Qwen3-8B costs about $40 a month** for every eligible post (45 tokens a post). It needs no GPU or inference node; E2 calls the API, so D5 doesn't need a GPU node pool for embeddings.
- **The API throughput is enough:** 60 posts/s at concurrency 6 is about 5M posts a day, and concurrency can rise.
- **Storage is the bigger long-run cost.** At 1,024 dims, a day of posts is about 12 GB in float32 or 6 GB in float16, before the index. That's comparable to the whole lake's daily growth.
- **Vendor risk:** OpenRouter (and Qwen's hosted endpoint behind it) can change prices or drop the model. The weights are open (Apache 2.0), so self-hosting on a GPU stays a fallback, and the vectors don't need recomputing if the same weights are served.

## Cost optimizations (not yet measured)

- **Skip posts that don't need a vector:** spam and adult once C2 runs, and empty or very short posts. Each needs measuring against the 2.9M.
- **Embed identical text once.** Bot and copy-paste posts repeat; key the cache by a text hash.
- **Store fewer bytes:** float16, int8 or binary quantization in the index, or 512/256 dims with a full-precision rerank. Measure each with this harness, which already supports truncation.
- **Search recent posts at full fidelity and older ones compressed,** if the index grows faster than queries need.

## Clustering

k-means (k = 40) on each model's vectors.

| Model | Topic AMI vs C1 | Non-English posts in single-language clusters |
|---|---|---|
| granite-97m | 0.23 | 0% |
| nomic-v2-moe | 0.20 | 1% |
| qwen3-0.6b | 0.21 | 22% |
| qwen3-8b | 0.22 | 22% |
| openai-3-small | 0.27 | 24% |
| e5-small | 0.14 | 32% |
| harrier-0.6b | 0.17 | 40% |
| gemini-2 | 0.29 | 42% |
| harrier-270m | 0.18 | 46% |

- **AMI doesn't separate the models.** It's 0.14–0.29 on only about 140 English C1 posts.
- **nomic clusters by subject across languages;** Qwen3-8B puts a fifth of non-English posts in language clusters. (granite also has 0% but searches poorly.)
- **For topic clustering (C1's topics, E3 aggregates), test before deciding:**
  - nomic vectors locally, since they're cheap to compute (about 10 CPU-hours a day);
  - Qwen3-8B vectors with the language direction removed, e.g. subtracting each language's mean vector;
  - a density-based method (HDBSCAN after UMAP) instead of k-means, with LLM-named clusters. The HN thread on Wilson Lin's Hackerverse map (news.ycombinator.com/item?id=40307519) covers this approach.

## Decision

- **Model:** `qwen/qwen3-embedding-8b` via OpenRouter, query prompt `QWEN_QUERY` (`embed/src/bsky_embed/models.py`), documents without a prompt, vectors normalized and truncated to the first 1,024 dims and renormalized.
- **`text_spec`:** v1.
- **Hardware:** none for inference. E2 is API-bound; size its pod for concurrency, not compute.
- **Revisit** if the cost optimizations don't hold the bill near $40/month, if OpenRouter's price or availability changes, or when the clustering tests above pick a second model for topics.
