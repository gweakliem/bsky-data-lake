# Search eval (E3), 2026-11-13

E3's acceptance check: on the E1 query set, hybrid search must match or beat vector-only search. `bsky_search.evaluate` builds a local index from E1's corpus (100,300 posts of 2026-11-09 with embedding spec v1's vectors, `qwen3-8b-d1024` in E1's names) using the production index settings. It runs the 48 queries through the production `Searcher` and adds the top 10s to E1's results as `e3-*` systems. Jev then judges the new pairs as in E1 (`p >= 0.5` counts as relevant), and `bsky_embed.report` scores them. Recall is against the pool, so adding systems shifts every system's numbers slightly.

## Results

nDCG@10 (recall@10 against the pool). `en` is English queries, `native` is non-English queries, and `cross` is English queries whose matches are mostly in another language.

| system | all | en | native | cross |
|---|---|---|---|---|
| exact vector search (E1's `qwen3-8b-d1024`) | 0.91/0.91 | 0.89/0.89 | 0.98/0.97 | 0.95/0.92 |
| IVF-PQ, nprobes 20, no refine | 0.88/0.87 | 0.88/0.87 | 0.95/0.92 | 0.75/0.68 |
| **IVF-PQ, nprobes 20, refine 10 (`e3-vector`)** | 0.92/0.91 | 0.90/0.89 | 0.99/0.99 | 0.96/0.95 |
| LanceDB's built-in hybrid (equal-weight RRF, unrefined vectors) | 0.79/0.76 | 0.80/0.77 | 0.87/0.85 | 0.57/0.47 |
| RRF, equal weights | 0.85/0.84 | 0.86/0.85 | 0.97/0.96 | 0.55/0.53 |
| RRF, 0.7 vector / 0.3 text | 0.89/0.90 | 0.89/0.89 | 0.97/0.99 | 0.77/0.80 |
| RRF, 0.8 / 0.2 | 0.91/0.91 | 0.90/0.90 | 0.96/0.97 | 0.84/0.85 |
| **RRF, 0.9 / 0.1 (`e3-hybrid`)** | 0.91/0.91 | 0.90/0.89 | 0.98/0.97 | 0.89/0.90 |
| BM25, LanceDB full-text, icu tokenizer (`e3-text`) | 0.63/0.61 | 0.66/0.64 | 0.75/0.74 | 0.17/0.12 |
| BM25, E1's `bm25s` | 0.58/0.56 | 0.66/0.64 | 0.43/0.41 | 0.18/0.12 |

The tuning variants were measured from a scratch script with nprobes 50. The bold rows are the production code, with the vector index trained at `sample_rate` 64; at the default 256, `e3-vector` scored 0.91/0.91 overall.

## Findings

- **IVF-PQ loses quality unless the top candidates are re-ranked.** With 64 sub-vectors, PQ codes lose enough to cost 0.03 nDCG overall and 0.20 on cross-language queries. A refine factor of 10 re-reads the full vectors of the top 100 candidates and recovers exact search's numbers. On S3 that is about 200 KB a query.
- **Equal-weight fusion hurts.** Text search alone scores 0.63, so giving it half the weight pulls good vector results down. Cross-language queries suffer most, since BM25 can't match across languages. At 0.9/0.1, text mostly breaks ties and lifts English slightly, at some cost on cross-language queries. The E1 queries are descriptive, so they barely test what BM25 is for (exact names, hashtags, quoted phrases). The weight is `VECTOR_WEIGHT` in `query.py`, and a request can still ask for `mode=text`.
- **The icu tokenizer helps non-English text search.** It scores 0.75 on native-language queries against `bm25s`'s 0.43, because it splits Japanese and Chinese into words.
- **Hybrid ties vector-only within noise** (0.91 against 0.92 overall, equal on English). With 48 queries, differences of 0.01 to 0.02 are noise, so the acceptance check (match or beat) passes on that reading, not clearly. Hybrid stays the default for queries that need exact terms. If a query set with names and hashtags shows no gain, make vector the default.

## Follow-ups

- Recency boost (`half_life_days`) is implemented but off by default. The E1 corpus is a single day, so it can't be tuned on this set.
- LanceDB ships a `TypeSafeReranker`, which may be the simplest way to try the Jev rerank of the top 50.
- nprobes scales with the index: about 6% of its `sqrt(rows)` partitions, as here (20 of 317), so about 370 of 6,100 at 37M rows. Check latency and recall on the real index.
