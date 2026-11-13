"""E3's acceptance check: hybrid against vector-only search on the E1 query set.

Builds a local index from E1's corpus (100,000 posts of 2026-11-09 and their
qwen3-8b-d1024 vectors, i.e. embedding spec v1) with the production index
settings, runs the 48 queries through `Searcher` in each mode, and merges the
top 10s into E1's results as systems `e3-*`. Then E1's tools judge the new
pairs and score every system:

    uv run python -m bsky_search.evaluate                 # in search/; needs embed/eval/.cache
    cd ../embed && uv run python -m bsky_embed.judge      # Jev judges pairs not yet judged
    uv run python -m bsky_embed.report                    # nDCG@10 / recall@10 per system

Rerun this after `bsky_embed.search`, which rewrites the results file.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import shutil
import sys

import numpy as np
import pyarrow as pa

from bsky_embed import corpus, search, spec
from bsky_embed.client import Embeddings
from bsky_search import index, store
from bsky_search.query import Searcher

MODEL = "qwen3-8b-d1024"  # E1's name for embedding spec v1's vectors
LANCE = corpus.CACHE / "lance"
SYSTEMS = {
    "e3-hybrid": dict(mode="hybrid"),
    "e3-vector": dict(mode="vector"),  # through the IVF-PQ index, as in production
    "e3-text": dict(mode="text"),
}


class CachedQueries:
    """An embedder that answers from E1's cached query vectors (made with spec.QUERY_PROMPT)."""

    def __init__(self, texts: list[str], vectors: np.ndarray):
        self.by_text = dict(zip(texts, vectors, strict=True))

    async def embed(self, texts, prompt=""):
        assert prompt == spec.QUERY_PROMPT
        return Embeddings(np.stack([self.by_text[t] for t in texts]), ["cache"] * len(texts))


def build(posts: list[dict]) -> object:
    v = np.load(corpus.CACHE / "vectors" / f"{MODEL}.npy")
    assert v.shape == (len(posts), spec.DIMS), v.shape
    day = dt.date.fromisoformat(corpus.DAY)
    rows = pa.table({
        "uri": [p["uri"] for p in posts], "cid": pa.array([None] * len(posts), pa.string()),
        "did": [p["did"] for p in posts], "day": pa.array([day] * len(posts), pa.date32()),
        "created_at": pa.array([None] * len(posts), store.TS),
        "langs": pa.array([[p["lang"]] if p.get("lang") else [] for p in posts], pa.list_(pa.string())),
        "text": [p["text"] for p in posts], "external_uri": pa.array([None] * len(posts), pa.string()),
        "bronze_ingested_at": pa.array([None] * len(posts), store.TS),
        "vector": store.vectors(pa.array(spec.encode_vectors(v.astype(np.float32)), pa.binary())),
    }, schema=store.SCHEMA)
    shutil.rmtree(LANCE, ignore_errors=True)
    table = store.connect(str(LANCE)).create_table(store.TABLE, rows)
    index.maintain(table)
    return table


async def query_all(searcher: Searcher, queries: list[dict], positions: dict[str, int]) -> dict[str, dict[str, list[int]]]:
    out: dict[str, dict[str, list[int]]] = {}
    for system, kw in SYSTEMS.items():
        out[system] = {}
        for q in queries:
            hits = await searcher.search(q["query"], k=search.K, **kw)
            out[system][q["id"]] = [positions[h["uri"]] for h in hits]
    return out


def main() -> None:
    posts = corpus.load()
    queries = search.load_queries()
    texts = [q["query"] for q in queries]
    table = build(posts)
    print(f"index: {table.count_rows():,} rows, {[i.index_type for i in table.list_indices()]}", file=sys.stderr)
    searcher = Searcher(table, CachedQueries(texts, search.query_vectors(MODEL, texts)))
    new = asyncio.run(query_all(searcher, queries, {p["uri"]: i for i, p in enumerate(posts)}))
    results = json.loads(search.RESULTS.read_text())
    results.update(new)
    search.RESULTS.write_text(json.dumps(results))
    print(f"{', '.join(new)} -> {search.RESULTS}; now judge the new pairs (bsky_embed.judge) and report", file=sys.stderr)


if __name__ == "__main__":
    main()
