"""Run the E1 queries against each model's vectors and BM25, keeping the top 10.

Query vectors are cached per model; results go to eval/.cache/results.json as
{system: {query id: [corpus index, ...]}}.

    uv run python -m bsky_embed.search
"""

from __future__ import annotations

import hashlib
import json
import sys

import numpy as np

from bsky_embed import corpus, embed
from bsky_embed.models import MODELS

QUERIES = corpus.EVAL / "queries.jsonl"
RESULTS = corpus.CACHE / "results.json"
K = 10
# Matryoshka-trained models also work truncated to their first dimensions, which cuts storage.
TRUNCATED = {"qwen3-8b@1024": ("qwen3-8b", 1024), "qwen3-8b@256": ("qwen3-8b", 256),
             "openai-3-small@512": ("openai-3-small", 512), "nomic-v2-moe@256": ("nomic-v2-moe", 256)}


def load_queries() -> list[dict]:
    return [json.loads(line) for line in QUERIES.open()]


def query_vectors(name: str, queries: list[str]) -> np.ndarray:
    key = hashlib.sha256("\n".join(queries).encode()).hexdigest()[:12]
    path = corpus.CACHE / "queries" / f"{name}-{key}.npy"
    if path.exists():
        return np.load(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    v = embed.encode(MODELS[name], queries, "query")
    np.save(path, v)
    return v


def top_k(docs: np.ndarray, q: np.ndarray, k: int = K) -> list[list[int]]:
    scores = q @ docs.T
    idx = np.argpartition(-scores, k, axis=1)[:, :k]
    return [row[np.argsort(-scores[i, row])].tolist() for i, row in enumerate(idx)]


def bm25(texts: list[str], queries: list[str], k: int = K) -> list[list[int]]:
    import bm25s

    retriever = bm25s.BM25()
    retriever.index(bm25s.tokenize(texts, show_progress=False), show_progress=False)
    docs, _ = retriever.retrieve(bm25s.tokenize(queries, show_progress=False), k=k, show_progress=False)
    return docs.tolist()


def main() -> None:
    posts = corpus.load()
    queries = load_queries()
    texts = [q["query"] for q in queries]
    results: dict[str, dict[str, list[int]]] = {}
    for name in MODELS:
        if not (embed.VECTORS / f"{name}.npy").exists():
            print(f"{name}: no vectors yet, skipped", file=sys.stderr)
            continue
        hits = top_k(embed.vectors(name), query_vectors(name, texts))
        results[name] = {q["id"]: h for q, h in zip(queries, hits)}
    for variant, (name, dims) in TRUNCATED.items():
        if name in results:
            hits = top_k(embed.normalize(embed.vectors(name)[:, :dims]), embed.normalize(query_vectors(name, texts)[:, :dims]))
            results[variant] = {q["id"]: h for q, h in zip(queries, hits)}
    results["bm25"] = {q["id"]: h for q, h in zip(queries, bm25([p["text"] for p in posts], texts))}
    RESULTS.write_text(json.dumps(results))
    print(f"{len(results)} systems x {len(queries)} queries -> {RESULTS}", file=sys.stderr)


if __name__ == "__main__":
    main()
