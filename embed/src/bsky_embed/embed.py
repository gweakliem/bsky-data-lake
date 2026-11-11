"""Embed the corpus with each candidate, and benchmark CPU throughput.

Vectors are L2-normalized and cached as float16 in eval/.cache/vectors/, one
row per corpus post in corpus order, with a .json of timings and token counts.
Local models run on MPS when available for the full corpus; `--bench` times
2,000 posts on CPU with 4 threads, a stand-in for a 4-vCPU node.

    uv run python -m bsky_embed.embed                 # every model not yet cached
    uv run python -m bsky_embed.embed e5-small qwen3-8b
    uv run python -m bsky_embed.embed --bench         # CPU throughput of the local models
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

import numpy as np

from bsky_embed import corpus, spec
from bsky_embed.models import MODELS, Model

VECTORS = corpus.CACHE / "vectors"
MAX_TOKENS = 256  # posts are at most 300 characters plus alt texts; caps long-context models


def normalize(v: np.ndarray) -> np.ndarray:
    return spec.normalize(v)


def local_model(m: Model, device: str):
    from sentence_transformers import SentenceTransformer

    st = SentenceTransformer(m.id, device=device, trust_remote_code=m.trust_remote_code)
    st.max_seq_length = min(st.max_seq_length or MAX_TOKENS, MAX_TOKENS)
    return st


def device() -> str:
    import torch

    return "mps" if torch.backends.mps.is_available() else "cpu"


def encode_local(st, texts: list[str], prompt: str, batch_size: int = 64) -> np.ndarray:
    v = st.encode(texts, prompt=prompt, batch_size=batch_size, convert_to_numpy=True, show_progress_bar=len(texts) > 5000)
    return normalize(v.astype(np.float32))


async def encode_hosted(m: Model, texts: list[str], prompt: str, batch: int = 96, concurrency: int = 6) -> tuple[np.ndarray, int, float]:
    """Vectors, prompt tokens and the cost OpenRouter reports (USD)."""
    from dotenv import load_dotenv

    from bsky_embed.client import OpenRouter

    load_dotenv(corpus.EVAL.parents[1] / ".env")
    async with OpenRouter.from_env(m.id, batch=batch, concurrency=concurrency, dimensions=m.dimensions) as client:
        v = (await client.embed(texts, prompt)).vectors
    return normalize(v), client.usage.prompt_tokens, client.usage.cost_usd


def encode(m: Model, texts: list[str], kind: str, st=None) -> np.ndarray:
    """Embed queries or documents with the model's prompt for that kind."""
    prompt = m.query_prompt if kind == "query" else m.doc_prompt
    if m.hosted:
        return asyncio.run(encode_hosted(m, texts, prompt))[0]
    return encode_local(st or local_model(m, device()), texts, prompt)


def vectors(name: str) -> np.ndarray:
    return np.load(VECTORS / f"{name}.npy").astype(np.float32)


def run(m: Model, texts: list[str]) -> None:
    VECTORS.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()
    meta: dict = {"model": m.id, "posts": len(texts)}
    if m.hosted:
        v, tokens, cost = asyncio.run(encode_hosted(m, texts, m.doc_prompt))
        meta |= {"device": "openrouter", "prompt_tokens": tokens, "cost_usd": cost}
    else:
        dev = device()
        st = local_model(m, dev)
        v = encode_local(st, texts, m.doc_prompt)
        meta |= {"device": dev}
    meta |= {"seconds": round(time.perf_counter() - start, 1), "dims": int(v.shape[1])}
    meta["posts_per_sec"] = round(len(texts) / meta["seconds"], 1)
    np.save(VECTORS / f"{m.name}.npy", v.astype(np.float16))
    (VECTORS / f"{m.name}.json").write_text(json.dumps(meta, indent=1))
    print(f"{m.name}: {meta}", file=sys.stderr)


def bench(m: Model, texts: list[str], threads: int = 4) -> dict:
    import torch

    torch.set_num_threads(threads)
    st = local_model(m, "cpu")
    encode_local(st, texts[:64], m.doc_prompt, batch_size=32)  # warm-up
    start = time.perf_counter()
    encode_local(st, texts, m.doc_prompt, batch_size=32)
    seconds = time.perf_counter() - start
    return {"cpu_threads": threads, "cpu_posts": len(texts), "cpu_posts_per_sec": round(len(texts) / seconds, 1)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("models", nargs="*", help=f"default: every model not yet cached ({', '.join(MODELS)})")
    parser.add_argument("--bench", action="store_true", help="time 2,000 posts on 4 CPU threads for each local model")
    parser.add_argument("--force", action="store_true", help="re-embed models already cached")
    args = parser.parse_args()
    names = args.models or list(MODELS)
    texts = [p["text"] for p in corpus.load()]
    if args.bench:
        sample = texts[:: max(1, len(texts) // 2000)][:2000]
        for name in names:
            if MODELS[name].hosted:
                continue
            path = VECTORS / f"{name}.json"
            meta = json.loads(path.read_text()) if path.exists() else {}
            meta |= bench(MODELS[name], sample)
            VECTORS.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(meta, indent=1))
            print(f"{name}: {meta['cpu_posts_per_sec']} posts/s on {meta['cpu_threads']} CPU threads", file=sys.stderr)
        return
    for name in names:
        if (VECTORS / f"{name}.npy").exists() and not args.force:
            continue
        run(MODELS[name], texts)


if __name__ == "__main__":
    main()
