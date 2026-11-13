"""The E1 candidates: local sentence-transformers models and hosted ones via OpenRouter.

Each model gets the query and document prompts its card asks for. Posts are
embedded once, as documents, and that vector is used for both search and
clustering, since production stores one vector per post.
"""

from __future__ import annotations

from dataclasses import dataclass

from bsky_embed import spec

QWEN_QUERY = spec.QUERY_PROMPT  # the same instruction E2/E3 use


@dataclass(frozen=True)
class Model:
    name: str  # short name, used for cache files
    id: str  # Hugging Face repo or OpenRouter model id
    hosted: bool = False
    query_prompt: str = ""
    doc_prompt: str = ""
    trust_remote_code: bool = False
    usd_per_mtok: float = 0.0  # hosted only, OpenRouter list price 2026-11-10
    dimensions: int | None = None  # hosted only: ask the API for shorter vectors


MODELS = {
    m.name: m
    for m in [
        Model("granite-97m", "ibm-granite/granite-embedding-97m-multilingual-r2"),
        Model("e5-small", "intfloat/multilingual-e5-small", query_prompt="query: ", doc_prompt="passage: "),
        Model("harrier-270m", "microsoft/harrier-oss-v1-270m", query_prompt=QWEN_QUERY),
        Model("nomic-v2-moe", "nomic-ai/nomic-embed-text-v2-moe", query_prompt="search_query: ", doc_prompt="search_document: ", trust_remote_code=True),
        Model("harrier-0.6b", "microsoft/harrier-oss-v1-0.6b", query_prompt=QWEN_QUERY),
        Model("qwen3-0.6b", "Qwen/Qwen3-Embedding-0.6B", query_prompt=QWEN_QUERY),
        Model("qwen3-8b", "qwen/qwen3-embedding-8b", hosted=True, query_prompt=QWEN_QUERY, usd_per_mtok=0.01),
        # The API's 1,024-dim output (E2's spec). It equals truncating; the first comparison's gap was a provider difference.
        Model("qwen3-8b-d1024", "qwen/qwen3-embedding-8b", hosted=True, query_prompt=QWEN_QUERY, usd_per_mtok=0.01, dimensions=1024),
        Model("openai-3-small", "openai/text-embedding-3-small", hosted=True, usd_per_mtok=0.02),
        Model("gemini-2", "google/gemini-embedding-2", hosted=True, usd_per_mtok=0.20),
    ]
}
