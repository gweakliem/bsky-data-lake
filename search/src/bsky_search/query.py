"""Hybrid search over the index: vector similarity and BM25 over the text, fused by rank.

The query is embedded with embedding spec v1's query instruction (the same
model, dimensions and provider as the posts). Vector search goes through the
IVF-PQ index and re-ranks its top REFINE x k candidates on the full vectors,
which brings it back to exact search's quality. Hybrid is weighted reciprocal
rank fusion, VECTOR_WEIGHT to the vector ranking and the rest to BM25: on the
E1 query set equal weights scored nDCG@10 0.85 and 0.9/0.1 scored 0.92 against
vector-only 0.91 (2026-11-13; docs/plans/notes/search-eval.md). An optional
recency boost halves a result's score every `half_life_days` of age. Spam and
adult filtering arrives with C2/C3's columns.
"""

from __future__ import annotations

import datetime as dt
import math
import re
from dataclasses import dataclass
from typing import Literal, Protocol

import pyarrow as pa
import pyarrow.compute as pc

from bsky_embed import spec
from bsky_embed.client import Embeddings
from bsky_search import store

Mode = Literal["hybrid", "vector", "text"]
COLUMNS = ["uri", "did", "day", "created_at", "langs", "text", "external_uri"]
# The index has about sqrt(rows) partitions (index.maintain). Probe about 6% of them, as in the
# E1 eval (20 of 317), so recall doesn't fall as the index grows.
PROBE_SHARE = 0.06
MIN_NPROBES = 20
REFINE = 10
VECTOR_WEIGHT = 0.9
RRF_K = 60
CANDIDATES = 50  # from each ranking, before fusion
LANG = re.compile(r"^[A-Za-z]{2,3}(-[A-Za-z0-9]{1,8})*$")


class Embedder(Protocol):
    async def embed(self, texts: list[str], prompt: str = "") -> Embeddings: ...


@dataclass(frozen=True)
class Filters:
    langs: tuple[str, ...] = ()  # any of these
    since: dt.date | None = None

    def where(self) -> str | None:
        parts = []
        if self.langs:
            for lang in self.langs:
                if not LANG.match(lang):
                    raise ValueError(f"not a language tag: {lang!r}")
            parts.append(f"array_has_any(langs, [{', '.join(repr(lang) for lang in self.langs)}])")
        if self.since:
            parts.append(f"day >= date '{self.since.isoformat()}'")
        return " AND ".join(parts) or None


class Searcher:
    def __init__(self, table, embedder: Embedder | None, vector_weight: float = VECTOR_WEIGHT):
        self.table = table
        self.embedder = embedder
        self.vector_weight = vector_weight

    def nprobes(self) -> int:
        return max(MIN_NPROBES, round(PROBE_SHARE * math.sqrt(self.table.count_rows())))

    async def vector(self, query: str):
        if self.embedder is None:
            raise RuntimeError("vector search needs an embedder (OPENROUTER_API_KEY)")
        e = await self.embedder.embed([query], spec.QUERY_PROMPT)
        return store.query_vector(e.vectors[0])

    def _vector_hits(self, v, n: int, where: str | None) -> pa.Table:
        q = self.table.search(v, query_type="vector").distance_type("cosine").nprobes(self.nprobes()).refine_factor(REFINE)
        if where:
            q = q.where(where, prefilter=True)
        hits = q.select(COLUMNS).limit(n).to_arrow()
        return hits.append_column("score", pc.subtract(1.0, hits["_distance"].cast(pa.float64())))  # cosine similarity

    def _text_hits(self, query: str, n: int, where: str | None) -> pa.Table:
        q = self.table.search(query, query_type="fts")
        if where:
            q = q.where(where, prefilter=True)
        hits = q.select([*COLUMNS, "_score"]).limit(n).to_arrow()
        return hits.append_column("score", hits["_score"].cast(pa.float64()))

    async def search(
        self,
        query: str,
        k: int = 10,
        mode: Mode = "hybrid",
        filters: Filters = Filters(),
        half_life_days: float | None = None,
        now: dt.datetime | None = None,
    ) -> list[dict]:
        where = filters.where()
        fetch = k * 3 if half_life_days else k
        if mode == "text":
            hits = self._text_hits(query, fetch, where)
        elif mode == "vector":
            hits = self._vector_hits(await self.vector(query), fetch, where)
        else:
            n = max(CANDIDATES, fetch)
            hits = fuse(self._vector_hits(await self.vector(query), n, where), self._text_hits(query, n, where),
                        self.vector_weight).slice(0, fetch)
        if half_life_days:
            hits = boost(hits, half_life_days, now or dt.datetime.now(dt.UTC)).slice(0, k)
        return [{c: r[c] for c in [*COLUMNS, "score"]} for r in hits.select([*COLUMNS, "score"]).to_pylist()]


def fuse(vector: pa.Table, text: pa.Table, vector_weight: float, k: int = RRF_K) -> pa.Table:
    """Weighted reciprocal rank fusion of two rankings, best first; `score` is the fused score."""
    scores: dict[str, float] = {}
    rows: dict[str, dict] = {}
    for hits, weight in ((vector, vector_weight), (text, 1 - vector_weight)):
        for rank, row in enumerate(hits.select(COLUMNS).to_pylist()):
            scores[row["uri"]] = scores.get(row["uri"], 0.0) + weight / (k + rank)
            rows.setdefault(row["uri"], row)
    order = sorted(scores, key=lambda u: -scores[u])
    out = pa.Table.from_pylist([rows[u] for u in order], schema=vector.select(COLUMNS).schema)
    return out.append_column("score", pa.array([scores[u] for u in order], pa.float64()))


def boost(hits: pa.Table, half_life_days: float, now: dt.datetime) -> pa.Table:
    """Multiply scores by 0.5 per half-life of age, then re-sort."""
    ages = pc.divide(pc.cast(pc.subtract(pa.scalar(now, store.TS), hits["created_at"]), pa.int64()), 86_400e6)
    ages = pc.max_element_wise(pc.fill_null(ages, 0.0), 0.0)  # future-dated posts count as new
    factor = pc.power(0.5, pc.divide(ages, half_life_days))
    hits = hits.set_column(hits.schema.get_field_index("score"), "score", pc.multiply(hits["score"], factor))
    return hits.sort_by([("score", "descending")])
