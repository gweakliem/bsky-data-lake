import asyncio
import re
import datetime as dt

import numpy as np
import pytest
from conftest import add_embeddings, vector
from fastapi.testclient import TestClient

from bsky_embed import spec
from bsky_embed.client import Embeddings
from bsky_enrich import testing as fake
from bsky_enrich.testing import t
from bsky_search import index, store
from bsky_search.api import create_app
from bsky_search.query import Filters, Searcher, boost

TODAY = fake.DAY + dt.timedelta(days=1)


class FakeEmbedder:
    """Embeds a query as the vector of the post it names ("seed N"), so vector search has a known answer."""

    def __init__(self):
        self.prompts = []

    async def embed(self, texts, prompt=""):
        self.prompts.append(prompt)
        seeds = [int(m.group(1)) if (m := re.search(r"seed (\d+)", t)) else 99 for t in texts]
        return Embeddings(np.stack([vector(s) for s in seeds]), ["Nebius"] * len(texts))


@pytest.fixture
def searcher(cat, db):
    posts = [("at://a/1", "the senate passed the budget bill tonight", 1, "en"),
             ("at://a/2", "my cat knocked the plant off the shelf again", 2, "en"),
             ("at://a/3", "o senado aprovou o orçamento nesta noite", 3, "pt")]
    for i, (uri, text, seed, lang) in enumerate(posts):
        fake.add_posts(cat, [(uri, t(i + 1))], text)  # the helper's langs are always ["en"]
    fake.add_runs(cat, t(5))
    add_embeddings(cat, [(uri, t(i + 1), seed) for i, (uri, _, seed, _) in enumerate(posts)])
    index.run(cat, db, today=TODAY)
    return Searcher(db.open_table(store.TABLE), FakeEmbedder())


def run(coro):
    return asyncio.run(coro)


def test_vector_search_finds_the_nearest_post_with_the_query_prompt(searcher):
    hits = run(searcher.search("seed 2", k=1, mode="vector"))
    assert hits[0]["uri"] == "at://a/2" and hits[0]["score"] == pytest.approx(1.0, abs=1e-2)
    assert searcher.embedder.prompts == [spec.QUERY_PROMPT]


def test_text_search_needs_no_embedder(searcher):
    searcher.embedder = None
    assert [h["uri"] for h in run(searcher.search("budget", mode="text"))] == ["at://a/1"]


def test_hybrid_fuses_both(searcher):
    hits = run(searcher.search("seed 3 budget", k=3))
    assert {h["uri"] for h in hits[:2]} == {"at://a/1", "at://a/3"}  # a/1 by text, a/3 by vector
    assert all(h["score"] > 0 for h in hits)


def test_filters_build_a_where_clause_and_refuse_bad_tags():
    assert Filters(("en", "pt-BR"), dt.date(2026, 11, 1)).where() == (
        "array_has_any(langs, ['en', 'pt-BR']) AND day >= date '2026-11-01'")
    assert Filters().where() is None
    with pytest.raises(ValueError):
        Filters(("en'); drop",)).where()


def test_language_filter(searcher):
    assert run(searcher.search("seed 3", k=3, mode="vector", filters=Filters(("pt",)))) == []  # all fake posts are en
    assert len(run(searcher.search("seed 3", k=3, mode="vector", filters=Filters(("en",))))) == 3


def test_recency_boost_halves_per_half_life():
    import pyarrow as pa

    now = dt.datetime(2026, 11, 10, tzinfo=dt.UTC)
    hits = pa.table({"created_at": pa.array([now - dt.timedelta(days=2), now], store.TS), "score": [1.0, 0.6]})
    out = boost(hits, 2.0, now)
    assert out["score"].to_pylist() == pytest.approx([0.6, 0.5])


def test_api(searcher):
    with TestClient(create_app(searcher)) as client:
        r = client.get("/search", params={"q": "seed 2", "k": 1, "mode": "vector"})
        assert r.status_code == 200 and r.json()["results"][0]["uri"] == "at://a/2"
        assert client.get("/search", params={"q": "x", "lang": "bad'"}).status_code == 400
        assert client.get("/search", params={"q": ""}).status_code == 422
        assert client.get("/healthz").json()["rows"] == 3


def test_fuse_weights_the_vector_ranking():
    import pyarrow as pa

    from bsky_search.query import COLUMNS, fuse

    def ranking(*uris):
        return pa.table({c: pa.array([u if c == "uri" else None for u in uris],
                                     pa.list_(pa.string()) if c == "langs" else pa.string()) for c in COLUMNS})

    out = fuse(ranking("v1", "both"), ranking("t1", "t2", "both"), 0.9)
    assert out["uri"].to_pylist()[:2] == ["both", "v1"]  # in both rankings beats first in one
    assert out["uri"].to_pylist().index("t1") > out["uri"].to_pylist().index("v1")  # text counts for less
