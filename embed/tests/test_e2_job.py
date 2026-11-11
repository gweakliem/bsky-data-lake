"""The embedding job end to end on a local catalog, with a fake embedder (no network, no AWS)."""

import asyncio

import numpy as np
import pytest

from bsky_embed import job, spec
from bsky_enrich import incremental as inc
from bsky_enrich.testing import add_posts, add_runs, silver_catalog, t


class FakeEmbedder:
    """Wider than DIMS, like the real model, with a vector derived from the text."""

    def __init__(self, fail_on_call: int | None = None):
        self.calls: list[list[str]] = []
        self.fail_on_call = fail_on_call

    async def embed(self, texts, prompt=""):
        self.calls.append(list(texts))
        if len(self.calls) == self.fail_on_call:
            raise RuntimeError("boom")
        v = np.zeros((len(texts), spec.DIMS + 16), dtype=np.float32)
        for i, text in enumerate(texts):
            v[i, len(text) % spec.DIMS] = 2.0
        return v


@pytest.fixture
def cat(tmp_path):
    c = silver_catalog(tmp_path)
    c.warehouse = f"{tmp_path}/warehouse"
    return c


def out_table(cat):
    return job.output_table(cat, location=f"{cat.warehouse}/{job.TABLE}")


def embed(cat, client, **kwargs) -> job.Stats:
    return asyncio.run(job.run(cat, out_table(cat), client, **kwargs))


def stored(cat):
    return out_table(cat).scan().to_arrow().sort_by("uri")


def test_writes_one_normalized_row_per_eligible_post(cat):
    add_posts(cat, [("p1", t(1)), ("p2", t(2))], text="a post long enough to embed")
    add_posts(cat, [("short", t(2))], text="hi")
    add_runs(cat, t(5))
    client = FakeEmbedder()
    stats = embed(cat, client)
    assert (stats.read, stats.skipped, stats.embedded, stats.texts) == (3, 1, 2, 1)
    assert client.calls == [["a post long enough to embed"]]  # identical text is sent once
    table = stored(cat)
    assert table["uri"].to_pylist() == ["p1", "p2"]
    assert set(table["model"].to_pylist()) == {spec.MODEL}
    assert set(table["spec_version"].to_pylist()) == {spec.SPEC_VERSION}
    v = spec.decode_vector(table["vector"][0].as_py())
    assert v.shape == (spec.DIMS,) and np.isclose(np.linalg.norm(v), 1.0)


def test_rerun_adds_nothing_and_new_windows_continue(cat):
    add_posts(cat, [("p1", t(1))], text="first post, long enough")
    add_runs(cat, t(5))
    embed(cat, FakeEmbedder())
    client = FakeEmbedder()
    assert embed(cat, client).embedded == 0 and client.calls == []
    # Posts committed by silver but not yet in a finished window wait for it.
    add_posts(cat, [("p2", t(8))], text="second post, long enough")
    assert embed(cat, FakeEmbedder()).embedded == 0
    add_runs(cat, t(10))
    assert embed(cat, FakeEmbedder()).embedded == 1
    assert stored(cat)["uri"].to_pylist() == ["p1", "p2"]


def test_a_crash_between_chunks_resumes_without_gaps_or_duplicates(cat):
    # Three loader commits; chunks of 2 posts at least, never splitting a commit.
    add_posts(cat, [("a1", t(1)), ("a2", t(1)), ("b1", t(2)), ("b2", t(2)), ("b3", t(2)), ("c1", t(3))],
              text="the same long enough text")
    add_runs(cat, t(5))
    with pytest.raises(RuntimeError, match="boom"):
        embed(cat, FakeEmbedder(fail_on_call=2), chunk_posts=2)
    assert stored(cat)["uri"].to_pylist() == ["a1", "a2"]
    assert inc.committed_through(out_table(cat)) == t(1)
    embed(cat, FakeEmbedder(), chunk_posts=2)
    assert stored(cat)["uri"].to_pylist() == ["a1", "a2", "b1", "b2", "b3", "c1"]


def test_max_posts_stops_after_the_chunk_that_reaches_it(cat):
    add_posts(cat, [(f"p{i}", t(i)) for i in range(1, 6)], text="this one is long enough to embed")
    add_runs(cat, t(10))
    stats = embed(cat, FakeEmbedder(), chunk_posts=2, max_posts=3)
    assert stats.embedded == 4
    assert stored(cat).num_rows == 4


def test_dry_run_counts_without_writing(cat):
    add_posts(cat, [("p1", t(1))], text="this one is long enough to embed")
    add_runs(cat, t(5))
    stats = asyncio.run(job.run(cat, None, None))
    assert (stats.embedded, stats.texts) == (1, 1)
    assert not cat.table_exists(f"{inc.SILVER}.{job.TABLE}")
