import json
import math

import numpy as np

from bsky_embed import report, search, spec


def test_text_spec_v1_joins_text_alts_and_link_title():
    row = {"text": " hello ", "image_alts": ["a cat", ""], "external_title": "Cats"}
    assert spec.text(row) == "hello\na cat\nCats"
    assert spec.text({"text": None, "image_alts": None, "external_title": None}) == ""


def test_top_k_orders_by_score():
    docs = np.eye(4, dtype=np.float32)
    q = np.array([[0.1, 0.9, 0.5, 0.0]], dtype=np.float32)
    assert search.top_k(docs, q, k=3) == [[1, 2, 0]]


def test_search_metrics_against_pool():
    posts = [{"uri": f"u{i}", "lang": "en" if i < 3 else "ja"} for i in range(5)]
    queries = {"q1": {"id": "q1", "kind": "news", "lang": "en"}}
    # Relevant in the pool: u0 and u3. The system finds u3 first, then two misses.
    rel = {("q1", "u0"): True, ("q1", "u3"): True, ("q1", "u1"): False}
    m = report.search_metrics({"s": {"q1": [3, 1, 2]}}, queries, posts, rel)["s"]
    idcg = 1 + 1 / math.log2(3)
    assert math.isclose(m["all"]["ndcg"], 1 / idcg)
    assert math.isclose(m["all"]["recall"], 0.5)
    assert m["all"]["xlang"] == 1.0  # the one relevant hit is Japanese for an English query
    assert set(m) == {"en", "all"}


def test_pool_is_union_of_systems(tmp_path, monkeypatch):
    from bsky_embed import judge

    path = tmp_path / "results.json"
    path.write_text(json.dumps({"a": {"q1": [1, 2]}, "b": {"q1": [2, 3], "q2": [1]}}))
    monkeypatch.setattr(search, "RESULTS", path)
    assert judge.pool() == [("q1", 1), ("q1", 2), ("q1", 3), ("q2", 1)]
