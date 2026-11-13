import datetime as dt

import numpy as np
import pyarrow as pa
import pytest
from conftest import add_embeddings, vector

from bsky_embed import spec
from bsky_enrich import incremental as inc
from bsky_enrich import testing as fake
from bsky_enrich.testing import t
from bsky_search import index, store

TODAY = fake.DAY + dt.timedelta(days=1)
TEXT = "a post that is long enough to be indexed"


def build(cat, db, **kw):
    kw.setdefault("maintain_indexes", False)
    return index.run(cat, db, today=kw.pop("today", TODAY), **kw)


def rows(db) -> dict[str, dict]:
    return {r["uri"]: r for r in db.open_table(store.TABLE).to_arrow().to_pylist()}


def test_vectors_convert_the_stored_bytes():
    v = np.stack([vector(1), vector(2)])
    out = store.vectors(pa.array(spec.encode_vectors(v), pa.binary()))
    assert out.type == store.VECTOR
    assert np.allclose(np.array(out.to_pylist(), dtype=np.float32), v, atol=1e-3)
    with pytest.raises(ValueError):
        store.vectors(pa.array([b"\x00" * 10], pa.binary()))


def test_spans_merge_windows_and_stop_at_the_embeddings_watermark():
    ws = [inc.Window(t(0), t(15)), inc.Window(t(15), t(30)), inc.Window(t(30), t(45)), inc.Window(t(45), t(60)),
          inc.Window(t(60), t(75))]
    assert list(index.spans(ws, t(70), 30)) == [inc.Window(t(0), t(30)), inc.Window(t(30), t(60)), inc.Window(t(60), t(70))]
    assert list(index.spans(ws, t(30), 60)) == [inc.Window(t(0), t(30))]
    assert list(index.spans(ws, t(200), 60)) == [inc.Window(t(0), t(60)), inc.Window(t(60), t(75))]


def test_builds_from_embeddings_joined_to_posts_and_records_state(cat, db):
    fake.add_posts(cat, [("at://a/1", t(1)), ("at://a/2", t(2)), ("at://a/3", t(3))], TEXT)
    fake.add_runs(cat, t(5))
    add_embeddings(cat, [("at://a/1", t(1), 1), ("at://a/2", t(2), 2)])  # a/3 was too short, or not embedded yet
    stats = build(cat, db)
    got = rows(db)
    assert set(got) == {"at://a/1", "at://a/2"} and stats.appended == 2 and stats.rows == 2
    assert got["at://a/1"]["text"] == TEXT and got["at://a/1"]["langs"] == ["en"]
    assert np.allclose(np.array(got["at://a/2"]["vector"], dtype=np.float32), vector(2), atol=1e-3)
    assert index.watermark(db) == t(2)  # post_embeddings' watermark, not silver's
    assert build(cat, db).appended == 0  # nothing new: nothing appended twice


def test_follows_new_embeddings_updates_and_deletes(cat, db):
    fake.add_posts(cat, [("at://a/1", t(1)), ("at://a/2", t(2))], TEXT)
    fake.add_runs(cat, t(5))
    add_embeddings(cat, [("at://a/1", t(1), 1), ("at://a/2", t(2), 2)])
    build(cat, db)
    fake.add_posts(cat, [("at://a/1", t(11))], TEXT + " (edited)", cid="c2", operation="update")
    fake.add_deletes(cat, [("at://a/2", t(12))])
    fake.add_posts(cat, [("at://a/3", t(13))], TEXT)
    fake.add_runs(cat, t(15))
    add_embeddings(cat, [("at://a/1", t(11), 3)], cid="c2")
    add_embeddings(cat, [("at://a/3", t(13), 4)])
    stats = build(cat, db)
    got = rows(db)
    assert set(got) == {"at://a/1", "at://a/3"}
    assert got["at://a/1"]["cid"] == "c2" and got["at://a/1"]["text"].endswith("(edited)")
    assert (stats.appended, stats.replaced, stats.deleted) == (2, 1, 1)


def test_ignores_other_spec_versions(cat, db):
    fake.add_posts(cat, [("at://a/1", t(1))], TEXT)
    fake.add_runs(cat, t(5))
    add_embeddings(cat, [("at://a/1", t(1), 1)], spec_version="v0")
    add_embeddings(cat, [("at://a/1", t(1), 2)])
    build(cat, db)
    assert np.allclose(np.array(rows(db)["at://a/1"]["vector"], dtype=np.float32), vector(2), atol=1e-3)


def test_keeps_only_the_window(cat, db):
    fake.add_posts(cat, [("at://a/1", t(1))], TEXT)
    fake.add_runs(cat, t(5))
    add_embeddings(cat, [("at://a/1", t(1), 1)])
    build(cat, db)
    later = fake.DAY + dt.timedelta(days=store.WINDOW_DAYS)  # fake.DAY is now one day too old
    stats = build(cat, db, today=later)
    assert stats.expired == 1 and stats.rows == 0


def test_removes_rows_past_the_state_left_by_an_interrupted_run(cat, db):
    fake.add_posts(cat, [("at://a/1", t(1)), ("at://a/2", t(2))], TEXT)
    fake.add_runs(cat, t(5))
    add_embeddings(cat, [("at://a/1", t(1), 1), ("at://a/2", t(2), 2)])
    build(cat, db)
    # A run that appended its span and died before recording it:
    fake.add_posts(cat, [("at://a/3", t(12))], TEXT)
    fake.add_runs(cat, t(15))
    add_embeddings(cat, [("at://a/3", t(12), 3)])
    rows_, _, _ = index.span_rows(cat, inc.Window(t(2), t(12)), store.cutoff(TODAY))
    db.open_table(store.TABLE).add(rows_)
    assert db.open_table(store.TABLE).count_rows() == 3
    build(cat, db)
    assert sorted(rows(db)) == ["at://a/1", "at://a/2", "at://a/3"]  # once each


def test_dry_run_writes_nothing(cat, db):
    fake.add_posts(cat, [("at://a/1", t(1))], TEXT)
    fake.add_runs(cat, t(5))
    add_embeddings(cat, [("at://a/1", t(1), 1)])
    stats = build(cat, db, dry_run=True)
    assert stats.appended == 1 and index.exists(db, store.TABLE) is False and index.exists(db, store.RUNS) is False


def test_maintain_creates_the_text_and_filter_indexes(cat, db):
    fake.add_posts(cat, [("at://a/1", t(1))], "東京で猫カフェに行きました、とても楽しかった")
    fake.add_posts(cat, [("at://a/2", t(2))], TEXT)
    fake.add_runs(cat, t(5))
    add_embeddings(cat, [("at://a/1", t(1), 1), ("at://a/2", t(2), 2)])
    build(cat, db, maintain_indexes=True)
    table = db.open_table(store.TABLE)
    assert {i.columns[0] for i in table.list_indices()} >= {"text", "uri", "bronze_ingested_at", "day", "langs"}
    assert table.search("猫カフェ", query_type="fts").limit(5).to_arrow()["uri"].to_pylist() == ["at://a/1"]


def test_splits_a_crowded_span_by_time_and_keeps_equal_timestamps_together(cat, db):
    posts = [(f"at://a/{i}", t(1 + i // 2)) for i in range(7)]  # pairs share a timestamp: t1 t1 t2 t2 t3 t3 t4
    fake.add_posts(cat, posts, TEXT)
    fake.add_runs(cat, t(5))
    add_embeddings(cat, [(u, m, i) for i, (u, m) in enumerate(posts)])
    span = inc.Window(None, t(5))
    # 3 rows reach into t2's pair, so the first cut is after t2; the 3 left fit.
    assert index.split(cat, span, 3) == [inc.Window(None, t(2)), inc.Window(t(2), t(5))]
    assert index.split(cat, span, 2) == [inc.Window(None, t(1)), inc.Window(t(1), t(2)), inc.Window(t(2), t(3)),
                                         inc.Window(t(3), t(5))]
    assert index.split(cat, span, 7) == [span]
    stats = build(cat, db, max_span_rows=2)
    assert stats.appended == 7 and stats.spans == 4 and len(rows(db)) == 7
    assert index.watermark(db) == t(4)


def test_rebuild_leaves_out_later_deletes_and_edits_without_table_deletes(cat, db):
    fake.add_posts(cat, [("at://a/1", t(1)), ("at://a/2", t(2))], TEXT)
    fake.add_runs(cat, t(5))
    add_embeddings(cat, [("at://a/1", t(1), 1), ("at://a/2", t(2), 2)])
    build(cat, db)  # an earlier index, which the rebuild replaces
    fake.add_posts(cat, [("at://a/1", t(11))], TEXT + " (edited)", cid="c2", operation="update")
    fake.add_deletes(cat, [("at://a/2", t(12))])
    fake.add_posts(cat, [("at://a/3", t(13))], TEXT)
    fake.add_runs(cat, t(15))
    add_embeddings(cat, [("at://a/1", t(11), 3)], cid="c2")
    add_embeddings(cat, [("at://a/3", t(13), 4)])
    stats = build(cat, db, rebuild=True, span_minutes=1)  # one span per window: the delete and edit come later
    got = rows(db)
    assert set(got) == {"at://a/1", "at://a/3"}
    assert got["at://a/1"]["cid"] == "c2" and got["at://a/1"]["text"].endswith("(edited)")
    assert (stats.appended, stats.replaced, stats.deleted) == (2, 0, 0)  # left out, not deleted
    assert index.watermark(db) == t(13)
    assert build(cat, db).appended == 0  # the hourly run carries on from the rebuild's state


def test_superseded_drops_by_uri_hash_and_handles_no_deletes():
    rows = pa.table({"uri": ["at://a/1", "at://a/2", "at://a/3"],
                     "bronze_ingested_at": pa.array([t(1), t(2), t(3)], store.TS)})
    s = index.Superseded(np.sort(index.uri_hashes(["at://a/2", "at://z/9"])), {"at://a/3": t(4)})
    assert s.drop(rows)["uri"].to_pylist() == ["at://a/1"]  # a/2 deleted, a/3 has a newer edit
    none = index.Superseded(np.empty(0, np.uint64), {})
    assert none.drop(rows).num_rows == 3
