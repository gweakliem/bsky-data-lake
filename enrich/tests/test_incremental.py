"""Watermarks, windows, reads and chunking on a local SQLite catalog (no AWS)."""


import pyarrow as pa
import pytest
from pyiceberg.exceptions import CommitFailedException

from bsky_enrich import incremental as inc
from bsky_enrich.testing import DAY, OUT, TS, add_deletes, add_posts, add_runs, silver_catalog, t


@pytest.fixture
def cat(tmp_path):
    return silver_catalog(tmp_path)


def test_windows_follow_silver_runs_after_the_watermark(cat):
    add_runs(cat, t(10), t(30), t(20))
    assert inc.windows(cat, None) == [inc.Window(None, t(10)), inc.Window(t(10), t(20)), inc.Window(t(20), t(30))]
    assert inc.windows(cat, t(15)) == [inc.Window(t(15), t(20)), inc.Window(t(20), t(30))]
    assert inc.windows(cat, t(30)) == []


def test_read_posts_in_window_drops_same_window_deletes(cat):
    add_posts(cat, [("p0", t(5)), ("p2", t(15)), ("p1", t(12)), ("gone", t(12)), ("later", t(25))])
    add_deletes(cat, [("gone", t(14))])
    add_deletes(cat, [("p1", t(14))], collection="app.bsky.feed.like")
    rows = inc.read_posts(cat, inc.Window(t(5), t(20)))
    assert rows["uri"].to_pylist() == ["p1", "p2"]  # after is exclusive, sorted, a like's delete doesn't count


def test_committed_through_is_the_outputs_max_watermark(cat):
    out = inc.ensure_table(cat, f"{inc.SILVER}.out", OUT, f"{cat.properties['warehouse']}/out")
    assert inc.committed_through(out) is None
    for ms in ([t(3), t(9)], [t(7)]):
        inc.append(out, pa.table({"uri": ["u"] * len(ms), "day": [DAY] * len(ms), "bronze_ingested_at": pa.array(ms, TS)}))
    assert inc.committed_through(out) == t(9)
    assert inc.ensure_table(cat, f"{inc.SILVER}.out", OUT, f"{cat.properties['warehouse']}/out").name() == out.name()
    assert [f.name for f in out.spec().fields] == ["day"]


def test_location_prefers_the_silver_location(monkeypatch):
    monkeypatch.setenv("BSKY_WAREHOUSE_BUCKET", "wh")
    monkeypatch.delenv("BSKY_SILVER_LOCATION", raising=False)
    assert inc.location("t") == "s3://wh/iceberg/bsky_silver/t"
    monkeypatch.setenv("BSKY_SILVER_LOCATION", "s3://other/iceberg/bsky_silver/")
    assert inc.location("t") == "s3://other/iceberg/bsky_silver/t"


def test_chunks_end_only_where_the_watermark_changes():
    marks = [t(1)] * 3 + [t(2)] * 4 + [t(3)] + [t(4)] * 2
    rows = pa.table({"bronze_ingested_at": pa.array(marks, TS)})
    sizes = [c.num_rows for c in inc.chunks(rows, 2)]
    assert sizes == [3, 4, 3]  # each group stays whole; a short tail joins the last chunk only if it shares a mark
    assert sum(sizes) == len(marks)
    assert [c.num_rows for c in inc.chunks(rows, 100)] == [10]
    assert list(inc.chunks(rows.slice(0, 0), 5)) == []


def test_append_retries_a_lost_commit_race():
    class Flaky:
        def __init__(self):
            self.calls, self.refreshed = 0, 0

        def append(self, rows):
            self.calls += 1
            if self.calls < 3:
                raise CommitFailedException("conflict")

        def refresh(self):
            self.refreshed += 1

        def name(self):
            return ("ns", "t")

    table, waits = Flaky(), []
    inc.append(table, pa.table({"x": [1]}), sleep=waits.append)
    assert (table.calls, table.refreshed, waits) == (3, 2, [5, 15])
