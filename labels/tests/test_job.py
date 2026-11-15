"""The labels job and backfill on a local catalog, against a fake label stream (no network, no AWS)."""

import asyncio
import datetime as dt

import cbor2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bsky_enrich.testing import silver_catalog
from bsky_labels import backfill, job, stream

NOW = dt.datetime(2026, 11, 15, 12, tzinfo=dt.UTC)


def frame(seq, labels=None, t="#labels"):
    body = {"seq": seq, "labels": labels or []} if t == "#labels" else {"name": "OutdatedCursor"}
    return cbor2.dumps({"op": 1, "t": t}) + cbor2.dumps(body)


def label(uri, val="porn", neg=False, cts="2026-11-13T10:00:00.000Z"):
    return {"src": stream.SOURCE, "uri": uri, "val": val, "neg": neg, "cts": cts, "sig": b"x"}


class FakeStream:
    """Events 1..n; a cursor-less connection sends only the newest, as the live stream would."""

    def __init__(self, events):
        self.events = events  # {seq: [labels]}
        self.urls = []

    def __call__(self, url):
        self.urls.append(url)
        stream_ = self
        cursor = int(url.split("cursor=")[1]) if "cursor=" in url else None

        class WS:
            async def __aenter__(self):
                top = max(stream_.events)
                seqs = [top] if cursor is None else [s for s in sorted(stream_.events) if s > cursor]
                self.frames = [frame(0, t="#info")] + [frame(s, stream_.events[s]) for s in seqs]
                return self

            async def __aexit__(self, *exc):
                return False

            async def recv(self):
                if not self.frames:
                    await asyncio.sleep(3600)  # a live stream with nothing new
                return self.frames.pop(0)

        return WS()


@pytest.fixture
def table(tmp_path):
    cat = silver_catalog(tmp_path)
    return job.output_table(cat, location=f"{tmp_path}/warehouse/{job.TABLE}")


def run(table, after, events, chunk=2):
    return asyncio.run(job.run(table, after, chunk, connect=FakeStream(events), now=lambda: NOW))


def test_appends_every_label_after_the_watermark_in_chunks(table):
    events = {1: [label("at://a/app.bsky.feed.post/1")], 2: [label("did:plc:b", "spam"), label("at://c/x/1")],
              3: [label("at://a/app.bsky.feed.post/1", neg=True, cts="2026-11-14T00:00:00Z")]}
    stats = run(table, 0, events)
    assert (stats.head, stats.events, stats.labels, stats.commits) == (3, 3, 4, 2)
    rows = table.scan().to_arrow().sort_by([("seq", "ascending"), ("uri", "ascending")]).to_pylist()
    assert [(r["seq"], r["uri"], r["neg"]) for r in rows] == [
        (1, "at://a/app.bsky.feed.post/1", False), (2, "at://c/x/1", False), (2, "did:plc:b", False),
        (3, "at://a/app.bsky.feed.post/1", True)]
    assert rows[-1]["day"] == dt.date(2026, 11, 14) and rows[0]["ingested_at"] == NOW
    assert job.committed_seq(table) == 3


def test_resumes_and_stops_at_head(table):
    events = {1: [label("u1")], 2: [label("u2")], 3: [label("u3")]}
    run(table, 0, {1: events[1]})
    stats = run(table, job.committed_seq(table), events)
    assert (stats.after, stats.labels) == (1, 2)
    assert sorted(table.scan().to_arrow()["seq"].to_pylist()) == [1, 2, 3]
    assert run(table, 3, events).labels == 0  # caught up: nothing replayed


def test_dry_run_writes_nothing(table):
    stats = run(None, 0, {1: [label("u1")]})
    assert stats.labels == 1 and table.current_snapshot() is None


def test_unreadable_times_become_null_and_the_ingest_day():
    (r,) = stream.rows(5, [label("u", cts="not a time")], NOW)
    assert r["cts"] is None and r["day"] == NOW.date()


def test_backfill_loads_a_replay_after_the_watermark(table, tmp_path):
    replay = tmp_path / "replay"
    replay.mkdir()
    pq.write_table(pa.table({
        "seq": [1, 2, 3], "src": [stream.SOURCE] * 3, "uri": ["u1", "u2", "u3"], "cid": [None] * 3,
        "val": ["porn", "sexual", "spam"], "neg": [False, False, True],
        "cts": ["2026-11-13T10:00:00.000Z", "2026-11-13T10:00:00Z", "garbage"], "exp": [None, "2027-01-01T00:00:00Z", None],
    }), replay / "chunk-00000.parquet")
    run(table, 0, {1: [label("u1")]})  # the job already has seq 1
    after = job.committed_seq(table)
    for raw in backfill.batches(replay, after, 1):
        job.inc.append(table, backfill.convert(raw, NOW))
    rows = table.scan().to_arrow().sort_by("seq").to_pylist()
    assert [r["seq"] for r in rows] == [1, 2, 3]
    assert rows[1]["exp"] == dt.datetime(2027, 1, 1, tzinfo=dt.UTC)
    assert rows[2]["cts"] is None and rows[2]["day"] == NOW.date()
