import datetime as dt

import pyarrow as pa
import pyarrow.parquet as pq

from bsky_cluster import filters, modlabels

T = dt.datetime(2026, 11, 13, 12, tzinfo=dt.UTC)


def post(when: dt.datetime, did: str = "did:plc:a") -> str:
    return f"at://{did}/app.bsky.feed.post/{modlabels.tid(when)}"


def test_tid_round_trips_and_sorts_by_time():
    assert filters.tid_time(modlabels.tid(T)) == T
    assert modlabels.tid(T) < modlabels.tid(T + dt.timedelta(microseconds=1))
    assert filters.tid_time("not-a-tid") is None


def test_active_resolves_retractions_expiry_and_the_post_window(tmp_path, monkeypatch):
    monkeypatch.setattr(modlabels, "OUT", tmp_path)
    src = modlabels.SOURCE
    retracted, expired, kept, old = post(T), post(T, "did:plc:b"), post(T, "did:plc:c"), post(T - dt.timedelta(days=90))
    rows = [  # seq, src, uri, val, neg, exp
        (1, src, retracted, "porn", False, None),
        (2, src, retracted, "porn", True, None),          # retracted later
        (3, src, expired, "sexual", False, "2026-11-14T00:00:00Z"),
        (4, src, kept, "nudity", False, None),
        (5, "did:plc:other", post(T, "did:plc:d"), "porn", False, None),  # another labeller
        (6, src, old, "porn", False, None),               # post outside the window
        (7, src, "did:plc:e", "porn", False, None),       # account label: no window
        (8, src, kept, "spam", False, None),              # not an adult value
    ]
    cols = list(zip(*rows))
    pq.write_table(pa.table({"seq": cols[0], "src": cols[1], "uri": cols[2], "cid": [None] * len(rows),
                             "val": cols[3], "neg": cols[4], "cts": ["2026-11-13T00:00:00Z"] * len(rows),
                             "exp": cols[5]}, schema=modlabels.SCHEMA), tmp_path / "chunk-00000.parquet")
    got = modlabels.active(filters.ADULT_LABELS, T - dt.timedelta(days=30), T + dt.timedelta(days=1),
                           now=dt.datetime(2026, 11, 15, tzinfo=dt.UTC))
    assert sorted(zip(got["uri"].to_pylist(), got["val"].to_pylist())) == sorted([(kept, "nudity"), ("did:plc:e", "porn")])


def test_suffix_keeps_default_names():
    assert filters.suffix("exclude") == ""
    assert filters.suffix("only") == "-adult-only"
