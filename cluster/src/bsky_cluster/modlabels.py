"""Bluesky's moderation labels, replayed from mod.bsky.app into local Parquet.

    uv run python -m bsky_cluster.modlabels [--workers 4]

Our firehose data has only authors' self-labels. Bluesky's own moderation service
publishes its labels (porn, sexual, nudity, spam, !takedown, ...) on a public
stream, com.atproto.label.subscribeLabels, which replays from any sequence number:
about 46M labels since April 2023. Each connection is served at about 2,000
labels/s, so the history is split into chunks of CHUNK sequence numbers fetched
by a few connections at once (about 1.6 h with 4).

Writes data/cluster/modlabels/chunk-<n>.parquet for every finished chunk and
tail-<n>.parquet for the one that was still filling when the replay caught up.
A rerun skips finished chunks and redoes the tail, so it resumes and catches up.

A label applies to a post (at://did/app.bsky.feed.post/rkey) or an account (the
bare did, or its profile record). neg=True retracts an earlier label, and exp
is an optional expiry; active() resolves both.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import io
import logging
import time

import cbor2
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import websockets

from bsky_cluster.fetch import DATA

log = logging.getLogger("bsky_cluster.modlabels")
URL = "wss://mod.bsky.app/xrpc/com.atproto.label.subscribeLabels"
SOURCE = "did:plc:ar7c4by46qjdydhdevvrndac"  # mod.bsky.app
OUT = DATA / "modlabels"
CHUNK = 1_000_000
SCHEMA = pa.schema([("seq", pa.int64()), ("src", pa.string()), ("uri", pa.string()), ("cid", pa.string()),
                    ("val", pa.string()), ("neg", pa.bool_()), ("cts", pa.string()), ("exp", pa.string())])


def frames(frame: bytes) -> tuple[dict, dict]:
    """An event-stream frame is two DAG-CBOR objects: a header ({op, t}) and the body."""
    d = cbor2.CBORDecoder(io.BytesIO(frame))
    return d.decode(), d.decode()


async def head() -> int:
    """The newest sequence number: the first frame of a live (cursor-less) subscription."""
    async with websockets.connect(URL, max_size=None) as ws:
        while True:
            h, b = frames(await asyncio.wait_for(ws.recv(), 120))
            if h.get("t") == "#labels":
                return b["seq"]


async def fetch_chunk(n: int, stop: int) -> None:
    """Labels with seq in [n*CHUNK, (n+1)*CHUNK), or up to `stop` for the tail chunk."""
    start, end = n * CHUNK, min((n + 1) * CHUNK, stop + 1)
    rows: dict[str, list] = {f.name: [] for f in SCHEMA}
    t0 = time.monotonic()
    async with websockets.connect(f"{URL}?cursor={max(start - 1, 0)}", max_size=None, ping_timeout=120) as ws:
        while True:
            h, b = frames(await asyncio.wait_for(ws.recv(), 120))
            if h.get("op") == -1:
                raise RuntimeError(f"chunk {n}: stream error {b}")
            if h.get("t") != "#labels":
                log.info("chunk %d: %s %s", n, h.get("t"), b)
                continue
            seq = b["seq"]
            if seq < start:
                continue
            if seq >= end:
                break
            for label in b["labels"]:
                rows["seq"].append(seq)
                for k in ("src", "uri", "cid", "val", "cts", "exp"):
                    rows[k].append(label.get(k))
                rows["neg"].append(bool(label.get("neg", False)))
            if seq == end - 1:
                break
    full = end == (n + 1) * CHUNK
    pq.write_table(pa.table(rows, schema=SCHEMA), OUT / f"{'chunk' if full else 'tail'}-{n:05d}.parquet",
                   compression="zstd")
    log.info("chunk %d%s: %d labels in %.0f s", n, "" if full else " (tail)", len(rows["seq"]), time.monotonic() - t0)


async def replay(workers: int) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for tail in OUT.glob("tail-*.parquet"):
        tail.unlink()
    top = await head()
    todo = [n for n in range(top // CHUNK + 1) if not (OUT / f"chunk-{n:05d}.parquet").exists()]
    log.info("head seq %d; %d chunks to fetch with %d connections", top, len(todo), workers)
    queue: asyncio.Queue[int] = asyncio.Queue()
    for n in todo:
        queue.put_nowait(n)

    async def worker() -> None:
        while not queue.empty():
            n = queue.get_nowait()
            for attempt in range(5):
                try:
                    await fetch_chunk(n, top)
                    break
                except (OSError, asyncio.TimeoutError, websockets.WebSocketException) as e:
                    log.warning("chunk %d attempt %d: %r", n, attempt + 1, e)
                    await asyncio.sleep(10 * (attempt + 1))
            else:
                raise RuntimeError(f"chunk {n} failed 5 times")

    await asyncio.gather(*(worker() for _ in range(workers)))


TID_CHARS = "234567abcdefghijklmnopqrstuvwxyz"


def tid(when: dt.datetime) -> str:
    """The smallest TID (record key) for a time: microseconds above 10 clock-id bits, sortable base32."""
    v = int(when.timestamp() * 1e6) << 10
    return "".join(TID_CHARS[(v >> (5 * i)) & 31] for i in range(12, -1, -1))


def active(vals: list[str], posts_from: dt.datetime, posts_to: dt.datetime, now: dt.datetime | None = None) -> pa.Table:
    """(uri, val) for labels from mod.bsky.app that stand: the latest event for each wins, unless it
    retracts the label or has expired. Post labels only for posts created in [posts_from, posts_to),
    by their record key; labels on accounts and profiles always."""
    now = (now or dt.datetime.now(dt.UTC)).strftime("%Y-%m-%dT%H:%M:%S")
    con = duckdb.connect()
    return con.execute(f"""
        SELECT uri, val FROM (
            SELECT uri, val, neg, exp,
                   row_number() OVER (PARTITION BY uri, val ORDER BY seq DESC) AS r
            FROM read_parquet('{OUT}/*.parquet')
            WHERE src = '{SOURCE}' AND val IN (SELECT unnest(?))
              AND (uri NOT LIKE '%/app.bsky.feed.post/%' OR split_part(uri, '/', 5) BETWEEN ? AND ?))
        WHERE r = 1 AND NOT neg AND (exp IS NULL OR exp > ?)""",
                       [vals, tid(posts_from), tid(posts_to), now]).fetch_arrow_table()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=4)
    asyncio.run(replay(p.parse_args().workers))


if __name__ == "__main__":
    main()
