"""mod.bsky.app's label stream (com.atproto.label.subscribeLabels) and the silver rows it becomes.

The stream replays from any sequence number: a subscription with `cursor=N`
sends every event after N, then stays open for live ones. Each WebSocket frame
is two DAG-CBOR objects, a header ({op, t}) and a body; `#labels` bodies carry a
`seq` and one or more labels. One connection is served at about 2,000 labels/s.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import io
from collections.abc import AsyncIterator, Callable
from typing import Any

import cbor2
import pyarrow as pa
import websockets

URL = "wss://mod.bsky.app/xrpc/com.atproto.label.subscribeLabels"
SOURCE = "did:plc:ar7c4by46qjdydhdevvrndac"  # mod.bsky.app's DID: every label it signs has this src
RECV_TIMEOUT = 120.0

TS = pa.timestamp("us", tz="UTC")
ARROW = pa.schema([
    ("seq", pa.int64()), ("src", pa.string()), ("uri", pa.string()), ("cid", pa.string()), ("val", pa.string()),
    ("neg", pa.bool_()), ("cts", TS), ("exp", TS), ("day", pa.date32()), ("ingested_at", TS),
])

Connect = Callable[[str], Any]  # websockets.connect, or a fake in tests


def decode(frame: bytes) -> tuple[dict, dict]:
    d = cbor2.CBORDecoder(io.BytesIO(frame))
    return d.decode(), d.decode()


def timestamp(value: str | None) -> dt.datetime | None:
    """A label's ISO 8601 time as UTC, or None when it is missing or unreadable."""
    if not value:
        return None
    try:
        t = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    return t.replace(tzinfo=dt.UTC) if t.tzinfo is None else t.astimezone(dt.UTC)


def rows(seq: int, labels: list[dict], ingested_at: dt.datetime) -> list[dict]:
    """One row per label. `day` (the partition) is the label's creation day, or the ingest day without one."""
    out = []
    for label in labels:
        cts = timestamp(label.get("cts"))
        out.append({"seq": seq, "src": label.get("src"), "uri": label.get("uri"), "cid": label.get("cid"),
                    "val": label.get("val"), "neg": bool(label.get("neg", False)), "cts": cts,
                    "exp": timestamp(label.get("exp")), "day": (cts or ingested_at).date(),
                    "ingested_at": ingested_at})
    return out


def table(batch: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(batch, schema=ARROW)


async def head(connect: Connect = websockets.connect) -> int | None:
    """The newest sequence number: the first event of a live (cursor-less) subscription, or None if
    no label arrives within RECV_TIMEOUT."""
    async with connect(URL) as ws:
        while True:
            try:
                h, b = decode(await asyncio.wait_for(ws.recv(), RECV_TIMEOUT))
            except TimeoutError:
                return None
            if h.get("op") == -1:
                raise RuntimeError(f"label stream error: {b}")
            if h.get("t") == "#labels":
                return int(b["seq"])


async def replay(after: int, through: int, connect: Connect = websockets.connect) -> AsyncIterator[tuple[int, list[dict]]]:
    """(seq, labels) for every event with after < seq <= through, in order."""
    async with connect(f"{URL}?cursor={after}") as ws:
        while True:
            h, b = decode(await asyncio.wait_for(ws.recv(), RECV_TIMEOUT))
            if h.get("op") == -1:
                raise RuntimeError(f"label stream error: {b}")
            if h.get("t") != "#labels":  # #info, e.g. OutdatedCursor
                continue
            seq = int(b["seq"])
            if seq <= after:
                continue
            if seq > through:
                return
            yield seq, b.get("labels", [])
            if seq == through:
                return
