"""bsky-embed: embed new silver posts into bsky_silver.post_embeddings (WP E2).

Reads posts one silver window at a time after the table's own watermark (see
bsky_enrich.incremental), skips posts the length rule rejects, embeds each
distinct text once per chunk, and appends a chunk at a time, so a crash loses at
most one chunk and a rerun picks up where the last commit ended.

    uv run bsky-embed --dry-run              # what a run would read and embed; no API calls, no writes
    uv run bsky-embed --max-posts 10000      # stop after a chunk once this many are embedded
    uv run bsky-embed                        # until caught up with silver

Needs AWS credentials, BSKY_WAREHOUSE_BUCKET and OPENROUTER_API_KEY (the repo's
.env locally; a Kubernetes Secret in the cluster).
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import resource
import sys
import time
from dataclasses import asdict, dataclass
from typing import Protocol

import numpy as np
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import BinaryType, DateType, NestedField, StringType, TimestamptzType

from bsky_embed import spec
from bsky_enrich import incremental as inc

log = logging.getLogger("bsky_embed.job")

TABLE = "post_embeddings"
SCHEMA = Schema(
    NestedField(1, "uri", StringType(), required=False),
    NestedField(2, "cid", StringType(), required=False),
    NestedField(3, "day", DateType(), required=False),
    NestedField(4, "model", StringType(), required=False),
    NestedField(5, "spec_version", StringType(), required=False),
    NestedField(6, "embedded_at", TimestamptzType(), required=False),
    NestedField(7, "bronze_ingested_at", TimestamptzType(), required=False),
    NestedField(8, "vector", BinaryType(), required=False),
)
TS = pa.timestamp("us", tz="UTC")


class Embedder(Protocol):
    async def embed(self, texts: list[str], prompt: str = "") -> np.ndarray: ...


@dataclass
class Stats:
    windows: int = 0
    read: int = 0
    skipped: int = 0  # the length rule
    embedded: int = 0  # rows written
    texts: int = 0  # distinct texts sent to the API
    chunks: int = 0


def output_table(cat: Catalog, namespace: str = inc.SILVER, location: str | None = None) -> Table:
    return inc.ensure_table(cat, f"{namespace}.{TABLE}", SCHEMA, location or inc.location(TABLE))


def rows(posts: list[dict], vectors: dict[str, bytes], embedded_at: dt.datetime) -> pa.Table:
    n = len(posts)
    return pa.table({
        "uri": [p["uri"] for p in posts],
        "cid": [p["cid"] for p in posts],
        "day": pa.array([p["day"] for p in posts], pa.date32()),
        "model": [spec.MODEL] * n,
        "spec_version": [spec.SPEC_VERSION] * n,
        "embedded_at": pa.array([embedded_at] * n, TS),
        "bronze_ingested_at": pa.array([p["bronze_ingested_at"] for p in posts], TS),
        "vector": pa.array([vectors[spec.text(p)] for p in posts], pa.binary()),
    })


async def run(
    cat: Catalog,
    out: Table | None,
    client: Embedder | None,
    *,
    namespace: str = inc.SILVER,
    chunk_posts: int = 5000,
    max_posts: int | None = None,
) -> Stats:
    """Embed and append until caught up or past max_posts.

    client=None is a dry run; out=None means the table doesn't exist yet (dry run only).
    """
    stats = Stats()
    after = inc.committed_through(out) if out is not None else None
    log.info("watermark %s", after.isoformat() if after else "none (empty table)")
    for w in inc.windows(cat, after, namespace):
        stats.windows += 1
        posts = inc.read_posts(cat, w, namespace)
        stats.read += posts.num_rows
        for chunk in inc.chunks(posts, chunk_posts):
            batch = chunk.to_pylist()
            keep = [p for p in batch if spec.eligible(p)]
            stats.skipped += len(batch) - len(keep)
            if not keep:
                continue
            texts = list(dict.fromkeys(spec.text(p) for p in keep))
            stats.texts += len(texts)
            stats.embedded += len(keep)
            stats.chunks += 1
            if client is not None:
                vectors = dict(zip(texts, spec.encode_vectors(await client.embed(texts, spec.DOC_PROMPT)), strict=True))
                inc.append(out, rows(keep, vectors, dt.datetime.now(dt.UTC)))
                log.info("window (%s, %s]: %d posts committed, %d so far", w.after, w.through, len(keep), stats.embedded)
            if max_posts is not None and stats.embedded >= max_posts:
                return stats
    return stats


def _peak_rss_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 2**20 if sys.platform == "darwin" else peak / 2**10  # bytes on macOS, KiB on Linux


def _cpu_seconds() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


async def _main(args: argparse.Namespace) -> Stats:
    from bsky_embed.client import OpenRouter

    cat = inc.catalog()
    if args.dry_run:  # never creates the table
        name = f"{args.namespace}.{TABLE}"
        out = cat.load_table(name) if cat.table_exists(name) else None
        return await run(cat, out, None, namespace=args.namespace, chunk_posts=args.chunk_posts, max_posts=args.max_posts)
    out = output_table(cat, args.namespace)
    async with OpenRouter.from_env(spec.MODEL, concurrency=args.concurrency, batch=args.batch, dimensions=spec.DIMS) as client:
        before = await client.key_info()
        log.info("key: limit %s, remaining %s, usage today %s", before.get("limit"), before.get("limit_remaining"), before.get("usage_daily"))
        try:
            stats = await run(cat, out, client, namespace=args.namespace, chunk_posts=args.chunk_posts, max_posts=args.max_posts)
        finally:
            u = client.usage
            log.info("api: %d requests, %d retries, %d prompt tokens, $%.4f", u.requests, u.retries, u.prompt_tokens, u.cost_usd)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--chunk-posts", type=int, default=5000, help="posts per commit, at least (default 5000)")
    parser.add_argument("--max-posts", type=int, help="stop after the chunk that reaches this many")
    parser.add_argument("--concurrency", type=int, default=20, help="requests in flight (default 20)")
    parser.add_argument("--batch", type=int, default=96, help="texts per request (default 96)")
    parser.add_argument("--namespace", default=inc.SILVER)
    parser.add_argument("--dry-run", action="store_true", help="read and count only: no API calls, no writes")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)

    from dotenv import load_dotenv

    from bsky_enrich import lake

    load_dotenv(lake.REPO / ".env")
    start = time.monotonic()
    stats = asdict(asyncio.run(_main(args)))
    seconds = time.monotonic() - start
    cpu = _cpu_seconds()
    per_100k = round(cpu / stats["embedded"] * 100_000, 1) if stats["embedded"] else None
    stats |= {"dry_run": args.dry_run, "seconds": round(seconds, 1), "cpu_seconds": round(cpu, 1),
              "cpu_seconds_per_100k": per_100k, "peak_rss_mb": round(_peak_rss_mb())}
    # One JSON line per run, for a CloudWatch metric filter (D6) and the E2 acceptance numbers.
    log.info("run %s", json.dumps(stats))


if __name__ == "__main__":
    main()
