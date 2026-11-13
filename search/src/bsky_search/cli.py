"""bsky-search: query the index from a terminal, or serve the API.

    uv run bsky-search query "2026 midterm results" -k 5 --lang en
    uv run bsky-search query "猫" --mode text
    uv run bsky-search serve --port 8000

Needs AWS credentials, BSKY_WAREHOUSE_BUCKET (or BSKY_LANCE_URI) and, except
for --mode text, OPENROUTER_API_KEY; locally they come from the repo's .env.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import sys


def _query(args: argparse.Namespace) -> None:
    from bsky_embed import spec
    from bsky_embed.client import OpenRouter
    from bsky_search import store
    from bsky_search.query import Filters, Searcher

    async def go():
        table = store.connect().open_table(store.TABLE)
        filters = Filters(tuple(args.lang or ()), args.since)
        if args.mode == "text":
            return await Searcher(table, None).search(args.query, args.k, "text", filters, args.half_life)
        async with OpenRouter.from_env(spec.MODEL, concurrency=1, batch=1, dimensions=spec.DIMS,
                                       provider=spec.PROVIDER) as client:
            return await Searcher(table, client).search(args.query, args.k, args.mode, filters, args.half_life)

    for hit in asyncio.run(go()):
        if args.json:
            print(json.dumps(hit, default=str, ensure_ascii=False))
        else:
            text = " ".join(hit["text"].split())
            print(f"{hit['score']:.4f}  {hit['created_at']:%Y-%m-%d %H:%M}  {','.join(hit['langs'] or [])}  {hit['uri']}")
            print(f"        {text[:200]}")


def _serve(args: argparse.Namespace) -> None:
    import uvicorn

    from bsky_search.api import create_app

    uvicorn.run(create_app(), host=args.host, port=args.port, log_level="info")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    q = sub.add_parser("query", help="search and print the results")
    q.add_argument("query")
    q.add_argument("-k", type=int, default=10)
    q.add_argument("--mode", choices=["hybrid", "vector", "text"], default="hybrid")
    q.add_argument("--lang", action="append", help="any of these languages (repeatable)")
    q.add_argument("--since", type=dt.date.fromisoformat, help="posts from this day on (YYYY-MM-DD)")
    q.add_argument("--half-life", type=float, help="recency boost: score halves every this many days of age")
    q.add_argument("--json", action="store_true", help="one JSON object per result")
    s = sub.add_parser("serve", help="run the HTTP API")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    from dotenv import load_dotenv

    from bsky_enrich import lake

    load_dotenv(lake.REPO / ".env")
    try:
        (_query if args.command == "query" else _serve)(args)
    except BrokenPipeError:
        sys.exit(0)


if __name__ == "__main__":
    main()
