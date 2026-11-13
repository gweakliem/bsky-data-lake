"""The search API: GET /search over the index, read-only (WP E3).

    uv run bsky-search serve                 # http://127.0.0.1:8000/search?q=...
    curl 'localhost:8000/search?q=midterm+results&k=5&lang=en'

In the cluster it is a Deployment behind a ClusterIP Service, reached with
kubectl port-forward. It reopens the index's newest version at most a minute
after the index job commits one.
"""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query

from bsky_search import store
from bsky_search.query import Filters, Mode, Searcher

RELOAD = dt.timedelta(minutes=1)


def create_app(searcher: Searcher | None = None) -> FastAPI:
    """With no searcher, one is built at startup from the environment (S3 and OPENROUTER_API_KEY)."""
    state: dict = {"searcher": searcher}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if state["searcher"] is not None:
            yield
            return
        from bsky_embed import spec
        from bsky_embed.client import OpenRouter

        table = store.connect(read_consistency=RELOAD).open_table(store.TABLE)
        async with OpenRouter.from_env(spec.MODEL, concurrency=4, batch=1, dimensions=spec.DIMS,
                                       provider=spec.PROVIDER) as client:
            state["searcher"] = Searcher(table, client)
            yield

    app = FastAPI(title="bsky search", lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict:
        table = state["searcher"].table
        return {"table": store.TABLE, "version": table.version, "rows": table.count_rows()}

    @app.get("/search")
    async def search(
        q: Annotated[str, Query(min_length=1, max_length=500)],
        k: Annotated[int, Query(ge=1, le=100)] = 10,
        mode: Mode = "hybrid",
        lang: Annotated[list[str] | None, Query(description="any of these languages")] = None,
        since: dt.date | None = None,
        half_life_days: Annotated[float | None, Query(gt=0)] = None,
    ) -> dict:
        try:
            filters = Filters(tuple(lang or ()), since)
            filters.where()
        except ValueError as e:
            raise HTTPException(400, str(e)) from e
        hits = await state["searcher"].search(q, k, mode, filters, half_life_days)
        return {"query": q, "mode": mode, "results": hits}

    return app
