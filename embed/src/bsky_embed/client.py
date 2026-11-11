"""OpenRouter embeddings client: batched, bounded concurrency, retries.

- 5xx, dropped connections and 429s are retried with exponential backoff; a 429
  honors Retry-After. Upstream providers throttle paid models this way.
- 400, 401, 402 and 403 raise Fatal at once. A 402 means credits or the key's
  limit are exhausted, which retrying can't fix.
- A batch that keeps failing raises Fatal after max_attempts.

Vectors come back as the model returns them (not truncated or normalized); see
spec.encode_vectors. They are requested as base64 float32 by default: the same
values as JSON floats (checked 2026-11-11) in about half the bytes, and far
cheaper to parse. Bytes matter because the cluster's traffic goes through a NAT
gateway billed per GB. `dimensions` asks the API for shorter vectors; its output
is not the same as truncating (cosine about 0.998 on 2026-11-11).
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx
import numpy as np

log = logging.getLogger(__name__)

BASE_URL = "https://openrouter.ai/api/v1"
FATAL_STATUS = {400, 401, 402, 403}


class Fatal(RuntimeError):
    """An error retrying won't fix: bad request, auth, or credits exhausted."""


@dataclass
class Usage:
    requests: int = 0
    retries: int = 0
    prompt_tokens: int = 0
    cost_usd: float = 0.0


def _embedding(e: list[float] | str) -> np.ndarray:
    if isinstance(e, str):
        return np.frombuffer(base64.b64decode(e), dtype="<f4")
    return np.asarray(e, dtype=np.float32)


def _retry_after(r: httpx.Response) -> float | None:
    try:
        return max(0.0, float(r.headers["retry-after"]))
    except (KeyError, ValueError):
        return None


class OpenRouter:
    def __init__(
        self,
        api_key: str,
        model: str,
        *,
        concurrency: int = 20,
        batch: int = 96,
        max_attempts: int = 8,
        encoding_format: str = "base64",
        dimensions: int | None = None,
        base_url: str = BASE_URL,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if model.endswith(":free"):
            raise ValueError(f"{model}: free variants are capped at 20 requests/min and 1,000/day")
        self.model = model
        self.batch = batch
        self.max_attempts = max_attempts
        self.encoding_format = encoding_format
        self.dimensions = dimensions
        self.usage = Usage()
        self._gate = asyncio.Semaphore(concurrency)
        self._sleep = sleep
        self._http = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=120,
            transport=transport,
        )

    @classmethod
    def from_env(cls, model: str, **kwargs) -> OpenRouter:
        return cls(os.environ["OPENROUTER_API_KEY"], model, **kwargs)

    async def __aenter__(self) -> OpenRouter:
        return self

    async def __aexit__(self, *exc) -> None:
        await self._http.aclose()

    async def key_info(self) -> dict:
        """The key's limit, remaining credit and usage (GET /key)."""
        r = await self._http.get("/key")
        r.raise_for_status()
        return r.json()["data"]

    async def embed(self, texts: list[str], prompt: str = "") -> np.ndarray:
        """One row per text, in order."""
        chunks = [texts[i : i + self.batch] for i in range(0, len(texts), self.batch)]
        parts = await asyncio.gather(*(self._batch(i, c, prompt) for i, c in enumerate(chunks)))
        return np.vstack(parts) if parts else np.empty((0, 0), dtype=np.float32)

    async def _batch(self, i: int, texts: list[str], prompt: str) -> np.ndarray:
        # The API rejects empty strings (a link post can have no text and no title).
        body = {"model": self.model, "input": [(prompt + t) or " " for t in texts], "encoding_format": self.encoding_format}
        if self.dimensions:
            body["dimensions"] = self.dimensions
        for attempt in range(self.max_attempts):
            wait = min(2.0**attempt, 60.0)
            try:
                async with self._gate:
                    self.usage.requests += 1
                    r = await self._http.post("/embeddings", json=body)
            except httpx.TransportError as e:
                log.warning("batch %d: %s, retrying", i, type(e).__name__)
            else:
                if r.status_code in FATAL_STATUS:
                    raise Fatal(f"{self.model}: {r.status_code} {r.text[:300]}")
                try:
                    j = r.json() if r.status_code == 200 else {}
                except ValueError:
                    j = {}
                if "data" in j:
                    data = sorted(j["data"], key=lambda d: d["index"])
                    if len(data) != len(texts):
                        raise Fatal(f"{self.model}: {len(data)} embeddings for {len(texts)} inputs")
                    usage = j.get("usage") or {}
                    self.usage.prompt_tokens += usage.get("prompt_tokens") or 0
                    self.usage.cost_usd += usage.get("cost") or 0.0
                    return np.vstack([_embedding(d["embedding"]) for d in data])
                # A 200 without data is how OpenRouter reports some upstream errors.
                if r.status_code == 429:
                    wait = _retry_after(r) or wait
                log.warning("batch %d: %s, retrying in %.0f s", i, r.status_code, wait)
            self.usage.retries += 1
            await self._sleep(wait)
        raise Fatal(f"{self.model}: batch {i} kept failing after {self.max_attempts} attempts")
