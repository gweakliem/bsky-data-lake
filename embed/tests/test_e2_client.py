"""The OpenRouter client and the stored-vector spec, against a fake transport (no network)."""

import asyncio
import base64

import httpx
import numpy as np
import pytest

from bsky_embed import spec
from bsky_embed.client import Fatal, OpenRouter


def fake(handler, **kwargs) -> tuple[OpenRouter, list[float]]:
    """A client whose requests go to handler(request, body) and whose sleeps are recorded, not waited."""
    waits: list[float] = []

    async def sleep(s: float) -> None:
        waits.append(s)

    def route(request: httpx.Request) -> httpx.Response:
        import json

        return handler(request, json.loads(request.content) if request.content else None)

    kwargs.setdefault("model", "test/model")
    client = OpenRouter("key", transport=httpx.MockTransport(route), sleep=sleep, **kwargs)
    return client, waits


def ok(body: dict, dims: int = 4, b64: bool = False) -> httpx.Response:
    """One vector per input, whose first value is the input's length, so order can be checked."""
    data = []
    for i, t in enumerate(body["input"]):
        v = np.zeros(dims, dtype="<f4")
        v[0] = len(t)
        e = base64.b64encode(v.tobytes()).decode() if b64 else v.tolist()
        data.append({"index": i, "embedding": e})
    data.reverse()  # the API doesn't promise order; the client sorts by index
    return httpx.Response(200, json={"data": data, "usage": {"prompt_tokens": len(body["input"]), "cost": 0.001}})


def run(client: OpenRouter, texts: list[str]) -> np.ndarray:
    async def go():
        async with client:
            return await client.embed(texts)

    return asyncio.run(go())


def test_batches_keep_input_order_and_tally_usage():
    sizes = []

    def handler(request, body):
        sizes.append(len(body["input"]))
        return ok(body)

    client, _ = fake(handler, batch=3)
    texts = ["a" * n for n in range(1, 8)]
    v = run(client, texts)
    assert sorted(sizes) == [1, 3, 3]
    assert v[:, 0].tolist() == list(range(1, 8))
    assert client.usage.prompt_tokens == 7
    assert client.usage.requests == 3


def test_empty_text_is_sent_as_a_space():
    seen = []

    def handler(request, body):
        seen.extend(body["input"])
        return ok(body)

    run(fake(handler)[0], ["", "x"])
    assert seen == [" ", "x"]


def test_base64_vectors_are_decoded():
    client, _ = fake(lambda request, body: ok(body, b64=True), encoding_format="base64")
    assert run(client, ["abc"])[0, 0] == 3


def test_429_honors_retry_after_then_succeeds():
    calls = []

    def handler(request, body):
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return ok(body)

    client, waits = fake(handler)
    run(client, ["x"])
    assert waits == [7.0]
    assert client.usage.retries == 1


def test_5xx_200_without_data_and_dropped_connections_back_off():
    responses = iter(["drop", httpx.Response(502), httpx.Response(200, json={"error": {"code": 502}}), "ok"])

    def handler(request, body):
        r = next(responses)
        if r == "drop":
            raise httpx.ConnectError("reset")
        return ok(body) if r == "ok" else r

    client, waits = fake(handler)
    run(client, ["x"])
    assert waits == [1.0, 2.0, 4.0]


@pytest.mark.parametrize("status", [400, 401, 402, 403])
def test_client_errors_and_exhausted_credit_are_fatal(status):
    client, waits = fake(lambda request, body: httpx.Response(status, text="no"))
    with pytest.raises(Fatal, match=str(status)):
        run(client, ["x"])
    assert waits == []


def test_a_batch_that_keeps_failing_is_fatal():
    client, waits = fake(lambda request, body: httpx.Response(503), max_attempts=3)
    with pytest.raises(Fatal, match="kept failing"):
        run(client, ["x"])
    assert len(waits) == 3


def test_free_variants_are_refused():
    with pytest.raises(ValueError, match="free"):
        OpenRouter("key", "qwen/qwen3-embedding-8b:free")


def test_stored_vectors_are_truncated_then_normalized_float16():
    v = np.zeros((2, spec.DIMS + 8), dtype=np.float32)
    v[0, 0], v[0, spec.DIMS] = 3.0, 100.0  # beyond DIMS: dropped before normalizing
    v[1, :2] = [3.0, 4.0]
    stored = spec.encode_vectors(v)
    assert [len(b) for b in stored] == [2 * spec.DIMS] * 2
    a, b = (spec.decode_vector(s) for s in stored)
    assert a[0] == 1.0 and np.count_nonzero(a) == 1
    assert np.allclose(b[:2], [0.6, 0.8], atol=1e-3)
    assert np.isclose(np.linalg.norm(b), 1.0, atol=1e-3)


def test_too_narrow_vectors_are_rejected():
    with pytest.raises(ValueError):
        spec.encode_vectors(np.ones((1, spec.DIMS - 1), dtype=np.float32))
