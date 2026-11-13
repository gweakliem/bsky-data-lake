import datetime as dt

import numpy as np
import pyarrow as pa
import pytest

from bsky_embed import job, spec
from bsky_enrich import incremental as inc
from bsky_enrich import testing as fake
from bsky_search import store


def vector(seed: int) -> np.ndarray:
    return spec.normalize(np.random.default_rng(seed).standard_normal((1, spec.DIMS)))[0]


def add_embeddings(cat, rows: list[tuple[str, dt.datetime, int]], cid: str = "c", spec_version: str = spec.SPEC_VERSION) -> None:
    """post_embeddings rows by (uri, bronze_ingested_at, vector seed)."""
    n = len(rows)
    stored = spec.encode_vectors(np.stack([vector(s) for _, _, s in rows]))
    cat.load_table(f"{inc.SILVER}.{job.TABLE}").append(pa.table({
        "uri": [u for u, _, _ in rows], "cid": [cid] * n, "day": pa.array([fake.DAY] * n, pa.date32()),
        "model": [spec.MODEL] * n, "spec_version": [spec_version] * n,
        "embedded_at": pa.array([fake.T0] * n, job.TS), "bronze_ingested_at": pa.array([m for _, m, _ in rows], job.TS),
        "vector": pa.array(stored, pa.binary()), "provider": ["Nebius"] * n,
    }))


@pytest.fixture
def cat(tmp_path):
    c = fake.silver_catalog(tmp_path)
    job.output_table(c, location=f"file://{tmp_path}/warehouse/post_embeddings")
    return c


@pytest.fixture
def db(tmp_path):
    return store.connect(str(tmp_path / "lance"))
