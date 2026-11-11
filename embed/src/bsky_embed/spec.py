"""Embedding spec v1: which posts are embedded, the text, the model and the stored vector.

Changing any of these is a new SPEC_VERSION, since vectors are only comparable
within one (the post_embeddings_current view selects one version). The model is
fixed inside a version, so the view needs no model filter.

The stored vector is the model's output truncated to DIMS, L2-normalized after
truncating (truncation breaks unit length), as DIMS little-endian float16
values in a binary column. Iceberg has no float16 type; this is half the size
of list<float32>.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from bsky_enrich.spec import eligible  # same length rule as classification

__all__ = ["DIMS", "DOC_PROMPT", "DTYPE", "MODEL", "SPEC_VERSION", "decode_vector", "eligible", "encode_vectors", "text"]

SPEC_VERSION = "v1"
MODEL = "qwen/qwen3-embedding-8b"  # paid variant: never a :free id (20/min, 1,000/day caps)
DIMS = 1024
DOC_PROMPT = ""  # Qwen3-Embedding puts its instruction on queries only
DTYPE = np.dtype("<f2")


def text(row: dict[str, Any]) -> str:
    """text_spec v1: post text, image alt texts and the link title, one per line; never the reply parent."""
    parts = [row.get("text") or "", *(row.get("image_alts") or []), row.get("external_title") or ""]
    return "\n".join(p.strip() for p in parts if p and p.strip())


def normalize(v: np.ndarray) -> np.ndarray:
    return v / np.linalg.norm(v, axis=1, keepdims=True).clip(1e-12)


def encode_vectors(v: np.ndarray) -> list[bytes]:
    """Rows of model output (at least DIMS wide) to the stored bytes, one per row."""
    if v.ndim != 2 or v.shape[1] < DIMS:
        raise ValueError(f"expected (n, >= {DIMS}) vectors, got {v.shape}")
    stored = normalize(v[:, :DIMS].astype(np.float32)).astype(DTYPE)
    return [row.tobytes() for row in stored]


def decode_vector(b: bytes) -> np.ndarray:
    return np.frombuffer(b, dtype=DTYPE).astype(np.float32)
