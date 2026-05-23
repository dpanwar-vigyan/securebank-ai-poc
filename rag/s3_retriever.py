"""
S3 + NumPy vector retriever — Lambda-compatible replacement for ChromaDB.

On cold start: downloads vectors.npy + metadata.json from S3 (~1-2s, ~4MB).
On warm invocations: uses module-level cache — no S3 call, <5ms search.

Cost: ~$0.0001/month (S3 storage for 4MB). No external accounts needed.
"""

import io
import json
import os
import time

import boto3
import numpy as np
import rag.config  # noqa: F401

S3_BUCKET    = os.getenv("S3_VECTORS_BUCKET", "")
VECTORS_KEY  = "vectors.npy"
METADATA_KEY = "metadata.json"
TOP_K        = 20

# How long (seconds) before the in-memory vector cache is considered stale.
# Default 300s (5 min) — matches the Lambda warming interval so new docs
# are searchable within one warm cycle after the pipeline completes.
# Set VECTOR_CACHE_TTL_S=0 to disable TTL (reload only on cold start).
CACHE_TTL_S = int(os.getenv("VECTOR_CACHE_TTL_S", "300"))

# Module-level cache — persists for the lifetime of the Lambda container.
_vectors:   np.ndarray | None = None
_metadata:  list[dict] | None = None
_loaded_at: float             = 0.0   # epoch seconds of last S3 load


def _load(force: bool = False):
    """
    Download vectors + metadata from S3.

    Skips the download (returns immediately) when:
      - Already loaded AND
      - Cache is within TTL AND
      - force=False

    Pass force=True from /pipeline/step/complete to make a newly ingested
    document immediately searchable without waiting for TTL expiry.
    """
    global _vectors, _metadata, _loaded_at

    now = time.time()
    cache_age = now - _loaded_at

    if _vectors is not None and not force:
        if CACHE_TTL_S == 0 or cache_age < CACHE_TTL_S:
            return   # warm and fresh — skip S3 download

    if not S3_BUCKET:
        raise S3RetrieverUnavailableError("S3_VECTORS_BUCKET not set")

    t0 = time.time()
    s3 = boto3.client("s3")

    obj = s3.get_object(Bucket=S3_BUCKET, Key=VECTORS_KEY)
    _vectors = np.load(io.BytesIO(obj["Body"].read())).astype(np.float32)

    obj = s3.get_object(Bucket=S3_BUCKET, Key=METADATA_KEY)
    _metadata = json.loads(obj["Body"].read().decode())

    _loaded_at = time.time()
    elapsed    = round(_loaded_at - t0, 2)
    reason     = "forced reload" if force else ("cold start" if cache_age >= 9e9 else f"TTL expired ({cache_age:.0f}s old)")
    print(f"S3 retriever loaded: {_vectors.shape[0]} vectors × {_vectors.shape[1]} dims in {elapsed}s [{reason}]")


def force_reload():
    """Force an immediate reload from S3. Call after pipeline writes new vectors."""
    _load(force=True)


class S3NumpyRetriever:
    """
    Cosine similarity search over all vectors loaded from S3.
    Same retrieve() interface as ChromaDB path — drop-in replacement.
    """

    def __init__(self, embed_fn):
        self.embed = embed_fn
        _load()

    def retrieve(self, query: str, filters: dict) -> list[dict]:
        # 1. Embed the query
        query_vec  = np.array(self.embed([query])[0], dtype=np.float32)
        query_norm = query_vec / (np.linalg.norm(query_vec) or 1.0)

        # 2. Apply metadata filters to get candidate indices
        if filters:
            indices = [
                i for i, m in enumerate(_metadata)
                if all(str(m.get(k, "")) == str(v) for k, v in filters.items())
            ]
            if not indices:
                indices = list(range(len(_metadata)))   # fallback: ignore filter
        else:
            indices = list(range(len(_metadata)))

        # 3. Cosine similarity over filtered subset (vectorised — fast even for 50K vecs)
        subset   = _vectors[indices]                   # shape (M, D)
        norms    = np.linalg.norm(subset, axis=1, keepdims=True)
        norms    = np.where(norms == 0, 1.0, norms)
        scores   = (subset / norms) @ query_norm       # shape (M,)

        # 4. Top-K
        k        = min(TOP_K, len(indices))
        top_local = np.argpartition(scores, -k)[-k:]
        top_local = top_local[np.argsort(scores[top_local])[::-1]]

        chunks = []
        for local_idx in top_local:
            global_idx = indices[local_idx]
            meta = dict(_metadata[global_idx])
            text = meta.pop("chunk_text", "")
            chunks.append({
                "text":  text,
                "meta":  meta,
                "score": round(float(scores[local_idx]), 3),
            })
        return chunks

    @property
    def vector_count(self) -> int:
        return len(_metadata) if _metadata else 0


class S3RetrieverUnavailableError(Exception):
    pass
