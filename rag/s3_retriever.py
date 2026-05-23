"""
S3 + NumPy vector retriever — Lambda-compatible replacement for ChromaDB.

On cold start: downloads vectors.npy + metadata.json from S3 (~1-2s, ~4MB).
On warm invocations: checks S3 ETag (HEAD, ~1ms) at most every ETAG_CHECK_INTERVAL_S
seconds. Reloads only when vectors.npy has actually changed.

Why ETag and not TTL:
  Lambda scales horizontally — multiple warm containers each have their own
  in-memory cache. A TTL only fixes the container it runs in. ETag check is
  per-container and self-healing: every container independently detects when
  the pipeline has written new vectors and reloads automatically.

Cost: HEAD request ~$0.000004 per 10k checks. Negligible.
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

# How often (seconds) to HEAD-check S3 for a new ETag.
# Default 30s — balances freshness vs. S3 API cost.
# Set to 0 to check every request (max freshness, tiny cost increase).
ETAG_CHECK_INTERVAL_S = int(os.getenv("VECTOR_ETAG_CHECK_INTERVAL_S", "30"))

# Module-level cache — persists for the lifetime of the Lambda container.
_vectors:        np.ndarray | None = None
_metadata:       list[dict] | None = None
_loaded_etag:    str               = ""    # ETag of vectors.npy at last load
_last_etag_check: float            = 0.0  # epoch of last HEAD request


def _s3_current_etag(s3) -> str:
    """Lightweight HEAD request — returns ETag of vectors.npy on S3."""
    resp = s3.head_object(Bucket=S3_BUCKET, Key=VECTORS_KEY)
    return resp.get("ETag", "")


def _load(force: bool = False):
    """
    Load vectors + metadata from S3, reloading when content has changed.

    Decision logic (per container, per invocation):
      1. Cold start (no cache)         → always load
      2. force=True                    → always reload (called by /step/complete)
      3. ETag check interval not yet   → skip (cache is recent enough)
      4. ETag check interval elapsed   → HEAD vectors.npy, compare ETag
         a. ETag same → skip (nothing changed)
         b. ETag different → reload (pipeline wrote new vectors)
    """
    global _vectors, _metadata, _loaded_etag, _last_etag_check

    if not S3_BUCKET:
        raise S3RetrieverUnavailableError("S3_VECTORS_BUCKET not set")

    s3  = boto3.client("s3")
    now = time.time()

    # ── Cold start: no cache yet ──────────────────────────────────────────────
    if _vectors is None:
        _do_load(s3, reason="cold start")
        return

    # ── Forced reload (called from /pipeline/step/complete) ───────────────────
    if force:
        _do_load(s3, reason="pipeline complete — forced reload")
        return

    # ── ETag check interval not elapsed yet — use cache ──────────────────────
    if (now - _last_etag_check) < ETAG_CHECK_INTERVAL_S:
        return

    # ── Time to check — HEAD request to S3 ───────────────────────────────────
    try:
        _last_etag_check = now
        current_etag = _s3_current_etag(s3)
        if current_etag == _loaded_etag:
            return   # vectors.npy unchanged — keep cache
        _do_load(s3, reason=f"ETag changed ({_loaded_etag[:8]}…→{current_etag[:8]}…)")
    except Exception as exc:
        # If the HEAD check fails, keep using the cached data rather than error
        print(f"[s3_retriever] ETag check failed (using cache): {exc}")


def _do_load(s3, reason: str):
    """Actually download and replace the in-memory cache."""
    global _vectors, _metadata, _loaded_etag, _last_etag_check

    t0 = time.time()

    resp = s3.get_object(Bucket=S3_BUCKET, Key=VECTORS_KEY)
    _loaded_etag = resp.get("ETag", "")
    _vectors     = np.load(io.BytesIO(resp["Body"].read())).astype(np.float32)

    obj      = s3.get_object(Bucket=S3_BUCKET, Key=METADATA_KEY)
    _metadata = json.loads(obj["Body"].read().decode())

    _last_etag_check = time.time()
    elapsed          = round(_last_etag_check - t0, 2)
    print(f"S3 retriever loaded: {_vectors.shape[0]} vectors × {_vectors.shape[1]} dims "
          f"in {elapsed}s [{reason}]")


def force_reload():
    """
    Force an immediate reload from S3 regardless of ETag check interval.
    Called from /pipeline/step/complete so the new doc is searchable instantly
    on this container. Other containers detect the ETag change within
    VECTOR_ETAG_CHECK_INTERVAL_S seconds of their next request.
    """
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
        # Check ETag on every retrieve — reloads only when vectors.npy has changed.
        # HEAD request is throttled to ETAG_CHECK_INTERVAL_S so it's ~1ms overhead
        # on most calls, zero overhead when interval hasn't elapsed.
        _load()

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
            # Support both field names: new pipeline writes "chunk_text",
            # old bulk-loaded docs also use "chunk_text". Fall back to "text"
            # for any chunks written before this fix.
            text = meta.pop("chunk_text", None) or meta.pop("text", "")
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
