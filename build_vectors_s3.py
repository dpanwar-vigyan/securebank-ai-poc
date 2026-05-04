"""
One-time script: extract all vectors + metadata from ChromaDB → upload to S3.
Run locally BEFORE deploying Lambda. Safe to re-run (S3 PUT is idempotent).

Usage:
  1. Set S3_VECTORS_BUCKET in .env  (create the bucket first in AWS console)
  2. python build_vectors_s3.py

Your existing Streamlit app is untouched — this is read-only from ChromaDB.
"""

import io
import json
import os
import sys
import time
from pathlib import Path

import boto3
import chromadb
import numpy as np
from dotenv import load_dotenv

load_dotenv()

CHROMA_PATH = str(Path(__file__).parent / "chroma_db")
COLLECTION  = "banking_docs"
S3_BUCKET   = os.getenv("S3_VECTORS_BUCKET", "")
FETCH_BATCH = 500


def main():
    if not S3_BUCKET:
        print("ERROR: S3_VECTORS_BUCKET not set in .env")
        sys.exit(1)

    # ── 1. Read all chunks + embeddings from ChromaDB ─────────────────────────
    print(f"Reading ChromaDB from {CHROMA_PATH}...")
    chroma = chromadb.PersistentClient(path=CHROMA_PATH)
    col    = chroma.get_collection(COLLECTION)
    total  = col.count()

    if total == 0:
        print("ERROR: ChromaDB is empty — run the ingestion pipeline first.")
        sys.exit(1)

    print(f"Found {total} chunks. Fetching embeddings in batches of {FETCH_BATCH}...")
    t0 = time.time()

    all_ids, all_embs, all_metas, all_docs = [], [], [], []
    for offset in range(0, total, FETCH_BATCH):
        batch = col.get(
            limit=FETCH_BATCH,
            offset=offset,
            include=["embeddings", "metadatas", "documents"],
        )
        all_ids  += batch["ids"]
        all_embs += batch["embeddings"]
        all_metas += batch["metadatas"]
        all_docs  += batch["documents"]
        print(f"  fetched {len(all_ids)}/{total}...")

    print(f"Fetch complete in {time.time()-t0:.1f}s")

    # ── 2. Build numpy array ──────────────────────────────────────────────────
    dim     = len(all_embs[0])
    vectors = np.array(all_embs, dtype=np.float32)   # shape (N, dim)
    print(f"Vectors shape: {vectors.shape}  ({vectors.nbytes / 1024 / 1024:.1f} MB)")

    # ── 3. Build metadata list (include chunk text for retrieval) ─────────────
    metadata = []
    for chunk_id, meta, doc in zip(all_ids, all_metas, all_docs):
        entry = {"_id": chunk_id}
        for k, v in meta.items():
            entry[k] = str(v) if v is not None else ""
        entry["chunk_text"] = doc[:4000]
        metadata.append(entry)

    meta_json = json.dumps(metadata, default=str).encode("utf-8")
    print(f"Metadata JSON: {len(meta_json) / 1024:.0f} KB  ({len(metadata)} entries)")

    # ── 4. Upload to S3 ───────────────────────────────────────────────────────
    s3 = boto3.client("s3")
    print(f"\nUploading to s3://{S3_BUCKET}/...")

    # vectors.npy
    buf = io.BytesIO()
    np.save(buf, vectors)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key="vectors.npy", Body=buf.getvalue(),
                  ContentType="application/octet-stream")
    print(f"  ✅ vectors.npy  ({vectors.nbytes / 1024 / 1024:.1f} MB)")

    # metadata.json
    s3.put_object(Bucket=S3_BUCKET, Key="metadata.json", Body=meta_json,
                  ContentType="application/json")
    print(f"  ✅ metadata.json ({len(meta_json) / 1024:.0f} KB)")

    # ── 5. Verify ─────────────────────────────────────────────────────────────
    obj = s3.head_object(Bucket=S3_BUCKET, Key="vectors.npy")
    print(f"\n{'='*55}")
    print(f"Migration complete!")
    print(f"  Chunks migrated : {total}")
    print(f"  Vector dims     : {dim}")
    print(f"  S3 bucket       : {S3_BUCKET}")
    print(f"  vectors.npy     : {obj['ContentLength'] / 1024 / 1024:.1f} MB")
    print(f"\nNext steps:")
    print(f"  1. Set  S3_VECTORS_BUCKET={S3_BUCKET}  in Lambda env vars")
    print(f"  2. Deploy Lambda:  sam build && sam deploy")
    print(f"  3. Streamlit stays live — switch over only when Lambda is verified")


if __name__ == "__main__":
    main()
