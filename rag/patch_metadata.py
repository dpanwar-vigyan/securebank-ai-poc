"""
Patch existing ChromaDB chunks to add missing metadata fields
(complaint_type, dispute_type, request_type, statement_date etc.)
without re-downloading PDFs or re-embedding.

Run once:
    python rag/patch_metadata.py
"""

import boto3
import chromadb
from pathlib import Path

BUCKET_NAME = "banking-docs-poc-qahftr"
CHROMA_PATH = str(Path(__file__).parent.parent / "chroma_db")
COLLECTION  = "banking_docs"
AWS_REGION  = "us-east-1"
BATCH_SIZE  = 100

s3     = boto3.client("s3", region_name=AWS_REGION)
chroma = chromadb.PersistentClient(path=CHROMA_PATH)
col    = chroma.get_collection(COLLECTION)

print(f"ChromaDB chunks before patch: {col.count()}")

# Fetch all existing records (ids + current metadata)
all_records = col.get(include=["metadatas"])
ids         = all_records["ids"]
metas       = all_records["metadatas"]

print(f"Patching {len(ids)} chunks...\n")

# Group chunk IDs by doc_id
from collections import defaultdict
doc_chunks = defaultdict(list)   # doc_id → [(index_in_ids, chunk_id)]
for idx, (chunk_id, meta) in enumerate(zip(ids, metas)):
    doc_id = meta.get("doc_id", "")
    doc_chunks[doc_id].append((idx, chunk_id))

total_docs = len(doc_chunks)
updated    = 0
errors     = 0

for doc_num, (doc_id, chunks) in enumerate(doc_chunks.items()):
    try:
        # Infer S3 key from existing s3_path metadata
        sample_meta = metas[chunks[0][0]]
        s3_path = sample_meta.get("s3_path", "")
        # s3_path = "s3://bucket/prefix/doc.pdf"
        key = s3_path.replace(f"s3://{BUCKET_NAME}/", "")

        # Fetch updated metadata from S3 object
        resp     = s3.head_object(Bucket=BUCKET_NAME, Key=key)
        s3_meta  = resp.get("Metadata", {})

        # Build the extra fields to patch in
        extra = {
            "complaint_type":  s3_meta.get("complaint_type", ""),
            "dispute_type":    s3_meta.get("dispute_type", ""),
            "request_type":    s3_meta.get("request_type", ""),
            "statement_date":  s3_meta.get("statement_date", ""),
            "request_date":    s3_meta.get("request_date", ""),
            "compensation_paid": s3_meta.get("compensation_paid", ""),
            "dispute_amount":  s3_meta.get("dispute_amount", ""),
            "account_type":    s3_meta.get("account_type", ""),
            "rm_id":           s3_meta.get("rm_id", ""),
        }

        # Merge into existing metadata for each chunk of this doc
        patch_ids   = []
        patch_metas = []
        for idx, chunk_id in chunks:
            merged = {**metas[idx], **{k: v for k, v in extra.items() if v}}
            patch_ids.append(chunk_id)
            patch_metas.append(merged)

        # Update in ChromaDB (no re-embedding needed — documents not changed)
        col.update(ids=patch_ids, metadatas=patch_metas)

        updated += 1
        if doc_num % 100 == 0:
            print(f"  [{doc_num+1}/{total_docs}] Patched: {doc_id}")

    except Exception as e:
        errors += 1
        if errors <= 5:
            print(f"  ERROR {doc_id}: {e}")

print(f"""
========================================================
  Metadata patch complete
  Documents patched : {updated}
  Errors            : {errors}
  Total chunks      : {col.count()}
========================================================
""")
