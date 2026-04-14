"""
RAG Ingestion Pipeline
S3 PDFs → text extract → chunk → embed (Bedrock Titan) → ChromaDB

Run once to build the index, then incrementally for new docs.
Usage:
    python rag/ingest.py                  # ingest all docs from S3
    python rag/ingest.py --limit 50       # ingest first 50 (for testing)
    python rag/ingest.py --prefix disputes/ # ingest one folder only
"""

import argparse
import io
import json
import os
import sys
import time
from pathlib import Path

import boto3
import pdfplumber
import chromadb
from chromadb.utils.embedding_functions import EmbeddingFunction
from langchain.text_splitter import RecursiveCharacterTextSplitter

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BUCKET_NAME    = "banking-docs-poc-qahftr"
CHROMA_PATH    = str(Path(__file__).parent.parent / "chroma_db")
COLLECTION     = "banking_docs"
EMBED_MODEL    = "amazon.titan-embed-text-v2:0"
AWS_REGION     = "us-east-1"
CHUNK_SIZE     = 500
CHUNK_OVERLAP  = 80


# ---------------------------------------------------------------------------
# Bedrock Embedding Function (compatible with ChromaDB)
# ---------------------------------------------------------------------------
class BedrockEmbeddings(EmbeddingFunction):
    def __init__(self):
        self.client = boto3.client("bedrock-runtime", region_name=AWS_REGION)

    def __call__(self, texts: list[str]) -> list[list[float]]:
        embeddings = []
        for text in texts:
            body = json.dumps({
                "inputText": text[:8000],   # Titan v2 max
                "dimensions": 256,
                "normalize": True,
            })
            resp = self.client.invoke_model(
                modelId=EMBED_MODEL,
                body=body,
                contentType="application/json",
            )
            result = json.loads(resp["body"].read())
            embeddings.append(result["embedding"])
            time.sleep(0.05)   # stay within Bedrock rate limits
        return embeddings


# ---------------------------------------------------------------------------
# PDF Text Extraction
# ---------------------------------------------------------------------------
def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract all text from a PDF. Works on digitally-generated PDFs."""
    text_parts = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_parts.append(text.strip())
    except Exception as e:
        print(f"  Warning: PDF extraction error — {e}")
    return "\n\n".join(text_parts)


# ---------------------------------------------------------------------------
# Metadata extraction from S3 object tags
# ---------------------------------------------------------------------------
def get_s3_metadata(s3, key: str) -> dict:
    """Read the metadata stored on the S3 object when it was uploaded."""
    try:
        resp = s3.head_object(Bucket=BUCKET_NAME, Key=key)
        meta = resp.get("Metadata", {})
        return meta
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------------------------
def ingest(limit: int = None, prefix: str = ""):
    print(f"\nStarting ingestion from s3://{BUCKET_NAME}/{prefix}")
    print(f"ChromaDB path: {CHROMA_PATH}\n")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Setup ChromaDB
    chroma = chromadb.PersistentClient(path=CHROMA_PATH)
    embed_fn = BedrockEmbeddings()
    collection = chroma.get_or_create_collection(
        name=COLLECTION,
        embedding_function=embed_fn,
        metadata={"hnsw:space": "cosine"},
    )

    # Text splitter
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", ". ", " ", ""],
    )

    # List all PDFs in S3
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".pdf"):
                keys.append(obj["Key"])

    if limit:
        keys = keys[:limit]

    total = len(keys)
    print(f"Found {total} PDFs to ingest\n")

    # Check already-ingested docs (avoid re-processing)
    existing_ids = set(collection.get(include=[])["ids"])
    print(f"Already in ChromaDB: {len(existing_ids)} chunks\n")

    skipped = 0
    processed = 0
    errors = 0

    for i, key in enumerate(keys):
        doc_id = Path(key).stem   # e.g. STMT00001

        # Skip if already ingested (any chunk with this doc_id prefix exists)
        if any(eid.startswith(doc_id + "_") for eid in existing_ids):
            skipped += 1
            continue

        try:
            # Download PDF
            resp = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            pdf_bytes = resp["Body"].read()

            # Extract text
            text = extract_text_from_pdf(pdf_bytes)
            if not text.strip():
                print(f"  [{i+1}/{total}] SKIP (no text): {key}")
                continue

            # Get metadata from S3 object
            s3_meta = get_s3_metadata(s3, key)

            # Chunk the text
            chunks = splitter.split_text(text)
            if not chunks:
                continue

            # Build ChromaDB records
            ids       = [f"{doc_id}_{j}" for j in range(len(chunks))]
            documents = chunks
            metadatas = []
            for j, chunk in enumerate(chunks):
                metadatas.append({
                    # Core identity
                    "doc_id":        doc_id,
                    "doc_type":      s3_meta.get("doc_type", _infer_type(key)),
                    "s3_path":       f"s3://{BUCKET_NAME}/{key}",
                    "chunk_index":   j,
                    "total_chunks":  len(chunks),
                    # Customer
                    "customer_id":   s3_meta.get("customer_id", ""),
                    "customer_name": s3_meta.get("customer_name", ""),
                    # RM
                    "rm_name":       s3_meta.get("rm_name", ""),
                    "rm_id":         s3_meta.get("rm_id", ""),
                    # Branch
                    "branch_name":   s3_meta.get("branch_name", ""),
                    "branch_code":   s3_meta.get("branch_code", ""),
                    # Case fields
                    "case_status":   s3_meta.get("case_status", ""),
                    "priority":      s3_meta.get("priority", ""),
                    "filed_date":    s3_meta.get("filed_date", ""),
                    "closed_date":   s3_meta.get("closed_date", ""),
                    # Summary
                    "case_summary":  s3_meta.get("case_summary", ""),
                })

            # Upsert into ChromaDB (embed_fn called here)
            collection.upsert(
                ids=ids,
                documents=documents,
                metadatas=metadatas,
            )

            processed += 1
            print(f"  [{i+1}/{total}] OK ({len(chunks)} chunks): {key}")

        except Exception as e:
            errors += 1
            print(f"  [{i+1}/{total}] ERROR: {key} — {e}")

    print(f"""
========================================================
  Ingestion complete
  Processed : {processed}
  Skipped   : {skipped} (already indexed)
  Errors    : {errors}
  Total chunks in ChromaDB: {collection.count()}
========================================================
""")


def _infer_type(key: str) -> str:
    if "estatement" in key:  return "eStatement"
    if "dispute"    in key:  return "Dispute"
    if "complaint"  in key:  return "Complaint"
    if "maintenance" in key: return "AccountMaintenance"
    return "Unknown"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",  type=int, default=None, help="Max docs to ingest")
    parser.add_argument("--prefix", type=str, default="",   help="S3 prefix to filter")
    args = parser.parse_args()
    ingest(limit=args.limit, prefix=args.prefix)
