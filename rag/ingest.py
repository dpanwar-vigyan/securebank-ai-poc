"""
RAG Ingestion Pipeline — Dual-Write to ChromaDB + ClickHouse
S3 PDFs → text extract → chunk → embed (Bedrock Titan) → ChromaDB
                      → metadata row → ClickHouse (real-time pipeline)

Both stores are always kept in sync — no separate CSV import needed.

Run once to build the index, then incrementally for new docs.
Usage:
    python rag/ingest.py                   # ingest all docs from S3
    python rag/ingest.py --limit 50        # ingest first 50 (for testing)
    python rag/ingest.py --prefix disputes/ # ingest one folder only
    python rag/ingest.py --resync          # force re-ingest even if already in ChromaDB
"""

import argparse
import io
import json
import os
import sys
import time
from datetime import date
from pathlib import Path

import boto3
import pdfplumber
import chromadb
from chromadb.utils.embedding_functions import EmbeddingFunction
from langchain.text_splitter import RecursiveCharacterTextSplitter
import rag.config  # noqa: F401 — loads .env + st.secrets into os.environ

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BUCKET_NAME    = "banking-docs-poc-qahftr"
CHROMA_PATH    = str(Path(__file__).parent.parent / "chroma_db")
COLLECTION     = "banking_docs"
EMBED_MODEL    = os.getenv("EMBED_MODEL",    "amazon.titan-embed-text-v2:0")
LLM_MODEL      = os.getenv("LLM_MODEL",     "us.amazon.nova-lite-v1:0")
AWS_REGION     = os.getenv("BEDROCK_REGION", "us-east-1")
CHUNK_SIZE     = 500
CHUNK_OVERLAP  = 80

CH_HOST        = os.getenv("CLICKHOUSE_HOST")
CH_USER        = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS        = os.getenv("CLICKHOUSE_PASSWORD")
CH_TABLE       = "banking_docs.documents"


# ---------------------------------------------------------------------------
# Bedrock Embedding Function
# ---------------------------------------------------------------------------
class BedrockEmbeddings(EmbeddingFunction):
    def __init__(self):
        self.client = boto3.client("bedrock-runtime", region_name=AWS_REGION)

    def __call__(self, texts: list[str]) -> list[list[float]]:
        embeddings = []
        for text in texts:
            body = json.dumps({
                "inputText": text[:8000],
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
            time.sleep(0.05)
        return embeddings


# ---------------------------------------------------------------------------
# PDF Text Extraction
# ---------------------------------------------------------------------------
def extract_text_from_pdf(pdf_bytes: bytes) -> str:
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
    try:
        resp = s3.head_object(Bucket=BUCKET_NAME, Key=key)
        return resp.get("Metadata", {})
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------
def _connect_clickhouse():
    """Return a ClickHouse client, or None if credentials not configured."""
    if not CH_HOST or not CH_PASS:
        print("  ℹ️  ClickHouse credentials not set — skipping CH write.")
        return None
    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host=CH_HOST, user=CH_USER, password=CH_PASS, secure=True
        )
        print(f"  ✅ ClickHouse connected — {CH_HOST}")
        return client
    except Exception as e:
        print(f"  ⚠️  ClickHouse connection failed — {e}. Continuing without CH write.")
        return None


def _to_date(val):
    """Convert ISO date string to date, or None."""
    if not val:
        return None
    try:
        return date.fromisoformat(str(val)[:10])
    except Exception:
        return None


def _to_float(val):
    """Convert string/numeric to float, or None."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except Exception:
        return None


# Column order must match ClickHouse table definition exactly
CH_COLUMNS = [
    "doc_id", "doc_type", "s3_path",
    "customer_id", "customer_name", "customer_email", "customer_phone", "customer_address",
    "account_number", "account_type", "sort_code",
    "rm_id", "rm_name", "rm_email",
    "branch_code", "branch_name",
    "statement_date", "closing_balance",
    "case_status", "filed_date", "closed_date", "resolution",
    "dispute_type", "dispute_amount",
    "complaint_type", "priority", "compensation_paid",
    "request_type", "request_status", "request_date", "processed_date",
    "case_summary",
]


def _build_ch_row(doc_id: str, s3_meta: dict, key: str) -> list:
    """Build a single ClickHouse row from S3 metadata."""
    m = s3_meta
    return [
        doc_id,
        m.get("doc_type", _infer_type(key)),
        m.get("s3_path", f"s3://{BUCKET_NAME}/{key}"),
        m.get("customer_id", ""),
        m.get("customer_name", ""),
        m.get("customer_email", ""),
        m.get("customer_phone", ""),
        m.get("customer_address", ""),
        m.get("account_number", ""),
        m.get("account_type", ""),
        m.get("sort_code", ""),
        m.get("rm_id", ""),
        m.get("rm_name", ""),
        m.get("rm_email", ""),
        m.get("branch_code", ""),
        m.get("branch_name", ""),
        _to_date(m.get("statement_date")),
        _to_float(m.get("closing_balance")),
        m.get("case_status", ""),
        _to_date(m.get("filed_date")),
        _to_date(m.get("closed_date")),
        m.get("resolution", ""),
        m.get("dispute_type", ""),
        _to_float(m.get("dispute_amount")),
        m.get("complaint_type", ""),
        m.get("priority", ""),
        _to_float(m.get("compensation_paid")),
        m.get("request_type", ""),
        m.get("request_status", ""),
        _to_date(m.get("request_date")),
        _to_date(m.get("processed_date")),
        m.get("case_summary", ""),
    ]


def write_to_clickhouse(ch, doc_id: str, s3_meta: dict, key: str) -> bool:
    """
    Upsert one document row into ClickHouse.
    ReplacingMergeTree deduplicates by (doc_type, customer_id, doc_id) — latest ingested_at wins.
    Returns True on success.
    """
    if ch is None:
        return False
    try:
        row = _build_ch_row(doc_id, s3_meta, key)
        ch.insert(CH_TABLE, [row], column_names=CH_COLUMNS)
        return True
    except Exception as e:
        print(f"    ⚠️  ClickHouse write failed for {doc_id}: {e}")
        return False


# ---------------------------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------------------------
def ingest(limit: int = None, prefix: str = "", resync: bool = False):
    print(f"\nStarting dual-write ingestion from s3://{BUCKET_NAME}/{prefix}")
    print(f"  → ChromaDB path : {CHROMA_PATH}")
    print(f"  → ClickHouse    : {CH_HOST or 'not configured'}\n")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    # ── Setup ChromaDB ──────────────────────────────────────────────────────
    chroma = chromadb.PersistentClient(path=CHROMA_PATH)
    embed_fn = BedrockEmbeddings()
    collection = chroma.get_or_create_collection(
        name=COLLECTION,
        embedding_function=embed_fn,
        metadata={"hnsw:space": "cosine"},
    )

    # ── Setup ClickHouse ────────────────────────────────────────────────────
    ch = _connect_clickhouse()

    # ── Text splitter ───────────────────────────────────────────────────────
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", ". ", " ", ""],
    )

    # ── List all PDFs in S3 ─────────────────────────────────────────────────
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".pdf"):
                keys.append(obj["Key"])

    if limit:
        keys = keys[:limit]

    total = len(keys)
    print(f"Found {total} PDFs to process\n")

    # ── Check already-ingested docs ─────────────────────────────────────────
    existing_ids = set(collection.get(include=[])["ids"])
    print(f"Already in ChromaDB: {len(existing_ids)} chunks\n")

    skipped     = 0
    ch_written  = 0
    processed   = 0
    errors      = 0

    for i, key in enumerate(keys):
        doc_id = Path(key).stem  # e.g. STMT00001

        # Skip if already ingested (unless --resync flag)
        if not resync and any(eid.startswith(doc_id + "_") for eid in existing_ids):
            # Still sync to ClickHouse if not already there
            s3_meta = get_s3_metadata(s3, key)
            if write_to_clickhouse(ch, doc_id, s3_meta, key):
                ch_written += 1
            skipped += 1
            continue

        try:
            # ── Download PDF ────────────────────────────────────────────────
            resp = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            pdf_bytes = resp["Body"].read()

            # ── Extract text ────────────────────────────────────────────────
            text = extract_text_from_pdf(pdf_bytes)
            if not text.strip():
                print(f"  [{i+1}/{total}] SKIP (no text): {key}")
                continue

            # ── Get S3 metadata ─────────────────────────────────────────────
            s3_meta = get_s3_metadata(s3, key)

            # ── Chunk the text ──────────────────────────────────────────────
            chunks = splitter.split_text(text)
            if not chunks:
                continue

            # ── Build ChromaDB records ──────────────────────────────────────
            ids       = [f"{doc_id}_{j}" for j in range(len(chunks))]
            documents = chunks
            metadatas = [{
                "doc_id":        doc_id,
                "doc_type":      s3_meta.get("doc_type", _infer_type(key)),
                "s3_path":       f"s3://{BUCKET_NAME}/{key}",
                "chunk_index":   j,
                "total_chunks":  len(chunks),
                "customer_id":   s3_meta.get("customer_id", ""),
                "customer_name": s3_meta.get("customer_name", ""),
                "rm_name":       s3_meta.get("rm_name", ""),
                "rm_id":         s3_meta.get("rm_id", ""),
                "branch_name":   s3_meta.get("branch_name", ""),
                "branch_code":   s3_meta.get("branch_code", ""),
                "case_status":   s3_meta.get("case_status", ""),
                "priority":      s3_meta.get("priority", ""),
                "filed_date":    s3_meta.get("filed_date", ""),
                "closed_date":   s3_meta.get("closed_date", ""),
                "case_summary":  s3_meta.get("case_summary", ""),
            } for j, chunk in enumerate(chunks)]

            # ── Upsert into ChromaDB ────────────────────────────────────────
            collection.upsert(ids=ids, documents=documents, metadatas=metadatas)

            # ── Dual-write to ClickHouse ────────────────────────────────────
            if write_to_clickhouse(ch, doc_id, s3_meta, key):
                ch_written += 1

            processed += 1
            print(f"  [{i+1}/{total}] ✅ ChromaDB({len(chunks)} chunks) + CH: {key}")

        except Exception as e:
            errors += 1
            print(f"  [{i+1}/{total}] ❌ ERROR: {key} — {e}")

    print(f"""
========================================================
  Dual-write ingestion complete
  ─────────────────────────────────────────
  Processed (new)  : {processed}
  Skipped (exists) : {skipped}
  ClickHouse rows  : {ch_written} written
  Errors           : {errors}
  ─────────────────────────────────────────
  ChromaDB chunks  : {collection.count()}
  ClickHouse table : {CH_TABLE}
========================================================
""")


def _infer_type(key: str) -> str:
    if "estatement" in key:   return "eStatement"
    if "dispute"    in key:   return "Dispute"
    if "complaint"  in key:   return "Complaint"
    if "maintenance" in key:  return "AccountMaintenance"
    return "Unknown"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dual-write ingestor: S3 → ChromaDB + ClickHouse")
    parser.add_argument("--limit",  type=int,  default=None,  help="Max docs to process")
    parser.add_argument("--prefix", type=str,  default="",    help="S3 prefix to filter (e.g. disputes/)")
    parser.add_argument("--resync", action="store_true",      help="Force re-ingest even if doc already in ChromaDB")
    args = parser.parse_args()
    ingest(limit=args.limit, prefix=args.prefix, resync=args.resync)
