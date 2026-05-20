"""
Standalone pipeline step functions for AskMyBank document ingestion.

These are the same algorithms as workflows/orkes/tasks/pipeline_tasks.py but
without the Conductor WorkerInterface wrapper — callable directly from Lambda
HTTP endpoints (/pipeline/step/*).

This lets both architectures share the same logic:
  V1: Orkes SIMPLE tasks → conductor worker polls → calls Worker.execute()
  V2: Orkes HTTP tasks   → Orkes calls Lambda   → calls step_*(...)

Bug fix vs pipeline_tasks.py:
  Textract uses SOURCE_BUCKET (banking-docs-poc-qahftr) not S3_VECTORS_BUCKET
  (askmybank-vectors). Textract needs the original PDF, not the vectors bucket.
"""

import io
import json
import os
import re
import time
import uuid
from datetime import datetime

import boto3
import numpy as np
import clickhouse_connect

# ── Environment ───────────────────────────────────────────────────────────────
BEDROCK_REGION  = os.getenv("BEDROCK_REGION", "us-east-1")
LLM_MODEL       = os.getenv("LLM_MODEL", "us.amazon.nova-lite-v1:0")
EMBED_MODEL     = os.getenv("EMBED_MODEL", "amazon.titan-embed-text-v2:0")
S3_VECTORS_BUCKET = os.getenv("S3_VECTORS_BUCKET", "askmybank-vectors")
SOURCE_BUCKET   = os.getenv("S3_SOURCE_BUCKET", "banking-docs-poc-qahftr")  # ← bug fix

CH_HOST = os.getenv("CLICKHOUSE_HOST")
CH_USER = os.getenv("CLICKHOUSE_USER")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD")

CHUNK_SIZE    = 400
CHUNK_OVERLAP = 60


# ── AWS / DB clients ──────────────────────────────────────────────────────────

def _ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, user=CH_USER, password=CH_PASS,
        secure=True, connect_timeout=8, send_receive_timeout=30,
    )

def _bedrock():
    return boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

def _s3():
    return boto3.client("s3", region_name="us-east-1")

def _textract():
    return boto3.client("textract", region_name="us-east-1")


# ── Step 1: detect_doc_type ───────────────────────────────────────────────────
# NOTE: In V2 this step runs as an Orkes INLINE Graal JS task (zero infrastructure).
# This Python function is kept for V1 compatibility and local testing only.

def step_detect(s3_key: str, doc_type: str = "", customer_id: str = "") -> dict:
    key_lower = s3_key.lower()
    if not doc_type:
        if "dispute" in key_lower or "dsp" in key_lower:
            doc_type = "Dispute"
        elif "complaint" in key_lower or "cmp" in key_lower:
            doc_type = "Complaint"
        elif "statement" in key_lower or "stmt" in key_lower:
            doc_type = "eStatement"
        elif "maintenance" in key_lower or "mnt" in key_lower:
            doc_type = "AccountMaintenance"
        else:
            doc_type = "Complaint"

    prefix_map = {
        "Dispute": "DSP", "Complaint": "CMP",
        "eStatement": "STMT", "AccountMaintenance": "MNT",
    }
    prefix = prefix_map.get(doc_type, "DOC")
    doc_id = f"{prefix}{uuid.uuid4().hex[:5].upper()}"

    return {
        "doc_id":   doc_id,
        "doc_type": doc_type,
        "s3_key":   s3_key,
        "metadata": {
            "doc_id":      doc_id,
            "doc_type":    doc_type,
            "customer_id": customer_id,
            "s3_path":     s3_key,
            "ingested_at": datetime.utcnow().isoformat(),
        },
    }


# ── Step 2: textract_extract ──────────────────────────────────────────────────
# BUG FIX: Uses SOURCE_BUCKET (banking-docs-poc-qahftr) not S3_VECTORS_BUCKET.
# Textract needs the source PDF, which lives in the source bucket — not the
# vectors bucket. The vectors bucket holds vectors.npy + metadata.json.

def step_textract(s3_key: str, doc_id: str) -> dict:
    tx = _textract()

    resp = tx.start_document_text_detection(
        DocumentLocation={"S3Object": {"Bucket": SOURCE_BUCKET, "Name": s3_key}}
    )
    job_id = resp["JobId"]
    print(f"[textract] Started job {job_id} for s3://{SOURCE_BUCKET}/{s3_key}")

    raw_lines  = []
    page_count = 0

    for attempt in range(24):          # 24 × 5s = 2 min max
        time.sleep(5)
        status_resp = tx.get_document_text_detection(JobId=job_id)
        job_status  = status_resp["JobStatus"]

        if job_status == "SUCCEEDED":
            blocks     = status_resp.get("Blocks", [])
            next_token = status_resp.get("NextToken")
            while next_token:
                page_resp  = tx.get_document_text_detection(JobId=job_id, NextToken=next_token)
                blocks    += page_resp.get("Blocks", [])
                next_token = page_resp.get("NextToken")

            for block in blocks:
                if block["BlockType"] == "LINE":
                    raw_lines.append(block.get("Text", ""))
                if block["BlockType"] == "PAGE":
                    page_count += 1
            break

        if job_status == "FAILED":
            raise RuntimeError(f"Textract job {job_id} FAILED for {s3_key}")

    full_text = "\n".join(raw_lines)
    print(f"[textract] ✅ {doc_id} — {len(full_text)} chars, {page_count} pages")
    return {
        "raw_text":   full_text,
        "page_count": max(page_count, 1),
        "char_count": len(full_text),
        "job_id":     job_id,
    }


# ── Step 3: chunk_text ────────────────────────────────────────────────────────

def step_chunk(raw_text: str, doc_id: str, doc_type: str, page_count: int = 1) -> dict:
    sentences   = re.split(r'(?<=[.!?])\s+', raw_text.strip())
    chunks      = []
    current     = []
    current_len = 0

    for sent in sentences:
        if current_len + len(sent) > CHUNK_SIZE and current:
            chunk_text = " ".join(current)
            chunks.append({
                "chunk_id":  f"{doc_id}_c{len(chunks):03d}",
                "doc_id":    doc_id,
                "doc_type":  doc_type,
                "text":      chunk_text,
                "chunk_idx": len(chunks),
            })
            current     = current[-1:] + [sent]
            current_len = sum(len(s) for s in current)
        else:
            current.append(sent)
            current_len += len(sent)

    if current:
        chunks.append({
            "chunk_id":  f"{doc_id}_c{len(chunks):03d}",
            "doc_id":    doc_id,
            "doc_type":  doc_type,
            "text":      " ".join(current),
            "chunk_idx": len(chunks),
        })

    print(f"[chunk] {doc_id} → {len(chunks)} chunks")
    return {"chunks": chunks, "chunk_count": len(chunks), "doc_id": doc_id}


# ── Step 4a: generate_embeddings (FORK Branch A) ──────────────────────────────

def step_embed(chunks: list, doc_id: str) -> dict:
    bedrock    = _bedrock()
    embeddings = []

    for chunk in chunks:
        body = json.dumps({
            "inputText":  chunk["text"][:8000],
            "dimensions": 256,
            "normalize":  True,
        })
        resp = bedrock.invoke_model(
            modelId=EMBED_MODEL, body=body, contentType="application/json",
        )
        emb = json.loads(resp["body"].read())["embedding"]
        embeddings.append(emb)
        time.sleep(0.05)    # avoid Bedrock throttle

    print(f"[embed] ✅ {doc_id} — {len(embeddings)} vectors")
    return {"embeddings": embeddings, "count": len(embeddings), "dimensions": 256, "doc_id": doc_id}


# ── Step 4b: update_s3_vectors (FORK Branch A cont.) ─────────────────────────

def step_s3vec(doc_id: str, embeddings: list, chunks: list, metadata: dict) -> dict:
    s3 = _s3()

    # Download existing vectors.npy
    try:
        obj      = s3.get_object(Bucket=S3_VECTORS_BUCKET, Key="vectors.npy")
        existing = np.load(io.BytesIO(obj["Body"].read()))
    except Exception:
        existing = np.empty((0, 256), dtype=np.float32)

    # Download existing metadata.json
    try:
        obj           = s3.get_object(Bucket=S3_VECTORS_BUCKET, Key="metadata.json")
        existing_meta = json.loads(obj["Body"].read())
    except Exception:
        existing_meta = []

    # Append new vectors
    new_vecs = np.array(embeddings, dtype=np.float32)
    combined = np.vstack([existing, new_vecs]) if existing.shape[0] else new_vecs

    buf = io.BytesIO()
    np.save(buf, combined)
    buf.seek(0)
    s3.put_object(Bucket=S3_VECTORS_BUCKET, Key="vectors.npy", Body=buf.getvalue())

    # Append metadata entries
    for i, chunk in enumerate(chunks):
        entry = {**metadata, **chunk, "vector_idx": int(len(existing)) + i}
        existing_meta.append(entry)

    s3.put_object(
        Bucket=S3_VECTORS_BUCKET, Key="metadata.json",
        Body=json.dumps(existing_meta), ContentType="application/json",
    )

    print(f"[s3vec] ✅ {doc_id} — added {len(embeddings)} vectors (total {combined.shape[0]})")
    return {
        "vectors_added": len(embeddings),
        "total_vectors": int(combined.shape[0]),
        "doc_id":        doc_id,
    }


# ── Step 4c: parse_metadata (FORK Branch B) ───────────────────────────────────

def step_meta(raw_text: str, doc_id: str, doc_type: str, customer_id: str) -> dict:
    prompt = f"""Extract structured metadata from this banking document.

Document type: {doc_type}
Document ID: {doc_id}
Text (first 2000 chars):
{raw_text[:2000]}

Return ONLY valid JSON with these fields (use null if not found):
{{
  "customer_name": "...",
  "account_number": "...",
  "branch_name": "...",
  "rm_name": "...",
  "case_status": "...",
  "filed_date": "YYYY-MM-DD or null",
  "dispute_amount": null,
  "complaint_type": "...",
  "dispute_type": "...",
  "priority": "Critical|High|Medium|Low",
  "resolution": "...",
  "case_summary": "one sentence summary"
}}"""

    try:
        bedrock = _bedrock()
        body    = json.dumps({
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {"maxTokens": 400, "temperature": 0.0},
        })
        resp = bedrock.invoke_model(modelId=LLM_MODEL, body=body, contentType="application/json")
        raw  = json.loads(resp["body"].read())["output"]["message"]["content"][0]["text"]

        match     = re.search(r'\{.*\}', raw, re.DOTALL)
        extracted = json.loads(match.group(0)) if match else {}

        structured = {
            "doc_id": doc_id, "doc_type": doc_type, "customer_id": customer_id,
            **{k: extracted.get(k) for k in [
                "customer_name", "account_number", "branch_name", "rm_name",
                "case_status", "filed_date", "dispute_amount", "complaint_type",
                "dispute_type", "priority", "resolution", "case_summary",
            ]},
        }
        print(f"[meta] ✅ {doc_id} — extracted {len(structured)} fields")
        return {"structured_metadata": structured, "doc_id": doc_id}

    except Exception as exc:
        # Graceful fallback — don't fail the pipeline over metadata extraction
        print(f"[meta] ⚠️  {doc_id} metadata fallback: {exc}")
        return {
            "structured_metadata": {
                "doc_id": doc_id, "doc_type": doc_type,
                "customer_id": customer_id, "case_summary": "Auto-ingested document",
            },
            "fallback": True, "error": str(exc),
        }


# ── Step 4d: upsert_clickhouse (FORK Branch B cont.) ─────────────────────────

def step_clickhouse(doc_id: str, metadata: dict, s3_key: str) -> dict:
    ch  = _ch()
    now = datetime.utcnow()

    def safe(key, default=""):
        val = metadata.get(key, default)
        return val if val is not None else default

    ch.insert(
        table="banking_docs.documents",
        data=[[
            safe("doc_id", doc_id),
            safe("doc_type"),
            f"s3://{SOURCE_BUCKET}/{s3_key}",    # source PDF path
            safe("customer_id"),
            safe("customer_name"),
            safe("account_number"),
            "", "", "",                          # account_type, sort_code, branch_code
            safe("branch_name"),
            "", safe("rm_name"), "",             # rm_id, rm_name, rm_email
            safe("case_status"),
            None, None,                          # filed_date, closed_date
            safe("resolution"),
            safe("dispute_type"),
            metadata.get("dispute_amount"),
            safe("complaint_type"),
            safe("priority", "Medium"),
            metadata.get("compensation_paid"),
            "", "", None, None,                  # request_type/status/date/processed_date
            None, None,                          # statement_date, closing_balance
            safe("case_summary"),
            now,
        ]],
        column_names=[
            "doc_id", "doc_type", "s3_path",
            "customer_id", "customer_name", "account_number", "account_type", "sort_code",
            "branch_code", "branch_name", "rm_id", "rm_name", "rm_email",
            "case_status", "filed_date", "closed_date", "resolution",
            "dispute_type", "dispute_amount", "complaint_type", "priority", "compensation_paid",
            "request_type", "request_status", "request_date", "processed_date",
            "statement_date", "closing_balance",
            "case_summary", "ingested_at",
        ],
    )

    print(f"[clickhouse] ✅ {doc_id} inserted into banking_docs.documents")
    return {"rows_inserted": 1, "doc_id": doc_id, "table": "banking_docs.documents"}


# ── Step 5: pipeline_complete ─────────────────────────────────────────────────

def step_complete(doc_id: str, vector_count: int, ch_inserted: int, source_system: str) -> dict:
    completed_at = datetime.utcnow().isoformat()
    print(f"[complete] ✅ {doc_id} | {vector_count} vectors | CH rows: {ch_inserted} | src: {source_system}")
    return {
        "doc_id":          doc_id,
        "vector_count":    vector_count,
        "pipeline_status": "completed",
        "completed_at":    completed_at,
    }
