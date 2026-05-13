"""
Orkes task workers for the askmybank_document_pipeline workflow.

Tasks:
  askmybank_detect_doc_type      — classify doc, assign doc_id
  askmybank_textract_extract     — AWS Textract PDF → text
  askmybank_chunk_text           — split text into passages
  askmybank_generate_embeddings  — Bedrock Titan embeddings  [FORK Branch A]
  askmybank_update_s3_vectors    — append to vectors.npy + metadata.json [FORK A]
  askmybank_parse_metadata       — extract structured fields  [FORK Branch B]
  askmybank_upsert_clickhouse    — INSERT/UPDATE documents table [FORK B]
  askmybank_pipeline_complete    — log completion
"""

import io
import json
import os
import re
import time
import uuid
from datetime import datetime
from pathlib import Path

import boto3
import numpy as np
import clickhouse_connect
from conductor.client.worker.worker_interface import WorkerInterface
from conductor.client.http.models.task import Task
from conductor.client.http.models.task_result import TaskResult
from conductor.client.http.models.task_result_status import TaskResultStatus

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent.parent / ".env")
except Exception:
    pass

CH_HOST        = os.getenv("CLICKHOUSE_HOST")
CH_USER        = os.getenv("CLICKHOUSE_USER")
CH_PASS        = os.getenv("CLICKHOUSE_PASSWORD")
BEDROCK_REGION = os.getenv("BEDROCK_REGION", "us-east-1")
LLM_MODEL      = os.getenv("LLM_MODEL", "us.amazon.nova-lite-v1:0")
EMBED_MODEL    = os.getenv("EMBED_MODEL", "amazon.titan-embed-text-v2:0")
S3_BUCKET      = os.getenv("S3_VECTORS_BUCKET", "askmybank-vectors")

CHUNK_SIZE    = 400   # tokens / chars per chunk
CHUNK_OVERLAP = 60


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


# ── 1. Detect / classify document ────────────────────────────────────────────
class DetectDocTypeWorker(WorkerInterface):
    def __init__(self):
        super().__init__("askmybank_detect_doc_type")

    def execute(self, task: Task) -> TaskResult:
        result   = self.get_task_result_from_task(task)
        s3_key   = task.input_data.get("s3_key", "")
        doc_type = task.input_data.get("doc_type", "")
        cust_id  = task.input_data.get("customer_id", "")

        # Infer doc_type from s3_key name if not provided
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
                doc_type = "Complaint"   # sensible default

        # Generate doc_id
        prefix_map = {
            "Dispute": "DSP", "Complaint": "CMP",
            "eStatement": "STMT", "AccountMaintenance": "MNT",
        }
        prefix = prefix_map.get(doc_type, "DOC")
        doc_id = f"{prefix}{uuid.uuid4().hex[:5].upper()}"

        result.status = TaskResultStatus.COMPLETED
        result.output_data = {
            "doc_id":   doc_id,
            "doc_type": doc_type,
            "s3_key":   s3_key,
            "metadata": {
                "doc_id":      doc_id,
                "doc_type":    doc_type,
                "customer_id": cust_id,
                "s3_path":     s3_key,
                "ingested_at": datetime.utcnow().isoformat(),
            },
        }
        print(f"[detect_doc_type] {doc_id} ({doc_type}) ← {s3_key}")
        return result


# ── 2. Textract: extract text from PDF ───────────────────────────────────────
class TextractExtractWorker(WorkerInterface):
    def __init__(self):
        super().__init__("askmybank_textract_extract")

    def execute(self, task: Task) -> TaskResult:
        result = self.get_task_result_from_task(task)
        s3_key = task.input_data.get("s3_key", "")
        doc_id = task.input_data.get("doc_id", "")

        try:
            tx = _textract()
            # Start async Textract job
            resp = tx.start_document_text_detection(
                DocumentLocation={"S3Object": {"Bucket": S3_BUCKET, "Name": s3_key}}
            )
            job_id = resp["JobId"]
            print(f"[textract_extract] Started job {job_id} for {s3_key}")

            # Poll until complete (max 2 min)
            raw_text   = []
            page_count = 0
            for _ in range(24):   # 24 × 5s = 2 min
                time.sleep(5)
                status_resp = tx.get_document_text_detection(JobId=job_id)
                job_status  = status_resp["JobStatus"]

                if job_status == "SUCCEEDED":
                    blocks = status_resp.get("Blocks", [])
                    # Paginate if needed
                    next_token = status_resp.get("NextToken")
                    while next_token:
                        page_resp  = tx.get_document_text_detection(JobId=job_id, NextToken=next_token)
                        blocks    += page_resp.get("Blocks", [])
                        next_token = page_resp.get("NextToken")

                    for block in blocks:
                        if block["BlockType"] == "LINE":
                            raw_text.append(block.get("Text", ""))
                        if block["BlockType"] == "PAGE":
                            page_count += 1
                    break

                if job_status == "FAILED":
                    raise RuntimeError(f"Textract job {job_id} failed")

            full_text = "\n".join(raw_text)
            result.status = TaskResultStatus.COMPLETED
            result.output_data = {
                "raw_text":   full_text,
                "page_count": max(page_count, 1),
                "char_count": len(full_text),
                "job_id":     job_id,
            }
            print(f"[textract_extract] ✅ {doc_id} — {len(full_text)} chars, {page_count} pages")

        except Exception as exc:
            result.status = TaskResultStatus.FAILED
            result.reason_for_incompletion = f"Textract failed: {exc}"
        return result


# ── 3. Chunk text into semantic passages ──────────────────────────────────────
class ChunkTextWorker(WorkerInterface):
    def __init__(self):
        super().__init__("askmybank_chunk_text")

    def execute(self, task: Task) -> TaskResult:
        result   = self.get_task_result_from_task(task)
        raw_text = task.input_data.get("raw_text", "")
        doc_id   = task.input_data.get("doc_id", "")
        doc_type = task.input_data.get("doc_type", "")

        # Split by sentences, then group into chunks of ~CHUNK_SIZE chars
        sentences = re.split(r'(?<=[.!?])\s+', raw_text.strip())
        chunks    = []
        current   = []
        current_len = 0

        for sent in sentences:
            if current_len + len(sent) > CHUNK_SIZE and current:
                chunk_text = " ".join(current)
                chunks.append({
                    "chunk_id": f"{doc_id}_c{len(chunks):03d}",
                    "doc_id":   doc_id,
                    "doc_type": doc_type,
                    "text":     chunk_text,
                    "chunk_idx": len(chunks),
                })
                # Overlap: keep last sentence
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

        result.status = TaskResultStatus.COMPLETED
        result.output_data = {
            "chunks":      chunks,
            "chunk_count": len(chunks),
            "doc_id":      doc_id,
        }
        print(f"[chunk_text] {doc_id} → {len(chunks)} chunks")
        return result


# ── 4a. Generate Bedrock Titan embeddings [FORK Branch A] ─────────────────────
class GenerateEmbeddingsWorker(WorkerInterface):
    def __init__(self):
        super().__init__("askmybank_generate_embeddings")

    def execute(self, task: Task) -> TaskResult:
        result = self.get_task_result_from_task(task)
        chunks = task.input_data.get("chunks", [])
        doc_id = task.input_data.get("doc_id", "")

        try:
            bedrock    = _bedrock()
            embeddings = []
            for chunk in chunks:
                body = json.dumps({
                    "inputText":  chunk["text"][:8000],
                    "dimensions": 256,
                    "normalize":  True,
                })
                resp = bedrock.invoke_model(
                    modelId=EMBED_MODEL, body=body, contentType="application/json"
                )
                emb = json.loads(resp["body"].read())["embedding"]
                embeddings.append(emb)
                time.sleep(0.05)   # avoid Bedrock throttle

            result.status = TaskResultStatus.COMPLETED
            result.output_data = {
                "embeddings":  embeddings,
                "count":       len(embeddings),
                "dimensions":  256,
                "doc_id":      doc_id,
            }
            print(f"[generate_embeddings] ✅ {doc_id} — {len(embeddings)} vectors")
        except Exception as exc:
            result.status = TaskResultStatus.FAILED
            result.reason_for_incompletion = str(exc)
        return result


# ── 4b. Update S3 vectors.npy + metadata.json [FORK Branch A cont.] ───────────
class UpdateS3VectorsWorker(WorkerInterface):
    def __init__(self):
        super().__init__("askmybank_update_s3_vectors")

    def execute(self, task: Task) -> TaskResult:
        result     = self.get_task_result_from_task(task)
        doc_id     = task.input_data.get("doc_id", "")
        embeddings = task.input_data.get("embeddings", [])
        chunks     = task.input_data.get("chunks", [])
        metadata   = task.input_data.get("metadata", {})

        try:
            s3_client = _s3()

            # ── Download existing vectors.npy ──────────────────────────────
            try:
                obj      = s3_client.get_object(Bucket=S3_BUCKET, Key="vectors.npy")
                existing = np.load(io.BytesIO(obj["Body"].read()))
            except Exception:
                existing = np.empty((0, 256), dtype=np.float32)

            # ── Download existing metadata.json ────────────────────────────
            try:
                obj      = s3_client.get_object(Bucket=S3_BUCKET, Key="metadata.json")
                existing_meta = json.loads(obj["Body"].read())
            except Exception:
                existing_meta = []

            # ── Append new vectors ─────────────────────────────────────────
            new_vecs = np.array(embeddings, dtype=np.float32)
            combined = np.vstack([existing, new_vecs]) if existing.shape[0] else new_vecs

            buf = io.BytesIO()
            np.save(buf, combined)
            buf.seek(0)
            s3_client.put_object(Bucket=S3_BUCKET, Key="vectors.npy", Body=buf.getvalue())

            # ── Append new metadata entries ────────────────────────────────
            for i, chunk in enumerate(chunks):
                entry = {**metadata, **chunk}
                entry["vector_idx"] = len(existing) + i
                existing_meta.append(entry)

            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key="metadata.json",
                Body=json.dumps(existing_meta),
                ContentType="application/json",
            )

            result.status = TaskResultStatus.COMPLETED
            result.output_data = {
                "vectors_added":  len(embeddings),
                "total_vectors":  combined.shape[0],
                "doc_id":         doc_id,
            }
            print(f"[update_s3_vectors] ✅ {doc_id} — added {len(embeddings)} vectors (total {combined.shape[0]})")
        except Exception as exc:
            result.status = TaskResultStatus.FAILED
            result.reason_for_incompletion = str(exc)
        return result


# ── 4c. Parse structured metadata [FORK Branch B] ─────────────────────────────
class ParseMetadataWorker(WorkerInterface):
    def __init__(self):
        super().__init__("askmybank_parse_metadata")

    def execute(self, task: Task) -> TaskResult:
        result      = self.get_task_result_from_task(task)
        raw_text    = task.input_data.get("raw_text", "")
        doc_id      = task.input_data.get("doc_id", "")
        doc_type    = task.input_data.get("doc_type", "")
        customer_id = task.input_data.get("customer_id", "")

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
  "dispute_amount": null or number,
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

            # Extract JSON from response
            match     = re.search(r'\{.*\}', raw, re.DOTALL)
            extracted = json.loads(match.group(0)) if match else {}

            structured = {
                "doc_id":      doc_id,
                "doc_type":    doc_type,
                "customer_id": customer_id,
                **{k: extracted.get(k) for k in [
                    "customer_name", "account_number", "branch_name", "rm_name",
                    "case_status", "filed_date", "dispute_amount", "complaint_type",
                    "dispute_type", "priority", "resolution", "case_summary",
                ]},
            }

            result.status = TaskResultStatus.COMPLETED
            result.output_data = {
                "structured_metadata": structured,
                "doc_id": doc_id,
            }
            print(f"[parse_metadata] ✅ {doc_id} — extracted {len(structured)} fields")
        except Exception as exc:
            # Fallback to minimal metadata
            result.status = TaskResultStatus.COMPLETED
            result.output_data = {
                "structured_metadata": {
                    "doc_id": doc_id, "doc_type": doc_type,
                    "customer_id": customer_id, "case_summary": "Auto-ingested document",
                },
                "fallback": True, "error": str(exc),
            }
        return result


# ── 4d. Upsert ClickHouse documents table [FORK Branch B cont.] ───────────────
class UpsertClickhouseWorker(WorkerInterface):
    def __init__(self):
        super().__init__("askmybank_upsert_clickhouse")

    def execute(self, task: Task) -> TaskResult:
        result   = self.get_task_result_from_task(task)
        doc_id   = task.input_data.get("doc_id", "")
        metadata = task.input_data.get("metadata", {})
        s3_key   = task.input_data.get("s3_key", "")

        try:
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
                    f"s3://{S3_BUCKET}/{s3_key}",
                    safe("customer_id"),
                    safe("customer_name"),
                    safe("account_number"),
                    "",   # account_type
                    "",   # sort_code
                    "",   # branch_code
                    safe("branch_name"),
                    "",   # rm_id
                    safe("rm_name"),
                    "",   # rm_email
                    safe("case_status"),
                    None, # filed_date
                    None, # closed_date
                    safe("resolution"),
                    safe("dispute_type"),
                    metadata.get("dispute_amount"),
                    safe("complaint_type"),
                    safe("priority", "Medium"),
                    metadata.get("compensation_paid"),
                    "",   # request_type
                    "",   # request_status
                    None, # request_date
                    None, # processed_date
                    None, # statement_date
                    None, # closing_balance
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

            result.status = TaskResultStatus.COMPLETED
            result.output_data = {
                "rows_inserted": 1,
                "doc_id": doc_id,
                "table":  "banking_docs.documents",
            }
            print(f"[upsert_clickhouse] ✅ {doc_id} inserted into ClickHouse")
        except Exception as exc:
            result.status = TaskResultStatus.FAILED
            result.reason_for_incompletion = str(exc)
        return result


# ── 5. Pipeline complete ──────────────────────────────────────────────────────
class PipelineCompleteWorker(WorkerInterface):
    def __init__(self):
        super().__init__("askmybank_pipeline_complete")

    def execute(self, task: Task) -> TaskResult:
        result        = self.get_task_result_from_task(task)
        doc_id        = task.input_data.get("doc_id", "")
        vector_count  = task.input_data.get("vector_count", 0)
        ch_inserted   = task.input_data.get("ch_inserted", 0)
        source_system = task.input_data.get("source_system", "manual")

        print(f"[pipeline_complete] ✅ {doc_id} | {vector_count} vectors | CH rows: {ch_inserted} | src: {source_system}")
        result.status = TaskResultStatus.COMPLETED
        result.output_data = {
            "doc_id":          doc_id,
            "vector_count":    vector_count,
            "pipeline_status": "completed",
            "completed_at":    datetime.utcnow().isoformat(),
        }
        return result


ALL_WORKERS = [
    DetectDocTypeWorker(),
    TextractExtractWorker(),
    ChunkTextWorker(),
    GenerateEmbeddingsWorker(),
    UpdateS3VectorsWorker(),
    ParseMetadataWorker(),
    UpsertClickhouseWorker(),
    PipelineCompleteWorker(),
]
