"""
AskMyBank.ai — AWS Lambda entry point
FastAPI app wrapped with Mangum for Lambda + API Gateway HTTP API.

The RAG object is initialised once per Lambda container (module level).
Warm invocations reuse it — no cold start penalty for subsequent calls.

HITL endpoints (Phase 2):
  POST /hitl              — create a back-office ticket
  GET  /hitl/pending      — list pending tickets (back-office UI)
  GET  /hitl/status/{id}  — poll ticket status (chat UI)
  POST /hitl/{id}/decide  — approve / reject a ticket (back-office UI)
  POST /hitl/gap          — log a content gap (catch-all escalation)

Pipeline endpoints (Phase 3 — queued, scheduled, controlled throughput):
  POST /pipeline/ingest          — enqueue a document (writes to SQS intake queue)
  POST /pipeline/drain           — pop a batch from queue → start Orkes workflows
                                   also triggered by EventBridge every 30 min
  GET  /pipeline/queue/status    — queue depth, DLQ depth, batch size
  POST /pipeline/step/detect     — Orkes HTTP task: classify doc_type, generate doc_id
  POST /pipeline/step/textract   — Orkes HTTP task: Textract PDF → raw text
  POST /pipeline/step/chunk      — Orkes HTTP task: chunk text into passages
  POST /pipeline/step/embed      — Orkes HTTP task: Bedrock embeddings
  POST /pipeline/step/s3vec      — Orkes HTTP task: append to S3 vectors (idempotent)
  POST /pipeline/step/meta       — Orkes HTTP task: parse structured metadata
  POST /pipeline/step/clickhouse — Orkes HTTP task: upsert ClickHouse (idempotent)
  POST /pipeline/step/complete   — Orkes HTTP task: log completion

Direct Lambda invocation (EventBridge):
  {"source": "warming"}        — keep-warm ping (existing)
  {"source": "pipeline_drain"} — scheduled drain (every 30 min)
"""

import json
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel, field_validator

from rag.chain import BankingRAG
from rag.hitl_client import (
    HITLClient, HITLUnavailableError, HITLNotFoundError,
    TEMPORAL_WORKFLOWS,
)
from rag.pipeline_steps import (
    step_detect, step_textract, step_chunk, step_embed,
    step_s3vec, step_meta, step_clickhouse, step_complete,
)

# ── SQS client (shared — HITL queue + Pipeline intake queue) ─────────────────
_sqs              = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "us-east-1"))
HITL_QUEUE_URL    = os.getenv("HITL_SQS_QUEUE_URL", "")
PIPELINE_QUEUE_URL = os.getenv("PIPELINE_QUEUE_URL", "")
PIPELINE_BATCH_SIZE = int(os.getenv("PIPELINE_BATCH_SIZE", "20"))

def _publish_to_sqs(ticket: dict, req_extras: dict) -> str:
    """
    Publish a HITL case to SQS for Appian to pick up.
    Returns the SQS MessageId, or "" on failure (non-fatal — ticket still created).

    Message contains everything Appian needs to create a case:
      - ticket details (id, action, docs, query, address)
      - callback_url so Appian knows where to POST the decision
      - orkes_ui_url so Appian can link to the live workflow DAG
    """
    if not HITL_QUEUE_URL:
        return ""
    try:
        api_base = os.getenv("API_BASE_URL",
                             "https://r6v15i892m.execute-api.us-east-1.amazonaws.com")
        message  = {
            **ticket,
            **req_extras,                     # customer_id, delivery_address, doc_ids
            "callback_url": f"{api_base}/hitl/{ticket['ticket_id']}/decide",
            "source":       "askmybank_hitl",
        }
        resp = _sqs.send_message(
            QueueUrl    = HITL_QUEUE_URL,
            MessageBody = json.dumps(message),
            # Route different case types to dedicated Appian process models
            MessageAttributes={
                "action": {
                    "StringValue": ticket.get("action", ""),
                    "DataType":    "String",
                },
                "ticket_id": {
                    "StringValue": ticket.get("ticket_id", ""),
                    "DataType":    "String",
                },
            },
        )
        msg_id = resp.get("MessageId", "")
        print(f"SQS published ticket {ticket['ticket_id']} → MessageId={msg_id}")
        return msg_id
    except ClientError as exc:
        # Non-fatal — ticket is already in ClickHouse + Orkes running
        print(f"SQS publish failed ({exc}) — ticket still active in ClickHouse/Orkes")


# ── Pipeline failure → HITL ops ticket ───────────────────────────────────────
def _pipeline_failure_hitl(step: str, doc_id: str, s3_key: str, error: str) -> None:
    """
    Raise a HITL ops ticket when a pipeline step fails all retries.
    Ops team sees it in review.html, inspects the Orkes execution, fixes root
    cause, then re-triggers POST /pipeline/ingest for the same s3_key.

    Non-fatal — if the HITL ticket itself fails, we still surface the error
    to Orkes (via the HTTP 500 response) so it shows FAILED in the Orkes UI.
    """
    try:
        api_base = os.getenv("API_BASE_URL",
                             "https://r6v15i892m.execute-api.us-east-1.amazonaws.com")
        hitl.log_content_gap(
            original_query = f"[PIPELINE FAILURE] step={step} doc={doc_id} s3={s3_key}",
            ai_response    = f"Error: {error}",
            user_feedback  = (
                f"Auto-raised by pipeline step '{step}'. "
                f"Fix root cause then re-ingest:\n"
                f"  curl -X POST {api_base}/pipeline/ingest \\\n"
                f"    -H 'Content-Type: application/json' \\\n"
                f"    -d '{{\"s3_key\":\"{s3_key}\",\"source_system\":\"retry\"}}'"
            ),
        )
        print(f"[pipeline_failure_hitl] Ops ticket raised for {step}/{doc_id}")
    except Exception as exc:
        print(f"[pipeline_failure_hitl] Failed to raise ticket: {exc}")


# ── Pipeline drain — shared logic (HTTP endpoint + EventBridge direct invoke) ─
def _run_pipeline_drain() -> dict:
    """
    Pop up to PIPELINE_BATCH_SIZE messages from PipelineQueue and start
    one Orkes workflow per document. Idempotency is enforced inside each
    step endpoint so re-draining the same message is safe.

    Called from:
      POST /pipeline/drain      — manual trigger (demo, ops)
      EventBridge pipeline_drain — every 30 minutes (scheduled)
    """
    if not PIPELINE_QUEUE_URL:
        return {"error": "PIPELINE_QUEUE_URL not configured", "processed": 0}

    api_base        = os.getenv("API_BASE_URL",
                                "https://r6v15i892m.execute-api.us-east-1.amazonaws.com")
    remaining_batch = PIPELINE_BATCH_SIZE
    started         = []
    failed          = []

    # SQS returns max 10 per call — loop until we have the full batch or queue empty
    while remaining_batch > 0:
        fetch = min(remaining_batch, 10)
        resp  = _sqs.receive_message(
            QueueUrl            = PIPELINE_QUEUE_URL,
            MaxNumberOfMessages = fetch,
            WaitTimeSeconds     = 1,     # short poll — don't hold the Lambda
            VisibilityTimeout   = 600,   # 10 min — workflow start + first step
        )
        messages = resp.get("Messages", [])
        if not messages:
            break   # queue empty

        for msg in messages:
            try:
                body = json.loads(msg["Body"])

                # Handle two message formats:
                # 1. POST /pipeline/ingest → {"s3_key": "raw-docs/file.pdf", ...}
                # 2. S3 event notification → {"Records": [{"s3": {"object": {"key": ...}}}]}
                s3_key = body.get("s3_key", "")
                if not s3_key:
                    records = body.get("Records", [])
                    if records:
                        s3_key = records[0].get("s3", {}).get("object", {}).get("key", "")
                        # S3 URL-encodes keys with spaces — decode just in case
                        import urllib.parse
                        s3_key = urllib.parse.unquote_plus(s3_key)
                        # Enrich body with parsed fields for workflow input
                        body["s3_key"]        = s3_key
                        body["doc_type"]      = body.get("doc_type", "")
                        body["customer_id"]   = body.get("customer_id", "")
                        body["source_system"] = "s3_event"
                    else:
                        print(f"[drain] ⚠️  Skipping message — no s3_key and no Records: {msg['MessageId']}")
                        _sqs.delete_message(QueueUrl=PIPELINE_QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])
                        continue

                wf_input = {
                    **body,
                    "api_base_url":  api_base,
                    "backoffice_key": BACKOFFICE_KEY,
                }
                wf_id = hitl.trigger_orkes_workflow(
                    workflow_name  = "askmybank_document_pipeline",
                    workflow_input = wf_input,
                    correlation_id = f"pipeline-{s3_key}",
                )

                if wf_id:
                    # Workflow started — delete the message so it's not re-processed
                    _sqs.delete_message(
                        QueueUrl      = PIPELINE_QUEUE_URL,
                        ReceiptHandle = msg["ReceiptHandle"],
                    )
                    started.append({
                        "s3_key":      s3_key,
                        "workflow_id": wf_id,
                        "orkes_ui":    f"https://developer.orkescloud.com/execution/{wf_id}",
                    })
                    print(f"[drain] ✅ Started {wf_id} for {s3_key}")
                else:
                    # Orkes unavailable — leave message visible so it retries
                    failed.append({"s3_key": s3_key, "reason": "orkes_unavailable"})
                    print(f"[drain] ⚠️  Orkes unavailable for {s3_key} — message left in queue")

            except Exception as exc:
                failed.append({"s3_key": body.get("s3_key", "?"), "reason": str(exc)})
                print(f"[drain] ❌ {exc}")

        remaining_batch -= len(messages)

    # Get current queue depth for visibility
    try:
        attrs = _sqs.get_queue_attributes(
            QueueUrl       = PIPELINE_QUEUE_URL,
            AttributeNames = ["ApproximateNumberOfMessages",
                              "ApproximateNumberOfMessagesNotVisible"],
        )
        queue_depth    = int(attrs["Attributes"].get("ApproximateNumberOfMessages", 0))
        in_flight      = int(attrs["Attributes"].get("ApproximateNumberOfMessagesNotVisible", 0))
    except Exception:
        queue_depth = in_flight = -1

    return {
        "batch_size":      PIPELINE_BATCH_SIZE,
        "started":         len(started),
        "failed":          len(failed),
        "queue_remaining": queue_depth,
        "in_flight":       in_flight,
        "workflows":       started,
        "errors":          failed,
    }


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="AskMyBank API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://askmybank.ai", "https://www.askmybank.ai",
                   "https://dpanwar-vigyan.github.io", "http://localhost:3000", "*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── Singletons — initialised once, reused across warm invocations ─────────────
print("Initialising BankingRAG...")
rag = BankingRAG()
print("BankingRAG ready.")

# HITL client — lazy, connects on first ticket operation
hitl = HITLClient()

# Back-office key — simple auth for /hitl/pending and /hitl/{id}/decide
BACKOFFICE_KEY = os.getenv("DEMO_API_KEY", "")


# ── Models ───────────────────────────────────────────────────────────────────
class AskRequest(BaseModel):
    query: str
    source: str = "user"   # "user" | "warming"

class HITLActionRequest(BaseModel):
    action:           str               # reopen_case | send_duplicate | legal_export | escalate_to_it
    doc_ids:          list[str] = []    # source doc IDs from the RAG response
    original_query:   str = ""
    user_note:        str = ""
    delivery_address: str = ""          # required for send_duplicate
    customer_id:      str = ""          # passed for eStatement / address verify

class HITLDecisionRequest(BaseModel):
    decision:      str   # "approved" | "rejected"
    resolver_name: str
    resolver_note: str = ""

class ContentGapRequest(BaseModel):
    original_query: str
    ai_response:    str
    user_feedback:  str

class PipelineIngestRequest(BaseModel):
    s3_key:        str               # e.g. "raw-docs/new_complaint.pdf"
    doc_type:      str = ""          # Complaint | Dispute | eStatement | AccountMaintenance
    customer_id:   str = ""
    source_system: str = "manual"    # "manual" | "s3_event"

# ── Pipeline step request models (called by Orkes HTTP system tasks v2) ───────

class PipelineDetectRequest(BaseModel):
    s3_key:      str
    doc_type:    Optional[str] = ""   # Optional: Orkes may send null when workflow input missing
    customer_id: Optional[str] = ""

    model_config = {"populate_by_name": True}

    @field_validator("doc_type", "customer_id", mode="before")
    @classmethod
    def coerce_none_to_empty(cls, v):
        return v or ""  # null / None / "" → ""

class PipelineTextractRequest(BaseModel):
    s3_key: str
    doc_id: str

class PipelineChunkRequest(BaseModel):
    raw_text:   str
    doc_id:     str
    doc_type:   str = ""
    page_count: int = 1

class PipelineEmbedRequest(BaseModel):
    chunks: list
    doc_id: str

class PipelineS3VecRequest(BaseModel):
    doc_id:     str
    embeddings: list
    chunks:     list
    metadata:   dict = {}

class PipelineMetaRequest(BaseModel):
    raw_text:    str
    doc_id:      str
    doc_type:    str = ""
    customer_id: str = ""

class PipelineClickhouseRequest(BaseModel):
    doc_id:   str
    metadata: dict
    s3_key:   str

class PipelineCompleteRequest(BaseModel):
    doc_id:        str
    vector_count:  int = 0
    ch_inserted:   int = 0
    source_system: str = "manual"

# Orkes workflow names — only legal_export (data pipeline) stays on Orkes
# send_duplicate moved to Temporal (Signal-driven, no polling)
_ORKES_WORKFLOWS = {
    "legal_export": "askmybank_document_pipeline",
}


# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    """Health check — includes cache stats."""
    from rag.chain import _ANSWER_CACHE, _EMBED_CACHE
    return {
        "status":         "ok",
        "vector_backend": "s3_numpy" if os.getenv("S3_VECTORS_BUCKET") else "chromadb",
        "answer_cache":   len(_ANSWER_CACHE),
        "embed_cache":    len(_EMBED_CACHE),
    }


@app.post("/ask")
def ask(req: AskRequest):
    """Main RAG endpoint — called from the chat UI."""
    if req.source == "warming":
        # EventBridge ping — just confirm Lambda is warm, skip RAG
        return {"status": "warm", "message": "Lambda is warm"}

    if not req.query or not req.query.strip():
        raise HTTPException(status_code=400, detail="query cannot be empty")

    result = rag.ask(req.query.strip())
    return result


@app.get("/")
def root():
    return {"service": "AskMyBank API", "version": "2.1", "docs": "/docs"}


# ── HITL — create ticket ──────────────────────────────────────────────────────
@app.post("/hitl")
def create_hitl_ticket(req: HITLActionRequest):
    """
    Chat UI calls this when a banker clicks an action button.
    Returns ticket_id so the UI can poll /hitl/status/{ticket_id}.
    """
    valid_actions = {"reopen_case", "send_duplicate", "legal_export", "escalate_to_it"}
    if req.action not in valid_actions:
        raise HTTPException(status_code=400, detail=f"Unknown action: {req.action}")

    if req.action == "escalate_to_it":
        # Route to content gap log instead of workflow ticket
        try:
            result = hitl.log_content_gap(
                original_query = req.original_query,
                ai_response    = "",
                user_feedback  = req.user_note or "No additional details provided.",
            )
            return {"type": "gap", **result}
        except HITLUnavailableError as exc:
            raise HTTPException(status_code=503, detail=str(exc))

    try:
        ticket = hitl.create_ticket(
            action           = req.action,
            doc_ids          = req.doc_ids,
            original_query   = req.original_query,
            user_note        = req.user_note,
            delivery_address = req.delivery_address,
        )

        wf_input = {
            "ticket_id":       ticket["ticket_id"],
            "doc_ids":         req.doc_ids,
            "delivery_address":req.delivery_address,
            "original_query":  req.original_query,
            "customer_id":     req.customer_id,
        }

        # ── Temporal: Signal-driven HITL (send_duplicate, reopen, escalate) ─
        temporal_wf = TEMPORAL_WORKFLOWS.get(req.action)
        if temporal_wf:
            wf_id = hitl.trigger_temporal_workflow(
                workflow_name  = temporal_wf,
                workflow_input = wf_input,
                workflow_id    = ticket["ticket_id"],   # use ticket_id as workflow_id
            )                                           # so we can signal it by ticket_id
            if wf_id:
                hitl._store_workflow_run_id(ticket["ticket_id"], wf_id)
                ticket["workflow_run_id"] = wf_id
                ticket["temporal_ui_url"] = (
                    f"https://cloud.temporal.io/namespaces/"
                    f"{os.getenv('TEMPORAL_NAMESPACE','')}/workflows/{wf_id}"
                )

        # ── Orkes: visual DAG pipeline (legal_export only) ────────────────────
        orkes_wf = _ORKES_WORKFLOWS.get(req.action)
        if orkes_wf:
            wf_id = hitl.trigger_orkes_workflow(orkes_wf, wf_input, ticket["ticket_id"])
            if wf_id:
                hitl._store_workflow_run_id(ticket["ticket_id"], wf_id)
                ticket["workflow_run_id"] = wf_id
                ticket["orkes_ui_url"]    = (
                    f"https://developer.orkescloud.com/execution/{wf_id}"
                )

        # ── Publish to SQS — guaranteed delivery buffer for Appian ──────────
        # Non-blocking: if SQS publish fails, ticket still exists in ClickHouse
        # and Orkes workflow is already running. Appian can poll /hitl/pending
        # as a fallback to clear any backlog.
        req_extras = {
            "customer_id":      req.customer_id,
            "delivery_address": req.delivery_address,
            "doc_ids":          req.doc_ids,
        }
        sqs_msg_id = _publish_to_sqs(ticket, req_extras)
        if sqs_msg_id:
            ticket["sqs_message_id"] = sqs_msg_id

        return {"type": "ticket", **ticket}
    except HITLUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


# ── HITL — poll ticket status (chat UI polling) ───────────────────────────────
@app.get("/hitl/status/{ticket_id}")
def get_hitl_status(ticket_id: str):
    """Chat UI polls this every 5s to show live ticket status."""
    try:
        return hitl.get_ticket_status(ticket_id)
    except HITLNotFoundError:
        raise HTTPException(status_code=404, detail=f"Ticket {ticket_id} not found")
    except HITLUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


# ── HITL — list pending tickets (back-office review UI) ──────────────────────
@app.get("/hitl/pending")
def get_pending_tickets(x_backoffice_key: Optional[str] = Header(default=None)):
    """
    Returns all pending/in_progress tickets.
    Protected by X-Backoffice-Key header (same value as DEMO_API_KEY).
    """
    if BACKOFFICE_KEY and x_backoffice_key != BACKOFFICE_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Backoffice-Key header")
    try:
        tickets = hitl.get_pending_tickets()
        return {"tickets": tickets, "count": len(tickets)}
    except HITLUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


# ── HITL — record decision (back-office approve/reject) ───────────────────────
@app.post("/hitl/{ticket_id}/decide")
def decide_ticket(
    ticket_id: str,
    req: HITLDecisionRequest,
    x_backoffice_key: Optional[str] = Header(default=None),
):
    """
    Back-office manager approves or rejects a ticket.

    WORKERLESS design — Lambda does everything, no Temporal worker process needed:
      1. ClickHouse updated (source of truth + Orkes DO_WHILE for legal_export)
      2. Temporal Signal sent — workflow wakes and records completion
      3. If approved: Lambda runs fulfilment directly (S3 + cover note + dispatch log)
         This replaces what the Temporal activities/worker used to do.

    Appian calls this endpoint directly after the banker's decision.
    """
    if BACKOFFICE_KEY and x_backoffice_key != BACKOFFICE_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Backoffice-Key header")
    try:
        # Step 1: Update ClickHouse
        result = hitl.update_decision(
            ticket_id     = ticket_id,
            decision      = req.decision,
            resolver_name = req.resolver_name,
            resolver_note = req.resolver_note,
        )

        # Step 2: Signal Temporal — workflow records decision and completes
        signal_sent = hitl.send_temporal_signal(
            workflow_id    = ticket_id,
            signal_name    = "decision_received",
            signal_payload = {
                "decision":      req.decision,
                "resolver_name": req.resolver_name,
                "resolver_note": req.resolver_note,
            },
        )
        result["temporal_signal_sent"] = signal_sent

        # Step 3: Fulfilment — run directly in Lambda (no worker needed)
        # For send_duplicate tickets: generate S3 URL + cover note + dispatch log
        if req.decision == "approved":
            fulfilment = hitl.run_fulfilment(
                ticket_id     = ticket_id,
                resolver_name = req.resolver_name,
                resolver_note = req.resolver_note,
            )
            result["fulfilment"] = fulfilment

        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HITLNotFoundError:
        raise HTTPException(status_code=404, detail=f"Ticket {ticket_id} not found")
    except HITLUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


# ── HITL — log content gap (catch-all escalation) ─────────────────────────────
@app.post("/hitl/gap")
def log_content_gap(req: ContentGapRequest):
    """
    Chat UI calls this when banker clicks 'escalate_to_it'.
    Creates a content gap record for the IT/Content team.
    """
    try:
        result = hitl.log_content_gap(
            original_query = req.original_query,
            ai_response    = req.ai_response,
            user_feedback  = req.user_feedback,
        )
        return result
    except HITLUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


# ── Pipeline step endpoints — called by Orkes HTTP system tasks (v2) ─────────
# Auth: same X-Backoffice-Key as back-office HITL endpoints.
# Orkes includes the header in every HTTP system task call.

def _check_pipeline_auth(x_backoffice_key: str = Header(default="")):
    if BACKOFFICE_KEY and x_backoffice_key != BACKOFFICE_KEY:
        raise HTTPException(status_code=401, detail="Invalid X-Backoffice-Key")


@app.post("/pipeline/step/detect")
def pipeline_step_detect(req: PipelineDetectRequest,
                          x_backoffice_key: str = Header(default="")):
    """Orkes HTTP task: classify doc_type from s3_key, generate doc_id. ~1ms, no AWS calls."""
    _check_pipeline_auth(x_backoffice_key)
    try:
        return step_detect(s3_key=req.s3_key, doc_type=req.doc_type, customer_id=req.customer_id)
    except Exception as exc:
        _pipeline_failure_hitl("detect", "", req.s3_key, str(exc))
        raise HTTPException(status_code=500, detail=f"Detect failed: {exc}")


@app.post("/pipeline/step/textract")
def pipeline_step_textract(req: PipelineTextractRequest,
                            x_backoffice_key: str = Header(default="")):
    """Orkes HTTP task: Textract PDF → raw text. Uses SOURCE_BUCKET (not vectors bucket)."""
    _check_pipeline_auth(x_backoffice_key)
    try:
        return step_textract(s3_key=req.s3_key, doc_id=req.doc_id)
    except Exception as exc:
        _pipeline_failure_hitl("textract", req.doc_id, req.s3_key, str(exc))
        raise HTTPException(status_code=500, detail=f"Textract failed: {exc}")


@app.post("/pipeline/step/chunk")
def pipeline_step_chunk(req: PipelineChunkRequest,
                        x_backoffice_key: str = Header(default="")):
    """Orkes HTTP task: split raw text into semantic passages."""
    _check_pipeline_auth(x_backoffice_key)
    try:
        return step_chunk(
            raw_text   = req.raw_text,
            doc_id     = req.doc_id,
            doc_type   = req.doc_type,
            page_count = req.page_count,
        )
    except Exception as exc:
        _pipeline_failure_hitl("chunk", req.doc_id, req.s3_key if hasattr(req,"s3_key") else "", str(exc))
        raise HTTPException(status_code=500, detail=f"Chunk failed: {exc}")


@app.post("/pipeline/step/embed")
def pipeline_step_embed(req: PipelineEmbedRequest,
                        x_backoffice_key: str = Header(default="")):
    """Orkes HTTP task [FORK A]: Bedrock Titan embeddings for each chunk."""
    _check_pipeline_auth(x_backoffice_key)
    try:
        return step_embed(chunks=req.chunks, doc_id=req.doc_id)
    except Exception as exc:
        _pipeline_failure_hitl("embed", req.doc_id, "", str(exc))
        raise HTTPException(status_code=500, detail=f"Embedding failed: {exc}")


@app.post("/pipeline/step/s3vec")
def pipeline_step_s3vec(req: PipelineS3VecRequest,
                        x_backoffice_key: str = Header(default="")):
    """
    Orkes HTTP task [FORK A]: append embeddings to S3 vectors.npy + metadata.json.
    Idempotent — if doc_id already indexed, skips append and returns success.
    Safe to re-run after a failure without creating duplicate vectors.
    """
    _check_pipeline_auth(x_backoffice_key)
    try:
        return step_s3vec(
            doc_id     = req.doc_id,
            embeddings = req.embeddings,
            chunks     = req.chunks,
            metadata   = req.metadata,
        )
    except Exception as exc:
        _pipeline_failure_hitl("s3vec", req.doc_id, "", str(exc))
        raise HTTPException(status_code=500, detail=f"S3 vector update failed: {exc}")


@app.post("/pipeline/step/meta")
def pipeline_step_meta(req: PipelineMetaRequest,
                       x_backoffice_key: str = Header(default="")):
    """Orkes HTTP task [FORK B]: parse structured metadata with Nova Lite."""
    _check_pipeline_auth(x_backoffice_key)
    try:
        return step_meta(
            raw_text    = req.raw_text,
            doc_id      = req.doc_id,
            doc_type    = req.doc_type,
            customer_id = req.customer_id,
        )
    except Exception as exc:
        _pipeline_failure_hitl("meta", req.doc_id, "", str(exc))
        raise HTTPException(status_code=500, detail=f"Metadata parsing failed: {exc}")


@app.post("/pipeline/step/clickhouse")
def pipeline_step_clickhouse(req: PipelineClickhouseRequest,
                              x_backoffice_key: str = Header(default="")):
    """
    Orkes HTTP task [FORK B]: upsert document metadata into ClickHouse.
    Idempotent — checks for existing doc_id before inserting.
    ReplacingMergeTree handles eventual deduplication at the engine level.
    """
    _check_pipeline_auth(x_backoffice_key)
    try:
        return step_clickhouse(
            doc_id   = req.doc_id,
            metadata = req.metadata,
            s3_key   = req.s3_key,
        )
    except Exception as exc:
        _pipeline_failure_hitl("clickhouse", req.doc_id, req.s3_key, str(exc))
        raise HTTPException(status_code=500, detail=f"ClickHouse upsert failed: {exc}")


@app.post("/pipeline/step/complete")
def pipeline_step_complete(req: PipelineCompleteRequest,
                           x_backoffice_key: str = Header(default="")):
    """Orkes HTTP task: log pipeline completion. Straight-through — no HITL on success."""
    _check_pipeline_auth(x_backoffice_key)
    return step_complete(
        doc_id        = req.doc_id,
        vector_count  = req.vector_count,
        ch_inserted   = req.ch_inserted,
        source_system = req.source_system,
    )


# ── Document ingestion pipeline trigger ───────────────────────────────────────
@app.post("/pipeline/ingest")
def trigger_pipeline(req: PipelineIngestRequest):
    """
    Enqueue a document for AI ingestion.

    When PIPELINE_QUEUE_URL is set (production): writes to SQS intake queue.
    The scheduled drainer (EventBridge, every 30 min) pops the queue and starts
    Orkes workflows in controlled tranches. Queue depth = live backlog.

    When PIPELINE_QUEUE_URL is not set (local dev fallback): starts Orkes
    workflow directly (original v1 behaviour — no queue, no tranche control).

    Example:
        POST /pipeline/ingest
        {"s3_key": "raw-docs/CMP-DEMO-001.pdf", "doc_type": "Complaint"}
    """
    if not req.s3_key:
        raise HTTPException(status_code=400, detail="s3_key is required")

    import datetime
    message = {
        "s3_key":        req.s3_key,
        "doc_type":      req.doc_type,
        "customer_id":   req.customer_id,
        "source_system": req.source_system,
        "received_at":   datetime.datetime.utcnow().isoformat() + "Z",
    }

    # ── Production path: write to SQS, drain on schedule ─────────────────────
    if PIPELINE_QUEUE_URL:
        try:
            resp = _sqs.send_message(
                QueueUrl    = PIPELINE_QUEUE_URL,
                MessageBody = json.dumps(message),
                MessageAttributes={
                    "doc_type": {"StringValue": req.doc_type or "unknown", "DataType": "String"},
                    "source":   {"StringValue": req.source_system,         "DataType": "String"},
                },
            )
            msg_id = resp.get("MessageId", "")
            print(f"[ingest] Queued {req.s3_key} → SQS MessageId={msg_id}")

            # Queue depth for caller visibility
            try:
                attrs = _sqs.get_queue_attributes(
                    QueueUrl       = PIPELINE_QUEUE_URL,
                    AttributeNames = ["ApproximateNumberOfMessages"],
                )
                depth = int(attrs["Attributes"].get("ApproximateNumberOfMessages", 0))
            except Exception:
                depth = -1

            return {
                "status":         "queued",
                "message_id":     msg_id,
                "s3_key":         req.s3_key,
                "doc_type":       req.doc_type,
                "queue_depth":    depth,
                "note":           f"Document queued. Drainer runs every 30 min "
                                  f"(batch={PIPELINE_BATCH_SIZE}). "
                                  f"Trigger manually: POST /pipeline/drain",
            }
        except ClientError as exc:
            raise HTTPException(status_code=503, detail=f"SQS enqueue failed: {exc}")

    # ── Dev fallback: direct Orkes start (no queue) ───────────────────────────
    api_base = os.getenv("API_BASE_URL",
                         "https://r6v15i892m.execute-api.us-east-1.amazonaws.com")
    wf_input = {
        **message,
        "api_base_url":  api_base,
        "backoffice_key": BACKOFFICE_KEY,
    }
    wf_id = hitl.trigger_orkes_workflow(
        workflow_name  = "askmybank_document_pipeline",
        workflow_input = wf_input,
        correlation_id = f"pipeline-{req.s3_key}",
    )
    if not wf_id:
        raise HTTPException(status_code=503,
                            detail="No queue configured and Orkes unavailable")
    return {
        "status":       "started_direct",
        "workflow_id":  wf_id,
        "s3_key":       req.s3_key,
        "orkes_ui_url": f"https://developer.orkescloud.com/execution/{wf_id}",
        "note":         "Direct start (no queue — dev mode). Set PIPELINE_QUEUE_URL for production.",
    }


@app.post("/pipeline/drain")
def drain_pipeline(x_backoffice_key: str = Header(default="")):
    """
    Manually trigger a batch drain of the pipeline queue.
    Also called automatically by EventBridge every 30 minutes.

    Useful for:
      - Demo: trigger immediately after ingesting docs to show live DAG
      - Ops: clear backlog faster than the schedule allows
      - Testing: drain without waiting for EventBridge

    Returns workflow IDs + Orkes UI links for all started workflows.
    """
    _check_pipeline_auth(x_backoffice_key)
    result = _run_pipeline_drain()
    if result.get("error"):
        raise HTTPException(status_code=503, detail=result["error"])
    return result


@app.get("/pipeline/queue/status")
def pipeline_queue_status(x_backoffice_key: str = Header(default="")):
    """
    Live queue depth — how many documents are waiting to be ingested.
    Shows main queue + DLQ (docs that failed to start 3 times).
    """
    _check_pipeline_auth(x_backoffice_key)

    if not PIPELINE_QUEUE_URL:
        return {"configured": False, "note": "PIPELINE_QUEUE_URL not set"}

    pipeline_dlq_url = PIPELINE_QUEUE_URL.replace(
        f"askmybank-pipeline-", "askmybank-pipeline-dlq-"
    )

    def _depth(url):
        try:
            attrs = _sqs.get_queue_attributes(
                QueueUrl       = url,
                AttributeNames = ["ApproximateNumberOfMessages",
                                  "ApproximateNumberOfMessagesNotVisible"],
            )
            return {
                "waiting":   int(attrs["Attributes"].get("ApproximateNumberOfMessages", 0)),
                "in_flight": int(attrs["Attributes"].get("ApproximateNumberOfMessagesNotVisible", 0)),
            }
        except Exception as e:
            return {"error": str(e)}

    pipeline_stats = _depth(PIPELINE_QUEUE_URL)
    dlq_stats      = _depth(pipeline_dlq_url)

    waiting = pipeline_stats.get("waiting", 0)
    eta_hours = round((waiting / PIPELINE_BATCH_SIZE) * 0.5, 1) if waiting > 0 else 0

    return {
        "configured":   True,
        "batch_size":   PIPELINE_BATCH_SIZE,
        "schedule":     "every 30 minutes",
        "pipeline_queue": pipeline_stats,
        "dead_letter_queue": dlq_stats,
        "eta_hours":    eta_hours,
        "note":         f"{waiting} docs waiting · ETA ~{eta_hours}h at current batch size" if waiting else "Queue empty",
    }


# ── Lambda handler ────────────────────────────────────────────────────────────
# Mangum handles HTTP API Gateway events.
# EventBridge warming pings arrive as raw JSON (not HTTP) — intercept them first.
_mangum = Mangum(app, lifespan="off")

def handler(event, context):
    # EventBridge direct invocations — not routed through API Gateway / Mangum
    if isinstance(event, dict):
        src = event.get("source", "")

        if src == "warming":
            # Keep-warm ping — every 5 min
            print("EventBridge warming ping — Lambda is warm")
            return {"statusCode": 200, "body": '{"status":"warm"}'}

        if src == "pipeline_drain":
            # Scheduled drain — every 30 min (EventBridge PipelineDrainRule)
            print(f"EventBridge pipeline_drain — popping up to {PIPELINE_BATCH_SIZE} docs")
            result = _run_pipeline_drain()
            print(f"[drain] complete: {result['started']} started, "
                  f"{result['queue_remaining']} remaining in queue")
            return {"statusCode": 200, "body": json.dumps(result)}

    # All other events are HTTP requests via API Gateway → Mangum → FastAPI
    return _mangum(event, context)
