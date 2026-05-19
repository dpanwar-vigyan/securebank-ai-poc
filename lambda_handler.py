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
"""

import json
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel

from rag.chain import BankingRAG
from rag.hitl_client import (
    HITLClient, HITLUnavailableError, HITLNotFoundError,
    TEMPORAL_WORKFLOWS,
)

# ── SQS client — HITL case queue for Appian ───────────────────────────────────
_sqs           = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "us-east-1"))
HITL_QUEUE_URL = os.getenv("HITL_SQS_QUEUE_URL", "")

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


# ── Document ingestion pipeline trigger ───────────────────────────────────────
@app.post("/pipeline/ingest")
def trigger_pipeline(req: PipelineIngestRequest):
    """
    Trigger the Orkes document ingestion pipeline workflow.
    Starts: Textract → chunk → embed (parallel) → S3 vectors + ClickHouse.

    Example:
        POST /pipeline/ingest
        {"s3_key": "raw-docs/cmp_new.pdf", "doc_type": "Complaint", "customer_id": "CUST00099"}
    """
    if not req.s3_key:
        raise HTTPException(status_code=400, detail="s3_key is required")

    wf_input = {
        "s3_key":        req.s3_key,
        "doc_type":      req.doc_type,
        "customer_id":   req.customer_id,
        "source_system": req.source_system,
    }

    wf_id = hitl.trigger_orkes_workflow(
        workflow_name   = "askmybank_document_pipeline",
        workflow_input  = wf_input,
        correlation_id  = f"pipeline-{req.s3_key}",
    )

    if not wf_id:
        raise HTTPException(
            status_code=503,
            detail="Orkes not configured or unavailable — check ORKES_* env vars",
        )

    return {
        "workflow_id":   wf_id,
        "workflow_name": "askmybank_document_pipeline",
        "s3_key":        req.s3_key,
        "doc_type":      req.doc_type,
        "orkes_ui_url":  f"https://developer.orkescloud.com/execution/{wf_id}",
        "status":        "started",
    }


# ── Lambda handler ────────────────────────────────────────────────────────────
# Mangum handles HTTP API Gateway events.
# EventBridge warming pings arrive as raw JSON (not HTTP) — intercept them first.
_mangum = Mangum(app, lifespan="off")

def handler(event, context):
    # EventBridge sends {"source": "warming", "query": ""} directly to Lambda
    # — not through API Gateway, so Mangum can't parse it. Handle here.
    if isinstance(event, dict) and event.get("source") == "warming":
        print("EventBridge warming ping — Lambda is warm")
        return {"statusCode": 200, "body": '{"status":"warm"}'}
    # All other events are HTTP requests via API Gateway → Mangum → FastAPI
    return _mangum(event, context)
