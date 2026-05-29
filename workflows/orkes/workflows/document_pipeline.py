"""
Orkes Conductor workflow v2: askmybank_document_pipeline
────────────────────────────────────────────────────────────────────────────────
WORKERLESS ARCHITECTURE — no polling daemon required.

V1 (WORKFLOW_VERSION=1): Used SIMPLE tasks → required a Python worker daemon
  running on EC2/terminal to poll Orkes for tasks and call AWS services.
  Terminal 1: python worker.py (daemon polling 8 task types)
  Terminal 2: curl /pipeline/ingest (trigger)

V2 (WORKFLOW_VERSION=2): HTTP system tasks + INLINE task.
  Orkes itself calls Lambda /pipeline/step/* endpoints directly.
  Orkes manages retries, timeouts, and DAG execution.
  Terminal 1: (not needed — no daemon)
  Terminal 2: curl /pipeline/ingest (trigger)

Architecture:
  Orkes Cloud → HTTPS POST → API Gateway → Lambda → (Textract/Bedrock/S3/ClickHouse)

DAG (identical visual shape to V1/V2 — same Orkes UI appearance):

  detect_doc_type   ← HTTP → /pipeline/step/detect   (classify s3_key, gen doc_id — ~1ms)
       │
  textract_extract  ← HTTP → /pipeline/step/textract    (600s: Textract async)
       │
  chunk_text        ← HTTP → /pipeline/step/chunk
       │
  ┌────┴─────────────────────────────────────────────────────┐  FORK_JOIN
  │                                                         │
  [generate_embeddings]                           [parse_metadata]
  HTTP → /pipeline/step/embed                  HTTP → /pipeline/step/meta
       │                                                     │
  [update_s3_vectors]                          [store_metadata]
  HTTP → /pipeline/step/s3vec               HTTP → /pipeline/step/store
  └──────────────────────────────────JOIN──────────────────────┘
                              │
                    pipeline_complete ← HTTP → /pipeline/step/complete
                    (straight-through — HITL only fires on step exceptions)

Inputs (passed by /pipeline/ingest Lambda):
  s3_key         — S3 key of the source PDF (in banking-docs-poc-qahftr)
  doc_type       — Optional: Complaint|Dispute|eStatement|AccountMaintenance
  customer_id    — Customer identifier
  source_system  — "manual" or "s3_event"
  api_base_url   — Lambda API base, e.g. https://api.askmybank.ai
  backoffice_key — X-Backoffice-Key header value for Lambda auth
"""

WORKFLOW_NAME    = "askmybank_document_pipeline"
WORKFLOW_VERSION = 5   # bump from 4 → 5: rename /step/clickhouse → /step/store (backend-agnostic)


# ── Helper: build an HTTP system task definition ──────────────────────────────

def _http_task(name: str, ref: str, path: str, body: dict, timeout_s: int = 30) -> dict:
    """
    Build an Orkes HTTP system task that calls Lambda at api_base_url + path.

    Orkes resolves ${workflow.input.api_base_url} at runtime.
    No Python string interpolation needed — these are Conductor expression strings.
    """
    return {
        "name":              name,
        "taskReferenceName": ref,
        "type":              "HTTP",
        "inputParameters": {
            "http_request": {
                "uri":    "${workflow.input.api_base_url}" + path,
                "method": "POST",
                "body":   body,
                "headers": {
                    "Content-Type":   "application/json",
                    "X-Backoffice-Key": "${workflow.input.backoffice_key}",
                },
                "connectionTimeOut": 10_000,           # 10s connect timeout
                "readTimeOut":       timeout_s * 1000, # task read timeout (ms)
            }
        },
    }


# ── HTTP task: detect_doc_type (Lambda /pipeline/step/detect) ─────────────────
# v3: replaced INLINE Graal JS with a plain HTTP task — consistent with all other
# steps, easier to debug, same ~1ms cost (no Textract/Bedrock — pure Python logic).
# Output is at: ${detect_ref.output.response.body.<field>}  (standard HTTP task path)

_DETECT_TASK = _http_task(
    name      = "askmybank_detect_doc_type",
    ref       = "detect_ref",
    path      = "/pipeline/step/detect",
    body      = {
        "s3_key":      "${workflow.input.s3_key}",
        "doc_type":    "${workflow.input.doc_type}",
        "customer_id": "${workflow.input.customer_id}",
    },
    timeout_s = 10,    # classify + gen doc_id — should be <100ms
)


# ── Workflow definition ───────────────────────────────────────────────────────

WORKFLOW_DEF = {
    "name":        WORKFLOW_NAME,
    "version":     WORKFLOW_VERSION,
    "description": (
        "AskMyBank v4 — Straight-through on success, HITL only on failure: "
        "HTTP detect → HTTP Textract → chunk → FORK_JOIN(embed+s3vec ‖ meta+clickhouse) → complete. "
        "No WAIT step — pipeline completes without human intervention unless a step throws."
    ),
    "timeoutSeconds": 3600,          # 1h — Textract jobs can be slow for large PDFs
    "timeoutPolicy":  "TIME_OUT_WF",
    "inputParameters": [
        "s3_key", "doc_type", "customer_id", "source_system",
        "api_base_url", "backoffice_key",
    ],
    "outputParameters": {
        "doc_id":          "${detect_ref.output.response.body.doc_id}",
        "vector_count":    "${complete_ref.output.response.body.vector_count}",
        "pipeline_status": "${complete_ref.output.response.body.pipeline_status}",
    },
    "tasks": [

        # ── 1. INLINE: Detect / classify document type ───────────────────────
        # Graal JS runs inside Orkes engine — no Lambda hop, no cost, ~1ms
        _DETECT_TASK,

        # ── 2. HTTP: Textract — extract text from source PDF ─────────────────
        # Long timeout: Textract async jobs can take 30-90s for large PDFs
        _http_task(
            name      = "askmybank_textract_extract",
            ref       = "textract_ref",
            path      = "/pipeline/step/textract",
            body      = {
                "s3_key": "${workflow.input.s3_key}",
                "doc_id": "${detect_ref.output.response.body.doc_id}",
            },
            timeout_s = 600,    # 10 min — Textract async poll up to 2 min + buffer
        ),

        # ── 3. HTTP: Chunk raw text into semantic passages ───────────────────
        _http_task(
            name = "askmybank_chunk_text",
            ref  = "chunk_ref",
            path = "/pipeline/step/chunk",
            body = {
                "raw_text":   "${textract_ref.output.response.body.raw_text}",
                "doc_id":     "${detect_ref.output.response.body.doc_id}",
                "doc_type":   "${detect_ref.output.response.body.doc_type}",
                "page_count": "${textract_ref.output.response.body.page_count}",
            },
        ),

        # ── 4. FORK_JOIN: embed + metadata in parallel ───────────────────────
        {
            "name":              "parallel_processing",
            "taskReferenceName": "fork_ref",
            "type":              "FORK_JOIN",
            "forkTasks": [

                # ── Branch A: Generate embeddings → upload to S3 vectors ──────
                [
                    _http_task(
                        name = "askmybank_generate_embeddings",
                        ref  = "embed_ref",
                        path = "/pipeline/step/embed",
                        body = {
                            "chunks": "${chunk_ref.output.response.body.chunks}",
                            "doc_id": "${detect_ref.output.response.body.doc_id}",
                        },
                        timeout_s = 120,
                    ),
                    _http_task(
                        name = "askmybank_update_s3_vectors",
                        ref  = "s3vec_ref",
                        path = "/pipeline/step/s3vec",
                        body = {
                            "doc_id":     "${detect_ref.output.response.body.doc_id}",
                            "embeddings": "${embed_ref.output.response.body.embeddings}",
                            "chunks":     "${chunk_ref.output.response.body.chunks}",
                            "metadata":   "${detect_ref.output.response.body.metadata}",
                        },
                        timeout_s = 60,
                    ),
                ],

                # ── Branch B: Parse structured metadata → ClickHouse ──────────
                [
                    _http_task(
                        name = "askmybank_parse_metadata",
                        ref  = "meta_ref",
                        path = "/pipeline/step/meta",
                        body = {
                            "raw_text":    "${textract_ref.output.response.body.raw_text}",
                            "doc_id":      "${detect_ref.output.response.body.doc_id}",
                            "doc_type":    "${detect_ref.output.response.body.doc_type}",
                            "customer_id": "${workflow.input.customer_id}",
                        },
                        timeout_s = 60,
                    ),
                    _http_task(
                        name = "askmybank_store_metadata",
                        ref  = "store_ref",
                        path = "/pipeline/step/store",
                        body = {
                            "doc_id":   "${detect_ref.output.response.body.doc_id}",
                            "metadata": "${meta_ref.output.response.body.structured_metadata}",
                            "s3_key":   "${workflow.input.s3_key}",
                        },
                        timeout_s = 30,
                    ),
                ],
            ],
        },

        # ── 4b. JOIN — wait for both branches to finish ───────────────────────
        {
            "name":              "join_parallel",
            "taskReferenceName": "join_ref",
            "type":              "JOIN",
            "joinOn":            ["s3vec_ref", "store_ref"],
        },

        # ── 5. HTTP: Mark pipeline complete ──────────────────────────────────
        # Straight-through on success — no WAIT, no HITL noise.
        # HITL only fires on step exceptions via _pipeline_failure_hitl().
        _http_task(
            name = "askmybank_pipeline_complete",
            ref  = "complete_ref",
            path = "/pipeline/step/complete",
            body = {
                "doc_id":        "${detect_ref.output.response.body.doc_id}",
                "vector_count":  "${s3vec_ref.output.response.body.vectors_added}",
                "ch_inserted":   "${store_ref.output.response.body.rows_inserted}",
                "source_system": "${workflow.input.source_system}",
            },
        ),
    ],
}
