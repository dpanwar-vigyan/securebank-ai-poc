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

DAG (identical visual shape to V1 — same Orkes UI appearance):

  detect_doc_type   ← INLINE Graal JS   (classify s3_key, gen doc_id — zero infra)
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
  [update_s3_vectors]                          [upsert_clickhouse]
  HTTP → /pipeline/step/s3vec               HTTP → /pipeline/step/clickhouse
  └──────────────────────────────────JOIN──────────────────────┘
                              │
                    WAIT (2h auto-continue spot-check)
                              │
                    pipeline_complete ← HTTP → /pipeline/step/complete

Inputs (passed by /pipeline/ingest Lambda):
  s3_key         — S3 key of the source PDF (in banking-docs-poc-qahftr)
  doc_type       — Optional: Complaint|Dispute|eStatement|AccountMaintenance
  customer_id    — Customer identifier
  source_system  — "manual" or "s3_event"
  api_base_url   — Lambda API base, e.g. https://api.askmybank.ai
  backoffice_key — X-Backoffice-Key header value for Lambda auth
"""

WORKFLOW_NAME    = "askmybank_document_pipeline"
WORKFLOW_VERSION = 2   # bump from 1 (SIMPLE tasks) → 2 (HTTP system tasks)


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


# ── INLINE task: detect_doc_type (Graal JS — zero infrastructure) ─────────────
# Runs inside the Orkes engine — no Lambda call, no worker, no AWS cost.
# Classifies doc_type from s3_key suffix, generates doc_id.
# Output is at: ${detect_ref.output.result.<field>}

_DETECT_EXPRESSION = r"""
(function() {
  var s3_key     = $.s3_key     || '';
  var doc_type   = $.doc_type   || '';
  var customer_id = $.customer_id || '';
  var key_lower  = s3_key.toLowerCase();

  if (!doc_type) {
    if (key_lower.indexOf('dispute') !== -1 || key_lower.indexOf('dsp') !== -1) {
      doc_type = 'Dispute';
    } else if (key_lower.indexOf('complaint') !== -1 || key_lower.indexOf('cmp') !== -1) {
      doc_type = 'Complaint';
    } else if (key_lower.indexOf('statement') !== -1 || key_lower.indexOf('stmt') !== -1) {
      doc_type = 'eStatement';
    } else if (key_lower.indexOf('maintenance') !== -1 || key_lower.indexOf('mnt') !== -1) {
      doc_type = 'AccountMaintenance';
    } else {
      doc_type = 'Complaint';
    }
  }

  var prefixMap = {
    'Dispute': 'DSP', 'Complaint': 'CMP',
    'eStatement': 'STMT', 'AccountMaintenance': 'MNT'
  };
  var prefix = prefixMap[doc_type] || 'DOC';
  var suffix = Math.floor(Math.random() * 0xFFFFF)
                   .toString(16).toUpperCase().padStart(5, '0');
  var doc_id = prefix + suffix;

  return {
    doc_id:    doc_id,
    doc_type:  doc_type,
    s3_key:    s3_key,
    metadata: {
      doc_id:      doc_id,
      doc_type:    doc_type,
      customer_id: customer_id,
      s3_path:     s3_key,
      ingested_at: new Date().toISOString()
    }
  };
})();
""".strip()

_DETECT_TASK = {
    "name":              "askmybank_detect_doc_type_inline",
    "taskReferenceName": "detect_ref",
    "type":              "INLINE",
    "inputParameters": {
        "evaluatorType": "graaljs",
        "expression":    _DETECT_EXPRESSION,
        # Map workflow inputs into the JS $ scope
        "s3_key":        "${workflow.input.s3_key}",
        "doc_type":      "${workflow.input.doc_type}",
        "customer_id":   "${workflow.input.customer_id}",
    },
}


# ── Workflow definition ───────────────────────────────────────────────────────

WORKFLOW_DEF = {
    "name":        WORKFLOW_NAME,
    "version":     WORKFLOW_VERSION,
    "description": (
        "AskMyBank v2 — Workerless pipeline: "
        "INLINE detect → HTTP Textract → chunk → FORK_JOIN(embed+s3vec ‖ meta+clickhouse) "
        "→ WAIT spot-check → complete. "
        "No worker daemon needed — Orkes calls Lambda directly via HTTP system tasks."
    ),
    "timeoutSeconds": 3600,          # 1h — Textract jobs can be slow for large PDFs
    "timeoutPolicy":  "TIME_OUT_WF",
    "inputParameters": [
        "s3_key", "doc_type", "customer_id", "source_system",
        "api_base_url", "backoffice_key",
    ],
    "outputParameters": {
        "doc_id":          "${detect_ref.output.result.doc_id}",
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
                "doc_id": "${detect_ref.output.result.doc_id}",
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
                "doc_id":     "${detect_ref.output.result.doc_id}",
                "doc_type":   "${detect_ref.output.result.doc_type}",
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
                            "doc_id": "${detect_ref.output.result.doc_id}",
                        },
                        timeout_s = 120,
                    ),
                    _http_task(
                        name = "askmybank_update_s3_vectors",
                        ref  = "s3vec_ref",
                        path = "/pipeline/step/s3vec",
                        body = {
                            "doc_id":     "${detect_ref.output.result.doc_id}",
                            "embeddings": "${embed_ref.output.response.body.embeddings}",
                            "chunks":     "${chunk_ref.output.response.body.chunks}",
                            "metadata":   "${detect_ref.output.result.metadata}",
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
                            "doc_id":      "${detect_ref.output.result.doc_id}",
                            "doc_type":    "${detect_ref.output.result.doc_type}",
                            "customer_id": "${workflow.input.customer_id}",
                        },
                        timeout_s = 60,
                    ),
                    _http_task(
                        name = "askmybank_upsert_clickhouse",
                        ref  = "ch_ref",
                        path = "/pipeline/step/clickhouse",
                        body = {
                            "doc_id":   "${detect_ref.output.result.doc_id}",
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
            "joinOn":            ["s3vec_ref", "ch_ref"],
        },

        # ── 5. WAIT — optional content reviewer spot-check ───────────────────
        # Auto-continues after 2h if not actioned. Non-blocking for pipeline.
        {
            "name":              "askmybank_content_review_wait",
            "taskReferenceName": "review_wait_ref",
            "type":              "WAIT",
            "inputParameters": {
                "duration": "2 hours",
            },
            "optional": True,
        },

        # ── 6. HTTP: Mark pipeline complete ──────────────────────────────────
        _http_task(
            name = "askmybank_pipeline_complete",
            ref  = "complete_ref",
            path = "/pipeline/step/complete",
            body = {
                "doc_id":        "${detect_ref.output.result.doc_id}",
                "vector_count":  "${s3vec_ref.output.response.body.vectors_added}",
                "ch_inserted":   "${ch_ref.output.response.body.rows_inserted}",
                "source_system": "${workflow.input.source_system}",
            },
        ),
    ],
}
