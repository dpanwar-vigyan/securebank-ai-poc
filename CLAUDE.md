# CLAUDE.md — SecureBank AI / AskMyBank POC

Project context for Claude Code. Read this before touching any file.

## What This Is

**AskMyBank** — a RAG-based banking document Q&A system deployed on AWS Lambda.
Customers upload PDFs (complaints, statements, T&Cs) → AI pipeline ingests them →
customers query via chat → answers grounded in their documents.

Stack: FastAPI + Mangum on AWS Lambda · AWS Bedrock Nova Lite · S3 vectors · ClickHouse · Orkes Conductor (pipeline DAG) · Temporal (HITL workflows).

**Always use AWS Bedrock Nova Lite** (`us.amazon.nova-lite-v1:0`) — not Anthropic SDK.
Single billing, established pattern. Model ID is in `LLM_MODEL` env var.

---

## Repository Layout

```
lambda_handler.py        # FastAPI app + Lambda entry point (handler fn at bottom)
rag/
  chain.py               # RAG query chain — Bedrock LLM + retriever + answer cache
  s3_retriever.py        # ETag-based vector cache (NumPy cosine similarity, ~4MB)
  pipeline_steps.py      # step_detect / step_textract / step_chunk / step_embed / step_meta
  hitl_client.py         # HITLClient: ClickHouse tickets + Orkes/Temporal workflow triggers
  config.py              # Env loader: .env (local) → Streamlit secrets → os.environ
workflows/
  orkes/
    workflows/document_pipeline.py   # Orkes DAG definition — bump WORKFLOW_VERSION + register
    register_workflows.py            # Run this after any workflow change
  temporal/                          # Temporal HITL workflows (reopen, escalate, duplicate)
template.yaml            # SAM template — Lambda + API GW + SQS + EventBridge
samconfig.toml           # SAM deploy config (stack: askmybank-api, region: us-east-1)
linkedin/hitl.html       # Architecture diagram (5 HITL patterns)
```

---

## Architecture: Document Pipeline

```
S3 upload / POST /pipeline/ingest
    → SQS PipelineQueue (askmybank-pipeline-prod)
    → EventBridge drain rule (every 5 min for testing, change to 30 min for prod)
    → POST /pipeline/drain → trigger_orkes_workflow()
    → Orkes HTTP tasks call Lambda endpoints directly (no worker daemon):
        /pipeline/step/detect    → classify doc_type, generate doc_id
        /pipeline/step/textract  → AWS Textract → raw text
        /pipeline/step/chunk     → split into ~500-token chunks
        /pipeline/step/embed     → Bedrock embeddings → NumPy array
        /pipeline/step/meta      → write ClickHouse metadata
        /pipeline/step/complete  → update S3 vectors.npy + force vector cache reload
```

**No worker daemon** — Orkes calls Lambda via HTTP system tasks. Everything is serverless.

### Workflow Versioning (CRITICAL)

- Current version: `WORKFLOW_VERSION = 4` in `document_pipeline.py`
- After ANY workflow change: bump the version number AND run:
  ```bash
  python -m workflows.orkes.register_workflows
  ```
- `trigger_orkes_workflow()` omits `version` → Orkes picks latest registered automatically
- History: v1=SIMPLE tasks (needed worker), v2=HTTP+INLINE, v3=all HTTP, v4=removed WAIT step

### Known Gotchas

**chunk_text vs text field**: Pipeline writes `chunk_text` key in metadata.json.
Retriever reads `chunk_text` first, falls back to `text`. Both `step_chunk` and
`s3_retriever.py` must use `chunk_text`. Old bulk-loaded docs already patched in S3.

**Answer cache**: `chain.py` skips caching responses that contain "does not contain" /
"no matching" / "not contain any information" — prevents stale "no info" answers
surviving after a document is ingested.

**Pydantic v2 null handling**: Pipeline step request models use `Optional[str] = ""`
with `@field_validator(..., mode="before")` to coerce `null → ""`. Without this,
Orkes sending `null` for optional fields causes 422s.

**S3 event format**: S3 event notifications arrive as
`{"Records": [{"s3": {"object": {"key": "..."}}}]}` — different from ingest format
`{"s3_key": "..."}`. The drain function handles both.

---

## Vector Cache (ETag-based)

`rag/s3_retriever.py` — in-memory NumPy vectors, self-healing across Lambda containers:

- Cold start → always load from S3
- Every query → HEAD `vectors.npy` (throttled to every 30s) → reload only if ETag changed
- `POST /pipeline/step/complete` → calls `force_reload()` immediately on the container
  that processed the pipeline (other containers self-heal within 30s)
- `VECTOR_ETAG_CHECK_INTERVAL_S` env var controls throttle interval (default 30s)

---

## HITL Flows

Five patterns (see `linkedin/hitl.html` diagram):
1. Back-office ticket via ClickHouse → Temporal signal-driven (reopen, escalate, duplicate)
2. Legal export → Orkes DAG
3. Pipeline failure → `_pipeline_failure_hitl()` → auto-creates ticket + re-trigger curl
4. Content gap logging → `log_content_gap()` → ClickHouse `banking_docs.content_gaps`
5. *(removed)* WAIT spot-check — pipeline now straight-through on success (v4)

`WAIT` step was removed in workflow v4. Do NOT re-add it — it caused every successful
pipeline run to block waiting for human review.

---

## SAM Deploy

```bash
sam build && sam deploy --no-confirm-changeset
```

Stack: `askmybank-api` · Region: `us-east-1` · Account: `835422347653`
API base: `https://r6v15i892m.execute-api.us-east-1.amazonaws.com`

Auth header for pipeline endpoints: `X-Backoffice-Key: askmybank-demo-2025`
(stored as `DemoApiKey` CloudFormation parameter, mapped to `DEMO_API_KEY` env var)

Deploy is needed after changes to `lambda_handler.py`, `rag/`, `template.yaml`.
Workflow-only changes only need `python -m workflows.orkes.register_workflows`.

---

## Key Environment Variables (Lambda)

| Var | Purpose |
|-----|---------|
| `ORKES_API_URL` | `https://developer.orkescloud.com/api` |
| `ORKES_KEY_ID` / `ORKES_KEY_SECRET` | Orkes auth (module-level capture at import) |
| `PIPELINE_QUEUE_URL` | SQS URL for pipeline intake queue |
| `S3_VECTORS_BUCKET` | `askmybank-vectors` — stores vectors.npy + metadata.json |
| `LLM_MODEL` | `us.amazon.nova-lite-v1:0` |
| `CLICKHOUSE_HOST/USER/PASSWORD` | ClickHouse Cloud connection |
| `VECTOR_ETAG_CHECK_INTERVAL_S` | Vector cache freshness check interval (default 30) |
| `DEMO_API_KEY` | Pipeline endpoint auth key |

**Important**: `ORKES_API_URL`, `ORKES_KEY_ID`, `ORKES_KEY_SECRET` are captured as
module-level constants at import time. Lambda env vars are always set before Python
starts so this is safe, but restart the Lambda (redeploy) if you change these values.

---

## Drain Debugging

The drain log now shows: `started / failed / visible / in-flight`

If you see `started=0, in_flight=0, visible=0`:
→ Queue is genuinely empty. Enqueue first via `POST /pipeline/ingest`.

If you see `started=0, in_flight=N, visible=0`:
→ Previous drain received messages but Orkes failed. Messages will return after visibility
timeout. The drain now calls `change_message_visibility(..., VisibilityTimeout=0)` on
failure so messages return immediately.

If you see `started=0` but the pre-trigger log shows `ORKES_API_URL set=False`:
→ Lambda env vars not set. Check Lambda config and redeploy.

To test end-to-end manually:
```bash
# 1. Enqueue
curl -X POST https://r6v15i892m.execute-api.us-east-1.amazonaws.com/pipeline/ingest \
  -H "X-Backoffice-Key: askmybank-demo-2025" \
  -H "Content-Type: application/json" \
  -d '{"s3_key": "raw-docs/CMP-TRIAL-003.pdf", "doc_type": "Complaint"}'

# 2. Drain (wait ~2s for SQS propagation)
curl -X POST https://r6v15i892m.execute-api.us-east-1.amazonaws.com/pipeline/drain \
  -H "X-Backoffice-Key: askmybank-demo-2025"
```

---

## Pending / Production Readiness

- Change EventBridge schedule back to `rate(30 minutes)` before production (currently 5 min for testing)
- Wire S3 event notification: `banking-docs-poc-qahftr` bucket → `askmybank-pipeline-prod` SQS
  (SAM template has the `PipelineQueuePolicy` already; configure notification in S3 console)
- Register workflows after any DAG change: `python -m workflows.orkes.register_workflows`
