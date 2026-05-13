"""
Orkes Conductor workflow: askmybank_document_pipeline
──────────────────────────────────────────────────────────
Triggered when a new banking document (PDF) is uploaded to S3
and needs to be ingested into the RAG knowledge base.

Visual DAG (shown in Orkes UI) — this is the showcase:

  detect_doc_type
       │
  textract_extract     ← AWS Textract: PDF → raw text
       │
  chunk_text           ← split into semantic passages
       │
  ┌────┴──────────────────────────────────┐  FORK_JOIN (parallel)
  │                                       │
  [generate_embeddings]          [parse_structured_metadata]
       │                                  │
  [update_s3_vectors]            [upsert_clickhouse]
  │                                       │
  └───────────────────JOIN────────────────┘
                       │
               content_review_hitl      ← WAIT: human spot-checks extracted data
                       │
               pipeline_complete        ← log completion, notify

Inputs (from Lambda /pipeline/ingest):
  s3_key, doc_type, customer_id, source_system ("manual"|"s3_event")

The FORK_JOIN makes the embedding and metadata steps run in PARALLEL —
demonstrates Orkes' parallel execution in the visual designer.
"""

WORKFLOW_NAME    = "askmybank_document_pipeline"
WORKFLOW_VERSION = 1

WORKFLOW_DEF = {
    "name":        WORKFLOW_NAME,
    "version":     WORKFLOW_VERSION,
    "description": "AskMyBank — Document ingestion: Textract → chunk → embed+metadata (parallel) → HITL review → complete",
    "timeoutSeconds":  3600,    # 1h — Textract jobs can be slow for large PDFs
    "timeoutPolicy":   "TIME_OUT_WF",
    "inputParameters": ["s3_key", "doc_type", "customer_id", "source_system"],
    "outputParameters": {
        "doc_id":       "${complete_ref.output.doc_id}",
        "vector_count": "${complete_ref.output.vector_count}",
        "pipeline_status": "${complete_ref.output.pipeline_status}",
    },
    "tasks": [

        # ── 1. Detect / validate document type ──────────────────────────────
        {
            "name":              "askmybank_detect_doc_type",
            "taskReferenceName": "detect_ref",
            "type":              "SIMPLE",
            "inputParameters": {
                "s3_key":      "${workflow.input.s3_key}",
                "doc_type":    "${workflow.input.doc_type}",
                "customer_id": "${workflow.input.customer_id}",
            },
        },

        # ── 2. Textract: extract text from PDF ──────────────────────────────
        {
            "name":              "askmybank_textract_extract",
            "taskReferenceName": "textract_ref",
            "type":              "SIMPLE",
            "inputParameters": {
                "s3_key":  "${workflow.input.s3_key}",
                "doc_id":  "${detect_ref.output.doc_id}",
            },
        },

        # ── 3. Chunk raw text into semantic passages ─────────────────────────
        {
            "name":              "askmybank_chunk_text",
            "taskReferenceName": "chunk_ref",
            "type":              "SIMPLE",
            "inputParameters": {
                "raw_text":    "${textract_ref.output.raw_text}",
                "doc_id":      "${detect_ref.output.doc_id}",
                "doc_type":    "${detect_ref.output.doc_type}",
                "page_count":  "${textract_ref.output.page_count}",
            },
        },

        # ── 4. FORK_JOIN: embed + metadata in parallel ───────────────────────
        {
            "name":              "parallel_processing",
            "taskReferenceName": "fork_ref",
            "type":              "FORK_JOIN",
            "forkTasks": [

                # Branch A — Vector embeddings → S3
                [
                    {
                        "name":              "askmybank_generate_embeddings",
                        "taskReferenceName": "embed_ref",
                        "type":              "SIMPLE",
                        "inputParameters": {
                            "chunks":  "${chunk_ref.output.chunks}",
                            "doc_id":  "${detect_ref.output.doc_id}",
                        },
                    },
                    {
                        "name":              "askmybank_update_s3_vectors",
                        "taskReferenceName": "s3vec_ref",
                        "type":              "SIMPLE",
                        "inputParameters": {
                            "doc_id":     "${detect_ref.output.doc_id}",
                            "embeddings": "${embed_ref.output.embeddings}",
                            "chunks":     "${chunk_ref.output.chunks}",
                            "metadata":   "${detect_ref.output.metadata}",
                        },
                    },
                ],

                # Branch B — Parse structured fields → ClickHouse
                [
                    {
                        "name":              "askmybank_parse_metadata",
                        "taskReferenceName": "meta_ref",
                        "type":              "SIMPLE",
                        "inputParameters": {
                            "raw_text":    "${textract_ref.output.raw_text}",
                            "doc_id":      "${detect_ref.output.doc_id}",
                            "doc_type":    "${detect_ref.output.doc_type}",
                            "customer_id": "${workflow.input.customer_id}",
                        },
                    },
                    {
                        "name":              "askmybank_upsert_clickhouse",
                        "taskReferenceName": "ch_ref",
                        "type":              "SIMPLE",
                        "inputParameters": {
                            "doc_id":   "${detect_ref.output.doc_id}",
                            "metadata": "${meta_ref.output.structured_metadata}",
                            "s3_key":   "${workflow.input.s3_key}",
                        },
                    },
                ],
            ],
        },

        # ── 4b. JOIN — wait for both branches ────────────────────────────────
        {
            "name":              "join_parallel",
            "taskReferenceName": "join_ref",
            "type":              "JOIN",
            "joinOn":            ["s3vec_ref", "ch_ref"],
        },

        # ── 5. HITL: spot-check extracted data before going live ─────────────
        # Uses a WAIT task — content reviewer checks in Orkes UI and clicks Resume
        {
            "name":              "askmybank_content_review_wait",
            "taskReferenceName": "review_wait_ref",
            "type":              "WAIT",
            "inputParameters": {
                "duration": "2 hours",   # auto-approve after 2h if no action
            },
            "optional": True,            # don't fail workflow if skipped
        },

        # ── 6. Mark pipeline complete ────────────────────────────────────────
        {
            "name":              "askmybank_pipeline_complete",
            "taskReferenceName": "complete_ref",
            "type":              "SIMPLE",
            "inputParameters": {
                "doc_id":        "${detect_ref.output.doc_id}",
                "vector_count":  "${s3vec_ref.output.vectors_added}",
                "ch_inserted":   "${ch_ref.output.rows_inserted}",
                "source_system": "${workflow.input.source_system}",
            },
        },
    ],
}
