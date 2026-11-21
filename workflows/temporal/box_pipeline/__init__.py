"""Box + Temporal document pipeline — names shared by the Lambda (starts workflows) and the worker."""

WORKFLOW_NAME = "askmybank_box_document_pipeline"
TASK_QUEUE = "askmybank-box-pipeline"
# Vector writes rewrite one shared vectors.npy, so they run one at a time on their own queue
VECTOR_TASK_QUEUE = "askmybank-box-vectors"

STEPS = [
    "box_extract",
    "chunk_text",
    "embed_chunks",
    "extract_metadata",
    "write_vectors",
    "store_metadata",
    "complete_pipeline",
]
