"""
Box document pipeline — one workflow per Box file version.

  box_extract → chunk_text → ┬ embed_chunks     ┬ → write_vectors → store_metadata → complete_pipeline
                             └ extract_metadata ┘   (serialised queue)

Every step is an activity that invokes the pipeline Lambda, so Temporal owns
retries, timeouts and history while the step logic stays shared with the Orkes pattern.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from workflows.temporal.box_pipeline import VECTOR_TASK_QUEUE, WORKFLOW_NAME

RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=5,
    non_retryable_error_types=["BoxNotConfiguredError", "ValueError", "KeyError", "TypeError"],
)


@workflow.defn(name=WORKFLOW_NAME)
class BoxDocumentPipeline:
    def __init__(self):
        self._stage = "queued"

    @workflow.query
    def stage(self) -> str:
        return self._stage

    async def _step(self, name: str, args: dict, minutes: int = 5, task_queue: str | None = None) -> dict:
        self._stage = name
        return await workflow.execute_activity(
            name, args,
            start_to_close_timeout=timedelta(minutes=minutes),
            retry_policy=RETRY,
            task_queue=task_queue,
        )

    @workflow.run
    async def run(self, inp: dict) -> dict:
        ext = await self._step("box_extract",
                               {"file_id": inp["file_id"], "version_id": inp.get("version_id", "")},
                               minutes=10)
        doc_id, doc_type = ext["doc_id"], ext["doc_type"]

        chunked = await self._step("chunk_text", {"raw_text": ext["raw_text"], "doc_id": doc_id,
                                                  "doc_type": doc_type, "page_count": ext["page_count"]})

        self._stage = "embed_chunks + extract_metadata"
        emb, meta = await asyncio.gather(
            self._step("embed_chunks", {"chunks": chunked["chunks"], "doc_id": doc_id}),
            self._step("extract_metadata", {"raw_text": ext["raw_text"], "doc_id": doc_id,
                                            "doc_type": doc_type, "customer_id": ext["customer_id"]}),
        )

        vec = await self._step("write_vectors", {"doc_id": doc_id, "embeddings": emb["embeddings"],
                                                 "chunks": chunked["chunks"], "metadata": ext["metadata"]},
                               task_queue=VECTOR_TASK_QUEUE)

        # Box metadata is the system of record; the LLM only fills fields Box doesn't carry
        extracted = {k: v for k, v in meta["structured_metadata"].items() if v not in (None, "")}
        stored = await self._step("store_metadata", {"doc_id": doc_id, "s3_key": "",
                                                     "metadata": {**extracted, **ext["metadata"]}})

        await self._step("complete_pipeline", {"doc_id": doc_id, "vector_count": vec["vectors_added"],
                                               "ch_inserted": stored["rows_inserted"], "source_system": "box"})
        self._stage = "completed"
        return {"doc_id": doc_id, "doc_type": doc_type, "text_source": ext["text_source"],
                "chunks": chunked["chunk_count"], "vectors_added": vec["vectors_added"],
                "source_uri": ext["metadata"].get("source_uri", "")}
