#!/usr/bin/env python3
"""
Box pipeline worker — runs on Fargate (or locally: python -m workflows.temporal.box_pipeline.worker).

Two workers share one process:
  askmybank-box-pipeline — the workflow and every step except vector writes
  askmybank-box-vectors  — write_vectors only, one at a time

Env: TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY, PIPELINE_FUNCTION_NAME
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from workflows.temporal.box_pipeline import TASK_QUEUE, VECTOR_TASK_QUEUE
from workflows.temporal.box_pipeline.activities import ACTIVITIES, FUNCTION_NAME
from workflows.temporal.box_pipeline.workflow import BoxDocumentPipeline
from workflows.temporal.config import TEMPORAL_NAMESPACE, get_client


async def main():
    if not FUNCTION_NAME:
        raise SystemExit("PIPELINE_FUNCTION_NAME not set")
    client = await get_client()
    print(f"Connected to Temporal ({TEMPORAL_NAMESPACE}); invoking {FUNCTION_NAME}", flush=True)

    with ThreadPoolExecutor(max_workers=10) as pool:
        pipeline = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[BoxDocumentPipeline],
            activities=[fn for name, fn in ACTIVITIES.items() if name != "write_vectors"],
            activity_executor=pool,
            max_concurrent_activities=8,
        )
        vectors = Worker(
            client,
            task_queue=VECTOR_TASK_QUEUE,
            activities=[ACTIVITIES["write_vectors"]],
            activity_executor=pool,
            max_concurrent_activities=1,
        )
        print(f"Polling {TASK_QUEUE} and {VECTOR_TASK_QUEUE}", flush=True)
        await asyncio.gather(pipeline.run(), vectors.run())


if __name__ == "__main__":
    asyncio.run(main())
