#!/usr/bin/env python3
"""
AskMyBank — Temporal Worker (OPTIONAL)
========================================
This worker is NO LONGER REQUIRED for normal operation.

WORKERLESS design: the Temporal workflow is now a pure wait-state machine.
All post-approval work (S3, cover note, ClickHouse update) runs directly
in Lambda's /hitl/{id}/decide handler via HITLClient.run_fulfilment().

The workflow only has one job: hold durable state and accept a Signal.
That doesn't need a worker — Temporal Cloud itself manages it.

WHEN IS A WORKER STILL NEEDED?
  - If you re-add Temporal activities (e.g. long-running enrichment jobs
    that exceed Lambda's 29s API Gateway timeout or 15min max)
  - If you add a workflow that needs complex retry logic managed by Temporal

HOW TO RUN (if needed for future activities):
    cd /Users/dineshsinghpanwar/banking-poc
    source .venv/bin/activate
    python -m workflows.temporal.worker

The workflow class is registered here with NO activities — just the workflow
definition so Temporal Cloud can deserialise the workflow state correctly.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from workflows.temporal.config import get_client, TASK_QUEUE, TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE
from workflows.temporal.workflows.estatement_workflow import EstatementDuplicateWorkflow
from workflows.temporal.workflows.reopen_workflow import ReopenCaseWorkflow
from workflows.temporal.workflows.escalate_workflow import EscalateToITWorkflow


async def main():
    print(f"""
╔══════════════════════════════════════════════════════╗
║      AskMyBank — Temporal Worker (optional)          ║
║      {TEMPORAL_ADDRESS[:46]:<46}║
║      Namespace: {TEMPORAL_NAMESPACE[:37]:<37}║
╚══════════════════════════════════════════════════════╝

Task queue : {TASK_QUEUE}
Workflows  : askmybank_estatement_duplicate, askmybank_reopen_case, askmybank_escalate_to_it
Activities : none — fulfilment runs in Lambda

NOTE: This worker is not required. Lambda handles all fulfilment.
      Run it only if you add Temporal activities in the future.

⏳ Connecting to Temporal Cloud…
""")

    client = await get_client()
    print("✅ Connected\n⏳ Worker running — Ctrl+C to stop\n")

    from temporalio.worker import Worker

    # No activities registered — all three workflows are wait-only
    worker = Worker(
        client,
        task_queue = TASK_QUEUE,
        workflows  = [
            EstatementDuplicateWorkflow,
            ReopenCaseWorkflow,
            EscalateToITWorkflow,
        ],
        activities = [],
    )

    try:
        await worker.run()
    except KeyboardInterrupt:
        print("\n\n👋 Worker stopped.\n")


if __name__ == "__main__":
    asyncio.run(main())
