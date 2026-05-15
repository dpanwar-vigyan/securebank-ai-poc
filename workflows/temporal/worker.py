#!/usr/bin/env python3
"""
AskMyBank — Temporal Worker
Connects to Temporal Cloud and processes HITL workflow tasks.

Run from banking-poc root (keep terminal open during demo):
    cd /Users/dineshsinghpanwar/banking-poc
    source .venv/bin/activate
    python -m workflows.temporal.worker

Handles:
  - askmybank_estatement_duplicate  (Signal-driven eStatement HITL)
  - askmybank_reopen_case           (Signal-driven dispute reopen)     [future]
  - askmybank_escalate_to_it        (Signal-driven IT escalation)      [future]

Press Ctrl+C to stop.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from workflows.temporal.config import get_client, TASK_QUEUE, TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE
from workflows.temporal.workflows.estatement_workflow import EstatementDuplicateWorkflow
from workflows.temporal.activities.estatement_activities import ALL_ACTIVITIES


async def main():
    print(f"""
╔══════════════════════════════════════════════════════╗
║      AskMyBank — Temporal Worker                     ║
║      {TEMPORAL_ADDRESS[:46]:<46}║
║      Namespace: {TEMPORAL_NAMESPACE[:37]:<37}║
╚══════════════════════════════════════════════════════╝

Task queue : {TASK_QUEUE}
Workflows  : askmybank_estatement_duplicate
Activities : {len(ALL_ACTIVITIES)} registered

⏳ Connecting to Temporal Cloud…
""")

    client = await get_client()
    print("✅ Connected\n⏳ Starting worker — waiting for tasks… (Ctrl+C to stop)\n")

    from temporalio.worker import Worker

    worker = Worker(
        client,
        task_queue  = TASK_QUEUE,
        workflows   = [EstatementDuplicateWorkflow],
        activities  = ALL_ACTIVITIES,
    )

    try:
        await worker.run()
    except KeyboardInterrupt:
        print("\n\n👋 Worker stopped.\n")


if __name__ == "__main__":
    asyncio.run(main())
