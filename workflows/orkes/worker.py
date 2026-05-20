#!/usr/bin/env python3
"""
AskMyBank — Orkes Conductor Worker (OPTIONAL in v2)
Polls Orkes Cloud for tasks and executes them locally.

ARCHITECTURE NOTE:
  Workflow v1 (SIMPLE tasks):  worker REQUIRED to poll & execute all 8 pipeline tasks
  Workflow v2 (HTTP tasks):    worker NOT NEEDED for pipeline — Orkes calls Lambda directly

  The worker is now ONLY needed for tasks that still use SIMPLE type.
  Pipeline tasks (v2) are executed by Orkes via HTTP → Lambda /pipeline/step/* endpoints.

Run from banking-poc root (only if running v1 SIMPLE task workflows):
    cd /Users/dineshsinghpanwar/banking-poc
    source .venv/bin/activate
    python -m workflows.orkes.worker

Press Ctrl+C to stop.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from workflows.orkes.config import get_orkes_config, ORKES_API_URL

# Import all task workers
from workflows.orkes.tasks.common_tasks    import ALL_WORKERS as COMMON
from workflows.orkes.tasks.estatement_tasks import ALL_WORKERS as ESTATEMENT
from workflows.orkes.tasks.pipeline_tasks   import ALL_WORKERS as PIPELINE

ALL_WORKERS = COMMON + ESTATEMENT + PIPELINE


def main():
    print(f"""
╔══════════════════════════════════════════════════════╗
║      AskMyBank — Orkes Conductor Worker              ║
║      Connected: {ORKES_API_URL[:38]:<38}║
╚══════════════════════════════════════════════════════╝

Workers registered ({len(ALL_WORKERS)} tasks):
""")
    for w in ALL_WORKERS:
        print(f"  ▶  {w.task_definition_name}")

    print(f"\n⏳ Polling for tasks… (Ctrl+C to stop)\n")

    try:
        config = get_orkes_config()
    except Exception as exc:
        print(f"❌ Orkes config error: {exc}")
        print("   Check ORKES_API_URL, ORKES_KEY_ID, ORKES_KEY_SECRET in .env")
        sys.exit(1)

    try:
        from conductor.client.automator.task_handler import TaskHandler
        import signal, time
        task_handler = TaskHandler(
            workers        = ALL_WORKERS,
            configuration  = config,
            scan_for_annotated_workers = False,
        )
        task_handler.start_processes()

        # Block main thread until Ctrl+C — task runners execute in child processes
        stop = False
        def _handle_signal(sig, frame):
            nonlocal stop
            stop = True
        signal.signal(signal.SIGINT,  _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        while not stop:
            time.sleep(1)

        print("\n\n👋 Stopping workers…")
        task_handler.stop_processes()
        print("👋 Worker stopped.\n")

    except Exception as exc:
        print(f"\n❌ Worker error: {exc}")
        raise


if __name__ == "__main__":
    main()
