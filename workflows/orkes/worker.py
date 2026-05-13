#!/usr/bin/env python3
"""
AskMyBank — Orkes Conductor Worker
Polls Orkes Cloud for tasks and executes them locally.

Run from banking-poc root (keep this terminal open while demoing):
    cd /Users/dineshsinghpanwar/banking-poc
    source .venv/bin/activate
    python -m workflows.orkes.worker

The worker handles ALL tasks for both workflows:
  - askmybank_estatement_duplicate  (eStatement HITL)
  - askmybank_document_pipeline     (Textract → Embed → CH)

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
        task_handler = TaskHandler(
            workers        = ALL_WORKERS,
            configuration  = config,
            scan_for_annotated_workers = False,
        )
        task_handler.start_processes()
        task_handler.join(None)   # block until Ctrl+C

    except KeyboardInterrupt:
        print("\n\n👋 Worker stopped.\n")
    except Exception as exc:
        print(f"\n❌ Worker error: {exc}")
        raise


if __name__ == "__main__":
    main()
