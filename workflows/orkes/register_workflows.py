#!/usr/bin/env python3
"""
Register AskMyBank workflow definitions + task definitions with Orkes Cloud.

Run once (or whenever workflow definitions change):
    cd /Users/dineshsinghpanwar/banking-poc
    source .venv/bin/activate
    python -m workflows.orkes.register_workflows
"""

import json
import sys
from pathlib import Path

# Make sure we can import from banking-poc root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from workflows.orkes.config import orkes_request, ORKES_API_URL
from workflows.orkes.workflows.estatement_duplicate import WORKFLOW_DEF as ESTATEMENT_WF
from workflows.orkes.workflows.document_pipeline import WORKFLOW_DEF as PIPELINE_WF

# ── Task definitions (tell Orkes what tasks exist) ────────────────────────────
TASK_NAMES = [
    # Common
    "askmybank_check_ticket_status",
    "askmybank_get_final_decision",
    "askmybank_mark_rejected",
    # eStatement
    "askmybank_validate_request",
    "askmybank_check_address_auth",
    "askmybank_get_statement_s3",
    "askmybank_generate_cover_note",
    "askmybank_log_dispatch_complete",
    # Pipeline
    "askmybank_detect_doc_type",
    "askmybank_textract_extract",
    "askmybank_chunk_text",
    "askmybank_generate_embeddings",
    "askmybank_update_s3_vectors",
    "askmybank_parse_metadata",
    "askmybank_upsert_clickhouse",
    "askmybank_pipeline_complete",
    # Shared (sleep is a WAIT task — no worker needed, but register it)
    "askmybank_sleep",
]

TASK_DEFS = [
    {
        "name":                       name,
        "retryCount":                 2,
        "timeoutSeconds":             300,       # 5 min per task
        "responseTimeoutSeconds":     300,
        "pollTimeoutSeconds":         300,
        "retryLogic":                 "FIXED",
        "retryDelaySeconds":          10,
        "rateLimitPerFrequency":      0,
        "rateLimitFrequencyInSeconds":1,
    }
    for name in TASK_NAMES
    if name != "askmybank_sleep"   # WAIT tasks don't need a task def
]


def register_tasks():
    print("📋 Registering task definitions…")
    ok = 0
    for td in TASK_DEFS:
        try:
            orkes_request("POST", "/metadata/taskdefs", [td])   # POST with single-item list
            ok += 1
        except Exception as exc:
            print(f"   ⚠️  {td['name']}: {exc}")
    print(f"   ✅ {ok}/{len(TASK_DEFS)} task definitions registered")


def register_workflow(wf_def: dict):
    name = wf_def["name"]
    print(f"🔀 Registering workflow: {name} v{wf_def['version']}…")
    try:
        # PUT /metadata/workflow registers or updates a workflow definition
        orkes_request("PUT", "/metadata/workflow", [wf_def])
        print(f"   ✅ {name} registered")
    except Exception as exc:
        print(f"   ❌ Failed to register {name}: {exc}")
        raise


def verify_registration():
    print("\n🔍 Verifying registrations…")
    try:
        workflows = orkes_request("GET", "/metadata/workflow?access=READ")
        names = [w["name"] for w in (workflows if isinstance(workflows, list) else [])]
        for wf in [ESTATEMENT_WF, PIPELINE_WF]:
            status = "✅" if wf["name"] in names else "❌ NOT FOUND"
            print(f"   {status}  {wf['name']}")
    except Exception as exc:
        print(f"   ⚠️  Could not verify: {exc}")


if __name__ == "__main__":
    print(f"\n🎼 Connecting to Orkes: {ORKES_API_URL}\n")
    register_tasks()
    print()
    register_workflow(ESTATEMENT_WF)
    register_workflow(PIPELINE_WF)
    verify_registration()
    print("\n🎉 Done — open https://developer.orkescloud.com to see your workflows.\n")
