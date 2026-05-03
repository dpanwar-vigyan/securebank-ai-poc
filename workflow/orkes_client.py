"""
Orkes Cloud client for SecureBank AI — Dispute Resolution Workflow
Triggers and monitors durable workflows from AskMyBank.ai chat interface.

Setup:
  1. Sign up at https://cloud.orkes.io (free developer tier)
  2. Create workflow from workflow_definition.json
  3. Add to .env:
       ORKES_SERVER_URL=https://developer.orkescloud.com
       ORKES_ACCESS_TOKEN=<your-token>
"""

import os
import requests
import rag.config  # noqa: F401 — loads .env + st.secrets

ORKES_URL   = os.getenv("ORKES_SERVER_URL", "")
ORKES_TOKEN = os.getenv("ORKES_ACCESS_TOKEN", "")
WORKFLOW_NAME = "dispute_resolution_v1"

_HEADERS = lambda: {
    "X-Authorization": ORKES_TOKEN,
    "Content-Type":    "application/json",
}


# ---------------------------------------------------------------------------
# Start a dispute resolution workflow
# ---------------------------------------------------------------------------
def start_dispute_workflow(
    doc_id:         str,
    customer_id:    str,
    rm_email:       str,
    rm_name:        str,
    case_summary:   str,
    dispute_amount: float = 0.0,
    priority:       str   = "Medium",
    branch_name:    str   = "",
) -> dict:
    """
    Kick off a durable dispute resolution workflow in Orkes.
    Returns dict with workflowId and tracking URL.
    """
    if not ORKES_URL or not ORKES_TOKEN:
        raise OrkesUnavailableError("ORKES_SERVER_URL or ORKES_ACCESS_TOKEN not configured")

    payload = {
        "name":    WORKFLOW_NAME,
        "version": 1,
        "input": {
            "doc_id":          doc_id,
            "customer_id":     customer_id,
            "rm_email":        rm_email,
            "rm_name":         rm_name,
            "case_summary":    case_summary,
            "dispute_amount":  dispute_amount,
            "priority":        priority,
            "branch_name":     branch_name,
        },
        "correlationId": doc_id,   # idempotency key — prevents duplicate workflows
    }

    try:
        resp = requests.post(
            f"{ORKES_URL}/api/workflow",
            json=payload,
            headers=_HEADERS(),
            timeout=10,
        )
        resp.raise_for_status()
        wf_id = resp.text.strip().strip('"')
        return {
            "workflowId":   wf_id,
            "trackingUrl":  f"{ORKES_URL}/execution/{wf_id}",
            "status":       "RUNNING",
        }
    except requests.RequestException as exc:
        raise OrkesUnavailableError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Get workflow status
# ---------------------------------------------------------------------------
def get_workflow_status(workflow_id: str) -> dict:
    """Poll current status of a running workflow."""
    try:
        resp = requests.get(
            f"{ORKES_URL}/api/workflow/{workflow_id}",
            headers=_HEADERS(),
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "workflowId":    workflow_id,
            "status":        data.get("status", "UNKNOWN"),
            "currentTask":   _current_task(data),
            "startTime":     data.get("startTime"),
            "endTime":       data.get("endTime"),
            "output":        data.get("output", {}),
        }
    except requests.RequestException as exc:
        raise OrkesUnavailableError(str(exc)) from exc


def _current_task(wf_data: dict) -> str:
    tasks = wf_data.get("tasks", [])
    in_progress = [t for t in tasks if t.get("status") == "IN_PROGRESS"]
    if in_progress:
        return in_progress[-1].get("referenceTaskName", "")
    return ""


# ---------------------------------------------------------------------------
# Send RM acknowledgement signal
# ---------------------------------------------------------------------------
def send_rm_acknowledgement(workflow_id: str, rm_name: str, notes: str = "") -> bool:
    """
    Send the 'rm_acknowledged' signal to unblock the durable wait in the workflow.
    Called when the RM clicks Acknowledge in the Streamlit UI or Appian form.
    """
    try:
        resp = requests.post(
            f"{ORKES_URL}/api/workflow/{workflow_id}/signal/rm_acknowledged",
            json={"rm_name": rm_name, "notes": notes},
            headers=_HEADERS(),
            timeout=8,
        )
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        raise OrkesUnavailableError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Check if Orkes is available
# ---------------------------------------------------------------------------
def is_available() -> bool:
    """Quick health check — returns False if Orkes not configured or unreachable."""
    if not ORKES_URL or not ORKES_TOKEN:
        return False
    try:
        resp = requests.get(f"{ORKES_URL}/health", timeout=4)
        return resp.status_code == 200
    except Exception:
        return False


class OrkesUnavailableError(Exception):
    """Raised when Orkes is not configured or unreachable."""
