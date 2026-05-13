"""
Shared Orkes task workers used by multiple workflows.
  - askmybank_check_ticket_status  → polls ClickHouse backoffice_requests
  - askmybank_get_final_decision   → same, but called once after DO_WHILE exits
  - askmybank_mark_rejected        → sets ticket to rejected
"""

import os
from pathlib import Path

import clickhouse_connect
from conductor.client.worker.worker_interface import WorkerInterface
from conductor.client.http.models.task import Task
from conductor.client.http.models.task_result import TaskResult
from conductor.client.http.models.task_result_status import TaskResultStatus

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent.parent / ".env")
except Exception:
    pass

CH_HOST = os.getenv("CLICKHOUSE_HOST")
CH_USER = os.getenv("CLICKHOUSE_USER")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD")


def _ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, user=CH_USER, password=CH_PASS,
        secure=True, connect_timeout=8, send_receive_timeout=20,
    )


# ── Check ticket status (used inside DO_WHILE poll loop) ──────────────────────
class CheckTicketStatusWorker(WorkerInterface):
    def __init__(self):
        super().__init__("askmybank_check_ticket_status")

    def execute(self, task: Task) -> TaskResult:
        result = self.get_task_result_from_task(task)
        ticket_id = task.input_data.get("ticket_id", "")
        try:
            ch = _ch_client()
            rows = ch.query(
                "SELECT status, resolver_name, resolver_note "
                "FROM banking_docs.backoffice_requests "
                "WHERE ticket_id = {tid:String} "
                "ORDER BY created_at DESC LIMIT 1",
                parameters={"tid": ticket_id},
            ).result_rows

            status        = rows[0][0] if rows else "pending"
            resolver_name = rows[0][1] if rows else ""
            resolver_note = rows[0][2] if rows else ""

            result.status = TaskResultStatus.COMPLETED
            result.output_data = {
                "ticket_status": status,
                "resolver_name": resolver_name,
                "resolver_note": resolver_note,
            }
            print(f"[check_ticket_status] {ticket_id} → {status}")
        except Exception as exc:
            # Don't fail the workflow on a connectivity blip — return pending
            print(f"[check_ticket_status] CH error: {exc}")
            result.status = TaskResultStatus.COMPLETED
            result.output_data = {"ticket_status": "pending", "resolver_name": "", "resolver_note": ""}
        return result


# ── Get final decision (called once after DO_WHILE loop exits) ─────────────────
class GetFinalDecisionWorker(WorkerInterface):
    def __init__(self):
        super().__init__("askmybank_get_final_decision")

    def execute(self, task: Task) -> TaskResult:
        result = self.get_task_result_from_task(task)
        ticket_id = task.input_data.get("ticket_id", "")
        try:
            ch = _ch_client()
            rows = ch.query(
                "SELECT status, resolver_name, resolver_note "
                "FROM banking_docs.backoffice_requests "
                "WHERE ticket_id = {tid:String} "
                "ORDER BY created_at DESC LIMIT 1",
                parameters={"tid": ticket_id},
            ).result_rows

            status        = rows[0][0] if rows else "rejected"
            resolver_name = rows[0][1] if rows else ""
            resolver_note = rows[0][2] if rows else ""

            result.status = TaskResultStatus.COMPLETED
            result.output_data = {
                "status":        status,
                "resolver_name": resolver_name,
                "resolver_note": resolver_note,
            }
            print(f"[get_final_decision] {ticket_id} → {status}")
        except Exception as exc:
            result.status = TaskResultStatus.FAILED
            result.reason_for_incompletion = str(exc)
        return result


# ── Mark ticket rejected ───────────────────────────────────────────────────────
class MarkRejectedWorker(WorkerInterface):
    def __init__(self):
        super().__init__("askmybank_mark_rejected")

    def execute(self, task: Task) -> TaskResult:
        result = self.get_task_result_from_task(task)
        ticket_id = task.input_data.get("ticket_id", "")
        try:
            ch = _ch_client()
            ch.command(
                "ALTER TABLE banking_docs.backoffice_requests "
                "UPDATE status = 'rejected', updated_at = now() "
                "WHERE ticket_id = {tid:String}",
                parameters={"tid": ticket_id},
            )
            result.status = TaskResultStatus.COMPLETED
            result.output_data = {"ticket_id": ticket_id, "final_status": "rejected"}
            print(f"[mark_rejected] {ticket_id} marked rejected")
        except Exception as exc:
            result.status = TaskResultStatus.FAILED
            result.reason_for_incompletion = str(exc)
        return result


ALL_WORKERS = [
    CheckTicketStatusWorker(),
    GetFinalDecisionWorker(),
    MarkRejectedWorker(),
]
