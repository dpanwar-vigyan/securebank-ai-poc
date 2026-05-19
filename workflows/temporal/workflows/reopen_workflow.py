"""
Temporal workflow: askmybank_reopen_case
==========================================
Signal-driven dispute / complaint reopen workflow.

What it does:
  A customer calls to say a previously closed case was resolved incorrectly,
  or new evidence has come to light. The banker clicks "Reopen Case" in chat.html.
  A back-office manager reviews and decides whether the case should be formally
  reopened in the bank's case management system.

Design:
  Pure wait-state — no activities, no worker needed.
  Lambda creates the workflow and sends the Signal when Appian/review.html decides.
  Lambda handles all post-approval actions (ClickHouse update) via run_fulfilment().

Flow:
  Banker clicks "Reopen Case"
    → Lambda creates TKT-XXXXXXXX (ClickHouse, status=pending)
    → Lambda starts this workflow (sleeps up to 7 days)
    → Appian picks up ticket, manager reviews
    → POST /hitl/{id}/decide → Lambda sends Signal → workflow completes
    → Lambda run_fulfilment() updates ClickHouse to "reopened" or "closed"
"""

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow


@dataclass
class DecisionSignal:
    decision:      str    # "approved" (reopen granted) | "rejected" (stay closed)
    resolver_name: str = ""
    resolver_note: str = ""


@dataclass
class ReopenCaseInput:
    ticket_id:      str
    doc_ids:        list
    original_query: str
    customer_id:    str
    user_note:      str = ""


@workflow.defn(name="askmybank_reopen_case")
class ReopenCaseWorkflow:
    """
    Durable wait-state for case reopen decisions.

    No activities — fulfilment (ClickHouse case status update) runs in Lambda
    after the Signal wakes this workflow.

    SLA: 7 days. If no decision arrives, workflow times out and ClickHouse
    is updated to "timeout" by the decide endpoint.
    """

    def __init__(self):
        self._decision: DecisionSignal | None = None

    @workflow.signal(name="decision_received")
    def receive_decision(self, signal: DecisionSignal) -> None:
        """
        Receives approve/reject from Lambda /hitl/{id}/decide endpoint.
        Same signal name as eStatement workflow — Lambda always sends "decision_received".
        """
        workflow.logger.info(
            f"[ReopenCase] Signal: {signal.decision} by {signal.resolver_name}"
        )
        self._decision = signal

    @workflow.run
    async def run(self, inp: ReopenCaseInput) -> dict:
        workflow.logger.info(
            f"[{inp.ticket_id}] Reopen Case workflow started — "
            f"customer: {inp.customer_id} — waiting for back-office decision (7-day SLA)"
        )

        timed_out = not await workflow.wait_condition(
            lambda: self._decision is not None,
            timeout=timedelta(days=7),
        )

        if timed_out:
            workflow.logger.warning(
                f"[{inp.ticket_id}] ⏰ 7-day SLA breached — case reopen not decided"
            )
            return {
                "ticket_id":    inp.ticket_id,
                "final_status": "timeout",
                "note":         "Case reopen request expired — escalate to senior management",
            }

        decision = self._decision
        action   = "reopened" if decision.decision == "approved" else "reopen_denied"

        workflow.logger.info(
            f"[{inp.ticket_id}] ✅ {action} by {decision.resolver_name}"
        )

        return {
            "ticket_id":    inp.ticket_id,
            "final_status": action,
            "customer_id":  inp.customer_id,
            "resolver":     decision.resolver_name,
            "note":         decision.resolver_note,
        }
