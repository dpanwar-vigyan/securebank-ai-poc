"""
Temporal workflow: askmybank_escalate_to_it
=============================================
Signal-driven IT / content escalation workflow.

What it does:
  When the AI cannot answer a query (content gap) and the banker selects
  "Escalate to IT", an IT/content team member is asked to review the gap,
  add the missing content to the knowledge base, and confirm completion.
  The workflow tracks the SLA for IT response (48-hour default).

  Note: simple "escalate_to_it" clicks go directly to log_content_gap()
  and don't start this workflow. This workflow is for cases where the
  banker also wants a tracked approval loop — e.g. "this customer query
  revealed a compliance gap that needs sign-off".

Design:
  Pure wait-state — no activities, no worker needed.
  Lambda handles all post-signal actions via run_fulfilment().
  Shorter SLA: 48 hours (IT response expected faster than banker decisions).

Flow:
  Banker clicks "Escalate to IT" with HITL tracking
    → Lambda creates TKT-XXXXXXXX (ClickHouse, status=pending)
    → Lambda starts this workflow (sleeps up to 48 hours)
    → IT/content team reviews, marks resolved
    → POST /hitl/{id}/decide → Lambda sends Signal → workflow completes
    → Lambda run_fulfilment() updates ClickHouse to "resolved" or "deferred"
"""

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow


@dataclass
class DecisionSignal:
    decision:      str    # "approved" (gap resolved) | "rejected" (deferred/out of scope)
    resolver_name: str = ""
    resolver_note: str = ""


@dataclass
class EscalateToITInput:
    ticket_id:      str
    original_query: str
    user_note:      str = ""
    customer_id:    str = ""
    doc_ids:        list = None

    def __post_init__(self):
        if self.doc_ids is None:
            self.doc_ids = []


@workflow.defn(name="askmybank_escalate_to_it")
class EscalateToITWorkflow:
    """
    Durable wait-state for IT escalation / content gap resolution.

    48-hour SLA — if IT doesn't respond in time, the workflow times out
    and Lambda marks the ticket "sla_breached" in ClickHouse.
    The content gap record is always written immediately in log_content_gap()
    (separate from this workflow). This workflow provides the tracked
    approval loop on top of that.
    """

    def __init__(self):
        self._decision: DecisionSignal | None = None

    @workflow.signal(name="decision_received")
    def receive_decision(self, signal: DecisionSignal) -> None:
        """
        Receives resolution from IT/content team via Lambda /hitl/{id}/decide.
        Same signal name used across all HITL workflows.
        """
        workflow.logger.info(
            f"[EscalateIT] Signal: {signal.decision} by {signal.resolver_name}"
        )
        self._decision = signal

    @workflow.run
    async def run(self, inp: EscalateToITInput) -> dict:
        workflow.logger.info(
            f"[{inp.ticket_id}] IT Escalation workflow started — "
            f"48-hour SLA — query: {inp.original_query[:80]}…"
        )

        # Shorter SLA than banker approvals — IT should respond within 48h
        timed_out = not await workflow.wait_condition(
            lambda: self._decision is not None,
            timeout=timedelta(hours=48),
        )

        if timed_out:
            workflow.logger.warning(
                f"[{inp.ticket_id}] ⏰ 48-hour IT SLA breached — auto-escalating to Head of Content"
            )
            return {
                "ticket_id":    inp.ticket_id,
                "final_status": "sla_breached",
                "note":         (
                    "IT team did not respond within 48 hours. "
                    "Auto-escalated to Head of Content."
                ),
            }

        decision = self._decision
        status   = "gap_resolved" if decision.decision == "approved" else "gap_deferred"

        workflow.logger.info(
            f"[{inp.ticket_id}] ✅ {status} by {decision.resolver_name}: {decision.resolver_note}"
        )

        return {
            "ticket_id":    inp.ticket_id,
            "final_status": status,
            "resolver":     decision.resolver_name,
            "note":         decision.resolver_note,
            "query":        inp.original_query,
        }
