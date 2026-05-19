"""
Temporal workflow: askmybank_estatement_duplicate
===================================================
WORKERLESS design — no Temporal worker process needed.

This workflow is a pure wait-state machine:
  1. Records the ticket in Temporal Cloud (durable, survives crashes)
  2. Sleeps for up to 7 days at zero compute cost
  3. Wakes instantly when a Signal arrives from Lambda
  4. Returns the decision — Lambda handles all fulfillment work directly

The actual post-approval work (S3 fetch, cover note, ClickHouse update)
runs in the Lambda /hitl/{id}/decide handler — not here.

Why no activities?
  Activities need a long-running worker process polling Temporal Cloud.
  Lambda can't be a worker (15-min cap, no persistent connection).
  But Lambda CAN start workflows and send Signals — so we use Temporal
  purely for its durable-wait capability, and do the work in Lambda.

Flow:
  Lambda starts workflow  (ticket created)
       │
       └── wait_condition(decision is not None)  ← sleeps, zero cost
               │
          Signal "decision_received" from Lambda /hitl/{id}/decide
               │
          workflow returns {decision, resolver, ticket_id}
          Lambda (already running) does the fulfilment work
"""

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow


# ── Signal payload ────────────────────────────────────────────────────────────
@dataclass
class DecisionSignal:
    decision:      str    # "approved" | "rejected"
    resolver_name: str = ""
    resolver_note: str = ""


# ── Minimal input — no activity imports needed ────────────────────────────────
@dataclass
class EstatementInput:
    ticket_id:        str
    doc_ids:          list
    delivery_address: str
    original_query:   str
    customer_id:      str


# ── Workflow ──────────────────────────────────────────────────────────────────
@workflow.defn(name="askmybank_estatement_duplicate")
class EstatementDuplicateWorkflow:
    """
    Pure signal-wait workflow — no activities, no worker needed.

    Temporal Cloud stores this workflow's state durably for up to 7 days.
    It costs nothing while waiting. Lambda sends the Signal on banker decision.
    Lambda also does all the post-decision work — this workflow just records
    the outcome and completes.
    """

    def __init__(self):
        self._decision: DecisionSignal | None = None

    @workflow.signal(name="decision_received")
    def receive_decision(self, signal: DecisionSignal) -> None:
        """
        Receives the banker's decision from Lambda.
        Lambda calls: hitl.send_temporal_signal(ticket_id, "decision_received", {...})
        """
        workflow.logger.info(
            f"[Signal] {signal.decision} by {signal.resolver_name}"
        )
        self._decision = signal

    @workflow.run
    async def run(self, inp: EstatementInput) -> dict:
        """
        No activities — just wait.
        All real work happens in Lambda before/after this Signal.
        """
        workflow.logger.info(
            f"[{inp.ticket_id}] Workflow started — waiting for back-office decision "
            f"(SLA: 7 days)"
        )

        # ── THE ENTIRE WORKFLOW IS THIS ONE LINE ──────────────────────────────
        # Zero compute. Zero polling. Temporal Cloud persists the state.
        # A single Signal from Lambda wakes it instantly.
        timed_out = not await workflow.wait_condition(
            lambda: self._decision is not None,
            timeout=timedelta(days=7),
        )

        if timed_out:
            workflow.logger.warning(
                f"[{inp.ticket_id}] ⏰ 7-day SLA breached — no decision received"
            )
            return {
                "ticket_id":    inp.ticket_id,
                "final_status": "timeout",
                "note":         "No decision received within 7-day SLA window",
            }

        decision = self._decision
        workflow.logger.info(
            f"[{inp.ticket_id}] ✅ Decision: {decision.decision} "
            f"by {decision.resolver_name} — Lambda handles fulfilment"
        )

        return {
            "ticket_id":    inp.ticket_id,
            "final_status": decision.decision,   # "approved" | "rejected"
            "resolver":     decision.resolver_name,
            "note":         decision.resolver_note,
        }
