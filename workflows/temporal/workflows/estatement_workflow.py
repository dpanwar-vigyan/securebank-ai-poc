"""
Temporal workflow: askmybank_estatement_duplicate

Key difference from the Orkes version:
  Orkes: DO_WHILE loop polls ClickHouse every 20s (pull model)
  Temporal: workflow.wait_condition blocks on a Signal (push model)

The workflow literally sleeps — zero compute — until Appian/back-office
sends a Signal via POST /hitl/{ticket_id}/signal. No polling, instant
response, no wasted 20-second intervals.

Flow:
  validate → address_auth
      │
      └── wait_condition(decision is not None)  ← sleeps here, costs nothing
              │
         Signal received from Appian/back-office
              │
         approved? → get_statement_s3 → generate_cover_note → log_dispatch
         rejected? → mark_ticket_rejected
"""

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from workflows.temporal.activities.estatement_activities import (
        EstatementInput,
        validate_request,
        check_address_auth,
        get_statement_s3,
        generate_cover_note,
        log_dispatch_complete,
        mark_ticket_rejected,
    )

# ── Signal payload ────────────────────────────────────────────────────────────
@dataclass
class DecisionSignal:
    decision:      str   # "approved" | "rejected"
    resolver_name: str   = ""
    resolver_note: str   = ""


# ── Workflow ──────────────────────────────────────────────────────────────────
@workflow.defn(name="askmybank_estatement_duplicate")
class EstatementDuplicateWorkflow:
    """
    Signal-driven eStatement HITL workflow.

    The workflow WAITS at step 3 with zero compute cost.
    Appian sends a Signal when the banker decides → workflow continues instantly.
    """

    def __init__(self):
        self._decision: DecisionSignal | None = None

    # ── Signal handler — Appian calls POST /hitl/{id}/signal ─────────────────
    @workflow.signal(name="decision_received")
    def receive_decision(self, signal: DecisionSignal) -> None:
        """
        Called by the Lambda signal endpoint when Appian POSTs the decision.
        This is a push — no polling, no ClickHouse reads in the loop.
        """
        workflow.logger.info(
            f"Signal received: {signal.decision} by {signal.resolver_name}"
        )
        self._decision = signal

    # ── Main workflow logic ───────────────────────────────────────────────────
    @workflow.run
    async def run(self, inp: EstatementInput) -> dict:
        retry = RetryPolicy(
            maximum_attempts        = 3,
            initial_interval        = timedelta(seconds=10),
            backoff_coefficient     = 2.0,
            maximum_interval        = timedelta(minutes=2),
        )
        opts = dict(
            start_to_close_timeout = timedelta(minutes=5),
            retry_policy           = retry,
        )

        # ── Step 1: Validate ─────────────────────────────────────────────────
        await workflow.execute_activity(validate_request, inp, **opts)
        workflow.logger.info(f"[{inp.ticket_id}] Validated ✅")

        # ── Step 2: Address authorisation ─────────────────────────────────────
        auth_result = await workflow.execute_activity(check_address_auth, inp, **opts)
        workflow.logger.info(
            f"[{inp.ticket_id}] Address auth: {auth_result.get('address_verified')}"
        )

        # ── Step 3: WAIT FOR SIGNAL — zero compute cost ───────────────────────
        # The workflow is durably persisted in Temporal Cloud.
        # It costs nothing while sleeping here — no polling, no timers firing.
        # Timeout after 7 days if no decision arrives (back-office SLA breach).
        workflow.logger.info(
            f"[{inp.ticket_id}] ⏸  Waiting for back-office Signal (up to 7 days)…"
        )
        await workflow.wait_condition(
            lambda: self._decision is not None,
            timeout=timedelta(days=7),
        )

        decision = self._decision
        workflow.logger.info(
            f"[{inp.ticket_id}] ▶  Signal: {decision.decision} "
            f"by {decision.resolver_name}"
        )

        # ── Step 4a: Approved path ────────────────────────────────────────────
        if decision.decision == "approved":
            s3_result = await workflow.execute_activity(
                get_statement_s3, inp, **opts
            )
            cover_result = await workflow.execute_activity(
                generate_cover_note, inp, **opts
            )
            await workflow.execute_activity(
                log_dispatch_complete,
                args=[
                    inp.ticket_id,
                    inp.delivery_address,
                    s3_result.get("presigned_url", ""),
                    cover_result.get("cover_note", ""),
                    decision.resolver_name,
                ],
                **opts,
            )
            workflow.logger.info(f"[{inp.ticket_id}] ✅ Dispatch complete")
            return {
                "ticket_id":    inp.ticket_id,
                "final_status": "completed",
                "resolver":     decision.resolver_name,
                "s3_url":       s3_result.get("presigned_url", ""),
            }

        # ── Step 4b: Rejected path ────────────────────────────────────────────
        else:
            await workflow.execute_activity(
                mark_ticket_rejected,
                args=[inp.ticket_id, decision.resolver_name],
                **opts,
            )
            workflow.logger.info(f"[{inp.ticket_id}] ❌ Rejected")
            return {
                "ticket_id":    inp.ticket_id,
                "final_status": "rejected",
                "resolver":     decision.resolver_name,
            }
