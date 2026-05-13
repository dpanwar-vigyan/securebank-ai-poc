"""
Orkes Conductor workflow: askmybank_estatement_duplicate
──────────────────────────────────────────────────────────
Triggered when a banker requests a duplicate eStatement copy.

Visual DAG (shown in Orkes UI):
  validate_request
       │
  check_address_auth
       │
  ┌────┴──────────────────────────────┐
  │  DO_WHILE: poll_for_approval      │  ← loops every 20s until decision
  │    sleep_20s → check_ticket_status│  ← visible spinning in Orkes UI
  └────┬──────────────────────────────┘
       │
  get_final_decision           ← re-reads CH after loop exits
       │
  SWITCH ──approved──► get_statement_s3
  │                         │
  │                   generate_cover_note_ai
  │                         │
  │                   log_dispatch_and_complete  ← marks ticket completed
  │
  └──rejected──► mark_ticket_rejected

Inputs (from Lambda):
  ticket_id, doc_ids, delivery_address, original_query, customer_id
"""

WORKFLOW_NAME    = "askmybank_estatement_duplicate"
WORKFLOW_VERSION = 1

WORKFLOW_DEF = {
    "name":        WORKFLOW_NAME,
    "version":     WORKFLOW_VERSION,
    "description": "AskMyBank — eStatement duplicate copy: address verify → HITL approval → S3 retrieve → AI cover note → dispatch",
    "timeoutSeconds":   86400,   # 24h max — long enough for back-office to respond
    "timeoutPolicy":    "TIME_OUT_WF",
    "inputParameters":  ["ticket_id", "doc_ids", "delivery_address", "original_query", "customer_id"],
    "outputParameters": {
        "ticket_id":    "${final_decision_ref.output.status}",
        "final_status": "${final_decision_ref.output.status}",
    },
    "tasks": [

        # ── 1. Validate the incoming request ────────────────────────────────
        {
            "name":              "askmybank_validate_request",
            "taskReferenceName": "validate_ref",
            "type":              "SIMPLE",
            "inputParameters": {
                "ticket_id":       "${workflow.input.ticket_id}",
                "doc_ids":         "${workflow.input.doc_ids}",
                "delivery_address":"${workflow.input.delivery_address}",
            },
        },

        # ── 2. Check delivery address is authorised for this customer ────────
        {
            "name":              "askmybank_check_address_auth",
            "taskReferenceName": "auth_ref",
            "type":              "SIMPLE",
            "inputParameters": {
                "ticket_id":       "${workflow.input.ticket_id}",
                "customer_id":     "${workflow.input.customer_id}",
                "delivery_address":"${workflow.input.delivery_address}",
            },
        },

        # ── 3. DO_WHILE: poll ClickHouse every 20s until back-office decides ─
        {
            "name":              "poll_approval_loop",
            "taskReferenceName": "poll_loop_ref",
            "type":              "DO_WHILE",
            "loopCondition": (
                "if ( $.check_status_ref['output']['ticket_status'] == 'pending' "
                "|| $.check_status_ref['output']['ticket_status'] == 'in_progress' "
                ") { true; } else { false; }"
            ),
            "loopOver": [
                # 3a. Wait 20 seconds between polls
                {
                    "name":              "askmybank_sleep",
                    "taskReferenceName": "sleep_ref",
                    "type":              "WAIT",
                    "inputParameters":   {"duration": "20 seconds"},
                },
                # 3b. Check ticket status in ClickHouse
                {
                    "name":              "askmybank_check_ticket_status",
                    "taskReferenceName": "check_status_ref",
                    "type":              "SIMPLE",
                    "inputParameters": {
                        "ticket_id": "${workflow.input.ticket_id}",
                    },
                },
            ],
        },

        # ── 4. Read final decision (after loop exits) ────────────────────────
        {
            "name":              "askmybank_get_final_decision",
            "taskReferenceName": "final_decision_ref",
            "type":              "SIMPLE",
            "inputParameters": {
                "ticket_id": "${workflow.input.ticket_id}",
            },
        },

        # ── 5. SWITCH on approved vs rejected ───────────────────────────────
        {
            "name":              "on_decision",
            "taskReferenceName": "switch_ref",
            "type":              "SWITCH",
            "evaluatorType":     "value-param",
            "expression":        "decision",
            "inputParameters": {
                "decision": "${final_decision_ref.output.status}",
            },
            "decisionCases": {

                "approved": [
                    # 5a. Get S3 presigned URL for the statement PDF
                    {
                        "name":              "askmybank_get_statement_s3",
                        "taskReferenceName": "s3_ref",
                        "type":              "SIMPLE",
                        "inputParameters": {
                            "doc_ids":    "${workflow.input.doc_ids}",
                            "ticket_id":  "${workflow.input.ticket_id}",
                        },
                    },
                    # 5b. Generate AI cover note via Bedrock Nova Lite
                    {
                        "name":              "askmybank_generate_cover_note",
                        "taskReferenceName": "cover_ref",
                        "type":              "SIMPLE",
                        "inputParameters": {
                            "customer_id":     "${workflow.input.customer_id}",
                            "delivery_address":"${workflow.input.delivery_address}",
                            "original_query":  "${workflow.input.original_query}",
                            "doc_ids":         "${workflow.input.doc_ids}",
                        },
                    },
                    # 5c. Log dispatch and mark ticket completed
                    {
                        "name":              "askmybank_log_dispatch_complete",
                        "taskReferenceName": "complete_approved_ref",    # unique name
                        "type":              "SIMPLE",
                        "inputParameters": {
                            "ticket_id":       "${workflow.input.ticket_id}",
                            "delivery_address":"${workflow.input.delivery_address}",
                            "s3_url":          "${s3_ref.output.presigned_url}",
                            "cover_note":      "${cover_ref.output.cover_note}",
                        },
                    },
                ],

                "rejected": [
                    # 5d. Mark ticket rejected in ClickHouse
                    {
                        "name":              "askmybank_mark_rejected",
                        "taskReferenceName": "complete_rejected_ref",    # unique name
                        "type":              "SIMPLE",
                        "inputParameters": {
                            "ticket_id": "${workflow.input.ticket_id}",
                            "status":    "rejected",
                        },
                    },
                ],
            },
            "defaultCase": [],
        },
    ],
}
