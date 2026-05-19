/*
  SAIL Interface: HITLApprovalForm
  =================================
  Back-office banker approval form.
  Shows full ticket details + Approve / Reject buttons.

  INPUTS (define these in the Interface inputs tab):
    in!ticket_id       Text   — the ticket to act on
    in!on_complete     Any    — save target for parent process to handle result

  HOW TO USE:
  1. Design > New > Interface > name it "HITLApprovalForm"
  2. Paste this entire expression into the Expression Editor
  3. Set Interface Inputs:
       ticket_id  (Text, Required)
       on_complete (Any, Required — parent process variable)

  The form fetches the ticket live on load, so it always shows fresh status.
  When banker clicks Approve or Reject, it calls the integration and saves
  the result back to the parent process via in!on_complete.
*/

a!localVariables(
  /* ── Fetch the live ticket on form load ─────────────────────────────── */
  local!ticketResponse: rule!AskMyBank_GetTicketStatus(
    ticket_id: in!ticket_id
  ),
  local!ticket: local!ticketResponse.result.body,
  local!loadError: if(local!ticketResponse.error, true, false),

  /* ── Banker input state ─────────────────────────────────────────────── */
  local!bankerNote:     "",
  local!bankerDecision: "",   /* "approved" | "rejected" — set by button click */
  local!submitting:     false,
  local!submitResult:   null,
  local!submitError:    "",

  /* ── Derived display values ─────────────────────────────────────────── */
  local!actionLabel: choose(
    index(
      { "send_duplicate", "reopen_case", "legal_export", "escalate_to_it" },
      local!ticket.action,
      4
    ),
    "Send Duplicate eStatement",
    "Reopen Case",
    "Legal Export",
    "Escalate to IT"
  ),

  local!statusColour: choose(
    index(
      { "pending", "in_progress", "approved", "rejected" },
      local!ticket.status,
      1
    ),
    "#f59e0b",   /* amber  — pending     */
    "#3b82f6",   /* blue   — in_progress */
    "#10b981",   /* green  — approved    */
    "#ef4444"    /* red    — rejected    */
  ),

  /* ════════════════════════════════════════════════════════════════════ */
  a!formLayout(
    label: "Back-Office Approval — " & local!actionLabel,
    contents: {

      /* ── Error banner (load failure) ──────────────────────────────── */
      if(
        local!loadError,
        a!richTextDisplayField(
          value: a!richTextItem(
            text: "⚠ Could not load ticket " & in!ticket_id & ". Please refresh.",
            color: "NEGATIVE"
          )
        ),
        {}
      ),

      /* ── Ticket summary card ───────────────────────────────────────── */
      a!cardLayout(
        contents: {
          a!columnsLayout(
            columns: {
              a!columnLayout(
                contents: {
                  a!textDisplayField(
                    label: "Ticket ID",
                    value: local!ticket.ticket_id
                  ),
                  a!textDisplayField(
                    label: "Action Requested",
                    value: local!actionLabel
                  ),
                  a!textDisplayField(
                    label: "Customer ID",
                    value: if(
                      local!ticket.customer_id = "",
                      "—",
                      local!ticket.customer_id
                    )
                  ),
                  a!textDisplayField(
                    label: "Status",
                    value: upper(local!ticket.status)
                  )
                }
              ),
              a!columnLayout(
                contents: {
                  a!textDisplayField(
                    label: "Created At",
                    value: a!formatDate(
                      date: local!ticket.created_at,
                      format: "dd MMM yyyy HH:mm"
                    )
                  ),
                  a!textDisplayField(
                    label: "Delivery Address",
                    value: if(
                      or(
                        isnull(local!ticket.delivery_address),
                        local!ticket.delivery_address = ""
                      ),
                      "—",
                      local!ticket.delivery_address
                    )
                  ),
                  a!textDisplayField(
                    label: "Document IDs",
                    value: joinarray(local!ticket.doc_ids, ", ")
                  )
                }
              )
            }
          )
        },
        style: "STANDARD"
      ),

      /* ── Original query ────────────────────────────────────────────── */
      a!sectionLayout(
        label: "Original Customer Query",
        contents: {
          a!richTextDisplayField(
            value: a!richTextItem(
              text: if(
                or(isnull(local!ticket.original_query), local!ticket.original_query = ""),
                "(no query recorded)",
                local!ticket.original_query
              ),
              style: "PLAIN"
            )
          )
        }
      ),

      /* ── Banker note ───────────────────────────────────────────────── */
      a!sectionLayout(
        label: "Customer Notes (from chat)",
        contents: {
          a!richTextDisplayField(
            value: a!richTextItem(
              text: if(
                or(isnull(local!ticket.user_note), local!ticket.user_note = ""),
                "(no notes)",
                local!ticket.user_note
              ),
              style: "PLAIN"
            )
          )
        }
      ),

      /* ── Resolver note input ───────────────────────────────────────── */
      a!sectionLayout(
        label: "Your Decision Notes",
        contents: {
          a!paragraphField(
            label:         "Notes",
            labelPosition: "ABOVE",
            placeholder:   "Enter reason for approval or rejection (optional but recommended)...",
            value:         local!bankerNote,
            saveInto:      local!bankerNote,
            height:        "SHORT",
            maxLength:     500
          )
        }
      ),

      /* ── Already resolved banner ───────────────────────────────────── */
      if(
        or(
          local!ticket.status = "approved",
          local!ticket.status = "rejected"
        ),
        a!richTextDisplayField(
          value: {
            a!richTextItem(text: "✓ This ticket was already ", style: "PLAIN"),
            a!richTextItem(
              text: upper(local!ticket.status),
              style: "STRONG",
              color: if(local!ticket.status = "approved", "POSITIVE", "NEGATIVE")
            ),
            a!richTextItem(
              text: " by " & local!ticket.resolver_name
                   & if(
                       isnull(local!ticket.resolver_note),
                       "",
                       ": "" & local!ticket.resolver_note & """
                     ),
              style: "PLAIN"
            )
          }
        ),
        {}
      ),

      /* ── Submit error ─────────────────────────────────────────────── */
      if(
        local!submitError <> "",
        a!richTextDisplayField(
          value: a!richTextItem(
            text: "⚠ Submission failed: " & local!submitError,
            color: "NEGATIVE"
          )
        ),
        {}
      ),

      /* ── Success confirmation ─────────────────────────────────────── */
      if(
        not(isnull(local!submitResult)) and not(isnull(local!submitResult.result)),
        a!richTextDisplayField(
          value: {
            a!richTextItem(
              text: "✓ Decision submitted. Temporal workflow notified instantly. ",
              style: "PLAIN",
              color: "POSITIVE"
            ),
            a!richTextItem(
              text: if(
                local!submitResult.result.body.temporal_signal_sent = true,
                "(Signal sent ✓)",
                "(Signal pending)"
              ),
              style: "PLAIN"
            )
          }
        ),
        {}
      )
    },

    /* ── Buttons ─────────────────────────────────────────────────────── */
    buttons: a!buttonArrayLayout(
      buttons: {

        /* Approve */
        a!buttonWidget(
          label:     if(local!submitting and local!bankerDecision = "approved", "Approving...", "✓ Approve"),
          style:     "PRIMARY",
          color:     "POSITIVE",
          disabled:  or(
            local!submitting,
            local!ticket.status = "approved",
            local!ticket.status = "rejected"
          ),
          saveInto: {
            a!save(local!submitting, true),
            a!save(local!bankerDecision, "approved"),
            a!save(local!submitError, ""),
            a!save(
              local!submitResult,
              rule!AskMyBank_SubmitDecision(
                ticket_id:     in!ticket_id,
                decision:      "approved",
                resolver_name: loggedInUser().firstName & " " & loggedInUser().lastName,
                resolver_note: local!bankerNote
              )
            ),
            a!save(
              local!submitError,
              if(
                local!submitResult.error,
                local!submitResult.error.detail,
                ""
              )
            ),
            a!save(local!submitting, false),
            /* Notify parent process */
            if(
              not(local!submitResult.error),
              a!save(in!on_complete, "approved"),
              {}
            )
          }
        ),

        /* Reject */
        a!buttonWidget(
          label:     if(local!submitting and local!bankerDecision = "rejected", "Rejecting...", "✗ Reject"),
          style:     "SECONDARY",
          color:     "NEGATIVE",
          disabled:  or(
            local!submitting,
            local!ticket.status = "approved",
            local!ticket.status = "rejected"
          ),
          saveInto: {
            a!save(local!submitting, true),
            a!save(local!bankerDecision, "rejected"),
            a!save(local!submitError, ""),
            a!save(
              local!submitResult,
              rule!AskMyBank_SubmitDecision(
                ticket_id:     in!ticket_id,
                decision:      "rejected",
                resolver_name: loggedInUser().firstName & " " & loggedInUser().lastName,
                resolver_note: local!bankerNote
              )
            ),
            a!save(
              local!submitError,
              if(
                local!submitResult.error,
                local!submitResult.error.detail,
                ""
              )
            ),
            a!save(local!submitting, false),
            if(
              not(local!submitResult.error),
              a!save(in!on_complete, "rejected"),
              {}
            )
          }
        ),

        /* Refresh (re-fetch live status) */
        a!buttonWidget(
          label:   "↻ Refresh",
          style:   "LINK",
          saveInto: a!save(local!submitResult, null)
        )
      },
      align: "START"
    )
  )
)
