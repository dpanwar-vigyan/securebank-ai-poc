/*
  SAIL Interface: HITLPendingDashboard
  =====================================
  Back-office manager dashboard — shows all pending/in-progress tickets
  in a grid. Clicking a row opens the HITLApprovalForm inline.

  HOW TO USE:
  1. Design > New > Interface > name it "HITLPendingDashboard"
  2. Paste this expression into the Expression Editor
  3. No required inputs (self-contained)
  4. Publish and add to a Tempo Site tab or a Site Page

  Auto-refreshes every 60 seconds using a!refreshVariable().
*/

a!localVariables(
  /* ── Auto-refreshing ticket list ─────────────────────────────────── */
  local!ticketsResponse: a!refreshVariable(
    value: rule!AskMyBank_GetPendingTickets(),
    refreshInterval: 1   /* Appian refresh: 1 minute */
  ),
  local!tickets:  local!ticketsResponse.result.body.tickets,
  local!count:    local!ticketsResponse.result.body.count,
  local!loadError: local!ticketsResponse.error,

  /* ── Selected ticket for side-panel approval ─────────────────────── */
  local!selectedTicketId: null,
  local!approvalResult:   null,

  a!sectionLayout(
    label: "HITL Pending Approvals",
    contents: {

      /* ── Header row: count + last-refresh indicator ───────────────── */
      a!columnsLayout(
        columns: {
          a!columnLayout(
            contents: {
              a!richTextDisplayField(
                value: {
                  a!richTextItem(
                    text: if(
                      isnull(local!count),
                      "Loading...",
                      local!count & " ticket(s) awaiting approval"
                    ),
                    size:  "MEDIUM",
                    style: "STRONG"
                  )
                }
              )
            }
          ),
          a!columnLayout(
            contents: {
              a!richTextDisplayField(
                value: a!richTextItem(
                  text: "Last updated: " & text(now(), "HH:mm:ss"),
                  color: "SECONDARY",
                  size:  "SMALL"
                ),
                align: "END"
              )
            }
          )
        }
      ),

      /* ── Error banner ─────────────────────────────────────────────── */
      if(
        local!loadError,
        a!richTextDisplayField(
          value: a!richTextItem(
            text: "⚠ Could not reach Lambda API. Check X-Backoffice-Key and network.",
            color: "NEGATIVE"
          )
        ),
        {}
      ),

      /* ── Ticket grid ──────────────────────────────────────────────── */
      a!gridLayout(
        headerCells: {
          a!gridLayoutHeaderCell(label: "Ticket ID"),
          a!gridLayoutHeaderCell(label: "Action"),
          a!gridLayoutHeaderCell(label: "Customer"),
          a!gridLayoutHeaderCell(label: "Status"),
          a!gridLayoutHeaderCell(label: "Created"),
          a!gridLayoutHeaderCell(label: "")    /* action column */
        },
        columnConfigs: {
          a!gridLayoutColumnConfig(width: "DISTRIBUTE", weight: 2),
          a!gridLayoutColumnConfig(width: "DISTRIBUTE", weight: 2),
          a!gridLayoutColumnConfig(width: "DISTRIBUTE", weight: 1),
          a!gridLayoutColumnConfig(width: "DISTRIBUTE", weight: 1),
          a!gridLayoutColumnConfig(width: "DISTRIBUTE", weight: 2),
          a!gridLayoutColumnConfig(width: "DISTRIBUTE", weight: 1)
        },
        rows: a!forEach(
          items: if(isnull(local!tickets), {}, local!tickets),
          expression: a!gridRowLayout(
            id:       fv!item.ticket_id,
            contents: {
              /* Ticket ID */
              a!richTextDisplayField(
                value: a!richTextItem(
                  text: fv!item.ticket_id,
                  link: a!dynamicLink(
                    saveInto: a!save(local!selectedTicketId, fv!item.ticket_id)
                  ),
                  linkStyle: "STANDALONE"
                )
              ),
              /* Action */
              a!textDisplayField(
                value: substitute(
                  fv!item.action,
                  { "send_duplicate", "reopen_case", "legal_export", "escalate_to_it" },
                  { "Send eStatement", "Reopen Case", "Legal Export", "Escalate IT" }
                )
              ),
              /* Customer */
              a!textDisplayField(
                value: if(
                  or(isnull(fv!item.customer_id), fv!item.customer_id = ""),
                  "—",
                  fv!item.customer_id
                )
              ),
              /* Status badge */
              a!richTextDisplayField(
                value: a!richTextItem(
                  text: upper(fv!item.status),
                  style: "STRONG",
                  color: choose(
                    index(
                      { "pending", "in_progress", "approved", "rejected" },
                      fv!item.status,
                      1
                    ),
                    "WARN",
                    "HIGHLIGHT",
                    "POSITIVE",
                    "NEGATIVE"
                  )
                )
              ),
              /* Created at */
              a!textDisplayField(
                value: a!formatDate(
                  date: fv!item.created_at,
                  format: "dd MMM HH:mm"
                )
              ),
              /* Review button */
              a!richTextDisplayField(
                value: a!richTextItem(
                  text: if(
                    or(fv!item.status = "approved", fv!item.status = "rejected"),
                    "Resolved",
                    "Review →"
                  ),
                  link: if(
                    or(fv!item.status = "approved", fv!item.status = "rejected"),
                    null,
                    a!dynamicLink(
                      saveInto: a!save(local!selectedTicketId, fv!item.ticket_id)
                    )
                  ),
                  color: if(
                    or(fv!item.status = "approved", fv!item.status = "rejected"),
                    "SECONDARY",
                    "POSITIVE"
                  )
                )
              )
            }
          )
        ),
        emptyGridMessage: "No pending tickets. All caught up! ✓"
      ),

      /* ── Inline approval panel (appears when row clicked) ─────────── */
      if(
        not(isnull(local!selectedTicketId)),
        {
          a!separatorField(),
          a!sectionLayout(
            label: "Reviewing: " & local!selectedTicketId,
            contents: {
              /* Close button */
              a!richTextDisplayField(
                value: a!richTextItem(
                  text: "← Back to list",
                  link: a!dynamicLink(
                    saveInto: {
                      a!save(local!selectedTicketId, null),
                      a!save(local!approvalResult, null)
                    }
                  ),
                  color: "SECONDARY"
                )
              ),
              /* Embed the approval form */
              a!formElement(
                interface: "HITLApprovalForm",
                inputs: a!map(
                  ticket_id:   local!selectedTicketId,
                  on_complete: local!approvalResult
                )
              )
            }
          )
        },
        {}
      ),

      /* ── Post-approval feedback ───────────────────────────────────── */
      if(
        not(isnull(local!approvalResult)),
        a!richTextDisplayField(
          value: {
            a!richTextItem(
              text: "✓ Ticket " & local!selectedTicketId
                   & " " & local!approvalResult & ". ",
              color: "POSITIVE",
              style: "STRONG"
            ),
            a!richTextItem(
              text: "Temporal workflow was signalled instantly.",
              style: "PLAIN"
            )
          }
        ),
        {}
      )
    }
  )
)
