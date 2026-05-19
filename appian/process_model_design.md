# Appian Process Model — AskMyBank HITL Approval

## Process Model Name
`AskMyBank HITL Approval`

## Purpose
Polls Lambda every 60 seconds for new HITL tickets.
For each new ticket, creates a User Task assigned to `BackOfficeTeam`.
Banker reviews and decides → decision POSTed back to Lambda → Temporal Signal fires.

---

## Node-by-Node Configuration

### Node 1 — Timer Start Event

| Property | Value |
|----------|-------|
| Type | Timer Start Event |
| Name | `Poll for Tickets` |
| Trigger | Recurring: every **60 seconds** |
| Start immediately | Yes |

This fires the process repeatedly. Each execution handles the current batch of pending tickets.

---

### Node 2 — Script Task: Fetch Pending Tickets

| Property | Value |
|----------|-------|
| Type | Script Task |
| Name | `Fetch Pending Tickets` |
| Expression | See below |

**Expression (paste into Script field):**
```
pv!tickets: rule!AskMyBank_GetPendingTickets().result.body.tickets
pv!ticketCount: length(pv!tickets)
```

**Process Variables to create:**
| Name | Type | Default |
|------|------|---------|
| `tickets` | List of Dictionary | `{}` |
| `ticketCount` | Integer | `0` |
| `activeTicket` | Dictionary | `null` |
| `bankerDecision` | Text | `""` |
| `processComplete` | Boolean | `false` |

---

### Node 3 — Decision Gateway: Any New Tickets?

| Property | Value |
|----------|-------|
| Type | XOR Gateway |
| Name | `Any pending tickets?` |

**Outgoing paths:**

| Condition | Target |
|-----------|--------|
| `pv!ticketCount > 0` | Node 4 (For-Each) |
| `else` | Node 7 (End — nothing to do) |

---

### Node 4 — Sub-Process: For Each Ticket

| Property | Value |
|----------|-------|
| Type | Sub-Process (looping) |
| Name | `Process Each Ticket` |
| Loop over | `pv!tickets` |
| Loop variable | `pv!activeTicket` |

Inside the sub-process, nodes 4a → 4c run once per ticket.

#### Node 4a — Decision Gateway: Is This Ticket Already Processed?

Some tickets may have already been assigned a task in a previous poll cycle.
Check: if status is not `pending`, skip it.

```
pv!activeTicket.status = "pending"
```

- True → Node 4b (create User Task)
- False → end sub-process iteration (skip)

#### Node 4b — User Task: Banker Approval

| Property | Value |
|----------|-------|
| Type | User Input Task |
| Name | `Review Ticket: ` & pv!activeTicket.ticket_id |
| Assigned to | Group: `BackOfficeTeam` |
| Interface | `HITLApprovalForm` |
| Interface Inputs | `ticket_id = pv!activeTicket.ticket_id`, `on_complete = pv!bankerDecision` |
| Deadline | 7 days (matches Temporal workflow timeout) |
| Priority | Medium |

When the banker submits the form, `pv!bankerDecision` will be set to `"approved"` or `"rejected"`.

**Note:** The SAIL form itself calls `AskMyBank_SubmitDecision` on button click.
This means the Lambda is called instantly on click — no separate Appian integration call needed.
The User Task completion just marks the task done in Appian Case Manager.

#### Node 4c — Script Task: Log Outcome (optional)

```
/* Update Appian case record for audit trail */
pv!processComplete: true
```

---

### Node 5 — End Event

| Property | Value |
|----------|-------|
| Type | End Event |
| Name | `Done` |

---

### Node 6 — End Event (no tickets)

| Property | Value |
|----------|-------|
| Type | End Event |
| Name | `No tickets — idle` |

---

## Process Variable Summary

| Variable | Type | Description |
|----------|------|-------------|
| `tickets` | List of Dictionary | Tickets returned by Lambda |
| `ticketCount` | Integer | Number of tickets |
| `activeTicket` | Dictionary | Current loop ticket |
| `bankerDecision` | Text | "approved" or "rejected" |
| `processComplete` | Boolean | True when task submitted |

---

## Notification Configuration

In the User Task (Node 4b), configure Email Notification:
- **To:** `BackOfficeTeam` group
- **Subject:** `New HITL Ticket Requires Approval: {pv!activeTicket.ticket_id}`
- **Body:**
  ```
  A new back-office ticket requires your approval.

  Ticket ID:  {pv!activeTicket.ticket_id}
  Action:     {pv!activeTicket.action}
  Customer:   {pv!activeTicket.customer_id}
  Created:    {pv!activeTicket.created_at}

  Please log into Appian to review and approve/reject.
  ```

---

## Site Configuration (add to Tempo Site)

**Path:** Design > Sites > Back Office Portal > Add Page

| Page | Interface |
|------|-----------|
| "HITL Dashboard" | `HITLPendingDashboard` |
| "Ticket Review" | `HITLApprovalForm` (with ticket_id parameter) |

This gives the back-office team a single URL to bookmark.

---

## Data Flow Summary

```
Timer (60s)
  └── GET /hitl/pending           ← Lambda reads ClickHouse
        └── for each ticket
              └── User Task        ← Banker opens HITLApprovalForm
                    └── Approve / Reject click
                          └── POST /hitl/{id}/decide  ← Lambda
                                ├── ClickHouse updated (status = approved/rejected)
                                └── Temporal Signal sent  ← workflow wakes INSTANTLY
                                      └── Activities execute (send eStatement, etc.)
```

---

## Testing the Integration

1. From `chat.html`, create a `send_duplicate` ticket (click "Send Duplicate" action button)
2. Check Appian Process Monitor — new process instance should start within 60 seconds
3. Open "HITL Dashboard" in Appian — ticket should appear in grid
4. Click "Review" → `HITLApprovalForm` loads with ticket details
5. Enter a note, click "Approve"
6. Check `chat.html` tray — ticket status should update to "approved"
7. Check Temporal Cloud UI — workflow should show `WORKFLOW_EXECUTION_COMPLETED`

Total end-to-end time from Approve click to Temporal completion: **< 3 seconds**.
