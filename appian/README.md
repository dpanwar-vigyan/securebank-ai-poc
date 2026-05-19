# Appian HITL Integration — Configuration Guide

Everything you need to configure in Appian Designer.
Estimated setup time: **30–45 minutes**.

---

## What This Does

Appian polls `GET /hitl/pending` every 60 seconds (scheduled process).
When a new ticket appears, it creates an Appian Case and assigns a User Task to the back-office team.
The banker sees the ticket details in a SAIL form, clicks Approve or Reject.
Appian immediately POSTs the decision to `POST /hitl/{ticket_id}/decide`.
Lambda updates ClickHouse + sends the Temporal Signal — the workflow wakes instantly.

---

## Architecture

```
Lambda (creates ticket in ClickHouse + starts Temporal workflow)
    │
    ├── SQS (guaranteed delivery buffer — 14-day retention)
    │       │
    │       └── Appian (alternative: polls SQS instead of REST)
    │
    └── REST polling (Appian scheduled process → GET /hitl/pending)
            │
            └── Appian Case + User Task (SAIL approval form)
                    │
                    └── POST /hitl/{id}/decide → Lambda → Temporal Signal
```

---

## Step 1 — Create the HTTP Connected System

**Path:** Design > Plug-ins & Connected Systems > New Connected System > HTTP

| Field | Value |
|-------|-------|
| Name | `AskMyBank Lambda API` |
| Base URL | `https://r6v15i892m.execute-api.us-east-1.amazonaws.com` |
| Authentication | None (custom header) |

Under **Headers**, add:
| Header Name | Value |
|-------------|-------|
| `X-Backoffice-Key` | `<<your DEMO_API_KEY value>>` |
| `Content-Type` | `application/json` |

Click **Test Connection** → should return `{"service":"AskMyBank API","version":"2.1"}`.

Save as **`AskMyBank_Lambda_API`**.

---

## Step 2 — Create Integration Objects

### 2a. Fetch Pending Tickets

**Path:** Design > New > Integration

| Field | Value |
|-------|-------|
| Name | `AskMyBank_GetPendingTickets` |
| Connected System | `AskMyBank_Lambda_API` |
| Method | GET |
| URL path | `/hitl/pending` |
| Response type | JSON |

No request body.

### 2b. Submit Decision

| Field | Value |
|-------|-------|
| Name | `AskMyBank_SubmitDecision` |
| Connected System | `AskMyBank_Lambda_API` |
| Method | POST |
| URL path | `/hitl/{ticket_id}/decide` |
| Path parameter | `ticket_id` (Text, required) |
| Request body | JSON (see `expressions/submit_decision_body.expr`) |

---

## Step 3 — Create the Record Type (Case Data)

**Path:** Design > New > Record Type

Name: `HITLTicket`

Add these fields:

| Field | Type | Source |
|-------|------|--------|
| `ticket_id` | Text | Integration response |
| `action` | Text | Integration response |
| `status` | Text | Integration response |
| `original_query` | Text | Integration response |
| `user_note` | Text | Integration response |
| `delivery_address` | Text | Integration response |
| `customer_id` | Text | Integration response |
| `doc_ids` | List of Text | Integration response |
| `created_at` | DateTime | Integration response |
| `resolver_name` | Text | User input |
| `resolver_note` | Text | User input |

---

## Step 4 — Create the Approval Interface (SAIL)

**Path:** Design > New > Interface

Name: `HITLApprovalForm`

Paste the full SAIL from **`interfaces/approval_form.sail`** into the Expression Editor.

---

## Step 5 — Create the Process Model

**Path:** Design > New > Process Model

Name: `AskMyBank HITL Approval`

Node layout — see **`process_model_design.md`** for full node configuration.

Nodes in order:
1. **Timer Start Event** → fires every 60 seconds
2. **Call Integration** → `AskMyBank_GetPendingTickets`
3. **Decision Gateway** → "Any new tickets?"
4. **For-Each Loop** → one path per ticket in `tickets` array
5. **User Task** → SAIL form `HITLApprovalForm` assigned to group `BackOfficeTeam`
6. **Call Integration** → `AskMyBank_SubmitDecision` with banker's choice
7. **End Event**

---

## Step 6 — Configure the Group

**Path:** Administration > Groups > New Group

Name: `BackOfficeTeam`
Add all back-office managers who review HITL tickets.

---

## Step 7 — Activate

1. Publish the process model
2. Start an instance manually once to confirm the timer fires
3. Check Appian logs for `GET /hitl/pending` response
4. Create a test ticket from the chat UI → it should appear as a User Task within 60 seconds

---

## SQS Alternative (Optional)

Instead of REST polling, Appian can subscribe to SQS directly if you have the **Amazon SQS Plug-in** from Appian App Market.

| Setting | Value |
|---------|-------|
| Queue URL | (output of `sam deploy` → `HITLQueueUrl`) |
| Region | `us-east-1` |
| Max messages | 10 |
| Wait time | 20s (long poll) |

This is push-style — no polling delay. Messages arrive within milliseconds of Lambda publishing them.

---

## Environment Values Needed

| Variable | Where to get it |
|----------|----------------|
| `DEMO_API_KEY` | `.env` file / AWS Parameter Store |
| Lambda API base URL | API Gateway console or `sam deploy` output |
| SQS Queue URL | `sam deploy` output → `HITLQueueUrl` |
