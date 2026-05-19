# Connected System Configuration Spec

## AskMyBank Lambda API

This document contains the exact values to enter in Appian Designer
when creating the HTTP Connected System.

---

### Step-by-step in Appian Designer

1. Go to **Design** tab → **Plug-ins & Connected Systems** → **+ New Connected System**
2. Choose: **HTTP**
3. Fill in:

```
Name:         AskMyBank Lambda API
Description:  HTTP Connected System for AskMyBank HITL endpoints on AWS Lambda
Base URL:     https://r6v15i892m.execute-api.us-east-1.amazonaws.com
```

4. Under **Authentication**: select **None** (we pass auth via custom header)

5. Under **Default Request Headers**, add:

| Key | Value | Notes |
|-----|-------|-------|
| `X-Backoffice-Key` | `<<paste DEMO_API_KEY here>>` | From `.env` or AWS Parameter Store |
| `Content-Type` | `application/json` | Required for POST bodies |
| `Accept` | `application/json` | |

6. Click **Test Connection** → expects HTTP 200 with:
   ```json
   {"service":"AskMyBank API","version":"2.1","docs":"/docs"}
   ```

7. Click **Save**

---

### Integration Objects to create

| Integration Name | Method | Path |
|------------------|--------|------|
| `AskMyBank_GetPendingTickets` | GET | `/hitl/pending` |
| `AskMyBank_GetTicketStatus` | GET | `/hitl/status/{ticket_id}` |
| `AskMyBank_SubmitDecision` | POST | `/hitl/{ticket_id}/decide` |

---

### AskMyBank_GetPendingTickets

```
Method:        GET
Relative URL:  /hitl/pending
Request Body:  (none)
Response:      JSON
```

Test call (no parameters) → expects:
```json
{"tickets": [], "count": 0}
```

---

### AskMyBank_GetTicketStatus

```
Method:        GET
Relative URL:  /hitl/status/{ticket_id}
```

Path Parameter:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `ticket_id` | Text | Yes | The ticket ID from create-ticket response |

Test call with `ticket_id = tkt_test_123` → expects HTTP 404 (no such ticket).
This confirms routing and auth are working correctly.

---

### AskMyBank_SubmitDecision

```
Method:        POST
Relative URL:  /hitl/{ticket_id}/decide
```

Path Parameter:
| Name | Type | Required |
|------|------|----------|
| `ticket_id` | Text | Yes |

Request Body (JSON):
```json
{
  "decision":      "approved",
  "resolver_name": "Jane Smith",
  "resolver_note": "Address verified in CRM"
}
```

| Field | Type | Required | Allowed Values |
|-------|------|----------|----------------|
| `decision` | Text | Yes | `"approved"` or `"rejected"` |
| `resolver_name` | Text | Yes | Full name of the banker |
| `resolver_note` | Text | No | Free text, max 500 chars |

Response (success):
```json
{
  "ticket_id":            "tkt_abc123",
  "status":               "approved",
  "decision":             "approved",
  "resolver_name":        "Jane Smith",
  "resolver_note":        "Address verified in CRM",
  "resolved_at":          "2025-05-15T11:05:00Z",
  "temporal_signal_sent": true
}
```

`temporal_signal_sent: true` confirms the Temporal Signal reached the workflow
and it has woken up to execute the approval activities.

---

### Security Notes

- The `X-Backoffice-Key` is a simple shared secret. In production, replace with
  OAuth 2.0 client credentials flow (Appian supports this natively).
- The API Gateway endpoint is public — the key is the only auth layer for these
  back-office endpoints. Keep it out of version control.
- Appian stores the Connected System credentials encrypted at rest.

---

### Getting DEMO_API_KEY

The key is the value of `DEMO_API_KEY` in your Lambda environment.

To retrieve it:
```bash
aws lambda get-function-configuration \
  --function-name AskMyBankFunction \
  --query "Environment.Variables.DEMO_API_KEY" \
  --output text
```

Or from AWS Systems Manager Parameter Store if you stored it there:
```bash
aws ssm get-parameter --name /askmybank/demo_api_key --with-decryption --query Parameter.Value --output text
```
