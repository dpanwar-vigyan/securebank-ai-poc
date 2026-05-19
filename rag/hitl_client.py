"""
HITL Client — manages back-office workflow tickets in ClickHouse.

Two tables:
  banking_docs.backoffice_requests  — workflow tickets (reopen, duplicate, legal export, escalate)
  banking_docs.content_gaps         — AI quality feedback loop

Used by:
  - lambda_handler.py  POST /hitl        → create_ticket()
  - lambda_handler.py  GET  /hitl/status → get_ticket_status()
  - lambda_handler.py  GET  /hitl/pending→ get_pending_tickets()
  - lambda_handler.py  POST /hitl/decide → update_decision()
  - lambda_handler.py  POST /hitl/gap    → log_content_gap()
"""

import json
import os
import urllib.request
import urllib.error
import urllib.parse
import uuid
from datetime import datetime

import clickhouse_connect
import rag.config  # noqa: F401 — loads .env + st.secrets into os.environ

CH_HOST          = os.getenv("CLICKHOUSE_HOST")
CH_USER          = os.getenv("CLICKHOUSE_USER")
CH_PASS          = os.getenv("CLICKHOUSE_PASSWORD")
ORKES_API_URL    = os.getenv("ORKES_API_URL", "").rstrip("/")
ORKES_KEY_ID     = os.getenv("ORKES_KEY_ID", "")
ORKES_KEY_SECRET = os.getenv("ORKES_KEY_SECRET", "")

# Cached Orkes token (avoids re-auth on every warm Lambda invocation)
_orkes_token_cache: dict = {}

# ── Platform assignment per action ─────────────────────────────────────────────
ACTION_PLATFORM = {
    "reopen_case":    "temporal",   # Signal-driven dispute reopen
    "send_duplicate": "temporal",   # Signal-driven HITL (was Orkes DO_WHILE poll)
    "legal_export":   "orkes",      # visual DAG data pipeline — stays Orkes
    "escalate_to_it": "temporal",   # Signal-driven IT escalation
}

# Temporal workflow name per action
TEMPORAL_WORKFLOWS = {
    "send_duplicate": "askmybank_estatement_duplicate",
    "reopen_case":    "askmybank_reopen_case",
    "escalate_to_it": "askmybank_escalate_to_it",
}

# ── Action display labels ──────────────────────────────────────────────────────
ACTION_LABELS = {
    "reopen_case":    "Reopen Case",
    "send_duplicate": "Send Duplicate Copy",
    "legal_export":   "Export to Legal S3",
    "escalate_to_it": "Escalate to IT/Content",
}


class HITLClient:
    """
    Manages HITL tickets stored in ClickHouse.
    Lazy-init — only connects on first use, not at Lambda startup.
    """

    def __init__(self):
        self._ch = None
        self._available = False
        self._init_attempted = False

    def _get_ch(self):
        """Lazy ClickHouse connection — connects on first call."""
        if not self._init_attempted:
            self._init_attempted = True
            if not all([CH_HOST, CH_USER, CH_PASS]):
                print("HITLClient: ClickHouse credentials missing")
                return None
            try:
                self._ch = clickhouse_connect.get_client(
                    host=CH_HOST, user=CH_USER, password=CH_PASS,
                    secure=True, connect_timeout=8, send_receive_timeout=30,
                )
                self._ch.ping()
                self._available = True
                print(f"HITLClient: connected to {CH_HOST}")
            except Exception as exc:
                print(f"HITLClient: connection failed ({exc})")
        return self._ch if self._available else None

    # ── Create ticket ──────────────────────────────────────────────────────────
    def create_ticket(
        self,
        action: str,
        doc_ids: list[str],
        original_query: str,
        user_note: str = "",
        delivery_address: str = "",
    ) -> dict:
        """
        Insert a new HITL ticket. Returns {ticket_id, status, platform, action}.
        Raises HITLUnavailableError if ClickHouse is unreachable.
        """
        ch = self._get_ch()
        if not ch:
            raise HITLUnavailableError("ClickHouse unavailable — cannot create ticket")

        ticket_id = f"TKT-{datetime.utcnow().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"
        platform  = ACTION_PLATFORM.get(action, "temporal")
        now       = datetime.utcnow()

        ch.insert(
            table   = "banking_docs.backoffice_requests",
            data    = [[
                ticket_id,
                action,
                platform,
                json.dumps(doc_ids),
                original_query[:2000],
                user_note[:500],
                delivery_address[:500],
                "pending",
                now,        # created_at
                now,        # updated_at
                "",         # resolver_name
                "",         # resolver_note
                "",         # workflow_run_id
            ]],
            column_names = [
                "ticket_id", "action", "platform", "doc_ids",
                "original_query", "user_note", "delivery_address",
                "status", "created_at", "updated_at",
                "resolver_name", "resolver_note", "workflow_run_id",
            ],
        )

        print(f"HITLClient: created ticket {ticket_id} [{action}]")
        return {
            "ticket_id":    ticket_id,
            "action":       action,
            "action_label": ACTION_LABELS.get(action, action),
            "platform":     platform,
            "status":       "pending",
            "created_at":   now.isoformat(),
        }

    # ── Get ticket status ──────────────────────────────────────────────────────
    def get_ticket_status(self, ticket_id: str) -> dict:
        """
        Return current status of a ticket.
        Raises HITLUnavailableError or HITLNotFoundError.
        """
        ch = self._get_ch()
        if not ch:
            raise HITLUnavailableError("ClickHouse unavailable")

        result = ch.query(
            "SELECT ticket_id, action, platform, status, created_at, updated_at, "
            "resolver_name, resolver_note, doc_ids "
            "FROM banking_docs.backoffice_requests "
            "WHERE ticket_id = {tid:String} "
            "ORDER BY created_at DESC LIMIT 1",
            parameters={"tid": ticket_id},
        )
        rows = result.result_rows
        if not rows:
            raise HITLNotFoundError(f"Ticket {ticket_id} not found")

        r = rows[0]
        return {
            "ticket_id":    r[0],
            "action":       r[1],
            "action_label": ACTION_LABELS.get(r[1], r[1]),
            "platform":     r[2],
            "status":       r[3],
            "created_at":   str(r[4]),
            "updated_at":   str(r[5]),
            "resolver_name": r[6],
            "resolver_note": r[7],
            "doc_ids":      json.loads(r[8]) if r[8] else [],
        }

    # ── Get all pending tickets (back-office view) ─────────────────────────────
    def get_pending_tickets(self, limit: int = 50) -> list[dict]:
        """Return all pending/in_progress tickets for the back-office review UI."""
        ch = self._get_ch()
        if not ch:
            raise HITLUnavailableError("ClickHouse unavailable")

        result = ch.query(
            "SELECT ticket_id, action, platform, status, created_at, "
            "original_query, user_note, delivery_address, doc_ids, resolver_name "
            "FROM banking_docs.backoffice_requests "
            "WHERE status IN ('pending', 'in_progress') "
            "ORDER BY created_at DESC "
            f"LIMIT {int(limit)}",
        )
        cols = ["ticket_id", "action", "platform", "status", "created_at",
                "original_query", "user_note", "delivery_address", "doc_ids", "resolver_name"]
        tickets = []
        for row in result.result_rows:
            t = dict(zip(cols, row))
            t["action_label"] = ACTION_LABELS.get(t["action"], t["action"])
            t["doc_ids"]      = json.loads(t["doc_ids"]) if t.get("doc_ids") else []
            t["created_at"]   = str(t["created_at"])
            tickets.append(t)
        return tickets

    # ── Record decision (back-office approves / rejects) ─────────────────────
    def update_decision(
        self,
        ticket_id:     str,
        decision:      str,     # "approved" | "rejected"
        resolver_name: str,
        resolver_note: str = "",
    ) -> dict:
        """
        Update ticket status to approved/rejected.
        Uses ClickHouse ALTER TABLE ... UPDATE mutation.
        """
        if decision not in ("approved", "rejected"):
            raise ValueError(f"Invalid decision: {decision}")

        ch = self._get_ch()
        if not ch:
            raise HITLUnavailableError("ClickHouse unavailable")

        now = datetime.utcnow()
        ch.command(
            "ALTER TABLE banking_docs.backoffice_requests "
            "UPDATE status = {status:String}, "
            "       resolver_name = {rname:String}, "
            "       resolver_note = {rnote:String}, "
            "       updated_at    = {ts:DateTime} "
            "WHERE ticket_id = {tid:String}",
            parameters={
                "status": decision,
                "rname":  resolver_name,
                "rnote":  resolver_note,
                "ts":     now,
                "tid":    ticket_id,
            },
        )
        print(f"HITLClient: ticket {ticket_id} → {decision} by {resolver_name}")
        return {"ticket_id": ticket_id, "status": decision, "resolver_name": resolver_name}

    # ── Log content gap (catch-all escalation) ─────────────────────────────────
    def log_content_gap(
        self,
        original_query: str,
        ai_response:    str,
        user_feedback:  str,
    ) -> dict:
        """
        Insert a content gap record — feeds the AI improvement workflow.
        Returns {gap_id}.
        """
        ch = self._get_ch()
        if not ch:
            raise HITLUnavailableError("ClickHouse unavailable")

        gap_id = f"GAP-{datetime.utcnow().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"
        now    = datetime.utcnow()

        ch.insert(
            table   = "banking_docs.content_gaps",
            data    = [[
                gap_id,
                original_query[:2000],
                ai_response[:2000],
                user_feedback[:500],
                "new",
                now,   # created_at
                now,   # updated_at
                "",    # resolver_name
                "",    # fix_description
            ]],
            column_names = [
                "gap_id", "original_query", "ai_response",
                "user_feedback", "status", "created_at", "updated_at",
                "resolver_name", "fix_description",
            ],
        )

        print(f"HITLClient: logged content gap {gap_id}")
        return {
            "gap_id":     gap_id,
            "status":     "new",
            "created_at": now.isoformat(),
            "message":    "Your feedback has been logged. The IT/Content team will be notified.",
        }


    # ── Trigger Orkes workflow ─────────────────────────────────────────────────
    def trigger_orkes_workflow(
        self,
        workflow_name: str,
        workflow_input: dict,
        correlation_id: str = "",
    ) -> str:
        """
        Start an Orkes Conductor workflow via HTTP API.
        Returns the workflow instance ID (stored in backoffice_requests.workflow_run_id).
        Returns "" if Orkes is not configured (graceful degradation).
        """
        import time
        if not all([ORKES_API_URL, ORKES_KEY_ID, ORKES_KEY_SECRET]):
            print("HITLClient: Orkes not configured — skipping workflow trigger")
            return ""

        try:
            # ── Get / refresh token ──────────────────────────────────────────
            now = time.time()
            if not (_orkes_token_cache.get("token") and
                    _orkes_token_cache.get("exp", 0) - now > 300):
                payload = json.dumps({
                    "keyId": ORKES_KEY_ID, "keySecret": ORKES_KEY_SECRET
                }).encode()
                req = urllib.request.Request(
                    f"{ORKES_API_URL}/token",
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read())
                    _orkes_token_cache["token"] = data.get("token") or data.get("access_token")
                    _orkes_token_cache["exp"]   = now + 82_800

            token = _orkes_token_cache["token"]

            # ── Start workflow ───────────────────────────────────────────────
            body = json.dumps({
                "name":          workflow_name,
                "version":       1,
                "input":         workflow_input,
                "correlationId": correlation_id or workflow_input.get("ticket_id", ""),
            }).encode()
            req = urllib.request.Request(
                f"{ORKES_API_URL}/workflow",
                data=body,
                headers={
                    "X-Authorization": token,   # Orkes uses X-Authorization
                    "Content-Type":    "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                workflow_id = resp.read().decode().strip().strip('"')
                print(f"HITLClient: Orkes workflow started — {workflow_name} / {workflow_id}")
                return workflow_id

        except Exception as exc:
            print(f"HITLClient: Orkes trigger failed ({exc}) — continuing without workflow")
            return ""

    # ── Temporal SDK helpers (asyncio.run() from sync Lambda context) ─────────
    @staticmethod
    def _temporal_client_sync():
        """
        Return a connected Temporal client synchronously.
        Safe to call from FastAPI sync routes — creates a fresh event loop.
        """
        import asyncio
        from workflows.temporal.config import get_client
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(get_client()), loop
        except Exception:
            loop.close()
            raise

    # ── Trigger Temporal workflow ──────────────────────────────────────────────
    def trigger_temporal_workflow(
        self,
        workflow_name:  str,
        workflow_input: dict,
        workflow_id:    str = "",
    ) -> str:
        """
        Start a Temporal workflow using the SDK (gRPC to Temporal Cloud).
        Returns the workflow_id on success, "" on failure (non-fatal).

        workflow_id is set to ticket_id so we can Signal it later by ticket ID
        without needing to look up the run ID.
        """
        if not os.getenv("TEMPORAL_ADDRESS"):
            print("HITLClient: Temporal not configured — skipping")
            return ""

        import asyncio

        wf_id = workflow_id or f"{workflow_name}-{workflow_input.get('ticket_id','')}"

        async def _start():
            from workflows.temporal.config import get_client, TASK_QUEUE
            from workflows.temporal.workflows.estatement_workflow import (
                EstatementDuplicateWorkflow, EstatementInput,
            )
            from workflows.temporal.workflows.reopen_workflow import (
                ReopenCaseWorkflow, ReopenCaseInput,
            )
            from workflows.temporal.workflows.escalate_workflow import (
                EscalateToITWorkflow, EscalateToITInput,
            )
            from datetime import timedelta

            client = await get_client()

            # ── Route workflow name → class + typed input ─────────────────────
            if workflow_name == "askmybank_reopen_case":
                wf_class  = ReopenCaseWorkflow
                inp       = ReopenCaseInput(
                    ticket_id      = workflow_input.get("ticket_id", ""),
                    doc_ids        = workflow_input.get("doc_ids", []),
                    original_query = workflow_input.get("original_query", ""),
                    customer_id    = workflow_input.get("customer_id", ""),
                    user_note      = workflow_input.get("user_note", ""),
                )
                timeout = timedelta(days=7)

            elif workflow_name == "askmybank_escalate_to_it":
                wf_class  = EscalateToITWorkflow
                inp       = EscalateToITInput(
                    ticket_id      = workflow_input.get("ticket_id", ""),
                    original_query = workflow_input.get("original_query", ""),
                    user_note      = workflow_input.get("user_note", ""),
                    customer_id    = workflow_input.get("customer_id", ""),
                    doc_ids        = workflow_input.get("doc_ids", []),
                )
                timeout = timedelta(hours=48)   # IT has shorter SLA

            else:
                # Default: eStatement duplicate
                wf_class  = EstatementDuplicateWorkflow
                inp       = EstatementInput(
                    ticket_id        = workflow_input.get("ticket_id", ""),
                    doc_ids          = workflow_input.get("doc_ids", []),
                    delivery_address = workflow_input.get("delivery_address", ""),
                    original_query   = workflow_input.get("original_query", ""),
                    customer_id      = workflow_input.get("customer_id", ""),
                )
                timeout = timedelta(days=7)

            handle = await client.start_workflow(
                wf_class.run,
                inp,
                id                = wf_id,
                task_queue        = TASK_QUEUE,
                execution_timeout = timeout,
            )
            return handle.id

        try:
            loop = asyncio.new_event_loop()
            result = loop.run_until_complete(_start())
            loop.close()
            print(f"HITLClient: Temporal workflow started — {workflow_name} / {result}")
            return result
        except Exception as exc:
            print(f"HITLClient: Temporal start failed ({exc})")
            return ""

    def send_temporal_signal(
        self,
        workflow_id:    str,
        signal_name:    str,
        signal_payload: dict,
    ) -> bool:
        """
        Send a Signal to a running Temporal workflow.

        This is the core difference vs Orkes:
          Orkes: Lambda updates ClickHouse → workflow polls every 20s to detect it
          Temporal: Lambda sends Signal → workflow wakes INSTANTLY (milliseconds)

        Called by POST /hitl/{ticket_id}/decide after ClickHouse is updated.
        """
        if not os.getenv("TEMPORAL_ADDRESS"):
            print("HITLClient: Temporal not configured — cannot send signal")
            return False

        import asyncio
        from workflows.temporal.workflows.estatement_workflow import DecisionSignal

        async def _signal():
            from workflows.temporal.config import get_client
            client = await get_client()
            handle = client.get_workflow_handle(workflow_id)
            await handle.signal(
                "decision_received",
                DecisionSignal(
                    decision      = signal_payload.get("decision", ""),
                    resolver_name = signal_payload.get("resolver_name", ""),
                    resolver_note = signal_payload.get("resolver_note", ""),
                ),
            )

        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(_signal())
            loop.close()
            print(f"HITLClient: Signal '{signal_name}' → {workflow_id} ✅")
            return True
        except Exception as exc:
            print(f"HITLClient: Signal failed ({exc})")
            return False

    def run_fulfilment(
        self,
        ticket_id:     str,
        resolver_name: str,
        resolver_note: str = "",
    ) -> dict:
        """
        WORKERLESS design: runs post-approval fulfilment directly in Lambda.
        Replaces what the Temporal worker activities used to do.

        Fetches ticket details from ClickHouse, then:
          - send_duplicate: generates S3 presigned URL + Bedrock cover note
          - reopen_case / others: logs the action (extensible)

        Runs synchronously within the Lambda /hitl/{id}/decide handler.
        Total time: ~5s (S3 presign + Bedrock call + ClickHouse write).
        Well within Lambda's 29s API Gateway timeout.
        """
        import json
        import boto3
        from datetime import datetime, timezone

        ch = self._get_ch()
        if not ch:
            return {"skipped": True, "reason": "ClickHouse unavailable"}

        # ── Fetch ticket to get action + doc_ids + delivery_address ──────────
        try:
            rows = ch.query(
                "SELECT action, doc_ids, delivery_address, customer_id "
                "FROM banking_docs.backoffice_requests "
                "WHERE ticket_id = {tid:String} LIMIT 1",
                parameters={"tid": ticket_id},
            ).result_rows
        except Exception as exc:
            print(f"run_fulfilment: CH fetch failed ({exc})")
            return {"skipped": True, "reason": str(exc)}

        if not rows:
            return {"skipped": True, "reason": "ticket not found"}

        action, doc_ids_raw, delivery_address, customer_id = rows[0]
        doc_ids = json.loads(doc_ids_raw) if doc_ids_raw else []

        if action != "send_duplicate":
            # Other actions (reopen_case, escalate_to_it) — log and return
            print(f"run_fulfilment: no extra fulfilment needed for {action}")
            return {"action": action, "status": "noted"}

        # ── send_duplicate: S3 presigned URLs ────────────────────────────────
        s3      = boto3.client("s3", region_name=os.getenv("BEDROCK_REGION", "us-east-1"))
        bucket  = os.getenv("S3_VECTORS_BUCKET", "askmybank-vectors")
        urls    = []

        for doc_id in doc_ids[:5]:
            try:
                s3_rows = ch.query(
                    "SELECT s3_path FROM banking_docs.documents "
                    "WHERE doc_id = {did:String} LIMIT 1",
                    parameters={"did": doc_id},
                ).result_rows
                s3_path = s3_rows[0][0] if s3_rows else f"documents/{doc_id}.pdf"
            except Exception:
                s3_path = f"documents/{doc_id}.pdf"

            try:
                url = s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": bucket, "Key": s3_path},
                    ExpiresIn=86400,
                )
            except Exception:
                url = f"s3://{bucket}/{s3_path}"   # fallback for demo

            urls.append({"doc_id": doc_id, "url": url})

        # ── Bedrock cover note ────────────────────────────────────────────────
        today  = datetime.now(timezone.utc).strftime("%d %B %Y")
        prompt = (
            f"Write a brief professional cover letter for a bank statement duplicate.\n"
            f"Customer ID: {customer_id}, Docs: {doc_ids}, "
            f"Address: {delivery_address}, Date: {today}, Bank: SecureBank PLC.\n"
            f"Under 120 words. UK banking tone. Sign off as SecureBank Customer Services."
        )
        cover_note = ""
        try:
            bedrock = boto3.client(
                "bedrock-runtime",
                region_name=os.getenv("BEDROCK_REGION", "us-east-1"),
            )
            body = json.dumps({
                "messages":        [{"role": "user", "content": [{"text": prompt}]}],
                "inferenceConfig": {"maxTokens": 250, "temperature": 0.3},
            })
            resp       = bedrock.invoke_model(
                modelId=os.getenv("LLM_MODEL", "us.amazon.nova-lite-v1:0"),
                body=body, contentType="application/json",
            )
            cover_note = json.loads(resp["body"].read())["output"]["message"]["content"][0]["text"]
        except Exception as exc:
            print(f"run_fulfilment: Bedrock failed ({exc}) — using template")
            cover_note = (
                f"SecureBank PLC\n{today}\n\nDear Customer ({customer_id}),\n\n"
                f"Enclosed: certified duplicate copies {doc_ids}.\n"
                f"Delivery: {delivery_address}\n\nSecureBank Customer Services"
            )

        # ── Update ClickHouse with dispatch details ───────────────────────────
        now  = datetime.now(timezone.utc)
        note = (
            f"Dispatched {now.strftime('%Y-%m-%d %H:%M')} UTC | "
            f"Address: {delivery_address[:80]} | "
            f"S3: {'✅' if urls else '⚠️'} | "
            f"Approved by: {resolver_name}"
        )
        try:
            ch.command(
                "ALTER TABLE banking_docs.backoffice_requests "
                "UPDATE status = 'completed', resolver_note = {note:String}, "
                "       updated_at = {ts:DateTime} "
                "WHERE ticket_id = {tid:String}",
                parameters={"note": note, "ts": now, "tid": ticket_id},
            )
        except Exception as exc:
            print(f"run_fulfilment: CH update failed ({exc})")

        print(f"run_fulfilment: ✅ {ticket_id} dispatched — {len(urls)} docs")
        return {
            "action":      "send_duplicate",
            "status":      "dispatched",
            "doc_urls":    urls,
            "cover_note":  cover_note,
            "dispatch_at": now.isoformat(),
        }

    def _store_workflow_run_id(self, ticket_id: str, workflow_run_id: str):
        """Write the Orkes workflow instance ID back to the ClickHouse ticket."""
        if not workflow_run_id:
            return
        ch = self._get_ch()
        if not ch:
            return
        try:
            ch.command(
                "ALTER TABLE banking_docs.backoffice_requests "
                "UPDATE workflow_run_id = {wid:String} "
                "WHERE ticket_id = {tid:String}",
                parameters={"wid": workflow_run_id, "tid": ticket_id},
            )
        except Exception as exc:
            print(f"HITLClient: could not store workflow_run_id ({exc})")


# ── Exceptions ─────────────────────────────────────────────────────────────────
class HITLUnavailableError(Exception):
    """ClickHouse unreachable — HITL operations cannot proceed."""

class HITLNotFoundError(Exception):
    """Requested ticket or gap record does not exist."""
