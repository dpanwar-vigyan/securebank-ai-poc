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
    "reopen_case":    "temporal",   # durable, long-running approval chain
    "send_duplicate": "orkes",      # automated + address-verify flow
    "legal_export":   "orkes",      # visual DAG, impressive in demo
    "escalate_to_it": "temporal",   # feedback loop, needs durability
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
