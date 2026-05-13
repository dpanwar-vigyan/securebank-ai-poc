#!/usr/bin/env python3
"""
One-time setup: creates HITL workflow tables in ClickHouse Cloud.

Run from the banking-poc root:
    python setup_hitl_schema.py

Requires CLICKHOUSE_HOST / CLICKHOUSE_USER / CLICKHOUSE_PASSWORD
in .env (or already in environment).
"""
import os
import sys
from pathlib import Path

# ── Load .env ──────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except Exception:
    pass

import clickhouse_connect

CH_HOST = os.getenv("CLICKHOUSE_HOST")
CH_USER = os.getenv("CLICKHOUSE_USER")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD")

if not all([CH_HOST, CH_USER, CH_PASS]):
    print("ERROR: CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD must be set")
    sys.exit(1)

client = clickhouse_connect.get_client(
    host=CH_HOST, user=CH_USER, password=CH_PASS,
    secure=True, connect_timeout=10, send_receive_timeout=30,
)
client.ping()
print(f"✅ Connected to ClickHouse: {CH_HOST}\n")

# ── 1. backoffice_requests ─────────────────────────────────────────────────────
client.command("""
CREATE TABLE IF NOT EXISTS banking_docs.backoffice_requests
(
    ticket_id        String,
    action           String,     -- reopen_case | send_duplicate | legal_export | escalate_to_it
    platform         String,     -- temporal | orkes | (empty = not yet assigned)
    doc_ids          String,     -- JSON array: ["DSP00012","CMP00047"]
    original_query   String,
    user_note        String,
    delivery_address String,     -- eStatement: destination address
    status           String,     -- pending | in_progress | approved | rejected | completed
    created_at       DateTime DEFAULT now(),
    updated_at       DateTime DEFAULT now(),
    resolver_name    String,
    resolver_note    String,
    workflow_run_id  String
)
ENGINE = MergeTree()
ORDER BY (created_at, ticket_id)
""")
print("✅ banking_docs.backoffice_requests — created (or already exists)")

# ── 2. content_gaps ───────────────────────────────────────────────────────────
client.command("""
CREATE TABLE IF NOT EXISTS banking_docs.content_gaps
(
    gap_id           String,
    original_query   String,
    ai_response      String,
    user_feedback    String,
    status           String,     -- new | acknowledged | fixed | wont_fix
    created_at       DateTime DEFAULT now(),
    updated_at       DateTime DEFAULT now(),
    resolver_name    String,
    fix_description  String
)
ENGINE = MergeTree()
ORDER BY (created_at, gap_id)
""")
print("✅ banking_docs.content_gaps         — created (or already exists)")

# ── Verify ─────────────────────────────────────────────────────────────────────
tables = client.query("SHOW TABLES FROM banking_docs").result_rows
table_names = [r[0] for r in tables]
print(f"\nTables in banking_docs: {table_names}")
assert "backoffice_requests" in table_names, "backoffice_requests missing!"
assert "content_gaps"        in table_names, "content_gaps missing!"
print("\n🎉 Schema setup complete — ready to receive HITL tickets.\n")
