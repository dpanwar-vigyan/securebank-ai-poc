#!/usr/bin/env python3
"""
One-time setup: creates banking_docs schema + all tables in MotherDuck (DuckDB cloud).

Sign up free at https://motherduck.com → Settings → Access Tokens → copy token.

Usage:
    MOTHERDUCK_TOKEN=<token> python setup_motherduck_schema.py

Or add MOTHERDUCK_TOKEN to .env first:
    python setup_motherduck_schema.py
"""

import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except Exception:
    pass

import duckdb

TOKEN   = os.getenv("MOTHERDUCK_TOKEN", "")
DB_NAME = os.getenv("MOTHERDUCK_DB", "askmybank")

if not TOKEN:
    print("ERROR: MOTHERDUCK_TOKEN not set — add it to .env or pass as env var")
    sys.exit(1)

con = duckdb.connect(f"md:{DB_NAME}?motherduck_token={TOKEN}")
con.execute("SELECT 1")
print(f"✅ Connected to MotherDuck: md:{DB_NAME}\n")

con.execute("CREATE SCHEMA IF NOT EXISTS banking_docs")
print("✅ Schema banking_docs\n")

# ── documents ─────────────────────────────────────────────────────────────────
# DuckDB equivalent of ReplacingMergeTree — PRIMARY KEY enforces uniqueness,
# INSERT OR REPLACE handles re-ingestion without duplicates.
con.execute("""
CREATE TABLE IF NOT EXISTS banking_docs.documents (
    doc_id            VARCHAR PRIMARY KEY,
    doc_type          VARCHAR,
    s3_path           VARCHAR,

    customer_id       VARCHAR,
    customer_name     VARCHAR,
    customer_email    VARCHAR,
    customer_phone    VARCHAR,
    customer_address  VARCHAR,

    account_number    VARCHAR,
    account_type      VARCHAR,
    sort_code         VARCHAR,

    rm_id             VARCHAR,
    rm_name           VARCHAR,
    rm_email          VARCHAR,

    branch_code       VARCHAR,
    branch_name       VARCHAR,

    statement_date    DATE,
    closing_balance   DOUBLE,

    case_status       VARCHAR,
    filed_date        DATE,
    closed_date       DATE,
    resolution        VARCHAR,

    dispute_type      VARCHAR,
    dispute_amount    DOUBLE,

    complaint_type    VARCHAR,
    priority          VARCHAR,
    compensation_paid DOUBLE,

    request_type      VARCHAR,
    request_status    VARCHAR,
    request_date      DATE,
    processed_date    DATE,

    case_summary      VARCHAR,
    ingested_at       TIMESTAMP DEFAULT current_timestamp
)
""")
print("✅ banking_docs.documents")

# Views — no FINAL needed (standard table has no duplicates)
for name, doc_type in [
    ("disputes",   "Dispute"),
    ("complaints", "Complaint"),
    ("estatements","eStatement"),
    ("maintenance","AccountMaintenance"),
]:
    con.execute(f"""
        CREATE OR REPLACE VIEW banking_docs.{name} AS
        SELECT * FROM banking_docs.documents WHERE doc_type = '{doc_type}'
    """)
print("✅ Views: disputes, complaints, estatements, maintenance")

# ── backoffice_requests ────────────────────────────────────────────────────────
con.execute("""
CREATE TABLE IF NOT EXISTS banking_docs.backoffice_requests (
    ticket_id        VARCHAR PRIMARY KEY,
    action           VARCHAR,
    platform         VARCHAR,
    doc_ids          VARCHAR,
    original_query   VARCHAR,
    user_note        VARCHAR,
    delivery_address VARCHAR,
    status           VARCHAR,
    created_at       TIMESTAMP DEFAULT current_timestamp,
    updated_at       TIMESTAMP DEFAULT current_timestamp,
    resolver_name    VARCHAR,
    resolver_note    VARCHAR,
    workflow_run_id  VARCHAR
)
""")
print("✅ banking_docs.backoffice_requests")

# ── content_gaps ──────────────────────────────────────────────────────────────
con.execute("""
CREATE TABLE IF NOT EXISTS banking_docs.content_gaps (
    gap_id           VARCHAR PRIMARY KEY,
    original_query   VARCHAR,
    ai_response      VARCHAR,
    user_feedback    VARCHAR,
    status           VARCHAR,
    created_at       TIMESTAMP DEFAULT current_timestamp,
    updated_at       TIMESTAMP DEFAULT current_timestamp,
    resolver_name    VARCHAR,
    fix_description  VARCHAR
)
""")
print("✅ banking_docs.content_gaps")

# ── verify ────────────────────────────────────────────────────────────────────
tables = con.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'banking_docs' AND table_type = 'BASE TABLE'
    ORDER BY table_name
""").fetchall()
table_names = [r[0] for r in tables]
print(f"\nTables: {table_names}")
assert "documents"            in table_names
assert "backoffice_requests"  in table_names
assert "content_gaps"         in table_names

print("""
🎉 MotherDuck schema ready.

Next steps:
  1. Add to .env:
       MOTHERDUCK_TOKEN=<your token>
       MOTHERDUCK_DB=askmybank
       ANALYTICS_DB_BACKEND=motherduck

  2. Export ClickHouse data and import (see export_clickhouse.py)

  3. Add to Lambda env vars in template.yaml:
       MOTHERDUCK_TOKEN, MOTHERDUCK_DB, ANALYTICS_DB_BACKEND
""")
