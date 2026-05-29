#!/usr/bin/env python3
"""
Export all ClickHouse tables to CSV, then import into MotherDuck.

Run TODAY before the ClickHouse trial expires.

Usage:
    python export_clickhouse.py              # export only
    python export_clickhouse.py --import    # export + import into MotherDuck
"""

import csv
import os
import sys
from pathlib import Path
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except Exception:
    pass

import clickhouse_connect

CH_HOST = os.getenv("CLICKHOUSE_HOST")
CH_USER = os.getenv("CLICKHOUSE_USER")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD")

print("Connecting to ClickHouse...")
ch = clickhouse_connect.get_client(
    host=CH_HOST, user=CH_USER, password=CH_PASS,
    secure=True, connect_timeout=10, send_receive_timeout=60,
)
ch.ping()
print(f"✅ Connected: {CH_HOST}\n")

EXPORT_DIR = Path("clickhouse_export")
EXPORT_DIR.mkdir(exist_ok=True)

TABLES = [
    ("banking_docs.documents",           "SELECT * FROM banking_docs.documents FINAL"),
    ("banking_docs.backoffice_requests", "SELECT * FROM banking_docs.backoffice_requests"),
    ("banking_docs.content_gaps",        "SELECT * FROM banking_docs.content_gaps"),
]

exported = {}
for table_name, sql in TABLES:
    print(f"Exporting {table_name}...")
    result    = ch.query(sql)
    rows      = result.result_rows
    col_names = list(result.column_names)
    fname     = EXPORT_DIR / f"{table_name.replace('.', '_')}.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(col_names)
        writer.writerows(rows)
    exported[table_name] = (fname, col_names, len(rows))
    print(f"  → {fname} ({len(rows)} rows, {fname.stat().st_size} bytes)")

print(f"\n✅ Export complete → {EXPORT_DIR}/")
for t, (f, _, n) in exported.items():
    print(f"   {t}: {n} rows")

# ── Optional: import into MotherDuck ─────────────────────────────────────────
if "--import" not in sys.argv:
    print("\nTo import into MotherDuck, run:  python export_clickhouse.py --import")
    sys.exit(0)

TOKEN   = os.getenv("MOTHERDUCK_TOKEN", "")
DB_NAME = os.getenv("MOTHERDUCK_DB", "askmybank")
if not TOKEN:
    print("ERROR: MOTHERDUCK_TOKEN not set — cannot import")
    sys.exit(1)

import duckdb
print(f"\nConnecting to MotherDuck: md:{DB_NAME}...")
con = duckdb.connect(f"md:{DB_NAME}?motherduck_token={TOKEN}")
con.execute("SELECT 1")
print("✅ Connected\n")

for table_name, (fname, col_names, row_count) in exported.items():
    short = table_name.split(".")[-1]
    full  = f"banking_docs.{short}"
    print(f"Importing {full} from {fname}...")
    # DuckDB can read CSV directly — fast bulk load
    con.execute(f"""
        INSERT OR REPLACE INTO {full}
        SELECT * FROM read_csv_auto('{fname}', header=true, timestampformat='%Y-%m-%d %H:%M:%S')
    """)
    count = con.execute(f"SELECT COUNT(*) FROM {full}").fetchone()[0]
    print(f"  → {full}: {count} rows loaded")

print("\n🎉 Import complete. Your data is now in MotherDuck.")
print("Add ANALYTICS_DB_BACKEND=motherduck to .env to use it.")
