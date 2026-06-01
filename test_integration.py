"""
MotherDuck integration test — covers all three adapter-using components.
Run with: python3 test_integration.py

Components tested:
  1. Adapter layer        — get_adapter() → DuckDBAdapter → MotherDuck connection
  2. Pipeline write path  — step_store_metadata() writes a doc to banking_docs.documents
  3. HITL client          — HITLClient.create_ticket() writes a backoffice request
  4. NL→SQL analytics     — ClickHouseNLClient.query() runs NL→SQL against MotherDuck
"""

import os
from dotenv import load_dotenv
load_dotenv()
os.environ["ANALYTICS_DB_BACKEND"] = "motherduck"   # force MD regardless of .env

PASS = "✅"
FAIL = "❌"

results = []

def check(label, fn):
    try:
        result = fn()
        print(f"  {PASS} {label}: {result}")
        results.append((label, True, None))
    except Exception as e:
        print(f"  {FAIL} {label}: {e}")
        results.append((label, False, str(e)))


# ── 1. Adapter layer ──────────────────────────────────────────────────────────
print("\n── 1. Adapter layer ─────────────────────────────────────────")
from rag.db_adapters import get_adapter

db = get_adapter()
check("ping",        lambda: db.ping())
check("doc count",   lambda: db.query("SELECT COUNT(*) FROM banking_docs.documents").result_rows[0][0])
check("sample docs", lambda: [r[1] for r in db.query(
    "SELECT doc_id, doc_type FROM banking_docs.documents LIMIT 3").result_rows])


# ── 2. Pipeline write path (step_store_metadata) ──────────────────────────────
print("\n── 2. Pipeline: step_store_metadata ─────────────────────────")
from rag.pipeline_steps import step_store_metadata

TEST_DOC_ID = "TEST-MD-INTEGRATION-001"

def _pipeline_write():
    result = step_store_metadata(
        doc_id   = TEST_DOC_ID,
        metadata = {
            "doc_type":    "Complaint",
            "customer_id": "CUST-TEST-001",
            "status":      "processed",
        },
        s3_key = "raw-docs/test-integration.pdf",
    )
    if result.get("rows_inserted") != 1:
        raise ValueError(f"expected rows_inserted=1, got: {result}")
    return f"rows_inserted={result['rows_inserted']}"

check("write test doc", _pipeline_write)

def _pipeline_verify():
    row = db.query(
        "SELECT doc_type FROM banking_docs.documents WHERE doc_id = {doc_id:String}",
        parameters={"doc_id": TEST_DOC_ID},
    ).result_rows
    if not row:
        raise ValueError("doc not found in MotherDuck after write")
    return row[0][0]

check("verify doc in MD", _pipeline_verify)


# ── 3. HITL client ────────────────────────────────────────────────────────────
print("\n── 3. HITL client: create_ticket ────────────────────────────")
from rag.hitl_client import HITLClient

def _hitl_write():
    client = HITLClient()
    result = client.create_ticket(
        action           = "test_md_integration",
        doc_ids          = [TEST_DOC_ID],
        original_query   = "integration test — safe to delete",
    )
    if not result.get("ticket_id"):
        raise ValueError(f"no ticket_id in result: {result}")
    return result["ticket_id"]

check("create HITL ticket", _hitl_write)

def _hitl_verify():
    rows = db.query(
        "SELECT COUNT(*) FROM banking_docs.backoffice_requests WHERE doc_ids LIKE {doc_id:String}",
        parameters={"doc_id": f"%{TEST_DOC_ID}%"},
    ).result_rows
    count = rows[0][0]
    if count == 0:
        raise ValueError("ticket not found in MD")
    return f"{count} ticket(s) found"

check("verify ticket in MD", _hitl_verify)


# ── 4. NL→SQL analytics ───────────────────────────────────────────────────────
print("\n── 4. NL→SQL analytics (ClickHouseNLClient) ─────────────────")
from rag.clickhouse_client import ClickHouseNLClient

def _nlsql():
    client = ClickHouseNLClient()
    result = client.ask("How many documents are there by type?")
    answer = result.get("answer", "")
    if not answer or len(answer) < 5:
        raise ValueError(f"empty or too-short answer: {answer!r}")
    return answer[:120] + ("..." if len(answer) > 120 else "")

check("NL→SQL query", _nlsql)


# ── Cleanup ───────────────────────────────────────────────────────────────────
print("\n── Cleanup ──────────────────────────────────────────────────")
def _cleanup():
    db.command(
        "DELETE FROM banking_docs.documents WHERE doc_id = {doc_id:String}",
        parameters={"doc_id": TEST_DOC_ID},
    )
    db.command(
        "DELETE FROM banking_docs.backoffice_requests WHERE doc_ids LIKE {doc_id:String}",
        parameters={"doc_id": f"%{TEST_DOC_ID}%"},
    )
    return "test rows removed"

check("cleanup test data", _cleanup)


# ── Summary ───────────────────────────────────────────────────────────────────
print("\n── Summary ──────────────────────────────────────────────────")
passed = sum(1 for _, ok, _ in results if ok)
failed = sum(1 for _, ok, _ in results if not ok)
print(f"  {passed} passed  {failed} failed  (of {len(results)} checks)\n")
if failed:
    for label, ok, err in results:
        if not ok:
            print(f"  {FAIL} {label}: {err}")
