"""
ClickHouse NL→SQL Client
Converts natural language aggregation queries to SQL using Nova Lite,
executes them against ClickHouse Cloud, and returns structured results.
"""

import json
import os
import re
import time

import boto3
import clickhouse_connect
import rag.config  # noqa: F401 — loads .env + st.secrets into os.environ

# Module-level SQL cache — persists for the lifetime of the Streamlit process
# Key: normalised question string  Value: generated SQL
_SQL_CACHE: dict[str, str] = {}

# ---------------------------------------------------------------------------
# Config  (os.environ already populated by rag.config on import)
# ---------------------------------------------------------------------------
CH_HOST     = os.getenv("CLICKHOUSE_HOST")
CH_USER     = os.getenv("CLICKHOUSE_USER")
CH_PASS     = os.getenv("CLICKHOUSE_PASSWORD")
LLM_MODEL   = os.getenv("LLM_MODEL", "us.amazon.nova-lite-v1:0")
AWS_REGION  = os.getenv("BEDROCK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# Schema context injected into every NL→SQL prompt
# ---------------------------------------------------------------------------
SCHEMA = """
ClickHouse table: banking_docs.documents
One row per document. All 1,000 banking documents are here.

Columns:
  doc_id          String        -- e.g. CMP00047, DSP00012, STMT00001, MNT00003
  doc_type        String        -- eStatement | Dispute | Complaint | AccountMaintenance
  s3_path         String        -- S3 link to the PDF

  customer_id     String        -- e.g. CUST00012
  customer_name   String
  account_number  String
  account_type    String        -- Current | Savings | Business Current | Premier | ISA
  sort_code       String

  branch_code     String        -- e.g. BR001
  branch_name     String        -- London City | Manchester Central | Birmingham |
                                --   Edinburgh | Leeds | Bristol | Cardiff |
                                --   Glasgow | Liverpool | Sheffield

  rm_id           String        -- e.g. RM001
  rm_name         String        -- Relationship Manager full name
  rm_email        String

  -- Case fields (Disputes + Complaints)
  case_status     String        -- Closed-Won | Closed-Lost | Withdrawn | Referred to Ombudsman
  filed_date      Nullable(Date)
  closed_date     Nullable(Date)
  resolution      String        -- Resolved in customer favour | Resolved in bank favour |
                                --   Partial refund issued | Escalated to Ombudsman |
                                --   Withdrawn by customer | No further action

  -- Dispute-specific
  dispute_type    String        -- Unauthorised Transaction | Merchant Dispute | ATM Withdrawal Error |
                                --   Duplicate Charge | Card Not Present Fraud | Direct Debit Dispute |
                                --   Wire Transfer Error | Currency Conversion Dispute
  dispute_amount  Nullable(Float64)   -- USD amount

  -- Complaint-specific
  complaint_type  String        -- Poor Customer Service | Branch Service Complaint |
                                --   Online Banking Issue | Mortgage Related | Loan Processing Delay |
                                --   Fee Dispute | Account Closure Complaint | Staff Conduct |
                                --   Product Mis-selling
  priority        String        -- Critical | High | Medium | Low
  compensation_paid Nullable(Float64) -- USD amount

  -- Account Maintenance
  request_type    String        -- Address Change | Contact Number Update | Signature Update |
                                --   Overdraft Limit Change | Account Name Change | Beneficiary Addition |
                                --   Standing Order Amendment | Direct Debit Cancellation
  request_status  String        -- Completed | Pending | Rejected
  request_date    Nullable(Date)
  processed_date  Nullable(Date)

  -- eStatement
  statement_date  Nullable(Date)
  closing_balance Nullable(Float64)   -- USD amount

  case_summary    String        -- short plain-text summary
  ingested_at     DateTime
"""

NL_TO_SQL_PROMPT = f"""You are a ClickHouse SQL expert for SecureBank PLC's banking document database.
Generate a single valid ClickHouse SQL SELECT query based on the user's question.

{SCHEMA}

Important date field usage:
- For Disputes and Complaints: use filed_date for when the case was raised
- For eStatements: use statement_date
- For AccountMaintenance: use request_date
- NEVER use ingested_at for business date queries — it is a system timestamp only
- When user says "each year" or "per year" for cases: GROUP BY toYear(filed_date)
- When user says "each year" for statements: GROUP BY toYear(statement_date)

Rules:
- Use ClickHouse SQL syntax (not MySQL/PostgreSQL)
- For year extraction use: toYear(date_column)
- For month: formatDateTime(date_column, '%Y-%m')
- Always ORDER BY for aggregations (highest count first unless user specifies)
- LIMIT 50 unless user asks for all
- Never use UPDATE, DELETE, INSERT, DROP, ALTER, CREATE
- Only query banking_docs.documents
- Return ONLY the raw SQL query — no explanation, no markdown fences, no comments
"""

ANSWER_PROMPT = """You are a banking data analyst assistant for SecureBank PLC.
You have been given SQL query results from the banking document database.
Present the results clearly and professionally using markdown tables or bullet points.
Add brief insights where relevant (e.g. peak year, top branch, trends).
Keep it concise and banker-friendly.
All monetary amounts are in USD — format as $X,XXX.XX where shown.
"""


# ---------------------------------------------------------------------------
# Bedrock LLM (reuse pattern from chain.py)
# ---------------------------------------------------------------------------
class _LLM:
    def __init__(self):
        self.client = boto3.client("bedrock-runtime", region_name=AWS_REGION)

    def invoke(self, system: str, user: str, max_tokens: int = 512) -> str:
        body = json.dumps({
            "messages": [{"role": "user", "content": [{"text": f"{system}\n\n{user}"}]}],
            "inferenceConfig": {"maxTokens": max_tokens, "temperature": 0.0},
        })
        resp = self.client.invoke_model(modelId=LLM_MODEL, body=body, contentType="application/json")
        return json.loads(resp["body"].read())["output"]["message"]["content"][0]["text"].strip()


# ---------------------------------------------------------------------------
# ClickHouse Client
# ---------------------------------------------------------------------------
class ClickHouseNLClient:
    def __init__(self):
        self.llm       = _LLM()
        self.ch        = None
        self.available = False
        self._connect()

    def _connect(self):
        """Attempt to connect to ClickHouse — sets self.available = False on any failure."""
        if not all([CH_HOST, CH_USER, CH_PASS]):
            print("ClickHouse: credentials missing — aggregation will use ChromaDB fallback")
            return
        try:
            self.ch = clickhouse_connect.get_client(
                host=CH_HOST, user=CH_USER, password=CH_PASS,
                secure=True, connect_timeout=8, send_receive_timeout=30,
            )
            self.ch.ping()          # verify connection is live
            self.available = True
            print("ClickHouse connected —", CH_HOST)
        except Exception as exc:
            print(f"ClickHouse unavailable ({exc}) — aggregation will use ChromaDB fallback")

    def _generate_sql(self, question: str) -> tuple[str, bool]:
        """Use Nova Lite to convert NL question → ClickHouse SQL.
        Returns (sql, cache_hit) — cache_hit=True means Bedrock was NOT called."""
        cache_key = re.sub(r"\s+", " ", question.lower().strip())
        if cache_key in _SQL_CACHE:
            print(f"SQL cache hit: {cache_key[:60]}")
            return _SQL_CACHE[cache_key], True

        sql = self.llm.invoke(
            system=NL_TO_SQL_PROMPT,
            user=f"Question: {question}\n\nSQL query:",
            max_tokens=400,
        )
        # Strip any accidental markdown fences
        sql = re.sub(r"```sql|```", "", sql).strip()
        # Safety: reject any mutating statements
        if re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE)\b", sql, re.IGNORECASE):
            raise ValueError(f"Unsafe SQL generated: {sql}")

        _SQL_CACHE[cache_key] = sql
        return sql, False

    def _format_results(self, question: str, sql: str, rows: list, col_names: list) -> str:
        """Use Nova Lite to format raw results into a readable answer."""
        if not rows:
            return "No results found for this query."

        # Build a compact text table for the LLM
        header = " | ".join(col_names)
        divider = "-" * len(header)
        body = "\n".join(" | ".join(str(v) for v in row) for row in rows[:30])
        result_text = f"{header}\n{divider}\n{body}"
        if len(rows) > 30:
            result_text += f"\n... ({len(rows) - 30} more rows)"

        return self.llm.invoke(
            system=ANSWER_PROMPT,
            user=f"Question: {question}\n\nSQL used:\n{sql}\n\nResults:\n{result_text}\n\nPresent this clearly:",
            max_tokens=600,
        )

    def ask(self, question: str) -> dict:
        """
        Full NL→SQL→Execute→Format pipeline.
        Returns dict with answer, sql, rows, col_names.
        Raises ClickHouseUnavailableError if ClickHouse is not reachable.
        """
        if not self.available:
            raise ClickHouseUnavailableError("ClickHouse is not available")

        try:
            # 1. Generate SQL (cache checked inside)
            sql, cache_hit = self._generate_sql(question)

            # 2. Execute against ClickHouse
            result    = self.ch.query(sql)
            rows      = result.result_set
            col_names = result.column_names

            # 3. Format answer
            answer = self._format_results(question, sql, rows, list(col_names))

            # 4. Build table data for UI
            table_data = [dict(zip(col_names, row)) for row in rows]

            return {
                "answer":          answer,
                "sql":             sql,
                "sql_cached":      cache_hit,
                "table_data":      table_data,
                "col_names":       list(col_names),
                "row_count":       len(rows),
                "query_type":      "clickhouse_nl_sql",
                "sources":         [],
                "filters_applied": {},
            }
        except ClickHouseUnavailableError:
            raise
        except Exception as exc:
            # Connection dropped mid-session — mark unavailable for future calls
            self.available = False
            print(f"ClickHouse query failed ({exc}) — marking unavailable")
            raise ClickHouseUnavailableError(str(exc)) from exc


class ClickHouseUnavailableError(Exception):
    """Raised when ClickHouse is unreachable or the free tier has expired."""
