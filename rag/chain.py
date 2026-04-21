"""
RAG Chain — ChromaDB + ClickHouse + Bedrock Nova Lite

Query routing:
  1. aggregation  → ClickHouse NL→SQL  (counts, trends, breakdowns across ALL docs)
  2. content_qa   → ChromaDB vector RAG (what did customer say, summarise case)
  3. lookup       → ChromaDB filtered RAG (show complaints from Leeds)
"""

import json
import os
import re
import time
from collections import defaultdict
from pathlib import Path

import boto3
import chromadb
from chromadb.utils.embedding_functions import EmbeddingFunction
import rag.config  # noqa: F401 — loads .env + st.secrets into os.environ
from rag.clickhouse_client import ClickHouseNLClient, ClickHouseUnavailableError

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CHROMA_PATH  = str(Path(__file__).parent.parent / "chroma_db")
COLLECTION   = "banking_docs"
EMBED_MODEL  = os.getenv("EMBED_MODEL",   "amazon.titan-embed-text-v2:0")
LLM_MODEL    = os.getenv("LLM_MODEL",     "us.amazon.nova-lite-v1:0")
AWS_REGION   = os.getenv("BEDROCK_REGION", "us-east-1")
TOP_K        = 20    # chunks to retrieve for content/lookup queries

# Keywords that signal an aggregation query → route to ClickHouse NL→SQL
AGGREGATION_KEYWORDS = [
    "how many", "count", "total number", "number of", "how often",
    "per year", "by year", "each year", "per month", "by month",
    "per branch", "by branch", "per rm", "by rm", "per manager",
    "breakdown", "statistics", "stats", "trend", "distribution",
    "most common", "least common", "average", "highest", "lowest",
    "rank", "top ", "bottom ", "group by", "summarise all", "summary of all",
    "which branch", "which rm", "which manager", "which relationship",
    "most disputes", "most complaints", "most cases", "most referrals",
    "how much compensation", "total compensation", "total disputed",
    "total amount", "average amount", "across all", "overall",
]


# ---------------------------------------------------------------------------
# Bedrock clients
# ---------------------------------------------------------------------------
class BedrockEmbeddings(EmbeddingFunction):
    def __init__(self):
        self.client = boto3.client("bedrock-runtime", region_name=AWS_REGION)

    def __call__(self, texts: list[str]) -> list[list[float]]:
        embeddings = []
        for text in texts:
            body = json.dumps({"inputText": text[:8000], "dimensions": 256, "normalize": True})
            resp = self.client.invoke_model(modelId=EMBED_MODEL, body=body, contentType="application/json")
            embeddings.append(json.loads(resp["body"].read())["embedding"])
            time.sleep(0.05)
        return embeddings


class BedrockLLM:
    def __init__(self):
        self.client = boto3.client("bedrock-runtime", region_name=AWS_REGION)

    def invoke(self, system: str, user: str, max_tokens: int = 1024) -> str:
        body = json.dumps({
            "messages": [{"role": "user", "content": [{"text": f"{system}\n\n{user}"}]}],
            "inferenceConfig": {"maxTokens": max_tokens, "temperature": 0.1},
        })
        resp = self.client.invoke_model(modelId=LLM_MODEL, body=body, contentType="application/json")
        result = json.loads(resp["body"].read())
        return result["output"]["message"]["content"][0]["text"]


# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are a secure internal banking assistant for SecureBank PLC.
You help relationship managers and bank staff find information about customer documents,
cases, and account history. You have access to eStatements, dispute cases, complaint cases,
and account maintenance requests.

Rules:
- Only answer based on the provided context. Never invent or assume information.
- Always cite the source document ID (e.g. CMP00047, DSP00012) when answering about specific cases.
- For aggregation results, present counts clearly in a structured format.
- Format financial amounts with $ and commas (e.g. $6,200.00).
- Keep answers concise and professional.
- Do not reveal customer PII (email, phone, address) unless explicitly asked.
"""

AGGREGATION_PROMPT = """You are a data analyst assistant for SecureBank PLC.
You have been given pre-computed aggregation results from the banking document database.
Present these results clearly and professionally. Use tables or bullet points.
Add brief insights where obvious (e.g. highest year, trends). Keep it concise.
"""


# ---------------------------------------------------------------------------
# Intent detection
# ---------------------------------------------------------------------------
def is_aggregation_query(query: str) -> bool:
    """Return True if the query needs a full metadata scan rather than vector search."""
    q = query.lower()
    return any(kw in q for kw in AGGREGATION_KEYWORDS)


# ---------------------------------------------------------------------------
# Filter extraction
# ---------------------------------------------------------------------------
def extract_filters(query: str) -> dict:
    filters = {}
    q = query.lower()

    # Doc type
    if any(w in q for w in ["estatement", "statement", "bank statement"]):
        filters["doc_type"] = "eStatement"
    elif "dispute" in q:
        filters["doc_type"] = "Dispute"
    elif "complaint" in q:
        filters["doc_type"] = "Complaint"
    elif any(w in q for w in ["maintenance", "address change", "account change"]):
        filters["doc_type"] = "AccountMaintenance"

    # Priority
    for p in ["critical", "high", "medium", "low"]:
        if p in q:
            filters["priority"] = p.capitalize()
            break

    # Case status
    if "ombudsman" in q:
        filters["case_status"] = "Referred to Ombudsman"
    elif "closed-won" in q or "closed won" in q:
        filters["case_status"] = "Closed-Won"
    elif "closed-lost" in q or "closed lost" in q:
        filters["case_status"] = "Closed-Lost"
    elif "withdrawn" in q:
        filters["case_status"] = "Withdrawn"

    # Branch
    branches = ["london city", "manchester", "birmingham", "edinburgh",
                "leeds", "bristol", "cardiff", "glasgow", "liverpool", "sheffield"]
    for b in branches:
        if b in q:
            filters["branch_name"] = "London City" if b == "london city" else b.title()
            break

    # Specific doc ID
    doc_id_match = re.search(r'\b(CMP|DSP|STMT|MNT)\d{5}\b', query, re.IGNORECASE)
    if doc_id_match:
        filters["doc_id"] = doc_id_match.group(0).upper()

    # Customer ID
    cust_match = re.search(r'\bCUST\d{5}\b', query, re.IGNORECASE)
    if cust_match:
        filters["customer_id"] = cust_match.group(0).upper()

    return filters


# ---------------------------------------------------------------------------
# Aggregation engine — scans ALL metadata in ChromaDB
# ---------------------------------------------------------------------------
def run_aggregation(col, query: str, filters: dict) -> dict:
    """
    Fetch ALL matching document metadata from ChromaDB (no vector search),
    then compute counts/breakdowns in Python.
    Returns a structured result dict.
    """
    q = query.lower()

    # Build where clause for pre-filter
    where = None
    if filters:
        if len(filters) == 1:
            key, val = list(filters.items())[0]
            where = {key: {"$eq": val}}
        else:
            where = {"$and": [{k: {"$eq": v}} for k, v in filters.items()]}

    # Fetch ALL matching records (metadata only — no embedding needed)
    try:
        all_results = col.get(where=where, include=["metadatas"]) if where else col.get(include=["metadatas"])
    except Exception:
        all_results = col.get(include=["metadatas"])

    metas = all_results["metadatas"]

    # Deduplicate by doc_id (we have multiple chunks per doc)
    seen = {}
    for m in metas:
        did = m.get("doc_id", "")
        if did and did not in seen:
            seen[did] = m
    docs = list(seen.values())

    total_docs = len(docs)

    # ── Determine what aggregation to compute ──────────────────────────────

    # Determine grouping dimension
    group_by = None
    if any(kw in q for kw in ["per year", "by year", "each year", "per annum", "annually", "yearly"]):
        group_by = "year"
    elif any(kw in q for kw in ["per month", "by month", "monthly", "each month"]):
        group_by = "month"
    elif any(kw in q for kw in ["per branch", "by branch", "each branch"]):
        group_by = "branch"
    elif any(kw in q for kw in ["per rm", "by rm", "relationship manager", "per manager", "by manager"]):
        group_by = "rm"
    elif any(kw in q for kw in ["per type", "by type", "type of", "complaint type", "dispute type"]):
        group_by = "sub_type"
    elif any(kw in q for kw in ["status", "by status", "per status"]):
        group_by = "status"
    elif any(kw in q for kw in ["priority", "by priority"]):
        group_by = "priority"

    # Determine date field to use
    date_field = "filed_date"
    if filters.get("doc_type") == "eStatement":
        date_field = "statement_date"
    elif filters.get("doc_type") == "AccountMaintenance":
        date_field = "request_date"

    counts = defaultdict(int)

    if group_by == "year":
        for d in docs:
            raw = d.get(date_field, "") or d.get("filed_date", "") or d.get("statement_date", "")
            year = raw[:4] if raw and len(raw) >= 4 else "Unknown"
            counts[year] += 1

    elif group_by == "month":
        for d in docs:
            raw = d.get(date_field, "") or d.get("filed_date", "")
            ym = raw[:7] if raw and len(raw) >= 7 else "Unknown"
            counts[ym] += 1

    elif group_by == "branch":
        for d in docs:
            counts[d.get("branch_name", "Unknown")] += 1

    elif group_by == "rm":
        for d in docs:
            counts[d.get("rm_name", "Unknown")] += 1

    elif group_by == "sub_type":
        for d in docs:
            val = d.get("complaint_type") or d.get("dispute_type") or d.get("request_type") or "Unknown"
            counts[val] += 1

    elif group_by == "status":
        for d in docs:
            counts[d.get("case_status", "Unknown")] += 1

    elif group_by == "priority":
        for d in docs:
            counts[d.get("priority", "Unknown")] += 1

    else:
        # Default: count by doc_type
        for d in docs:
            counts[d.get("doc_type", "Unknown")] += 1

    sorted_counts = dict(sorted(counts.items(), key=lambda x: (-x[1], x[0])))

    return {
        "total":      total_docs,
        "group_by":   group_by or "doc_type",
        "counts":     sorted_counts,
        "filters":    filters,
        "doc_sample": docs[:3],   # sample for LLM context
    }


def format_aggregation_for_llm(agg: dict, query: str) -> str:
    """Turn aggregation result dict into a readable string for the LLM."""
    lines = [
        f"Query: {query}",
        f"Total matching documents: {agg['total']}",
        f"Applied filters: {agg['filters'] or 'none'}",
        f"Grouped by: {agg['group_by']}",
        "",
        "Results:",
    ]
    for k, v in agg["counts"].items():
        pct = round(v / agg["total"] * 100, 1) if agg["total"] else 0
        lines.append(f"  {k}: {v} ({pct}%)")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# RAG Retriever
# ---------------------------------------------------------------------------
class BankingRAG:
    def __init__(self):
        self.chroma = chromadb.PersistentClient(path=CHROMA_PATH)
        self.embed  = BedrockEmbeddings()
        self.llm    = BedrockLLM()
        self.col    = self.chroma.get_or_create_collection(
            name=COLLECTION,
            embedding_function=self.embed,
            metadata={"hnsw:space": "cosine"},
        )
        try:
            self.ch = ClickHouseNLClient()   # ClickHouse for aggregation queries
        except Exception as exc:
            print(f"ClickHouse init failed ({exc}) — will use ChromaDB fallback for aggregations")
            self.ch = None
        print(f"ChromaDB loaded — {self.col.count()} chunks indexed")

    def _build_where(self, filters: dict):
        if not filters:
            return None
        if len(filters) == 1:
            key, val = list(filters.items())[0]
            return {key: {"$eq": val}}
        return {"$and": [{k: {"$eq": v}} for k, v in filters.items()]}

    def retrieve(self, query: str, filters: dict) -> list[dict]:
        where = self._build_where(filters)
        try:
            results = self.col.query(
                query_texts=[query],
                n_results=min(TOP_K, self.col.count()),
                where=where,
                include=["documents", "metadatas", "distances"],
            )
        except Exception:
            results = self.col.query(
                query_texts=[query],
                n_results=min(TOP_K, self.col.count()),
                include=["documents", "metadatas", "distances"],
            )
        chunks = []
        for doc, meta, dist in zip(
            results["documents"][0],
            results["metadatas"][0],
            results["distances"][0],
        ):
            chunks.append({"text": doc, "meta": meta, "score": round(1 - dist, 3)})
        return chunks

    # ── Aggregation path → ClickHouse NL→SQL (with ChromaDB fallback) ───────
    def ask_aggregation(self, query: str, filters: dict) -> dict:
        # Try ClickHouse first
        if self.ch and self.ch.available:
            try:
                return self.ch.ask(query)
            except ClickHouseUnavailableError:
                pass   # fall through to ChromaDB

        # ── ChromaDB fallback ────────────────────────────────────────────────
        agg    = run_aggregation(self.col, query, filters)
        prompt = format_aggregation_for_llm(agg, query)
        answer = self.llm.invoke(system=AGGREGATION_PROMPT, user=prompt)

        note = (
            "\n\n> ⚠️ **Note:** Live ClickHouse analytics are temporarily unavailable. "
            "Results are based on the indexed document metadata."
        )

        return {
            "answer":          answer + note,
            "sources":         [],
            "filters_applied": filters,
            "query_type":      "content",   # renders without SQL panel
            "agg_data":        agg,
        }

    # ── Content RAG path ────────────────────────────────────────────────────
    def ask_content(self, query: str, filters: dict) -> dict:
        chunks = self.retrieve(query, filters)

        if not chunks:
            return {
                "answer": "I could not find any relevant documents for your query.",
                "sources": [],
                "filters_applied": filters,
                "query_type": "content",
            }

        context_parts = []
        seen_docs = {}
        for c in chunks:
            doc_id = c["meta"].get("doc_id", "unknown")
            if doc_id not in seen_docs:
                seen_docs[doc_id] = c["meta"]
            context_parts.append(
                f"[{doc_id} | {c['meta'].get('doc_type','')} | "
                f"Customer: {c['meta'].get('customer_name','')} | "
                f"Branch: {c['meta'].get('branch_name','')}]\n{c['text']}"
            )

        context = "\n\n---\n\n".join(context_parts)
        user_prompt = f"""Context from banking documents:

{context}

---

Banker's question: {query}

Please answer based only on the context above. Cite document IDs in your response."""

        answer = self.llm.invoke(system=SYSTEM_PROMPT, user=user_prompt)

        sources = []
        for doc_id, meta in seen_docs.items():
            sources.append({
                "doc_id":        doc_id,
                "doc_type":      meta.get("doc_type", ""),
                "customer_name": meta.get("customer_name", ""),
                "branch_name":   meta.get("branch_name", ""),
                "case_summary":  meta.get("case_summary", ""),
                "s3_path":       meta.get("s3_path", ""),
            })

        return {
            "answer":          answer,
            "sources":         sources,
            "filters_applied": filters,
            "query_type":      "content",
        }

    # ── Main entry point ────────────────────────────────────────────────────
    def ask(self, query: str) -> dict:
        filters = extract_filters(query)

        if is_aggregation_query(query):
            return self.ask_aggregation(query, filters)
        else:
            return self.ask_content(query, filters)
