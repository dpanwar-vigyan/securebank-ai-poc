"""
RAG Chain — ChromaDB/S3/Pinecone + ClickHouse + Bedrock Nova Lite

Query routing:
  1. aggregation  → ClickHouse NL→SQL  (counts, trends, breakdowns across ALL docs)
  2. content_qa   → vector RAG (what did customer say, summarise case)
  3. lookup       → filtered RAG (show complaints from Leeds)

Vector backend (auto-selected by env vars — priority order):
  1. S3_VECTORS_BUCKET set  → S3 + NumPy  (Lambda Phase 2 — no external accounts)
  2. PINECONE_API_KEY set   → Pinecone    (alternative serverless option)
  3. neither                → ChromaDB    (local default — Streamlit Phase 1, unchanged)
"""

import json
import os
import re
import time
from collections import defaultdict
from pathlib import Path

import boto3
import rag.config  # noqa: F401 — loads .env + st.secrets into os.environ

# ChromaDB is optional — not available in Lambda (uses S3+NumPy instead)
try:
    import chromadb
    from chromadb.utils.embedding_functions import EmbeddingFunction
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False
    EmbeddingFunction = object   # placeholder so class def below still works
    print("ChromaDB not available — S3+NumPy backend required")
from rag.clickhouse_client import ClickHouseNLClient, ClickHouseUnavailableError
from rag.pinecone_retriever import PineconeRetriever, PineconeUnavailableError
from rag.s3_retriever import S3NumpyRetriever, S3RetrieverUnavailableError

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
    # amount / value queries
    "high-value", "high value", "largest amount", "biggest amount",
    "highest amount", "largest dispute", "biggest dispute",
    "show me transaction", "transaction pattern", "dispute amount",
    "compensation amount", "closing balance", "account balance",
    # show me / list queries that need ClickHouse
    "show me all", "list all", "give me all", "show all",
    "show me the top", "show me the highest", "show me the largest",
    # which / what queries implying full-scan
    "which cases", "which customers", "which accounts",
    "what is the total", "what are the top", "what are the most",
]


# ---------------------------------------------------------------------------
# Module-level caches — persist for the lifetime of the Lambda container.
# On Streamlit they persist for the session lifetime.
# Keys normalised to lowercase+stripped so minor phrasing differences still hit.
# ---------------------------------------------------------------------------
_ANSWER_CACHE: dict[str, dict] = {}     # full RAG response  (saves ~3-4s per repeat)
_EMBED_CACHE:  dict[str, list] = {}     # Titan embeddings   (saves ~500ms per repeat)

ANSWER_CACHE_MAX = 200   # evict oldest when cache grows beyond this
EMBED_CACHE_MAX  = 500

def _cache_key(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().strip())

def _evict_if_full(cache: dict, max_size: int) -> None:
    """Remove oldest 20% of entries when cache is full."""
    if len(cache) >= max_size:
        evict_n = max(1, max_size // 5)
        for k in list(cache.keys())[:evict_n]:
            del cache[k]


# ---------------------------------------------------------------------------
# Bedrock clients
# ---------------------------------------------------------------------------
class BedrockEmbeddings(EmbeddingFunction):
    def __init__(self):
        self.client = boto3.client("bedrock-runtime", region_name=AWS_REGION)

    def __call__(self, texts: list[str]) -> list[list[float]]:
        embeddings = []
        for text in texts:
            key = _cache_key(text)
            if key in _EMBED_CACHE:
                embeddings.append(_EMBED_CACHE[key])
                continue
            body = json.dumps({"inputText": text[:8000], "dimensions": 256, "normalize": True})
            resp = self.client.invoke_model(modelId=EMBED_MODEL, body=body, contentType="application/json")
            emb = json.loads(resp["body"].read())["embedding"]
            _evict_if_full(_EMBED_CACHE, EMBED_CACHE_MAX)
            _EMBED_CACHE[key] = emb
            embeddings.append(emb)
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
cases, and account history.

The document repository contains ONLY:
  • Dispute cases (DSP xxxxx) — unauthorised transactions, merchant disputes, ATM errors, fraud
  • Complaint cases (CMP xxxxx) — service complaints, mortgage-related complaints, fee disputes, staff conduct
  • eStatements (STMT xxxxx) — account statements with closing balances
  • Account Maintenance requests (MNT xxxxx) — address changes, overdraft limits, beneficiary additions

Rules:
- Only answer based on the provided context. Never invent or assume information.
- Always cite the source document ID (e.g. CMP00047, DSP00012) when answering about specific cases.
- If the context does not contain the answer, say so clearly and explain what types of documents ARE available — do NOT suggest contacting external departments.
- If the query is about policy, eligibility criteria, fees, or product terms (not in the case files), say: "This system contains case files and statements, not product policy documents. Try asking about specific cases, disputes, complaints, or account analytics instead."
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


# ── HITL content-type detection ───────────────────────────────────────────────
_DISPUTE_KW   = ["dispute", "dsp", "unauthorised", "unauthorized", "fraud",
                 "chargeback", "merchant dispute", "atm error", "wire transfer error"]
_COMPLAINT_KW = ["complaint", "cmp", "complain", "mis-selling", "misselling",
                 "poor service", "staff conduct", "fee dispute", "mortgage complaint"]
_STATEMENT_KW = ["statement", "stmt", "estatement", "e-statement",
                 "closing balance", "bank statement", "monthly statement"]
_LEGAL_KW     = ["legal", "regulator", "regulatory", "bulk export", "fca", "pra",
                 "ombudsman bulk", "legal team", "bulk request", "legal pack"]

def detect_content_type(query: str, sources: list | None = None) -> str:
    """
    Classify the query/response into a HITL content type.
    Drives which action buttons appear in the chat UI.

    Returns: "dispute" | "complaint" | "estatement" | "legal_bulk" | "general"
    """
    q = query.lower()

    if any(kw in q for kw in _DISPUTE_KW):
        return "dispute"
    if any(kw in q for kw in _COMPLAINT_KW):
        return "complaint"
    if any(kw in q for kw in _STATEMENT_KW):
        return "estatement"
    if any(kw in q for kw in _LEGAL_KW):
        return "legal_bulk"

    # Infer from RAG source doc_types
    if sources:
        types = {(s.get("doc_type") or "").lower() for s in sources}
        if any("dispute" in t for t in types):
            return "dispute"
        if any("complaint" in t for t in types):
            return "complaint"
        if any("statement" in t for t in types):
            return "estatement"

    return "general"


def get_hitl_options(content_type: str, sources: list | None = None) -> list[dict]:
    """
    Return the list of HITL actions available for a given content type.

    Reopen Case is ONLY offered when there is exactly ONE source document
    (i.e. the banker is looking at a specific case, not a list).
    The case ID is embedded in the label so it's unambiguous in the UI.
    """
    catch_all = {
        "id":       "escalate_to_it",
        "label":    "Not what I need — Escalate",
        "icon":     "🚨",
        "desc":     "Flag this response for IT/Content team review",
        "platform": "temporal",
    }

    if content_type in ("dispute", "complaint"):
        # Reopen Case only when exactly ONE specific case is in context
        single_doc = sources and len(sources) == 1
        case_id    = sources[0].get("doc_id", "") if single_doc else ""
        if single_doc and case_id:
            return [
                {
                    "id":         "reopen_case",
                    "label":      f"Reopen {case_id}",
                    "icon":       "🔄",
                    "desc":       f"Raise a back-office request to reopen closed case {case_id}",
                    "platform":   "temporal",
                    "needs_note": True,
                    "case_id":    case_id,
                },
                catch_all,
            ]
        # Multiple cases returned → catch-all only (ambiguous which to reopen)
        return [catch_all]

    if content_type == "estatement":
        return [
            {
                "id":            "send_duplicate",
                "label":         "Send Duplicate Copy",
                "icon":          "📨",
                "desc":          "Request a duplicate statement sent to an authorised address",
                "platform":      "orkes",
                "needs_address": True,
            },
            catch_all,
        ]

    if content_type == "legal_bulk":
        return [
            {
                "id":         "legal_export",
                "label":      "Export to Legal S3",
                "icon":       "📦",
                "desc":       "Package documents + AI summary letter → legal secure S3 bucket",
                "platform":   "orkes",
                "needs_note": True,
            },
            catch_all,
        ]

    # general → catch-all only
    return [catch_all]


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
        self.embed = BedrockEmbeddings()
        self.llm   = BedrockLLM()

        # ── Vector backend — priority: S3 → Pinecone → ChromaDB ──────────────
        self.s3_retriever  = None
        self.pinecone      = None

        if os.getenv("S3_VECTORS_BUCKET"):
            try:
                self.s3_retriever = S3NumpyRetriever(embed_fn=self.embed)
                print(f"Vector backend: S3+NumPy ✅  ({self.s3_retriever.vector_count} vectors)")
            except Exception as exc:
                print(f"S3 retriever init failed ({exc}) — trying Pinecone/ChromaDB")

        if not self.s3_retriever and os.getenv("PINECONE_API_KEY"):
            try:
                self.pinecone = PineconeRetriever(embed_fn=self.embed)
                print("Vector backend: Pinecone ✅")
            except Exception as exc:
                print(f"Pinecone init failed ({exc}) — falling back to ChromaDB")

        # ChromaDB: only init if available (Streamlit) — not present in Lambda
        if CHROMADB_AVAILABLE:
            self.chroma = chromadb.PersistentClient(path=CHROMA_PATH)
            self.col    = self.chroma.get_or_create_collection(
                name=COLLECTION,
                embedding_function=self.embed,
                metadata={"hnsw:space": "cosine"},
            )
            chroma_info = f"ChromaDB chunks: {self.col.count()}"
        else:
            self.chroma = None
            self.col    = None
            chroma_info = "ChromaDB: not available (Lambda mode)"

        if   self.s3_retriever: backend = "S3+NumPy"
        elif self.pinecone:     backend = "Pinecone"
        elif self.col:          backend = "ChromaDB"
        else:                   backend = "NONE — check config"
        print(f"Vector backend active: {backend} | {chroma_info}")

        # ── ClickHouse — lazy init (don't connect at startup, too slow cross-region)
        # First aggregation query will trigger _get_ch() which connects on demand.
        self.ch = None
        self._ch_init_attempted = False

    def _get_ch(self):
        """Lazy ClickHouse init — connects on first aggregation query, not at startup."""
        if not self._ch_init_attempted:
            self._ch_init_attempted = True
            try:
                self.ch = ClickHouseNLClient()
            except Exception as exc:
                print(f"ClickHouse init failed ({exc})")
                self.ch = None
        return self.ch

    def _build_where(self, filters: dict):
        if not filters:
            return None
        if len(filters) == 1:
            key, val = list(filters.items())[0]
            return {key: {"$eq": val}}
        return {"$and": [{k: {"$eq": v}} for k, v in filters.items()]}

    def retrieve(self, query: str, filters: dict) -> list[dict]:
        # ── S3+NumPy (Phase 2 Lambda backend) ─────────────────────────────────
        if self.s3_retriever:
            try:
                chunks = self.s3_retriever.retrieve(query, filters)
                if chunks:
                    return chunks
            except Exception as exc:
                print(f"S3 retriever failed ({exc}) — falling back to ChromaDB")

        # ── Pinecone (optional alternative) ───────────────────────────────────
        if self.pinecone:
            try:
                chunks = self.pinecone.retrieve(query, filters)
                if chunks:
                    return chunks
            except Exception as exc:
                print(f"Pinecone retrieve failed ({exc}) — falling back to ChromaDB")

        # ── ChromaDB fallback (Streamlit only — not available in Lambda) ────────
        if not self.col:
            return []   # Lambda with no S3/Pinecone configured — return empty
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
        # Lazy-connect to ClickHouse on first aggregation query
        ch = self._get_ch()
        if ch and ch.available:
            try:
                result = ch.ask(query)
                ct = detect_content_type(query)
                result["content_type"]  = ct
                result["hitl_options"]  = get_hitl_options(ct)
                return result
            except ClickHouseUnavailableError:
                pass   # fall through to ChromaDB

        # ── ChromaDB fallback (Streamlit only) ──────────────────────────────────
        if not self.col:
            ct = detect_content_type(query)
        return {
                "answer": "⚠️ Aggregation queries require ClickHouse (currently unavailable). Please try again shortly.",
                "sources": [], "filters_applied": filters, "query_type": "content",
                "content_type": ct, "hitl_options": get_hitl_options(ct),
            }

        agg    = run_aggregation(self.col, query, filters)
        prompt = format_aggregation_for_llm(agg, query)
        answer = self.llm.invoke(system=AGGREGATION_PROMPT, user=prompt)

        note = (
            "\n\n> ⚠️ **Note:** Live ClickHouse analytics are temporarily unavailable. "
            "Results are based on the indexed document metadata."
        )

        ct = detect_content_type(query)
        return {
            "answer":          answer + note,
            "sources":         [],
            "filters_applied": filters,
            "query_type":      "content",   # renders without SQL panel
            "agg_data":        agg,
            "content_type":    ct,
            "hitl_options":    get_hitl_options(ct),
        }

    # ── Content RAG path ────────────────────────────────────────────────────
    def ask_content(self, query: str, filters: dict) -> dict:
        chunks = self.retrieve(query, filters)

        if not chunks:
            ct = detect_content_type(query)
            return {
                "answer": (
                    "No matching documents found for that query.\n\n"
                    "This system indexes **case files and statements only** — not product policy or eligibility guides. "
                    "Try asking about:\n"
                    "- Specific cases (e.g. *Tell me about CMP00047*)\n"
                    "- Dispute or complaint patterns (e.g. *How many disputes by branch?*)\n"
                    "- Account statements (e.g. *Show me statements for CUST00012*)\n"
                    "- Analytics (e.g. *Total compensation paid by year*)"
                ),
                "sources":         [],
                "filters_applied": filters,
                "query_type":      "content",
                "content_type":    ct,
                "hitl_options":    get_hitl_options(ct),
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

        ct = detect_content_type(query, sources)
        return {
            "answer":          answer,
            "sources":         sources,
            "filters_applied": filters,
            "query_type":      "content",
            "content_type":    ct,
            "hitl_options":    get_hitl_options(ct, sources),
        }

    # ── Main entry point ────────────────────────────────────────────────────
    def ask(self, query: str) -> dict:
        key = _cache_key(query)

        # ── Answer cache hit — return immediately, no Bedrock calls ──────────
        if key in _ANSWER_CACHE:
            cached = dict(_ANSWER_CACHE[key])
            cached["cached"] = True
            print(f"Answer cache hit: {key[:60]}")
            return cached

        filters = extract_filters(query)

        if is_aggregation_query(query):
            result = self.ask_aggregation(query, filters)
        else:
            result = self.ask_content(query, filters)

        # ── Store in answer cache (only cache successful, non-empty answers) ──
        # Never cache "no information found" responses — the document may be
        # ingested moments later and the next query would get the wrong answer.
        answer_text = result.get("answer", "")
        is_no_info = not result.get("sources") and (
            "does not contain" in answer_text.lower() or
            "no matching" in answer_text.lower() or
            "not contain any information" in answer_text.lower()
        )
        if not is_no_info:
            _evict_if_full(_ANSWER_CACHE, ANSWER_CACHE_MAX)
            _ANSWER_CACHE[key] = result
        return result
