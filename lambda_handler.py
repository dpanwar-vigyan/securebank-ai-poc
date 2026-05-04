"""
AskMyBank.ai — AWS Lambda entry point
FastAPI app wrapped with Mangum for Lambda + API Gateway HTTP API.

The RAG object is initialised once per Lambda container (module level).
Warm invocations reuse it — no cold start penalty for subsequent calls.
"""

import json
import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel

from rag.chain import BankingRAG

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="AskMyBank API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://askmybank.ai", "https://www.askmybank.ai", "*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── RAG — initialised once, reused across warm invocations ───────────────────
print("Initialising BankingRAG...")
rag = BankingRAG()
print("BankingRAG ready.")


# ── Models ───────────────────────────────────────────────────────────────────
class AskRequest(BaseModel):
    query: str
    source: str = "user"   # "user" | "warming"


# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    """Health check — includes cache stats."""
    from rag.chain import _ANSWER_CACHE, _EMBED_CACHE
    return {
        "status":         "ok",
        "vector_backend": "s3_numpy" if os.getenv("S3_VECTORS_BUCKET") else "chromadb",
        "answer_cache":   len(_ANSWER_CACHE),
        "embed_cache":    len(_EMBED_CACHE),
    }


@app.post("/ask")
def ask(req: AskRequest):
    """Main RAG endpoint — called from the chat UI."""
    if req.source == "warming":
        # EventBridge ping — just confirm Lambda is warm, skip RAG
        return {"status": "warm", "message": "Lambda is warm"}

    if not req.query or not req.query.strip():
        raise HTTPException(status_code=400, detail="query cannot be empty")

    result = rag.ask(req.query.strip())
    return result


@app.get("/")
def root():
    return {"service": "AskMyBank API", "version": "2.0", "docs": "/docs"}


# ── Lambda handler ────────────────────────────────────────────────────────────
# Mangum handles HTTP API Gateway events.
# EventBridge warming pings arrive as raw JSON (not HTTP) — intercept them first.
_mangum = Mangum(app, lifespan="off")

def handler(event, context):
    # EventBridge sends {"source": "warming", "query": ""} directly to Lambda
    # — not through API Gateway, so Mangum can't parse it. Handle here.
    if isinstance(event, dict) and event.get("source") == "warming":
        print("EventBridge warming ping — Lambda is warm")
        return {"statusCode": 200, "body": '{"status":"warm"}'}
    # All other events are HTTP requests via API Gateway → Mangum → FastAPI
    return _mangum(event, context)
