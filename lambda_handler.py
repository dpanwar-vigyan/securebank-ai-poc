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
    """Health check — also used by EventBridge warming rule."""
    return {
        "status": "ok",
        "vector_backend": "s3_numpy" if os.getenv("S3_VECTORS_BUCKET") else "chromadb",
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
handler = Mangum(app, lifespan="off")
