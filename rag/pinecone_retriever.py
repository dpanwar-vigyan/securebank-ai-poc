"""
Pinecone retriever for SecureBank AI.
Drop-in replacement for the ChromaDB retrieve() path.
Only activated when PINECONE_API_KEY is set in environment.
"""

import os
import rag.config  # noqa: F401

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
INDEX_NAME       = "banking-docs"
TOP_K            = 20


def _convert_filters(filters: dict) -> dict | None:
    """Convert ChromaDB-style filters to Pinecone filter syntax.
    ChromaDB: {"doc_type": {"$eq": "Dispute"}}
    Pinecone:  {"doc_type": {"$eq": "Dispute"}}  ← identical, nothing to change
    Multi:     {"$and": [...]}                    ← identical
    """
    if not filters:
        return None
    if len(filters) == 1:
        key, val = list(filters.items())[0]
        return {key: {"$eq": val}}
    return {"$and": [{k: {"$eq": v}} for k, v in filters.items()]}


class PineconeRetriever:
    """Wraps a Pinecone index with the same retrieve() interface as BankingRAG."""

    def __init__(self, embed_fn):
        if not PINECONE_API_KEY:
            raise PineconeUnavailableError("PINECONE_API_KEY not set")
        from pinecone import Pinecone
        pc = Pinecone(api_key=PINECONE_API_KEY)
        self.index  = pc.Index(INDEX_NAME)
        self.embed  = embed_fn
        stats = self.index.describe_index_stats()
        self.vector_count = stats["total_vector_count"]
        print(f"Pinecone connected — {self.vector_count} vectors in '{INDEX_NAME}'")

    def retrieve(self, query: str, filters: dict) -> list[dict]:
        """Return top-K chunks matching query + filters, same shape as ChromaDB path."""
        embedding    = self.embed([query])[0]
        pine_filter  = _convert_filters(filters)

        try:
            resp = self.index.query(
                vector=embedding,
                top_k=TOP_K,
                filter=pine_filter,
                include_metadata=True,
            )
        except Exception:
            # Retry without filters if filter fails (e.g. field doesn't exist)
            resp = self.index.query(
                vector=embedding,
                top_k=TOP_K,
                include_metadata=True,
            )

        chunks = []
        for match in resp.get("matches", []):
            meta      = dict(match.get("metadata", {}))
            chunk_text = meta.pop("chunk_text", "")   # stored during migration
            score      = round(match.get("score", 0), 3)
            chunks.append({"text": chunk_text, "meta": meta, "score": score})
        return chunks

    def is_available(self) -> bool:
        try:
            self.index.describe_index_stats()
            return True
        except Exception:
            return False


class PineconeUnavailableError(Exception):
    pass
