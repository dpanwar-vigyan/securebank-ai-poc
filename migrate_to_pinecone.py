"""
One-time migration: ChromaDB → Pinecone
Reads all vectors + metadata from the local chroma_db and upserts to Pinecone.

Usage:
  1. pip install pinecone
  2. Set PINECONE_API_KEY in .env
  3. python migrate_to_pinecone.py

Safe to re-run — Pinecone upsert is idempotent.
Does NOT touch the live app or ChromaDB.
"""

import os
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

PINECONE_API_KEY  = os.getenv("PINECONE_API_KEY")
INDEX_NAME        = "banking-docs"
CHROMA_PATH       = str(Path(__file__).parent / "chroma_db")
COLLECTION        = "banking_docs"
BATCH_SIZE        = 100   # Pinecone upsert batch limit
VECTOR_DIMENSION  = 256   # Titan Embed v2 with dimensions=256


def main():
    if not PINECONE_API_KEY:
        print("ERROR: PINECONE_API_KEY not set in .env")
        return

    # ── 1. Connect to Pinecone ────────────────────────────────────────────────
    print("Connecting to Pinecone...")
    from pinecone import Pinecone, ServerlessSpec
    pc = Pinecone(api_key=PINECONE_API_KEY)

    # Create index if it doesn't exist
    existing = [idx.name for idx in pc.list_indexes()]
    if INDEX_NAME not in existing:
        print(f"Creating Pinecone index '{INDEX_NAME}' (dimension={VECTOR_DIMENSION}, metric=cosine)...")
        pc.create_index(
            name=INDEX_NAME,
            dimension=VECTOR_DIMENSION,
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1"),
        )
        # Wait for index to be ready
        while not pc.describe_index(INDEX_NAME).status["ready"]:
            print("  waiting for index to be ready...")
            time.sleep(2)
        print("  Index ready.")
    else:
        print(f"Index '{INDEX_NAME}' already exists — will upsert (idempotent).")

    index = pc.Index(INDEX_NAME)

    # ── 2. Read all vectors from ChromaDB ────────────────────────────────────
    print(f"\nReading ChromaDB from {CHROMA_PATH}...")
    import chromadb
    chroma  = chromadb.PersistentClient(path=CHROMA_PATH)
    col     = chroma.get_collection(name=COLLECTION)
    total   = col.count()
    print(f"Found {total} chunks in ChromaDB.")

    if total == 0:
        print("ERROR: ChromaDB is empty. Run the ingestion pipeline first.")
        return

    # Fetch in batches (get() with limit/offset)
    FETCH_BATCH = 500
    all_ids, all_embeddings, all_metadatas, all_documents = [], [], [], []

    for offset in range(0, total, FETCH_BATCH):
        print(f"  Fetching ChromaDB records {offset}–{min(offset+FETCH_BATCH, total)}...")
        batch = col.get(
            limit=FETCH_BATCH,
            offset=offset,
            include=["embeddings", "metadatas", "documents"],
        )
        all_ids        += batch["ids"]
        all_embeddings += batch["embeddings"]
        all_metadatas  += batch["metadatas"]
        all_documents  += batch["documents"]

    print(f"Fetched {len(all_ids)} chunks total.")

    # ── 3. Upsert to Pinecone in batches ─────────────────────────────────────
    print(f"\nUpserting to Pinecone in batches of {BATCH_SIZE}...")
    upserted = 0

    for i in range(0, len(all_ids), BATCH_SIZE):
        batch_ids   = all_ids[i:i+BATCH_SIZE]
        batch_embs  = all_embeddings[i:i+BATCH_SIZE]
        batch_metas = all_metadatas[i:i+BATCH_SIZE]
        batch_docs  = all_documents[i:i+BATCH_SIZE]

        # Pinecone metadata: merge chunk text into metadata for retrieval
        vectors = []
        for chunk_id, emb, meta, doc_text in zip(batch_ids, batch_embs, batch_metas, batch_docs):
            # Pinecone metadata values must be str/int/float/bool/list
            clean_meta = {
                k: (str(v) if v is not None else "")
                for k, v in meta.items()
            }
            clean_meta["chunk_text"] = doc_text[:4000]  # store text in metadata for retrieval
            vectors.append({"id": chunk_id, "values": emb, "metadata": clean_meta})

        index.upsert(vectors=vectors)
        upserted += len(vectors)
        print(f"  Upserted {upserted}/{len(all_ids)} chunks...")

    # ── 4. Verify ─────────────────────────────────────────────────────────────
    time.sleep(2)  # allow index stats to update
    stats = index.describe_index_stats()
    pinecone_count = stats["total_vector_count"]

    print(f"\n{'='*50}")
    print(f"Migration complete!")
    print(f"  ChromaDB chunks : {len(all_ids)}")
    print(f"  Pinecone vectors: {pinecone_count}")
    if pinecone_count >= len(all_ids):
        print("  ✅ Counts match — migration successful")
        print(f"\nNext step: set PINECONE_API_KEY in your .env and Streamlit secrets")
        print(f"           The app will automatically use Pinecone when the key is present.")
    else:
        print(f"  ⚠️  Count mismatch — re-run this script (upsert is idempotent)")


if __name__ == "__main__":
    main()
