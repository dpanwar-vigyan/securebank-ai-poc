#!/bin/bash
# run.sh — wrapper that ensures the correct Python + installed packages are used
# Usage:
#   ./run.sh app                    # start Streamlit UI
#   ./run.sh ingest                 # run full ingestion (ChromaDB + ClickHouse)
#   ./run.sh ingest --limit 10      # ingest first 10 docs only
#   ./run.sh ingest --resync        # force re-ingest all docs
#   ./run.sh generate               # regenerate PDFs and upload to S3

PYTHON="/usr/bin/python3"
SITE="/Users/dineshsinghpanwar/Library/Python/3.9/lib/python/site-packages"
STREAMLIT="/Users/dineshsinghpanwar/Library/Python/3.9/bin/streamlit"

export PYTHONPATH="$SITE:$PYTHONPATH"
cd "$(dirname "$0")"

case "$1" in
  app)
    echo "Starting SecureBank AI Assistant on http://localhost:8501 ..."
    "$STREAMLIT" run app.py --server.port 8501 --browser.gatherUsageStats false
    ;;
  ingest)
    shift
    echo "Running dual-write ingestion: ChromaDB + ClickHouse ..."
    "$PYTHON" rag/ingest.py "$@"
    ;;
  generate)
    echo "Generating and uploading 1,000 sample documents to S3 ..."
    "$PYTHON" generate_and_upload.py
    ;;
  *)
    echo "Usage: ./run.sh [app|ingest|generate] [options]"
    echo ""
    echo "  app                   Start Streamlit UI"
    echo "  ingest                Dual-write all S3 docs → ChromaDB + ClickHouse"
    echo "  ingest --limit N      Ingest first N docs only"
    echo "  ingest --resync       Force re-ingest even if already indexed"
    echo "  generate              Regenerate all sample PDFs and upload to S3"
    ;;
esac
