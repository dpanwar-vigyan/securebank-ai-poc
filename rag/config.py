"""
Unified secrets / config loader for SecureBank AI POC.

Priority order:
  1. Streamlit Cloud secrets  (st.secrets)  — when deployed
  2. .env file                               — local development
  3. Hard-coded defaults                     — safe fallbacks

Injects all secrets into os.environ so boto3, clickhouse_connect,
and any other library picks them up automatically — no code changes needed
in chain.py or clickhouse_client.py.
"""

import os
from pathlib import Path

# ── 1. Load .env for local dev (silently ignored if file absent) ──────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env", override=False)
except Exception:
    pass

# ── 2. Inject Streamlit Cloud secrets into os.environ ────────────────────
#    boto3 reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY automatically.
#    clickhouse_client.py reads CLICKHOUSE_* via os.getenv().
_CLOUD_KEYS = [
    "APP_PASSWORD",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "BEDROCK_REGION",
    "LLM_MODEL",
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
]

try:
    import streamlit as st
    for _k in _CLOUD_KEYS:
        if _k in st.secrets:
            os.environ.setdefault(_k, str(st.secrets[_k]))
except Exception:
    pass  # Not running in Streamlit, or secrets not configured yet


def get(key: str, default: str = "") -> str:
    """Retrieve a config value (already loaded into os.environ above)."""
    return os.getenv(key, default)
