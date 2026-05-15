"""
Temporal Cloud connection config.
Reads credentials from .env / environment variables.

Usage:
    from workflows.temporal.config import get_client
    client = await get_client()
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except Exception:
    pass

TEMPORAL_ADDRESS   = os.getenv("TEMPORAL_ADDRESS", "")    # host:port
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TEMPORAL_API_KEY   = os.getenv("TEMPORAL_API_KEY", "")

# Task queue — all askmybank workers listen on this queue
TASK_QUEUE = "askmybank-hitl"


async def get_client():
    """
    Return a connected Temporal client for Temporal Cloud.
    Uses API-key auth (no mTLS needed for Temporal Cloud with API keys).
    """
    from temporalio.client import Client, TLSConfig
    from temporalio.service import HttpConnectProxyConfig
    import temporalio.service

    if not TEMPORAL_ADDRESS:
        raise RuntimeError("TEMPORAL_ADDRESS not set in .env")

    # Temporal Cloud with API key — pass key as Bearer in RPC metadata
    client = await Client.connect(
        TEMPORAL_ADDRESS,
        namespace  = TEMPORAL_NAMESPACE,
        tls        = TLSConfig(),           # TLS required for Temporal Cloud
        rpc_metadata = {"temporal-namespace": TEMPORAL_NAMESPACE,
                        "authorization": f"Bearer {TEMPORAL_API_KEY}"},
    )
    return client
