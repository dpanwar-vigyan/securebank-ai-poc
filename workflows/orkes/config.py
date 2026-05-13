"""
Orkes Conductor client configuration.
Reads credentials from .env / environment variables.

Usage:
    from workflows.orkes.config import get_orkes_config, get_token
    config = get_orkes_config()          # Configuration object for SDK
    token  = get_token()                 # Bearer token string for raw HTTP calls
"""

import json
import os
import time
import urllib.request
import urllib.error
from pathlib import Path

# ── Load .env if running locally ──────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except Exception:
    pass

ORKES_API_URL    = os.getenv("ORKES_API_URL", "").rstrip("/")   # e.g. https://developer.orkescloud.com/api
ORKES_KEY_ID     = os.getenv("ORKES_KEY_ID", "")
ORKES_KEY_SECRET = os.getenv("ORKES_KEY_SECRET", "")

# ── Cached token (refreshed when expired) ─────────────────────────────────────
_token_cache: dict = {}   # {"token": "...", "expires_at": float}


def get_token() -> str:
    """
    Return a valid Bearer token for Orkes API calls.
    Tokens are cached and refreshed automatically (5-min buffer before expiry).
    """
    now = time.time()
    if _token_cache.get("token") and _token_cache.get("expires_at", 0) - now > 300:
        return _token_cache["token"]

    if not all([ORKES_API_URL, ORKES_KEY_ID, ORKES_KEY_SECRET]):
        raise OrkesConfigError(
            "ORKES_API_URL, ORKES_KEY_ID and ORKES_KEY_SECRET must be set in .env"
        )

    payload = json.dumps({"keyId": ORKES_KEY_ID, "keySecret": ORKES_KEY_SECRET}).encode()
    req = urllib.request.Request(
        f"{ORKES_API_URL}/token",
        data    = payload,
        headers = {"Content-Type": "application/json"},
        method  = "POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data  = json.loads(resp.read())
            token = data.get("token") or data.get("access_token")
            if not token:
                raise OrkesConfigError(f"No token in response: {data}")
            # Orkes tokens typically expire in 30 days; cache for 23h to be safe
            _token_cache["token"]      = token
            _token_cache["expires_at"] = now + 82_800   # 23 hours
            return token
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        raise OrkesConfigError(f"Token request failed {exc.code}: {body}") from exc


def orkes_request(method: str, path: str, body: dict | None = None) -> dict | str:
    """
    Make an authenticated request to the Orkes API.
    path should start with /  e.g. "/metadata/workflow"
    Returns parsed JSON dict, or raw string for non-JSON responses.
    """
    token = get_token()
    url   = f"{ORKES_API_URL}{path}"
    data  = json.dumps(body).encode() if body is not None else None
    req   = urllib.request.Request(
        url,
        data    = data,
        headers = {
            "X-Authorization": token,        # Orkes uses X-Authorization, not Bearer
            "Content-Type":    "application/json",
            "Accept":          "application/json",
        },
        method  = method.upper(),
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return raw   # workflow IDs are returned as plain strings
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        raise OrkesAPIError(f"Orkes API {method} {path} → {exc.code}: {body}") from exc


def get_orkes_config():
    """
    Return a conductor-python Configuration object for use with SDK workers.
    """
    from conductor.client.configuration.configuration import Configuration
    from conductor.client.configuration.settings.authentication_settings import AuthenticationSettings

    if not all([ORKES_API_URL, ORKES_KEY_ID, ORKES_KEY_SECRET]):
        raise OrkesConfigError("ORKES_API_URL, ORKES_KEY_ID and ORKES_KEY_SECRET must be set")

    # conductor-python appends /api internally; strip it from our URL if present
    base = ORKES_API_URL.removesuffix("/api")

    return Configuration(
        base_url               = base,
        authentication_settings = AuthenticationSettings(
            key_id     = ORKES_KEY_ID,
            key_secret = ORKES_KEY_SECRET,
        ),
    )


# ── Exceptions ─────────────────────────────────────────────────────────────────
class OrkesConfigError(Exception):
    """Missing or invalid Orkes credentials."""

class OrkesAPIError(Exception):
    """Orkes API call failed."""
