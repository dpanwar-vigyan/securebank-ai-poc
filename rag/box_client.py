"""
Box API client — Client Credentials Grant (server-to-server, no user login).

Used by:
  load_s3_to_box.py     — bulk upload of test documents
  rag/pipeline_steps.py — fetch text + metadata for a Box file
  lambda_handler.py     — verify Box webhook signatures

Env: BOX_CLIENT_ID, BOX_CLIENT_SECRET, BOX_ENTERPRISE_ID
     BOX_WEBHOOK_PRIMARY_KEY, BOX_WEBHOOK_SECONDARY_KEY (webhook verification only)
"""

import base64
import hashlib
import hmac
import os
import time
from datetime import datetime, timezone

import requests

API = "https://api.box.com/2.0"
UPLOAD_API = "https://upload.box.com/api/2.0"
METADATA_TEMPLATE_KEY = "askmybankDocument"


class BoxNotConfiguredError(Exception):
    pass


class BoxClient:
    def __init__(self):
        self.client_id = os.getenv("BOX_CLIENT_ID", "")
        self.client_secret = os.getenv("BOX_CLIENT_SECRET", "")
        self.enterprise_id = os.getenv("BOX_ENTERPRISE_ID", "")
        if not (self.client_id and self.client_secret and self.enterprise_id):
            raise BoxNotConfiguredError("BOX_CLIENT_ID / BOX_CLIENT_SECRET / BOX_ENTERPRISE_ID not set")
        self._token = None
        self._token_expires = 0.0

    def _authenticate(self):
        r = requests.post("https://api.box.com/oauth2/token", data={
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "box_subject_type": "enterprise",
            "box_subject_id": self.enterprise_id,
        }, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"Box authentication failed ({r.status_code}): {r.text}")
        body = r.json()
        self._token = body["access_token"]
        self._token_expires = time.time() + body.get("expires_in", 3600) - 300

    def request(self, method, url, headers=None, **kwargs):
        """Box call with token refresh on expiry/401 and backoff on 429."""
        r = None
        for attempt in range(5):
            if not self._token or time.time() > self._token_expires:
                self._authenticate()
            r = requests.request(method, url, timeout=60,
                                 headers={"Authorization": f"Bearer {self._token}", **(headers or {})},
                                 **kwargs)
            if r.status_code == 401 and attempt == 0:
                self._token = None
                continue
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 2 ** attempt)))
                continue
            return r
        return r

    def get(self, path, **kwargs):
        r = self.request("GET", f"{API}{path}", **kwargs)
        r.raise_for_status()
        return r.json()

    def whoami(self):
        return self.get("/users/me")

    def file_info(self, file_id):
        return self.get(f"/files/{file_id}",
                        params={"fields": "id,name,file_version,path_collection,size,extension"})

    def download(self, file_id):
        r = self.request("GET", f"{API}/files/{file_id}/content")
        r.raise_for_status()
        return r.content

    def extracted_text(self, file_id, wait_s=45):
        """Box's own text extraction for the file, or None if Box can't provide it in time."""
        info = self.get(f"/files/{file_id}", params={"fields": "representations"},
                        headers={"x-rep-hints": "[extracted_text]"})
        entries = info.get("representations", {}).get("entries", [])
        if not entries:
            return None
        rep = entries[0]
        deadline = time.time() + wait_s
        while rep["status"]["state"] in ("pending", "none") and time.time() < deadline:
            if rep["status"]["state"] == "none":
                self.request("GET", rep["info"]["url"])  # asks Box to start generating it
            time.sleep(3)
            rep = self.get(rep["info"]["url"].removeprefix(API))
        if rep["status"]["state"] != "success":
            return None
        url = rep["content"]["url_template"].replace("{+asset_path}", "")
        r = self.request("GET", url)
        r.raise_for_status()
        return r.content.decode("utf-8", errors="replace")

    def document_metadata(self, file_id):
        """Values of the askmybankDocument metadata template on a file, or {} if absent."""
        r = self.request("GET", f"{API}/files/{file_id}/metadata/enterprise/{METADATA_TEMPLATE_KEY}")
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        return {k: v for k, v in r.json().items() if not k.startswith("$")}


def verify_webhook(body: bytes, headers: dict) -> bool:
    """Check a Box webhook delivery's HMAC-SHA256 signature and that it is under 10 minutes old."""
    headers = {k.lower(): v for k, v in headers.items()}
    timestamp = headers.get("box-delivery-timestamp", "")
    try:
        sent = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return False
    if abs((datetime.now(timezone.utc) - sent).total_seconds()) > 600:
        return False

    for key_env, header in (("BOX_WEBHOOK_PRIMARY_KEY", "box-signature-primary"),
                            ("BOX_WEBHOOK_SECONDARY_KEY", "box-signature-secondary")):
        key = os.getenv(key_env, "")
        signature = headers.get(header, "")
        if key and signature:
            expected = base64.b64encode(
                hmac.new(key.encode(), body + timestamp.encode(), hashlib.sha256).digest()).decode()
            if hmac.compare_digest(expected, signature):
                return True
    return False


def file_event(payload: dict):
    """(trigger, file_id, version_id, name) from a Box webhook payload, or None if not a file event."""
    source = payload.get("source") or {}
    if source.get("type") != "file":
        return None
    return (payload.get("trigger", ""), source.get("id", ""),
            (source.get("file_version") or {}).get("id", ""), source.get("name", ""))
