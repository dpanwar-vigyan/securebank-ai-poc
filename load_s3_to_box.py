#!/usr/bin/env python3
"""
Copy the banking test documents from S3 into Box, tagging each file with
business metadata so Box search and the analytics store agree.

Requires in .env:
    BOX_CLIENT_ID, BOX_CLIENT_SECRET, BOX_ENTERPRISE_ID   (Custom App, Client Credentials Grant)
    BOX_ROOT_FOLDER_ID                                    (Box folder the app can write to)

Usage:
    python load_s3_to_box.py --dry-run             # list what would be uploaded
    python load_s3_to_box.py --limit 5             # small trial
    python load_s3_to_box.py --doc-type eStatement
    python load_s3_to_box.py                       # everything
    python load_s3_to_box.py --no-metadata         # skip the metadata template

Re-runs are safe: existing folders and files are reused, not duplicated.
Writes box_manifest.json (doc_id -> Box file ID + version ID).
"""

import argparse
import json
import os
import sys
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from rag.box_client import API, METADATA_TEMPLATE_KEY as TEMPLATE_KEY, UPLOAD_API as UPLOAD  # noqa: E402
from rag.box_client import BoxClient, BoxNotConfiguredError  # noqa: E402
from rag.db_adapters import get_adapter  # noqa: E402

MANIFEST = Path(__file__).parent / "box_manifest.json"

TEMPLATE_FIELDS = [
    {"type": "string", "key": "docId", "displayName": "Document ID"},
    {"type": "string", "key": "docType", "displayName": "Document Type"},
    {"type": "string", "key": "customerId", "displayName": "Customer ID"},
    {"type": "string", "key": "accountNumber", "displayName": "Account Number"},
    {"type": "string", "key": "branchName", "displayName": "Branch"},
    {"type": "date", "key": "documentDate", "displayName": "Document Date"},
    {"type": "string", "key": "caseStatus", "displayName": "Case Status"},
]


class Box(BoxClient):
    def __init__(self):
        missing = [k for k in ("BOX_CLIENT_ID", "BOX_CLIENT_SECRET", "BOX_ENTERPRISE_ID", "BOX_ROOT_FOLDER_ID")
                   if not os.getenv(k)]
        if missing:
            sys.exit(f"Missing in .env: {', '.join(missing)}")
        try:
            super().__init__()
        except BoxNotConfiguredError as exc:
            sys.exit(str(exc))
        self.root_folder_id = os.environ["BOX_ROOT_FOLDER_ID"]
        self._folders = {}

    def ensure_folder(self, path_parts):
        """Return the Box folder ID for a path under the root, creating folders as needed."""
        parent = self.root_folder_id
        for i, name in enumerate(path_parts):
            key = "/".join(path_parts[: i + 1])
            if key not in self._folders:
                r = self.request("POST", f"{API}/folders", json={"name": name, "parent": {"id": parent}})
                if r.status_code == 201:
                    self._folders[key] = r.json()["id"]
                elif r.status_code == 409:
                    self._folders[key] = r.json()["context_info"]["conflicts"][0]["id"]
                else:
                    raise RuntimeError(f"Create folder '{key}' failed ({r.status_code}): {r.text}")
            parent = self._folders[key]
        return parent

    def upload(self, folder_id, name, content):
        """Upload a file. Returns (file_id, version_id, created)."""
        r = self.request("POST", f"{UPLOAD}/files/content",
                         data={"attributes": json.dumps({"name": name, "parent": {"id": folder_id}})},
                         files={"file": (name, content, "application/pdf")})
        if r.status_code == 201:
            entry = r.json()["entries"][0]
            return entry["id"], entry["file_version"]["id"], True
        if r.status_code == 409:
            conflict = r.json()["context_info"]["conflicts"]
            return conflict["id"], conflict.get("file_version", {}).get("id", ""), False
        raise RuntimeError(f"Upload '{name}' failed ({r.status_code}): {r.text}")

    def ensure_template(self):
        r = self.request("POST", f"{API}/metadata_templates/schema", json={
            "scope": "enterprise",
            "templateKey": TEMPLATE_KEY,
            "displayName": "AskMyBank Document",
            "fields": TEMPLATE_FIELDS,
        })
        if r.status_code in (201, 409):
            return
        if r.status_code == 403:
            sys.exit("Box refused to create the metadata template (403). Enable the app scope "
                     "'Manage enterprise properties' and re-authorise the app, or run with --no-metadata.")
        raise RuntimeError(f"Create metadata template failed ({r.status_code}): {r.text}")

    def apply_metadata(self, file_id, values):
        r = self.request("POST", f"{API}/files/{file_id}/metadata/enterprise/{TEMPLATE_KEY}", json=values)
        if r.status_code not in (201, 409):
            raise RuntimeError(f"Apply metadata to file {file_id} failed ({r.status_code}): {r.text}")


def fetch_documents(doc_type, limit):
    sql = ("SELECT doc_id, doc_type, s3_path, customer_id, account_number, branch_name, "
           "statement_date, filed_date, request_date, case_status FROM banking_docs.documents")
    if doc_type:
        sql += f" WHERE doc_type = '{doc_type}'"
    sql += " ORDER BY doc_type, doc_id"
    if limit:
        sql += f" LIMIT {int(limit)}"
    result = get_adapter().query(sql)
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def metadata_for(doc):
    doc_date = doc["statement_date"] or doc["filed_date"] or doc["request_date"]
    values = {
        "docId": doc["doc_id"],
        "docType": doc["doc_type"],
        "customerId": doc["customer_id"],
        "accountNumber": doc["account_number"],
        "branchName": doc["branch_name"],
        "caseStatus": doc["case_status"],
        "documentDate": f"{doc_date}T00:00:00Z" if doc_date else None,
    }
    return {k: v for k, v in values.items() if v}


def main():
    ap = argparse.ArgumentParser(description="Copy banking test documents from S3 into Box")
    ap.add_argument("--doc-type", choices=["eStatement", "Dispute", "Complaint", "AccountMaintenance"])
    ap.add_argument("--limit", type=int)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-metadata", action="store_true")
    args = ap.parse_args()

    docs = fetch_documents(args.doc_type, args.limit)
    print(f"{len(docs)} documents selected")
    if args.dry_run:
        for d in docs[:10]:
            print(f"  {d['doc_id']:<10} {d['s3_path']}")
        if len(docs) > 10:
            print(f"  ... and {len(docs) - 10} more")
        return

    box = Box()
    me = box.whoami()
    print(f"Box: authenticated as {me['name']} ({me['login']})")
    if not args.no_metadata:
        box.ensure_template()

    s3 = boto3.client("s3")
    manifest = json.loads(MANIFEST.read_text()) if MANIFEST.exists() else {}
    uploaded = reused = failed = 0

    for i, doc in enumerate(docs, 1):
        bucket, key = doc["s3_path"].removeprefix("s3://").split("/", 1)
        *folders, name = key.split("/")
        try:
            content = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            folder_id = box.ensure_folder(folders)
            file_id, version_id, created = box.upload(folder_id, name, content)
            if not args.no_metadata:
                box.apply_metadata(file_id, metadata_for(doc))
            manifest[doc["doc_id"]] = {"s3_key": key, "box_file_id": file_id, "box_file_version_id": version_id}
            uploaded += created
            reused += not created
        except Exception as exc:
            failed += 1
            print(f"  FAILED {doc['doc_id']}: {exc}")
        if i % 25 == 0 or i == len(docs):
            MANIFEST.write_text(json.dumps(manifest, indent=2))
            print(f"  {i}/{len(docs)}  uploaded={uploaded} already-in-box={reused} failed={failed}")

    print(f"Done. Manifest: {MANIFEST.name}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
