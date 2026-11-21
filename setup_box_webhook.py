#!/usr/bin/env python3
"""
Register the Box webhook that feeds the Box + Temporal pipeline.

    python setup_box_webhook.py            # create (skips if one already targets this address)
    python setup_box_webhook.py --list
    python setup_box_webhook.py --delete   # remove webhooks targeting this address

Needs the app scope "Manage webhooks" and BOX_ROOT_FOLDER_ID in .env.
"""

import argparse
import os
import sys
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from rag.box_client import API, BoxClient  # noqa: E402

TRIGGERS = ["FILE.UPLOADED"]


def webhook_address():
    outputs = boto3.client("cloudformation", region_name="us-east-1").describe_stacks(
        StackName="askmybank-box")["Stacks"][0]["Outputs"]
    api = next(o["OutputValue"] for o in outputs if o["OutputKey"] == "ApiUrl")
    return f"{api}/box/webhook"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--delete", action="store_true")
    args = ap.parse_args()

    box = BoxClient()
    address = webhook_address()
    hooks = box.get("/webhooks", params={"limit": 100})["entries"]
    ours = [h for h in hooks if box.get(f"/webhooks/{h['id']}")["address"] == address]

    if args.list:
        for h in hooks:
            d = box.get(f"/webhooks/{h['id']}")
            print(d["id"], d["target"], d["triggers"], d["address"])
        return
    if args.delete:
        for h in ours:
            box.request("DELETE", f"{API}/webhooks/{h['id']}").raise_for_status()
            print("deleted", h["id"])
        return
    if ours:
        print(f"Webhook {ours[0]['id']} already targets {address}")
        return

    r = box.request("POST", f"{API}/webhooks", json={
        "target": {"id": os.environ["BOX_ROOT_FOLDER_ID"], "type": "folder"},
        "address": address,
        "triggers": TRIGGERS,
    })
    if r.status_code == 403:
        sys.exit("Box refused (403): enable the 'Manage webhooks' scope and re-authorise the app.")
    r.raise_for_status()
    print(f"Created webhook {r.json()['id']} on folder {os.environ['BOX_ROOT_FOLDER_ID']} → {address}")


if __name__ == "__main__":
    main()
