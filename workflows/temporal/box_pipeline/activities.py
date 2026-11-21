"""
Thin activities — each one invokes the pipeline Lambda directly with
{"source": "pipeline_step", "step": <name>, "args": {...}}.

Lambda errors are re-raised as ApplicationError carrying the Python exception
type, so the workflow's retry policy can treat bad input as non-retryable.
"""

import json
import os

import boto3
from botocore.config import Config
from temporalio import activity
from temporalio.exceptions import ApplicationError

from workflows.temporal.box_pipeline import STEPS

FUNCTION_NAME = os.environ.get("PIPELINE_FUNCTION_NAME", "")

_lambda = boto3.client(
    "lambda",
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
    # Temporal owns retries; Lambda steps can run for minutes (Textract fallback)
    config=Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0}),
)


def _invoke(step: str, args: dict) -> dict:
    resp = _lambda.invoke(
        FunctionName=FUNCTION_NAME,
        Payload=json.dumps({"source": "pipeline_step", "step": step, "args": args}).encode(),
    )
    body = json.loads(resp["Payload"].read() or b"null")
    if resp.get("FunctionError"):
        body = body or {}
        raise ApplicationError(body.get("errorMessage", f"{step} failed"), type=body.get("errorType", "LambdaError"))
    return body


def _activity(step: str):
    @activity.defn(name=step)
    def run(args: dict) -> dict:
        return _invoke(step, args)
    return run


ACTIVITIES = {step: _activity(step) for step in STEPS}
