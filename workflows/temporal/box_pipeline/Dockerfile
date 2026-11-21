# Box pipeline Temporal worker — build from the repo root:
#   docker build --platform linux/amd64 -f workflows/temporal/box_pipeline/Dockerfile -t askmybank-box-worker .
FROM python:3.12-slim

WORKDIR /app
COPY workflows/temporal/box_pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY workflows/__init__.py workflows/
COPY workflows/temporal/__init__.py workflows/temporal/config.py workflows/temporal/
COPY workflows/temporal/box_pipeline/ workflows/temporal/box_pipeline/

ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "workflows.temporal.box_pipeline.worker"]
