FROM public.ecr.aws/lambda/python:3.11

WORKDIR ${LAMBDA_TASK_ROOT}

# Install Lambda dependencies (same list as requirements-lambda.txt)
# duckdb is included here — no ZIP size limit with container images
COPY requirements-lambda.txt .
RUN pip install --no-cache-dir -r requirements-lambda.txt && \
    pip install --no-cache-dir "duckdb==1.5.2"

# Copy application source
COPY lambda_handler.py .
COPY rag/ ./rag/

# Copy entire temporal workflows subtree — hitl_client imports workflow + signal types
COPY workflows/temporal/ ./workflows/temporal/
RUN touch workflows/__init__.py

# Lambda entry point
CMD ["lambda_handler.handler"]
