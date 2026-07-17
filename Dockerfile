# Backend image for Google Cloud Run (FastAPI via uvicorn).
# Local tests run on whatever Python the developer has; Cloud Run pins 3.12
# (no project runtime constraint file — 3.12-slim is a stable Cloud Run default).
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ api/
COPY workflow/ workflow/
COPY prompts/ prompts/

# Cloud Run injects PORT at runtime; default 8080 for local docker runs.
CMD exec uvicorn api.index:app --host 0.0.0.0 --port ${PORT:-8080}
