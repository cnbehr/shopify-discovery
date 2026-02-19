# Build from repo root: docker build -f services/discovery/Dockerfile .
FROM python:3.11-slim

WORKDIR /app

COPY services/discovery/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/discovery/workers/ ./workers/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "workers/discovery_worker.py"]
