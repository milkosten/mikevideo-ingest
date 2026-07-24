FROM python:3.12-slim

WORKDIR /app

# System certs only; no build toolchain needed for the pure-python deps.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ ./server/

ENV PORT=8080 \
    DATA_ROOT=/data/mikevideo/ingest

EXPOSE 8080

# Single worker: the receiver is fully async and holds per-session state/locks
# in-process. Uploads must not fan across worker processes.
CMD ["sh", "-c", "uvicorn server.app:app --host 0.0.0.0 --port ${PORT} --timeout-keep-alive 120"]
