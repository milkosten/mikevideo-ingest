# mikevideo-ingest — architecture & protocol

Canonical system doc: `mikeos-architecture/docs/services/video.md`. This is the self-contained build
spec for the data plane.

## Memory contract (both ends, non-negotiable)
The file is NEVER split on disk and NEVER loaded whole into RAM. The server holds at most one chunk
body (≤ chunk_size, capped) at a time. The only full-file pass is a STREAMED (1 MB buffer) hash at
finalize. (House rule from the 1.55 GB reboot-loop incident.)

## Session storage (DB-free) — `/data/mikevideo/ingest/{upload_id}/`
- `manifest.json` — client manifest (expected per-chunk hashes + lengths + file_hash).
- `data.bin` — sparse target, pre-`truncate`d to total_size; chunk i written at offset i*chunk_size.
- `bitmap.bin` — ceil(count/8) bytes; bit i set ⇒ chunk i received AND validated.
- `state.json` — {received_count, bytes, complete, created_at, ticket_sub}.

## Manifest (client-authored)
{ upload_id, filename, content_type, total_size, chunk_size, count, file_hash:"sha256:…",
  chunk_hash_algo:"md5"|"sha256", chunks:{ "0":"md5:…", … } }
  // last chunk length = total_size-(count-1)*chunk_size
  // chunk_hash_algo declares the per-chunk digest: Android=md5, browser(WebCrypto)=sha256.
  // Defaults to md5 when absent (backward-compatible). file_hash is ALWAYS sha256.

## Endpoints (Bearer ticket/token)
- POST /ingest/init  {manifest} → create OR resume (if upload_id/file_hash exists). Pre-alloc sparse
  data.bin. Reject if total_size>MAX_TOTAL_SIZE, chunk_size>MAX_CHUNK_SIZE, count!=ceil(total/chunk),
  or over quota (disk_usage) — BEFORE any bytes. → {upload_id, chunk_size, count, received:[], missing:[…]}
- PUT /ingest/{id}/chunk/{index}  (raw body) → validate: session exists · 0<=index<count ·
  len(body)==expected length · md5(body)==manifest.chunks[index]. Pass → write at offset, set bit i,
  update state. Mismatch/bad → 422 (index stays missing). Bit already set + hash match → 200 no-op.
- GET /ingest/{id}/status → {received:[…], missing:[…], complete, bytes, total_size}
- POST /ingest/{id}/finalize → (or auto when bitmap full) streamed sha256(data.bin)==file_hash?
  → complete + callback control plane ; else 409.
- GET /health → no auth.

## Resource isolation ("don't hog threads")
- Async ASGI (Starlette + aiofiles). Global asyncio.Semaphore(MAX_CONCURRENT_WRITES). Per-session
  asyncio.Lock for bitmap/state RMW. Caps: MAX_TOTAL_SIZE (e.g. 20GB), MAX_CHUNK_SIZE (e.g. 8MB),
  QUOTA_FREE_MIN. Stale-session GC (TTL) background task. Idempotent chunk writes.

## Auth
Verify an HMAC-signed ticket {upload_id,user_id,max_size,file_hash,exp} with INGEST_HMAC_SECRET
(no DB). P1 bootstrap: a static INGEST_TOKEN bearer so it can be tested before mikevideo-cloud exists.

## Config (env, .env.example)
PORT, DATA_ROOT=/data/mikevideo/ingest, INGEST_TOKEN, INGEST_HMAC_SECRET, MAX_TOTAL_SIZE,
MAX_CHUNK_SIZE, MAX_CONCURRENT_WRITES, SESSION_TTL_HOURS, CALLBACK_URL (control plane), CALLBACK_SECRET.
