# mikevideo-ingest — CLAUDE.md

## What this repo is
The **upload data plane** for **MikeVideo** (MikeOS's self-hosted YouTube). A dumb, fast, resilient
chunked byte-mover — its own repo/container precisely because long streaming uploads and the snappy
watch API have opposite resource profiles and must not share a worker pool.

- **Live:** `https://up.osmike.com` — runs as a Docker container on the **Hetzner box** (NOT Railway).
- **Siblings:** `mikevideo-cloud` (control plane, mints the tickets we trust) · `mikevideo-app` +
  the `video.osmike.com` browser (the clients).
- **Canonical architecture:** `mikeos-architecture/docs/services/video.md` (§3 = this protocol).
- **Server access + deploy:** `SERVER-ACCESS.md` in this repo (SSH, box, Caddy, DNS).

## Stack
Python **async** (Starlette + uvicorn + `aiofiles`), Docker. **DB-free** — sessions are filesystem
state under `DATA_ROOT` (`/data/mikevideo/ingest/{upload_id}/`): `manifest.json`, a **sparse**
`data.bin` (chunk `i` written at offset `i*chunk_size`), `bitmap.bin` (bit `i` ⇒ received+validated),
`state.json`. Code: `server/app.py`. Test client: `scripts/upload_test.py`.

## The chunk protocol (what you must not break)
Client "splits" a file into N fixed chunks *in thought* (never on disk), computes a **manifest** of
per-chunk hashes up front, sends it, then uploads chunks in any order / parallel. We validate each
chunk against the manifest and track a received-bitmap, so we always know exactly what's missing.
- `POST /ingest/init` (manifest) → create OR **resume** (if `upload_id`/`file_hash` exists). Rejects
  over `MAX_TOTAL_SIZE`, bad `chunk_size`, count mismatch, or over disk quota — **before any bytes**.
- `PUT /ingest/{id}/chunk/{index}` (raw body) → validate session · index range · `len(body)`==expected
  · `hash(body)`==`manifest.chunks[index]`. Pass → write at offset + set bit. Mismatch → **422**
  (index stays missing). Already-set + match → **200 no-op** (idempotent).
- `GET /ingest/{id}/status` → `{received, missing, complete, bytes, total_size}` (the client's resume
  source of truth).
- `POST /ingest/{id}/finalize` (or auto on full bitmap) → streamed **sha256(data.bin)** == `file_hash`?
  → complete + **callback** the control plane; else **409**.
- `GET /health` — no auth.

**`chunk_hash_algo`** in the manifest is `md5` **or** `sha256` (default md5). The Android app uses
**md5**; the browser uses **sha256** (WebCrypto has no md5). `file_hash` is always sha256. Keep both
paths working.

## THE memory rule (non-negotiable)
Never load the whole file. Hold at most one chunk body (≤ `chunk_size`, capped) in RAM. The only
full-file pass is a **streamed (1 MB buffer) sha256** at finalize. (House rule from the 1.55 GB
reboot-loop incident — verified: the container stays ~95 MiB through a multi-GB upload.)

## Auth & coupling
- Bearer: a static **`INGEST_TOKEN`** (bootstrap/testing) OR an **HMAC-signed ticket**
  `{upload_id,user_id,max_size,file_hash,exp}` verified with **`INGEST_HMAC_SECRET`** (no DB). That
  secret **MUST equal** `mikevideo-cloud`'s `INGEST_HMAC_SECRET`.
- On completion we POST `CALLBACK_URL` (= `https://video.osmike.com/api/internal/complete`) signed
  with **`CALLBACK_SECRET`** (must match cloud). Body carries `upload_id` + `path`; cloud matches on
  `upload_id`.
- Both containers mount the **same `/data/mikevideo`** so cloud can read our finished `data.bin`.

## Config (`.env` — never commit; live copy on the box at `/root/mikevideo-ingest/.env`)
`PORT` (8080, published on `172.17.0.1` for Caddy) · `DATA_ROOT` · `INGEST_TOKEN` ·
`INGEST_HMAC_SECRET` · `MAX_TOTAL_SIZE` · `MAX_CHUNK_SIZE` · `MAX_CONCURRENT_WRITES` ·
`SESSION_TTL_HOURS` · `CALLBACK_URL` · `CALLBACK_SECRET`.

## Build / run / deploy
```bash
# local
docker compose up -d --build && docker compose logs -f --tail=50
# on the box (see SERVER-ACCESS.md): SSH in, then
cd /root/mikevideo-ingest && git pull && docker compose up -d --build
# verify
curl -s https://up.osmike.com/health
python3 scripts/upload_test.py --url https://up.osmike.com --token "$INGEST_TOKEN" \
  --file /tmp/big.bin --upload-id t1 --kill-after 0.4   # then rerun (no --kill) to resume
```

## Gotchas (learned the hard way)
- **Caddy edits not landing?** The running config once had `up.osmike.com` while the on-disk
  `/root/mikeos-osm/Caddyfile` didn't (stale bind-mount inode). Rewrite the FULL Caddyfile (all
  hosts) in place, then `docker restart mikeos-caddy`. Keep `request_body { max_size 8GB }` on THIS
  host only.
- **Do NOT disturb the OSM stack** (`mikeos-{caddy,nominatim,overpass,osrm,basemap}`) or its tmux
  imports on the box. We reach Caddy via the docker bridge gateway `172.17.0.1:8080` — we do not
  join the OSM compose network.
- **Never-trust-200** everywhere; return clean 4xx, never crash on bad input.
- Reserved-word-safe, parameterized (N/A — no DB here), ISO-8601 if you ever add timestamps.
- **No paid services** — cost is zero. Self-hosted only.
