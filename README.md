# mikevideo-ingest

The **upload data plane** for MikeVideo — a dumb, fast, resilient chunked byte-mover. Its own repo /
container precisely because long streaming uploads and the snappy watch API have opposite resource
profiles and must not share a worker pool.

- **Host:** `up.osmike.com` (on the Hetzner box — **not** Railway). Deploy: `SERVER-ACCESS.md`.
- **Protocol:** manifest + per-chunk hash + received-bitmap. Full spec: `ARCHITECTURE.md` and
  `mikeos-architecture/docs/services/video.md` §3.
- **Never OOM:** streams ≤1 chunk to disk at a time; the file is never loaded whole (server or client).
- **DB-free:** filesystem-backed sessions under `/data/mikevideo/ingest/{upload_id}/`.
- **Stack:** Python async (Starlette + aiofiles + uvicorn), Docker.

`POST /ingest/init` (manifest) → `PUT /ingest/{id}/chunk/{i}` (hash-validated) → `GET /ingest/{id}/status`
(missing) → auto-finalize on full bitmap + whole-file hash → callback the control plane.
