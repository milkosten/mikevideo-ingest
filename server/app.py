"""mikevideo-ingest — the MikeVideo upload data plane.

A dumb, fast, resilient chunked byte-mover. DB-free, filesystem-backed.

Memory contract (non-negotiable — house rule from the 1.55 GB reboot-loop incident):
the whole file is NEVER loaded into RAM. The server holds at most ONE chunk body
(<= chunk_size, capped) at a time. The only full-file pass is a STREAMED (1 MB buffer)
sha256 at finalize.

See ARCHITECTURE.md / mikeos-architecture/docs/services/video.md §3 for the contract.
"""

import asyncio
import hashlib
import hmac
import json
import os
import shutil
import time
import base64
from pathlib import Path

import aiofiles
import aiofiles.os as aios
import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

# ----------------------------------------------------------------------------
# Config (env)
# ----------------------------------------------------------------------------
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/data/mikevideo/ingest"))
INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "")
INGEST_HMAC_SECRET = os.environ.get("INGEST_HMAC_SECRET", "")
MAX_TOTAL_SIZE = int(os.environ.get("MAX_TOTAL_SIZE", str(20 * 1024 * 1024 * 1024)))
MAX_CHUNK_SIZE = int(os.environ.get("MAX_CHUNK_SIZE", str(8 * 1024 * 1024)))
MAX_CONCURRENT_WRITES = int(os.environ.get("MAX_CONCURRENT_WRITES", "8"))
SESSION_TTL_HOURS = float(os.environ.get("SESSION_TTL_HOURS", "48"))
# Keep at least this many bytes free on the data volume (safety headroom).
QUOTA_FREE_MIN = int(os.environ.get("QUOTA_FREE_MIN", str(50 * 1024 * 1024 * 1024)))
CALLBACK_URL = os.environ.get("CALLBACK_URL", "").strip()
CALLBACK_SECRET = os.environ.get("CALLBACK_SECRET", "")
GC_INTERVAL_SECONDS = int(os.environ.get("GC_INTERVAL_SECONDS", "1800"))
HASH_STREAM_BUFFER = 1024 * 1024  # 1 MB streamed-hash buffer

# ----------------------------------------------------------------------------
# Concurrency primitives
# ----------------------------------------------------------------------------
_write_sem = asyncio.Semaphore(MAX_CONCURRENT_WRITES)
_session_locks: dict[str, asyncio.Lock] = {}
_session_locks_guard = asyncio.Lock()


async def _lock_for(upload_id: str) -> asyncio.Lock:
    async with _session_locks_guard:
        lk = _session_locks.get(upload_id)
        if lk is None:
            lk = asyncio.Lock()
            _session_locks[upload_id] = lk
        return lk


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def _safe_id(upload_id: str) -> bool:
    """upload_id must be filesystem-safe (no traversal)."""
    if not upload_id or len(upload_id) > 200:
        return False
    return all(c.isalnum() or c in "-_." for c in upload_id) and ".." not in upload_id


def _session_dir(upload_id: str) -> Path:
    return DATA_ROOT / upload_id


def _strip_algo(h: str) -> str:
    """Accept 'md5:xxx' / 'sha256:xxx' or a bare hex digest."""
    if ":" in h:
        return h.split(":", 1)[1].strip().lower()
    return h.strip().lower()


def _bitmap_bytes(count: int) -> int:
    return (count + 7) // 8


def _bit_get(bm: bytearray, i: int) -> bool:
    return bool(bm[i >> 3] & (1 << (i & 7)))


def _bit_set(bm: bytearray, i: int) -> None:
    bm[i >> 3] |= (1 << (i & 7))


def _received_missing(bm: bytearray, count: int):
    received, missing = [], []
    for i in range(count):
        (received if _bit_get(bm, i) else missing).append(i)
    return received, missing


def _expected_chunk_len(index: int, total_size: int, chunk_size: int, count: int) -> int:
    if index == count - 1:
        return total_size - (count - 1) * chunk_size
    return chunk_size


def _err(status: int, msg: str, **extra):
    body = {"error": msg}
    body.update(extra)
    return JSONResponse(body, status_code=status)


# ----------------------------------------------------------------------------
# Auth: static Bearer token OR HMAC-signed ticket
# ----------------------------------------------------------------------------
def _verify_ticket(token: str) -> dict | None:
    """Ticket format: base64url(json_payload) + '.' + base64url(hmac_sha256).

    payload = {upload_id, user_id, max_size, file_hash, exp}
    Returns the payload dict if valid & unexpired, else None.
    """
    if not INGEST_HMAC_SECRET or "." not in token:
        return None
    try:
        payload_b64, sig_b64 = token.rsplit(".", 1)
        payload_raw = base64.urlsafe_b64decode(payload_b64 + "==")
        want = base64.urlsafe_b64decode(sig_b64 + "==")
        got = hmac.new(INGEST_HMAC_SECRET.encode(), payload_raw, hashlib.sha256).digest()
        if not hmac.compare_digest(got, want):
            return None
        payload = json.loads(payload_raw)
        if float(payload.get("exp", 0)) < time.time():
            return None
        return payload
    except Exception:
        return None


def _authorize(request: Request) -> tuple[bool, dict | None]:
    """Returns (ok, ticket_payload_or_None). Static token → (True, None)."""
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return False, None
    token = auth[7:].strip()
    if INGEST_TOKEN and hmac.compare_digest(token, INGEST_TOKEN):
        return True, None
    ticket = _verify_ticket(token)
    if ticket is not None:
        return True, ticket
    return False, None


# ----------------------------------------------------------------------------
# Persisted state I/O
# ----------------------------------------------------------------------------
async def _read_json(path: Path) -> dict:
    async with aiofiles.open(path, "r") as f:
        return json.loads(await f.read())


async def _write_json_atomic(path: Path, obj: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    async with aiofiles.open(tmp, "w") as f:
        await f.write(json.dumps(obj))
        await f.flush()
    await aios.replace(tmp, path)


async def _read_bitmap(upload_id: str, count: int) -> bytearray:
    path = _session_dir(upload_id) / "bitmap.bin"
    n = _bitmap_bytes(count)
    async with aiofiles.open(path, "rb") as f:
        data = await f.read()
    bm = bytearray(n)
    bm[: len(data)] = data[:n]
    return bm


async def _write_bitmap(upload_id: str, bm: bytearray) -> None:
    path = _session_dir(upload_id) / "bitmap.bin"
    tmp = path.with_suffix(".tmp")
    async with aiofiles.open(tmp, "wb") as f:
        await f.write(bytes(bm))
        await f.flush()
    await aios.replace(tmp, path)


# ----------------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------------
async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok", "service": "mikevideo-ingest"})


async def init(request: Request) -> Response:
    ok, ticket = _authorize(request)
    if not ok:
        return _err(401, "unauthorized")

    try:
        manifest = await request.json()
    except Exception:
        return _err(400, "invalid json body")
    if not isinstance(manifest, dict):
        return _err(400, "manifest must be an object")

    upload_id = str(manifest.get("upload_id", ""))
    if not _safe_id(upload_id):
        return _err(400, "invalid upload_id")

    try:
        total_size = int(manifest["total_size"])
        chunk_size = int(manifest["chunk_size"])
        count = int(manifest["count"])
    except (KeyError, TypeError, ValueError):
        return _err(400, "manifest missing/invalid total_size, chunk_size or count")

    file_hash = str(manifest.get("file_hash", ""))
    chunks = manifest.get("chunks")
    if not isinstance(chunks, dict):
        return _err(400, "manifest.chunks must be an object")

    # --- validate the manifest BEFORE moving any bytes ---
    if total_size <= 0 or total_size > MAX_TOTAL_SIZE:
        return _err(400, f"total_size out of range (max {MAX_TOTAL_SIZE})")
    if chunk_size <= 0 or chunk_size > MAX_CHUNK_SIZE:
        return _err(400, f"chunk_size out of range (max {MAX_CHUNK_SIZE})")
    expected_count = (total_size + chunk_size - 1) // chunk_size
    if count != expected_count:
        return _err(400, f"count mismatch: expected {expected_count}, got {count}")
    if len(chunks) != count:
        return _err(400, f"chunks map size {len(chunks)} != count {count}")
    if not file_hash:
        return _err(400, "missing file_hash")

    # Ticket constraints (if a signed ticket was used).
    if ticket is not None:
        if ticket.get("upload_id") and ticket["upload_id"] != upload_id:
            return _err(403, "ticket upload_id mismatch")
        if ticket.get("file_hash") and _strip_algo(str(ticket["file_hash"])) != _strip_algo(file_hash):
            return _err(403, "ticket file_hash mismatch")
        if ticket.get("max_size") and total_size > int(ticket["max_size"]):
            return _err(403, "exceeds ticket max_size")

    sdir = _session_dir(upload_id)
    lock = await _lock_for(upload_id)
    async with lock:
        # --- resume path: session already exists with a matching manifest ---
        if (sdir / "manifest.json").exists():
            existing = await _read_json(sdir / "manifest.json")
            if _strip_algo(str(existing.get("file_hash", ""))) != _strip_algo(file_hash):
                return _err(409, "upload_id exists with a different file_hash")
            bm = await _read_bitmap(upload_id, count)
            received, missing = _received_missing(bm, count)
            return JSONResponse({
                "upload_id": upload_id,
                "chunk_size": chunk_size,
                "count": count,
                "received": received,
                "missing": missing,
                "resumed": True,
            })

        # --- quota check (disk_usage) BEFORE allocating ---
        try:
            DATA_ROOT.mkdir(parents=True, exist_ok=True)
            free = shutil.disk_usage(DATA_ROOT).free
        except Exception as e:
            return _err(500, f"disk stat failed: {e}")
        if free - total_size < QUOTA_FREE_MIN:
            return _err(507, "insufficient storage / over quota")

        # --- create the session: sparse data.bin, empty bitmap, state ---
        sdir.mkdir(parents=True, exist_ok=True)
        await _write_json_atomic(sdir / "manifest.json", manifest)
        # sparse pre-alloc via truncate
        async with aiofiles.open(sdir / "data.bin", "wb") as f:
            await f.truncate(total_size)
        await _write_bitmap(upload_id, bytearray(_bitmap_bytes(count)))
        state = {
            "received_count": 0,
            "bytes": 0,
            "complete": False,
            "created_at": time.time(),
            "ticket_sub": (ticket.get("user_id") if ticket else None),
        }
        await _write_json_atomic(sdir / "state.json", state)

    return JSONResponse({
        "upload_id": upload_id,
        "chunk_size": chunk_size,
        "count": count,
        "received": [],
        "missing": list(range(count)),
        "resumed": False,
    })


async def put_chunk(request: Request) -> Response:
    ok, _ticket = _authorize(request)
    if not ok:
        return _err(401, "unauthorized")

    upload_id = request.path_params["upload_id"]
    if not _safe_id(upload_id):
        return _err(400, "invalid upload_id")
    sdir = _session_dir(upload_id)
    if not (sdir / "manifest.json").exists():
        return _err(404, "unknown session")

    try:
        index = int(request.path_params["index"])
    except ValueError:
        return _err(400, "invalid index")

    manifest = await _read_json(sdir / "manifest.json")
    total_size = int(manifest["total_size"])
    chunk_size = int(manifest["chunk_size"])
    count = int(manifest["count"])

    if index < 0 or index >= count:
        return _err(400, "index out of range")

    expected_len = _expected_chunk_len(index, total_size, chunk_size, count)

    # Read at most ONE chunk body into RAM (bounded by chunk_size).
    body = await request.body()
    if len(body) != expected_len:
        return _err(422, f"length mismatch: expected {expected_len}, got {len(body)}")

    expected_md5 = _strip_algo(str(manifest["chunks"][str(index)]))
    got_md5 = hashlib.md5(body).hexdigest()
    if got_md5 != expected_md5:
        return _err(422, "chunk hash mismatch")

    lock = await _lock_for(upload_id)
    async with lock:
        bm = await _read_bitmap(upload_id, count)
        if _bit_get(bm, index):
            # already have this validated chunk → idempotent no-op
            return JSONResponse({"index": index, "stored": True, "noop": True})

        # write the chunk at its offset (concurrency-capped)
        async with _write_sem:
            async with aiofiles.open(sdir / "data.bin", "r+b") as f:
                await f.seek(index * chunk_size)
                await f.write(body)
                await f.flush()

        _bit_set(bm, index)
        await _write_bitmap(upload_id, bm)

        received, missing = _received_missing(bm, count)
        state = await _read_json(sdir / "state.json")
        state["received_count"] = len(received)
        state["bytes"] = state.get("bytes", 0) + len(body)
        await _write_json_atomic(sdir / "state.json", state)

        full = len(missing) == 0

    resp = {"index": index, "stored": True, "received_count": len(received),
            "missing_count": len(missing)}

    if full:
        fin = await _finalize_locked(upload_id)
        resp["finalize"] = fin
    return JSONResponse(resp)


async def status(request: Request) -> Response:
    ok, _ = _authorize(request)
    if not ok:
        return _err(401, "unauthorized")
    upload_id = request.path_params["upload_id"]
    if not _safe_id(upload_id):
        return _err(400, "invalid upload_id")
    sdir = _session_dir(upload_id)
    if not (sdir / "manifest.json").exists():
        return _err(404, "unknown session")

    manifest = await _read_json(sdir / "manifest.json")
    count = int(manifest["count"])
    total_size = int(manifest["total_size"])
    bm = await _read_bitmap(upload_id, count)
    received, missing = _received_missing(bm, count)
    state = await _read_json(sdir / "state.json")
    return JSONResponse({
        "upload_id": upload_id,
        "received": received,
        "missing": missing,
        "complete": bool(state.get("complete", False)),
        "bytes": int(state.get("bytes", 0)),
        "total_size": total_size,
        "count": count,
    })


async def _stream_sha256(path: Path) -> str:
    """Streamed whole-file sha256, 1 MB buffer — the ONLY full-file pass."""
    h = hashlib.sha256()
    async with aiofiles.open(path, "rb") as f:
        while True:
            buf = await f.read(HASH_STREAM_BUFFER)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


async def _fire_callback(upload_id: str, manifest: dict, sha: str) -> dict:
    if not CALLBACK_URL:
        return {"sent": False, "reason": "no CALLBACK_URL"}
    payload = {
        "upload_id": upload_id,
        "filename": manifest.get("filename"),
        "content_type": manifest.get("content_type"),
        "total_size": manifest.get("total_size"),
        "file_hash": f"sha256:{sha}",
        "path": str(_session_dir(upload_id) / "data.bin"),
    }
    raw = json.dumps(payload).encode()
    sig = hmac.new(CALLBACK_SECRET.encode(), raw, hashlib.sha256).hexdigest() if CALLBACK_SECRET else ""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                CALLBACK_URL, content=raw,
                headers={"Content-Type": "application/json", "X-Ingest-Signature": sig},
            )
        return {"sent": True, "status": r.status_code}
    except Exception as e:
        return {"sent": False, "error": str(e)}


async def _finalize_locked(upload_id: str) -> dict:
    """Assumes the per-session lock is HELD. Returns a result dict."""
    sdir = _session_dir(upload_id)
    manifest = await _read_json(sdir / "manifest.json")
    count = int(manifest["count"])
    bm = await _read_bitmap(upload_id, count)
    _, missing = _received_missing(bm, count)
    state = await _read_json(sdir / "state.json")

    if state.get("complete"):
        return {"complete": True, "already": True}
    if missing:
        return {"complete": False, "missing_count": len(missing)}

    want = _strip_algo(str(manifest["file_hash"]))
    got = await _stream_sha256(sdir / "data.bin")
    if got != want:
        return {"complete": False, "error": "file_hash mismatch",
                "expected": want, "got": got}

    state["complete"] = True
    state["completed_at"] = time.time()
    state["sha256"] = got
    await _write_json_atomic(sdir / "state.json", state)
    cb = await _fire_callback(upload_id, manifest, got)
    return {"complete": True, "sha256": got, "callback": cb}


async def finalize(request: Request) -> Response:
    ok, _ = _authorize(request)
    if not ok:
        return _err(401, "unauthorized")
    upload_id = request.path_params["upload_id"]
    if not _safe_id(upload_id):
        return _err(400, "invalid upload_id")
    sdir = _session_dir(upload_id)
    if not (sdir / "manifest.json").exists():
        return _err(404, "unknown session")

    lock = await _lock_for(upload_id)
    async with lock:
        res = await _finalize_locked(upload_id)
    if not res.get("complete"):
        return JSONResponse(res, status_code=409)
    return JSONResponse(res)


# ----------------------------------------------------------------------------
# Stale-session GC
# ----------------------------------------------------------------------------
async def _gc_loop():
    ttl = SESSION_TTL_HOURS * 3600
    while True:
        try:
            if DATA_ROOT.exists():
                now = time.time()
                for entry in DATA_ROOT.iterdir():
                    if not entry.is_dir():
                        continue
                    state_p = entry / "state.json"
                    try:
                        if state_p.exists():
                            st = json.loads(state_p.read_text())
                            if st.get("complete"):
                                continue
                            age = now - float(st.get("created_at", now))
                        else:
                            age = now - entry.stat().st_mtime
                        if age > ttl:
                            shutil.rmtree(entry, ignore_errors=True)
                    except Exception:
                        continue
        except Exception:
            pass
        await asyncio.sleep(GC_INTERVAL_SECONDS)


async def _startup():
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    asyncio.create_task(_gc_loop())


routes = [
    Route("/health", health, methods=["GET"]),
    Route("/ingest/init", init, methods=["POST"]),
    Route("/ingest/{upload_id}/chunk/{index}", put_chunk, methods=["PUT"]),
    Route("/ingest/{upload_id}/status", status, methods=["GET"]),
    Route("/ingest/{upload_id}/finalize", finalize, methods=["POST"]),
]

app = Starlette(routes=routes, on_startup=[_startup])
