#!/usr/bin/env python3
"""upload_test.py — a pure-stdlib resumable ingest client.

Builds the manifest by streamed 1 MB reads (md5 per chunk + sha256 whole file),
POSTs /ingest/init, PUTs the missing chunks, and can be killed and resumed at any
point — it re-reads /status and only sends what the server is still missing.

Memory contract: the whole file is NEVER loaded into RAM. We seek() to each
chunk offset and read exactly one chunk into a reused buffer.

Usage:
  upload_test.py --url https://up.osmike.com --token TOK --file /tmp/big.bin
  upload_test.py ... --upload-id my-id           # stable id → resume
  upload_test.py ... --parallel 4                # concurrent chunk PUTs
  upload_test.py ... --corrupt-chunk 3           # flip a byte in chunk 3 (test 422)
  upload_test.py ... --kill-after 0.4            # exit after ~40% of chunks (test resume)
"""

import argparse
import hashlib
import json
import os
import ssl
import sys
import threading
import urllib.request
import urllib.error
import uuid
from concurrent.futures import ThreadPoolExecutor

BUF = 1024 * 1024  # 1 MB streamed-read buffer


def build_manifest(path, chunk_size, upload_id):
    total_size = os.path.getsize(path)
    count = (total_size + chunk_size - 1) // chunk_size
    chunks = {}
    whole = hashlib.sha256()
    with open(path, "rb") as f:
        for i in range(count):
            f.seek(i * chunk_size)
            remaining = min(chunk_size, total_size - i * chunk_size)
            md5 = hashlib.md5()
            read = 0
            while read < remaining:
                buf = f.read(min(BUF, remaining - read))
                if not buf:
                    break
                md5.update(buf)
                whole.update(buf)
                read += len(buf)
            chunks[str(i)] = "md5:" + md5.hexdigest()
    return {
        "upload_id": upload_id,
        "filename": os.path.basename(path),
        "content_type": "application/octet-stream",
        "total_size": total_size,
        "chunk_size": chunk_size,
        "count": count,
        "file_hash": "sha256:" + whole.hexdigest(),
        "chunks": chunks,
    }


def _ctx(insecure):
    if not insecure:
        return None
    c = ssl.create_default_context()
    c.check_hostname = False
    c.verify_mode = ssl.CERT_NONE
    return c


def _req(method, url, token, data=None, ctx=None, ctype=None):
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", "Bearer " + token)
    if ctype:
        req.add_header("Content-Type", ctype)
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=120) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="base URL, e.g. https://up.osmike.com")
    ap.add_argument("--token", required=True)
    ap.add_argument("--file", required=True)
    ap.add_argument("--chunk-size", type=int, default=8 * 1024 * 1024)
    ap.add_argument("--upload-id", default=None)
    ap.add_argument("--parallel", type=int, default=4)
    ap.add_argument("--insecure", action="store_true", help="skip TLS verify (self-signed)")
    ap.add_argument("--corrupt-chunk", type=int, default=None,
                    help="flip a byte in this chunk before sending (expect 422)")
    ap.add_argument("--kill-after", type=float, default=None,
                    help="exit after this fraction of chunks are sent (test resume)")
    args = ap.parse_args()

    base = args.url.rstrip("/")
    ctx = _ctx(args.insecure)

    # Stable upload_id (cache next to the file so a re-run resumes automatically).
    idfile = args.file + ".uploadid"
    upload_id = args.upload_id
    if not upload_id and os.path.exists(idfile):
        upload_id = open(idfile).read().strip()
    if not upload_id:
        upload_id = uuid.uuid4().hex
    with open(idfile, "w") as f:
        f.write(upload_id)

    print(f"[*] building manifest for {args.file} (id={upload_id}) ...", flush=True)
    manifest = build_manifest(args.file, args.chunk_size, upload_id)
    print(f"    total_size={manifest['total_size']} count={manifest['count']} "
          f"file_hash={manifest['file_hash']}", flush=True)

    st, body = _req("POST", base + "/ingest/init", args.token,
                    data=json.dumps(manifest).encode(), ctx=ctx, ctype="application/json")
    if st != 200:
        print(f"[!] init failed HTTP {st}: {body.decode(errors='replace')}")
        sys.exit(1)
    init = json.loads(body)
    print(f"[*] init ok resumed={init.get('resumed')} missing={len(init['missing'])}", flush=True)

    # Source of truth for what to send: the server's /status missing list.
    st, body = _req("GET", f"{base}/ingest/{upload_id}/status", args.token, ctx=ctx)
    status = json.loads(body)
    missing = list(status["missing"])
    total_chunks = manifest["count"]
    print(f"[*] server missing {len(missing)}/{total_chunks} chunks", flush=True)

    chunk_size = manifest["chunk_size"]
    total_size = manifest["total_size"]
    sent = {"n": 0}
    lock = threading.Lock()
    file_lock = threading.Lock()
    kill_at = int(args.kill_after * total_chunks) if args.kill_after else None
    stop = threading.Event()
    errors = []

    def send_one(index):
        if stop.is_set():
            return
        remaining = min(chunk_size, total_size - index * chunk_size)
        with file_lock:
            with open(args.file, "rb") as f:
                f.seek(index * chunk_size)
                data = f.read(remaining)
        if args.corrupt_chunk is not None and index == args.corrupt_chunk:
            b = bytearray(data)
            b[0] ^= 0xFF
            data = bytes(b)
        st, resp = _req("PUT", f"{base}/ingest/{upload_id}/chunk/{index}",
                        args.token, data=data, ctx=ctx,
                        ctype="application/octet-stream")
        if st != 200:
            errors.append((index, st, resp[:200]))
            print(f"[!] chunk {index} -> HTTP {st}: {resp.decode(errors='replace')[:120]}", flush=True)
            return
        with lock:
            sent["n"] += 1
            if sent["n"] % 20 == 0 or sent["n"] == len(missing):
                print(f"    sent {sent['n']}/{len(missing)}", flush=True)
            if kill_at is not None and sent["n"] >= kill_at:
                print(f"[X] KILL: sent {sent['n']} chunks (~{args.kill_after*100:.0f}%), exiting hard.", flush=True)
                os._exit(3)

    with ThreadPoolExecutor(max_workers=args.parallel) as ex:
        list(ex.map(send_one, missing))

    if errors:
        print(f"[!] {len(errors)} chunk error(s); not finalizing. First: {errors[0]}", flush=True)
        sys.exit(2)

    # Explicit finalize (also auto-fires server-side on the last chunk).
    st, body = _req("POST", f"{base}/ingest/{upload_id}/finalize", args.token, ctx=ctx)
    print(f"[*] finalize HTTP {st}: {body.decode(errors='replace')}", flush=True)
    if st == 200:
        os.remove(idfile) if os.path.exists(idfile) else None
        print("[✓] upload complete + finalized.", flush=True)
        sys.exit(0)
    sys.exit(4)


if __name__ == "__main__":
    main()
