# xferkit

**English** | [한국어](README.ko.md)

**Chunked, resumable large-file upload/download SDK for Python servers — FastAPI-ready.**

xferkit moves very large files (tens or hundreds of GB) over plain HTTP:

- **Parallel, resumable uploads.** Files are sent in parts, several at once. An interrupted upload continues where it stopped, even after the client process restarts.
- **Parallel, resumable downloads.** HTTP range requests, `ETag`/`If-Range` validation, and automatic restart if the remote file changed.
- **End-to-end integrity.** Every part is SHA-256 verified on arrival, and both sides compare a composite checksum of the whole file without re-reading it.
- **No double disk usage.** Parts are written straight into their final position, so completing a 100 GB upload is a rename, not a 100 GB copy.
- **Framework-agnostic core.** The server core has zero dependencies. FastAPI is one integration; Flask, Django, aiohttp or your own server can call the same `UploadManager`.
- **Multi-worker safe.** State lives in per-part marker files with atomic renames, so several uvicorn/gunicorn workers can share one storage directory without a database.

```
pip install "xferkit[fastapi]"   # server with FastAPI
pip install "xferkit[client]"    # client + CLI
pip install "xferkit[all]"       # both
```

Requires Python 3.10+.

## Server (FastAPI)

```python
from fastapi import Depends, FastAPI, Request
from xferkit import UploadManager
from xferkit.integrations.fastapi import create_upload_router, range_file_response

manager = UploadManager(
    "/data/xferkit",            # storage root (one filesystem)
    max_upload_size=500 * 1024**3,
    ttl=24 * 3600,              # unfinished sessions expire after a day
)

async def on_complete(completed, request: Request):
    # completed.path is the assembled file; move it, register it in your DB, ...
    print(completed.upload_id, completed.filename, completed.size, completed.path)

app = FastAPI()
app.include_router(
    create_upload_router(
        manager,
        prefix="/uploads",
        dependencies=[Depends(require_user)],   # protect every endpoint
        on_complete=on_complete,
    )
)

# Serve any file with Range / If-Range / ETag support (resumable, parallel downloads)
@app.head("/files/{name}", include_in_schema=False)
@app.get("/files/{name}")
async def get_file(name: str, request: Request):
    return range_file_response(request, resolve_safely(name))
```

Call `manager.cleanup_expired()` periodically (e.g. from a scheduler) to delete abandoned sessions. See [examples/server_app.py](examples/server_app.py) for a runnable server.

## Client

```python
from xferkit.client import upload_file, download_file

result = upload_file(
    "https://example.com/uploads",
    "survey_2026.tif",
    metadata={"project": "coastal"},
    headers={"Authorization": "Bearer ..."},
    concurrency=6,
    state_dir="~/.cache/xferkit",      # enables resume across runs
)
print(result.upload_id, result.composite_checksum)

download_file("https://example.com/files/survey_2026.tif", "copy.tif", concurrency=6)
```

Async code uses the same objects directly:

```python
from xferkit.client import AsyncUploader, AsyncDownloader

async with AsyncUploader("https://example.com/uploads", concurrency=8) as uploader:
    result = await uploader.upload("big.bin", progress=lambda p: print(f"{p.fraction:.1%}"))

async with AsyncDownloader(concurrency=8, segment_size=32 * 1024**2) as downloader:
    await downloader.download("https://example.com/files/big.bin", "big.bin")
```

The downloader works with any server that supports range requests (nginx, S3, CDNs), not only xferkit servers.

## CLI

```
xferkit upload big.tif https://example.com/uploads -c 6 -H "Authorization: Bearer ..."
xferkit download https://example.com/files/big.tif ./big.tif -c 6
```

Interrupt with Ctrl+C and run the same command again to resume.

## Other frameworks

`UploadManager` is plain synchronous Python. Map the five protocol endpoints onto your framework and stream each part body into `manager.open_part()`:

```python
with manager.open_part(upload_id, index) as sink:
    for block in request_body_iterator:
        sink.write(block)
    record = sink.commit(request.headers.get("X-Xferkit-Checksum"))
```

`xferkit.server.plan_file_response()` + `iter_file_range()` do the same for range downloads.

## How it works

See [docs/PROTOCOL.md](docs/PROTOCOL.md) for the wire protocol and [docs/DESIGN.md](docs/DESIGN.md) for the storage design and its guarantees.

## Development

```
./scripts/setup_venv.sh          # macOS / Linux  (Windows: scripts\setup_venv.ps1)
source .venv/bin/activate
pytest
ruff check src tests && ruff format --check src tests
```

## License

MIT
