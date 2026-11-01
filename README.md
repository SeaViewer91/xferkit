# xferkit

**English** | [한국어](https://github.com/SeaViewer91/xferkit/blob/main/README.ko.md)

**Chunked, resumable large-file upload/download SDK for Python servers — FastAPI-ready, with a browser client.**

xferkit moves very large files (tens or hundreds of GB) over plain HTTP:

- **Parallel, resumable uploads.** Files are sent in parts, several at once. An interrupted upload continues where it stopped, even after the client process restarts or the page reloads.
- **Parallel, resumable downloads.** HTTP range requests, `ETag`/`If-Range` validation, and automatic restart if the remote file changed.
- **End-to-end integrity.** Every part is SHA-256 verified on arrival, and both sides compare a composite checksum of the whole file without re-reading it.
- **Folders and many small files.** Upload a whole folder (or thousands of files) with per-file results; small files cost one request each, and re-running skips what is already there.
- **Exactly once.** Session creation is idempotent and `idempotency_key` maps an application key to one upload, so crashes and retries never create duplicates.
- **Built-in access control.** Uploads belong to the caller that created them; signed grants (presigned URLs) give browsers and workers scoped, expiring access.
- **Safe under load.** Disk-space reservation, per-owner limits, rate limiting, retention of completion records, and crash-safe state (verified with fault injection).
- **Local disk or S3.** Parts are written straight into their final position on disk (completing a 100 GB upload is a rename, not a copy), or into an S3-compatible bucket as a multipart upload.
- **Framework-agnostic core.** The server core has zero dependencies. FastAPI is one integration; Flask, Django, aiohttp or your own server can call the same `UploadManager`.
- **Multi-worker safe.** Several uvicorn/gunicorn workers can share one storage directory without a database.

```
pip install "xferkit[fastapi]"   # server with FastAPI
pip install "xferkit[client]"    # Python client + CLI
pip install "xferkit[s3]"        # S3-compatible storage backend (boto3)
pip install "xferkit[all]"       # all of the above
```

Requires Python 3.10+. The browser client is bundled with the Python package (and published as `xferkit` on npm).

## Server (FastAPI)

```python
import os
from fastapi import FastAPI, Request
from xferkit import GrantSigner, LocalStorage, UploadManager
from xferkit.integrations.fastapi import add_cors, create_upload_router, mount_browser_client

manager = UploadManager(
    LocalStorage("/data/xferkit", min_free_bytes=10 * 1024**3),  # refuse uploads that would eat the last 10 GB
    max_upload_size=500 * 1024**3,
    ttl=24 * 3600,                  # unfinished uploads expire after a day
    max_pending_per_owner=20,       # uploads in progress per user
    completed_ttl=7 * 24 * 3600,    # how long completion records are kept
)
signer = GrantSigner(os.environ["XFERKIT_GRANT_SECRET"])

async def current_user(request: Request) -> str | None:
    ...  # your authentication: session cookie, JWT, API key → user id (or None)

async def on_complete(completed, request: Request):
    # completed.path is the assembled file, completed.owner who uploaded it
    ...

app = FastAPI()
app.include_router(
    create_upload_router(
        manager,
        prefix="/uploads",
        principal=current_user,   # who is calling
        grants=signer,            # also accept signed grants
        require_auth=True,        # no anonymous uploads
        on_complete=on_complete,
        expose_downloads=True,    # GET /uploads/{id}/content with Range support
    )
)
mount_browser_client(app)                       # serves /xferkit.mjs
add_cors(app, ["https://app.example.com"])      # if the web app lives on another origin
```

Call `manager.cleanup_expired()` periodically (e.g. hourly) to remove abandoned sessions, expired records and crash leftovers. See [examples/server_app.py](https://github.com/SeaViewer91/xferkit/blob/main/examples/server_app.py) for a runnable server with a drag-and-drop browser demo.

### Access control

- **Ownership.** An upload belongs to the caller that created it (`principal`'s return value or the grant's owner). Every other caller gets `404` for it, so ids cannot be probed. Uploads created without an owner are reachable by anyone who knows the id.
- **Signed grants.** Your application issues short-lived, scoped tokens; the transfer server verifies them with the shared secret, nothing is stored:

  ```python
  # Let alice upload through /uploads for an hour, files up to 20 GB:
  grant = signer.issue(owner="alice", prefix="/uploads", methods="*", ttl=3600, max_size=20 * 1024**3)

  # A 10-minute download link for one file (works in <a href>, curl, anything):
  url = signer.sign_url(f"https://files.example.com/uploads/{upload_id}/content", ttl=600)
  ```

  Grants travel in the `X-Xferkit-Grant` header or the `xk` query parameter. Old secrets can stay valid during rotation (`previous_secrets=`).
- **Policies.** `authorize=` runs your own check per action (`create`, `upload_part`, `complete`, `abort`, `download`, `status`); `before_create=` on the manager can refuse uploads (quotas, file types); `rate_limit=RateLimiter(rate, burst)` answers `429` with `Retry-After`, which all clients honour.

### S3-compatible storage

```python
from xferkit.server.s3 import S3Storage

manager = UploadManager(S3Storage("my-bucket", endpoint_url="https://s3.example.com"))  # AWS, MinIO, Ceph, R2, ...
```

Each upload becomes an S3 multipart upload, assembled inside the bucket. Parts are verified before they are sent, all state lives in the bucket (any number of servers can share it), `completed.location` is `s3://bucket/key`, and downloads redirect to presigned URLs. Parts are at least 5 MiB (an S3 rule). Add a CORS rule to the bucket if browsers download from it.

### Disk space and retention

- A new upload is refused with `507 insufficient_storage` unless its size plus the space still needed by unfinished uploads fits above `min_free_bytes`. With `preallocate=True` (Linux) the space is really allocated up front.
- Completion records (needed so crashed clients can recognise finished uploads) are dropped after `completed_ttl`; set `remove_files_on_expiry=True` if the files should go too.

## Python client

```python
from xferkit.client import upload_file, download_file

result = upload_file(
    "https://files.example.com/uploads",
    "survey_2026.tif",
    metadata={"project": "coastal"},
    grant=grant,                       # or headers={"Authorization": "Bearer ..."}
    concurrency=6,
    state_dir="~/.cache/xferkit",      # resume across runs
    idempotency_key="coastal/survey_2026.tif",  # re-running never uploads a second copy
)
print(result.upload_id, result.composite_checksum)

download_file(url, "copy.tif", concurrency=6)
```

Folders and many files:

```python
from xferkit.client import upload_folder

result = upload_folder(
    "https://files.example.com/uploads", "survey_2026/",
    include="*.JPG", metadata={"project": "coastal"}, grant=grant, concurrency=8,
)
for failure in result.failed:
    print(failure.path, failure.error)
```

Every file keeps its name and gets `metadata["relative_path"]`. Running the same call again resumes unfinished files and skips finished ones (after verifying them). `upload_many([...])` does the same for a list of paths.

Async code uses `AsyncUploader` / `AsyncDownloader` directly. The downloader works with any server that supports range requests (nginx, S3, CDNs).

## Browser client

```html
<script type="module">
  import { upload, download } from "/xferkit.mjs";   // served by mount_browser_client(app)

  const { grant } = await (await fetch("/api/upload-grant")).json();   // issued by your app
  const result = await upload(fileInput.files[0], {
    baseUrl: "https://files.example.com/uploads",
    grant,
    onProgress: (p) => (bar.value = p.fraction),
  });
</script>
```

A whole folder (`<input type="file" webkitdirectory>`) or a drop with folders:

```js
import { uploadFiles, filesFromDataTransfer } from "/xferkit.mjs";

const batch = await uploadFiles(folderInput.files, { baseUrl, grant, onProgress });
dropZone.ondrop = async (e) => { e.preventDefault(); await uploadFiles(await filesFromDataTransfer(e.dataTransfer), { baseUrl, grant }); };
```

Same guarantees as the Python client: parallel parts, SHA-256 per part, composite verification, resume after reload or `AbortController.abort()`, idempotency keys (shared with Python). See [clients/js](https://github.com/SeaViewer91/xferkit/blob/main/clients/js/README.md).

## CLI

```
xferkit upload big.tif https://example.com/uploads -c 6 --grant "$GRANT"
xferkit upload ./survey_2026 https://example.com/uploads --include '*.JPG'   # a folder
xferkit download "https://example.com/uploads/abc/content?xk=..." ./big.tif -c 6
```

Interrupt with Ctrl+C and run the same command again to resume.

## Other frameworks

`UploadManager` is plain synchronous Python. Map the five protocol endpoints onto your framework, check access with `manager.ensure_access(upload_id, caller)`, and stream each part body into `manager.open_part()`:

```python
with manager.open_part(upload_id, index) as sink:
    for block in request_body_iterator:
        sink.write(block)
    record = sink.commit(request.headers.get("X-Xferkit-Checksum"))
```

`xferkit.server.plan_file_response()` + `iter_file_range()` do the same for range downloads, and `GrantSigner.verify()` for grants.

## How it works

See [docs/PROTOCOL.md](https://github.com/SeaViewer91/xferkit/blob/main/docs/PROTOCOL.md) for the wire protocol and [docs/DESIGN.md](https://github.com/SeaViewer91/xferkit/blob/main/docs/DESIGN.md) for the storage design, the security model and their guarantees.

## Development

```
./scripts/setup_venv.sh          # macOS / Linux  (Windows: scripts\setup_venv.ps1)
source .venv/bin/activate
pytest                           # browser tests run if Playwright + Chromium are installed
ruff check src tests && ruff format --check src tests
cd clients/js && npm install && npm run build    # after changing the browser client
```

Load test (thousands of ~10 MB images, 10 concurrent sessions, fault injection): see [benchmarks/load](https://github.com/SeaViewer91/xferkit/blob/main/benchmarks/load/README.md).

The README is maintained in English and Korean ([README.ko.md](https://github.com/SeaViewer91/xferkit/blob/main/README.ko.md)); please update both when changing it.

## License

MIT
