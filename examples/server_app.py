"""Example xferkit server with a browser demo.

Run:
    pip install -e ".[dev]"
    uvicorn examples.server_app:app --port 8000

Then open http://127.0.0.1:8000 and drop some files or folders on the page, or
use the CLI:
    xferkit upload some_big_file.bin http://127.0.0.1:8000/uploads
    xferkit download http://127.0.0.1:8000/uploads/<upload_id>/content ./copy.bin

To keep uploads in an S3-compatible bucket instead of on disk:
    XFERKIT_S3_BUCKET=my-bucket [XFERKIT_S3_ENDPOINT=http://127.0.0.1:9000] uvicorn ...

The /demo/* endpoints stand in for your application: in a real service they sit
behind your login and issue grants only to authenticated users.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import secrets
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse

from xferkit import CompletedUpload, GrantSigner, LocalStorage, UploadManager
from xferkit.integrations.fastapi import (
    create_upload_router,
    mount_browser_client,
    range_file_response,
)

HERE = Path(__file__).resolve().parent
STORAGE = Path(os.environ.get("XFERKIT_STORAGE", "./xferkit-data")).resolve()
PUBLIC = STORAGE / "public"
PUBLIC.mkdir(parents=True, exist_ok=True)


def _grant_secret() -> str:
    """The app that issues grants and every transfer server (all workers!) must
    share one secret. Set XFERKIT_GRANT_SECRET in production; for the demo a
    secret is created once in the storage directory."""
    if os.environ.get("XFERKIT_GRANT_SECRET"):
        return os.environ["XFERKIT_GRANT_SECRET"]
    path = STORAGE / ".grant-secret"
    try:
        with open(path, "x", encoding="ascii") as fh:  # first worker creates it
            fh.write(secrets.token_urlsafe(32))
    except FileExistsError:
        pass
    return path.read_text(encoding="ascii").strip()


signer = GrantSigner(_grant_secret())


def _storage():
    """Local disk by default; an S3-compatible bucket when XFERKIT_S3_BUCKET is set
    (credentials from the usual AWS environment variables / config files)."""
    bucket = os.environ.get("XFERKIT_S3_BUCKET")
    if bucket:
        from xferkit.server.s3 import S3Storage  # pip install "xferkit[s3]"

        return S3Storage(bucket, endpoint_url=os.environ.get("XFERKIT_S3_ENDPOINT"))
    return LocalStorage(STORAGE / "store", min_free_bytes=2 * 1024**3)  # keep 2 GiB free


manager = UploadManager(
    _storage(),
    ttl=24 * 3600,  # unfinished uploads expire after a day
    max_pending_per_owner=50,
    completed_ttl=7 * 24 * 3600,  # completion records kept for a week
)


async def on_complete(completed: CompletedUpload, request: Request) -> None:
    # Your application logic goes here: move the file, write a DB row, queue a job...
    print(f"[xferkit] {completed.owner}: {completed.filename} ({completed.size} bytes)")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    async def janitor() -> None:
        while True:
            await asyncio.to_thread(manager.cleanup_expired)
            await asyncio.sleep(3600)

    task = asyncio.create_task(janitor())
    yield
    task.cancel()


app = FastAPI(title="xferkit example", lifespan=lifespan)
app.include_router(
    create_upload_router(
        manager,
        prefix="/uploads",
        grants=signer,  # accept signed grants (browser demo)
        on_complete=on_complete,
        expose_downloads=True,
    )
)
mount_browser_client(app)  # serves the browser client at /xferkit.mjs
# If the page is served from another origin:
#   from xferkit.integrations.fastapi import add_cors
#   add_cors(app, ["https://your-frontend.example"])


@app.get("/", include_in_schema=False)
async def demo_page():
    return FileResponse(HERE / "browser" / "index.html")


@app.get("/demo/grant")
async def demo_grant():
    """Upload grant for the demo user (protect this with your login!)."""
    return {
        "grant": signer.issue(owner="demo", prefix="/uploads", methods="*", ttl=3600),
        "baseUrl": "/uploads",
    }


@app.get("/demo/link/{upload_id}")
async def demo_link(upload_id: str, request: Request):
    """A 10-minute download link for one upload."""
    url = str(request.url_for("xferkit_uploads_status", upload_id=upload_id)) + "/content"
    return {"url": signer.sign_url(url, ttl=600)}


@app.head("/files/{name}", include_in_schema=False)
@app.get("/files/{name}")
async def get_public_file(name: str, request: Request):
    path = (PUBLIC / name).resolve()
    if path.parent != PUBLIC:  # block path traversal
        raise HTTPException(status_code=404)
    return range_file_response(request, path)
