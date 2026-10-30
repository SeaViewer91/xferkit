"""Minimal xferkit server.

Run:
    pip install -e ".[dev]"
    uvicorn examples.server_app:app --port 8000

Then, in another terminal:
    xferkit upload some_big_file.bin http://127.0.0.1:8000/uploads
    xferkit download http://127.0.0.1:8000/uploads/<upload_id>/content ./copy.bin
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request

from xferkit import CompletedUpload, UploadManager
from xferkit.integrations.fastapi import create_upload_router, range_file_response

STORAGE = Path(os.environ.get("XFERKIT_STORAGE", "./xferkit-data")).resolve()
PUBLIC = STORAGE / "public"
PUBLIC.mkdir(parents=True, exist_ok=True)

manager = UploadManager(STORAGE / "store", ttl=24 * 3600)


async def on_complete(completed: CompletedUpload, request: Request) -> None:
    # Your application logic goes here: move the file, write a DB row, queue a job...
    print(f"[xferkit] {completed.filename} ({completed.size} bytes) -> {completed.path}")


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
    create_upload_router(manager, prefix="/uploads", on_complete=on_complete, expose_downloads=True)
)


@app.head("/files/{name}", include_in_schema=False)
@app.get("/files/{name}")
async def get_public_file(name: str, request: Request):
    path = (PUBLIC / name).resolve()
    if path.parent != PUBLIC:  # block path traversal
        raise HTTPException(status_code=404)
    return range_file_response(request, path)
