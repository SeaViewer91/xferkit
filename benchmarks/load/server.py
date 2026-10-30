"""xferkit server used by the load test.

    XFERKIT_LOAD_STORE=/path/to/store \
    uvicorn server:app --app-dir benchmarks/load --workers 4 --port 8000

Every worker process builds its own UploadManager on the same storage
directory, which is exactly the multi-process setup the test checks.
"""

from __future__ import annotations

import os

from fastapi import FastAPI

from xferkit import UploadManager
from xferkit.integrations.fastapi import create_upload_router

STORE = os.environ.get("XFERKIT_LOAD_STORE", "./loadtest-data/store")

manager = UploadManager(STORE, ttl=6 * 3600)
app = FastAPI(title="xferkit load-test server")
app.include_router(create_upload_router(manager, prefix="/uploads", expose_downloads=True))


@app.get("/healthz")
async def healthz() -> dict[str, object]:
    return {"ok": True, "pid": os.getpid()}
