from __future__ import annotations

import os
import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import Response

from xferkit import UploadManager
from xferkit.integrations.fastapi import create_upload_router, range_file_response
from xferkit.protocol import KiB


class LiveServer:
    """Runs an ASGI app with uvicorn on a random local port in a thread."""

    def __init__(self, app: FastAPI) -> None:
        self.app = app
        self.server: uvicorn.Server | None = None
        self.thread: threading.Thread | None = None
        self.url = ""

    def start(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        config = uvicorn.Config(self.app, log_level="warning", lifespan="off")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(
            target=self.server.run, kwargs={"sockets": [sock]}, daemon=True
        )
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("server did not start")
            time.sleep(0.01)
        self.url = f"http://127.0.0.1:{port}"

    def stop(self) -> None:
        if self.server is not None:
            self.server.should_exit = True
        if self.thread is not None:
            self.thread.join(timeout=10)


@pytest.fixture
def manager(tmp_path: Path) -> UploadManager:
    return UploadManager(
        tmp_path / "store",
        min_chunk_size=1 * KiB,
        default_chunk_size=64 * KiB,
    )


@pytest.fixture
def app(manager: UploadManager, tmp_path: Path) -> FastAPI:
    app = FastAPI()
    completed: list = []

    def remember(record, request):
        completed.append(record)

    app.include_router(create_upload_router(manager, expose_downloads=True, on_complete=remember))

    public = tmp_path / "public"
    public.mkdir()

    @app.head("/files/{name}", include_in_schema=False)
    @app.get("/files/{name}")
    async def get_file(name: str, request: Request):
        return range_file_response(request, public / name)

    @app.get("/plain/{name}")
    async def get_plain(name: str):
        # A server without range support.
        return Response((public / name).read_bytes(), media_type="application/octet-stream")

    app.state.completed = completed
    app.state.public = public
    return app


@pytest.fixture
def live_server(app: FastAPI) -> Iterator[LiveServer]:
    server = LiveServer(app)
    server.start()
    yield server
    server.stop()


@pytest.fixture
def random_file(tmp_path: Path):
    def make(name: str, size: int, directory: Path | None = None) -> Path:
        path = (directory or tmp_path) / name
        path.write_bytes(os.urandom(size))
        return path

    return make
