"""Many small files: one ``upload()`` per file vs. ``upload_many()``.

    python benchmarks/small_files.py --files 2000 --size 64K --server-workers 2

Starts a uvicorn server on a temporary store, uploads the same set of files
twice (each mode into its own store) and prints files/s and HTTP requests per
file. A third run repeats ``upload_many()`` on the unchanged files to show the
cost of re-running a batch whose files are already uploaded.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from xferkit.client import AsyncUploader  # noqa: E402
from xferkit.protocol import parse_size  # noqa: E402

SERVER = """
import os
from fastapi import FastAPI, Request
from xferkit import LocalStorage, UploadManager
from xferkit.integrations.fastapi import create_upload_router

manager = UploadManager(LocalStorage(os.environ["STORE"], min_free_bytes=0, fsync=False))
app = FastAPI()
app.include_router(create_upload_router(manager))
COUNT = os.environ["COUNT"]

@app.middleware("http")
async def count(request: Request, call_next):
    with open(COUNT, "a") as fh:
        fh.write("x")
    return await call_next(request)
"""


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, workdir: Path, workers: int) -> None:
        self.store = workdir / "store"
        self.count = workdir / "requests"
        app = workdir / "bench_app.py"
        app.write_text(SERVER)
        self.port = free_port()
        env = {**os.environ, "STORE": str(self.store), "COUNT": str(self.count)}
        env["PYTHONPATH"] = os.pathsep.join([str(ROOT / "src"), str(workdir)])
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "bench_app:app",
                "--port",
                str(self.port),
                "--workers",
                str(workers),
                "--log-level",
                "warning",
            ],
            env=env,
            cwd=workdir,
        )
        self.url = f"http://127.0.0.1:{self.port}/uploads"
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                httpx.get(f"http://127.0.0.1:{self.port}/docs", timeout=1)
                break
            except httpx.TransportError:
                time.sleep(0.1)
        self.reset()

    def requests(self) -> int:
        return self.count.stat().st_size if self.count.exists() else 0

    def reset(self) -> None:
        self.count.write_text("")

    def stop(self) -> None:
        self.proc.terminate()
        self.proc.wait(10)


async def per_file(url: str, files: list[Path], concurrency: int) -> None:
    gate = asyncio.Semaphore(concurrency)
    async with AsyncUploader(url, concurrency=2) as uploader:

        async def one(path: Path) -> None:
            async with gate:
                await uploader.upload(path)

        await asyncio.gather(*(one(p) for p in files))


async def batched(url: str, root: Path, concurrency: int) -> None:
    async with AsyncUploader(url, concurrency=concurrency) as uploader:
        result = await uploader.upload_folder(root, file_concurrency=concurrency)
    result.raise_for_failures()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", type=int, default=2000)
    parser.add_argument("--size", type=parse_size, default=parse_size("64K"))
    parser.add_argument("--concurrency", type=int, default=16, help="files in flight")
    parser.add_argument("--server-workers", type=int, default=2)
    parser.add_argument("--workdir", type=Path, default=None)
    args = parser.parse_args()

    work = Path(args.workdir or tempfile.mkdtemp(prefix="xferkit-small-"))
    data = work / "files"
    data.mkdir(parents=True, exist_ok=True)
    for i in range(args.files):
        path = data / f"d{i // 500:02d}" / f"f{i:05d}.bin"
        path.parent.mkdir(exist_ok=True)
        if not path.exists():
            path.write_bytes(os.urandom(args.size))
    files = sorted(data.rglob("*.bin"))
    total_mib = len(files) * args.size / 2**20
    print(
        f"{len(files)} files × {args.size} B = {total_mib:.0f} MiB, "
        f"{args.concurrency} files in flight, {args.server_workers} server workers"
    )

    rows = []
    for mode in ("upload() per file", "upload_many()", "upload_many() again"):
        if mode != "upload_many() again":
            run_dir = work / mode.split("(")[0]
            shutil.rmtree(run_dir, ignore_errors=True)
            run_dir.mkdir()
            server = Server(run_dir, args.server_workers)
        server.reset()
        started = time.perf_counter()
        if mode.startswith("upload() per file"):
            asyncio.run(per_file(server.url, files, args.concurrency))
        else:
            asyncio.run(batched(server.url, data, args.concurrency))
        elapsed = time.perf_counter() - started
        requests = server.requests()
        rows.append(
            (mode, elapsed, len(files) / elapsed, total_mib / elapsed, requests / len(files))
        )
        if mode != "upload_many()":
            server.stop()

    print()
    print("| mode | seconds | files/s | MiB/s | requests/file |")
    print("|---|---:|---:|---:|---:|")
    for mode, elapsed, fps, mibps, rpf in rows:
        print(f"| {mode} | {elapsed:.1f} | {fps:,.0f} | {mibps:,.1f} | {rpf:.2f} |")
    if args.workdir is None:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
