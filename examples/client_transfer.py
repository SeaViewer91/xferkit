"""Upload a file to the example server and download it back.

Usage:
    python examples/client_transfer.py path/to/file [http://127.0.0.1:8000]
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from xferkit.client import AsyncDownloader, AsyncUploader


def show(label: str):
    def callback(progress) -> None:
        print(f"\r{label}: {progress.fraction:6.1%}", end="", flush=True)

    return callback


async def main(path: Path, server: str) -> None:
    async with AsyncUploader(
        f"{server}/uploads", concurrency=6, state_dir=Path.home() / ".cache" / "xferkit"
    ) as uploader:
        result = await uploader.upload(path, progress=show("upload"))
    print(f"\nuploaded {result.upload_id} {result.composite_checksum}")

    dest = path.with_name(path.stem + ".downloaded" + path.suffix)
    async with AsyncDownloader(concurrency=6) as downloader:
        await downloader.download(
            f"{server}/uploads/{result.upload_id}/content",
            dest,
            overwrite=True,
            progress=show("download"),
        )
    print(f"\nsaved {dest}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    asyncio.run(
        main(Path(sys.argv[1]), sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8000")
    )
