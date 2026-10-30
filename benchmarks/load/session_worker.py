"""One client session of the load test (runs as its own OS process).

A session uploads or downloads a list of files with several files in flight at
once, and appends one JSON line per finished file to ``--out``. With
``--resume`` it skips files already recorded as successful, so a session that
was killed can be restarted and continue.

Upload items:   {"name", "size", "sha256"}            (read from --images-dir)
Download items: {"name", "size", "sha256", "upload_id"}  (verified, then deleted)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import httpx

from xferkit.client import AsyncDownloader, AsyncUploader, RetryPolicy
from xferkit.protocol import MiB

RETRY_COUNT = 0


@dataclass(frozen=True)
class CountingRetry(RetryPolicy):
    """Default retry policy that also counts every back-off it takes."""

    def delay(self, attempt: int, retry_after: str | None = None) -> float:
        global RETRY_COUNT
        RETRY_COUNT += 1
        return super().delay(attempt, retry_after)


def _load_done(out: Path) -> set[str]:
    done: set[str] = set()
    if out.exists():
        for line in out.read_text(encoding="utf-8").splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if record.get("ok"):
                done.add(record["name"])
    return done


async def run(args: argparse.Namespace) -> int:
    items: list[dict] = json.loads(Path(args.items).read_text(encoding="utf-8"))
    out = Path(args.out)
    done = _load_done(out) if args.resume else set()
    todo = [item for item in items if item["name"] not in done]

    statuses: Counter[int] = Counter()

    async def count_status(response: httpx.Response) -> None:
        statuses[response.status_code] += 1

    in_flight = args.file_concurrency * args.part_concurrency
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=15.0),
        limits=httpx.Limits(max_connections=in_flight + 4, max_keepalive_connections=in_flight + 4),
        event_hooks={"response": [count_status]},
    )
    retry = CountingRetry()
    semaphore = asyncio.Semaphore(args.file_concurrency)
    log = open(out, "a", encoding="utf-8")  # noqa: SIM115
    started = time.time()

    def record(entry: dict) -> None:
        log.write(json.dumps(entry, ensure_ascii=False) + "\n")
        log.flush()

    if args.role == "upload":
        images_dir = Path(args.images_dir)
        uploader = AsyncUploader(
            f"{args.server}/uploads",
            client=client,
            concurrency=args.part_concurrency,
            chunk_size=args.chunk_size,
            retry=retry,
            state_dir=args.state_dir,
        )

        async def handle(item: dict) -> None:
            async with semaphore:
                t0 = time.time()
                entry = {"name": item["name"], "bytes": item["size"], "t_start": t0}
                try:
                    result = await uploader.upload(
                        images_dir / item["name"],
                        # Exactly-once even if this process dies after upload()
                        # returned but before the result line below is written.
                        idempotency_key=f"{args.key_prefix}/{item['name']}",
                    )
                    entry.update(ok=True, upload_id=result.upload_id, resumed=result.resumed)
                except Exception as exc:
                    entry.update(ok=False, error=f"{type(exc).__name__}: {exc}"[:300])
                entry.update(t_end=time.time(), seconds=time.time() - t0)
                record(entry)

    else:
        download_dir = Path(args.download_dir)
        download_dir.mkdir(parents=True, exist_ok=True)
        downloader = AsyncDownloader(
            client=client,
            concurrency=args.part_concurrency,
            segment_size=args.segment_size,
            retry=retry,
        )

        async def handle(item: dict) -> None:
            async with semaphore:
                t0 = time.time()
                entry = {"name": item["name"], "bytes": item["size"], "t_start": t0}
                dest = download_dir / item["name"]
                try:
                    await downloader.download(
                        f"{args.server}/uploads/{item['upload_id']}/content",
                        dest,
                        overwrite=True,
                        checksum=f"sha256={item['sha256']}",  # verified before rename
                    )
                    entry.update(ok=True, upload_id=item["upload_id"], verified=True)
                except Exception as exc:
                    entry.update(ok=False, error=f"{type(exc).__name__}: {exc}"[:300])
                finally:
                    dest.unlink(missing_ok=True)  # keep disk usage flat
                entry.update(t_end=time.time(), seconds=time.time() - t0)
                record(entry)

    try:
        await asyncio.gather(*(handle(item) for item in todo))
    finally:
        log.close()
        await client.aclose()

    lines = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines() if line]
    failed = sum(1 for line in lines if not line.get("ok"))
    summary = {
        "pid": os.getpid(),
        "role": args.role,
        "items": len(items),
        "skipped_already_done": len(done),
        "attempted": len(todo),
        "retries": RETRY_COUNT,
        "status_counts": {str(k): v for k, v in sorted(statuses.items())},
        "elapsed": time.time() - started,
    }
    Path(args.summary).write_text(json.dumps(summary), encoding="utf-8")
    return 1 if failed else 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["upload", "download"], required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--items", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--summary", required=True)
    parser.add_argument("--images-dir")
    parser.add_argument("--download-dir")
    parser.add_argument("--state-dir")
    parser.add_argument("--file-concurrency", type=int, default=4)
    parser.add_argument("--part-concurrency", type=int, default=2)
    parser.add_argument("--chunk-size", type=int)
    parser.add_argument("--segment-size", type=int, default=4 * MiB)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--key-prefix", default="", help="idempotency key namespace")
    raise SystemExit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
