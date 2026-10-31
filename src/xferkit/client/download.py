"""Parallel, resumable downloader based on HTTP range requests.

Works with any server that supports ``Range`` (S3, nginx, most CDNs, and
:func:`xferkit.integrations.fastapi.range_file_response`). Servers without
range support are downloaded in a single stream.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import re
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

import httpx

from ..errors import IntegrityError, RemoteChangedError, TransferError
from ..grants import GRANT_HEADER
from ..protocol import MiB, format_checksum, parse_checksum
from ._common import (
    Journal,
    ProgressCallback,
    ProgressTracker,
    RetryPolicy,
    make_client,
    response_error,
    run_sync,
)

__all__ = ["AsyncDownloader", "DownloadResult", "Downloader", "download_file"]

PART_SUFFIX = ".xferkit-part"
STATE_SUFFIX = ".xferkit-state"
_CONTENT_RANGE_RE = re.compile(r"^\s*bytes\s+(?:(\d+)-(\d+)|\*)/(\d+|\*)\s*$", re.IGNORECASE)
_WRITE_BLOCK = MiB


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    size: int
    etag: str | None
    resumed: bool
    ranged: bool


@dataclass(frozen=True)
class _Probe:
    size: int | None
    etag: str | None
    last_modified: str | None
    ranged: bool

    @property
    def validator(self) -> str | None:
        # Weak ETags cannot be used with If-Range; fall back to the date.
        if self.etag and not self.etag.startswith("W/"):
            return self.etag
        return self.last_modified


class _ShortRead(Exception):
    pass


def _parse_content_range(value: str | None) -> tuple[int | None, int | None, int | None]:
    match = _CONTENT_RANGE_RE.match(value or "")
    if match is None:
        return None, None, None
    start, end, total = match.groups()
    return (
        None if start is None else int(start),
        None if end is None else int(end),
        None if total in (None, "*") else int(total),
    )


def _write_at(fh: IO[bytes], offset: int, data: bytes) -> None:
    fh.seek(offset)
    fh.write(data)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(4 * MiB):
            digest.update(block)
    return digest.hexdigest()


class AsyncDownloader:
    """Downloads files with parallel range requests and resumes interrupted ones.

    Args:
        client: An existing :class:`httpx.AsyncClient` (not closed by us).
        headers: Extra headers for every request (e.g. ``Authorization``).
        concurrency: Segments downloaded at once.
        segment_size: Bytes per range request.
        retry: Retry policy for network errors and 5xx/429 responses.
        timeout: httpx timeout (default 60 s, 15 s connect).
        grant: A signed grant (see :mod:`xferkit.grants`) sent with every
            request, for servers that require one.

    While downloading, data goes to ``<dest>.xferkit-part`` and progress to
    ``<dest>.xferkit-state``. Running the same download again resumes, as long
    as the server reports the same ETag (or Last-Modified date).
    """

    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        headers: dict[str, str] | None = None,
        concurrency: int = 4,
        segment_size: int = 16 * MiB,
        retry: RetryPolicy | None = None,
        timeout: httpx.Timeout | float | None = None,
        grant: str | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if segment_size < 1:
            raise ValueError("segment_size must be >= 1")
        self.concurrency = concurrency
        self.segment_size = segment_size
        self.retry = retry or RetryPolicy()
        if grant is not None:
            headers = {**(headers or {}), GRANT_HEADER: grant}
        self._client, self._owns_client = make_client(
            client, headers=headers, timeout=timeout, concurrency=concurrency
        )
        self._headers = {} if self._owns_client else dict(headers or {})
        # Range offsets refer to the stored bytes, so ask for no compression.
        self._headers["Accept-Encoding"] = "identity"

    async def __aenter__(self) -> AsyncDownloader:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def download(
        self,
        url: str,
        dest: str | Path,
        *,
        overwrite: bool = False,
        resume: bool = True,
        checksum: str | None = None,
        progress: ProgressCallback | None = None,
    ) -> DownloadResult:
        """Download ``url`` to ``dest``.

        Args:
            overwrite: Replace ``dest`` if it already exists.
            resume: Continue an interrupted download of the same resource.
            checksum: Expected ``sha256=<hex>``; verified before ``dest`` is
                written.
            progress: Called with :class:`TransferProgress` as bytes arrive.
        """
        dest = Path(dest).expanduser()
        if dest.exists() and not overwrite:
            raise FileExistsError(str(dest))
        if checksum is not None:
            checksum = format_checksum(parse_checksum(checksum)[1])
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            return await self._download(url, dest, resume, checksum, progress)
        except RemoteChangedError:
            # The resource changed since the partial download began: start over once.
            return await self._download(url, dest, False, checksum, progress)

    # ------------------------------------------------------------------ internals

    async def _download(
        self,
        url: str,
        dest: Path,
        resume: bool,
        checksum: str | None,
        progress: ProgressCallback | None,
    ) -> DownloadResult:
        part = dest.with_name(dest.name + PART_SUFFIX)
        journal = Journal(dest.with_name(dest.name + STATE_SUFFIX))
        probe = await self._probe(url)
        tracker = ProgressTracker(probe.size or 0, progress)
        resumed = False

        if not probe.ranged or not probe.size:
            journal.remove()
            await self._download_whole(url, part, tracker)
            size = part.stat().st_size
        else:
            size = probe.size
            resumed = await self._download_ranged(url, part, journal, probe, resume, tracker)

        if checksum is not None:
            actual = format_checksum(await asyncio.to_thread(_sha256_file, part))
            if actual != checksum:
                journal.remove()
                with contextlib.suppress(FileNotFoundError):
                    part.unlink()
                raise IntegrityError(
                    "downloaded file does not match the expected checksum",
                    details={"expected": checksum, "actual": actual},
                )
        os.replace(part, dest)
        journal.remove()
        return DownloadResult(dest, size, probe.etag, resumed, probe.ranged)

    async def _probe(self, url: str) -> _Probe:
        headers = {**self._headers, "Range": "bytes=0-0"}
        for attempt in range(self.retry.attempts):
            last = attempt == self.retry.attempts - 1
            try:
                response = await self._client.send(
                    self._client.build_request("GET", url, headers=headers), stream=True
                )
            except httpx.TransportError:
                if last:
                    raise
                await asyncio.sleep(self.retry.delay(attempt))
                continue
            try:
                status = response.status_code
                etag = response.headers.get("etag")
                last_modified = response.headers.get("last-modified")
                if status == 206:
                    _, _, total = _parse_content_range(response.headers.get("content-range"))
                    if total is not None:
                        return _Probe(total, etag, last_modified, True)
                    return _Probe(None, etag, last_modified, False)
                if status == 416:
                    _, _, total = _parse_content_range(response.headers.get("content-range"))
                    return _Probe(total, etag, last_modified, False)
                if status == 200:
                    length = response.headers.get("content-length", "")
                    size = int(length) if length.isdigit() else None
                    return _Probe(size, etag, last_modified, False)
                await response.aread()
                if status in self.retry.retry_statuses and not last:
                    await asyncio.sleep(
                        self.retry.delay(attempt, response.headers.get("retry-after"))
                    )
                    continue
                raise response_error(response)
            finally:
                await response.aclose()
        raise AssertionError("unreachable")  # pragma: no cover

    async def _download_whole(self, url: str, part: Path, tracker: ProgressTracker) -> None:
        for attempt in range(self.retry.attempts):
            last = attempt == self.retry.attempts - 1
            written = 0
            try:
                async with self._client.stream("GET", url, headers=self._headers) as response:
                    if response.status_code != 200:
                        await response.aread()
                        if response.status_code in self.retry.retry_statuses and not last:
                            await asyncio.sleep(self.retry.delay(attempt))
                            continue
                        raise response_error(response)
                    length = response.headers.get("content-length", "")
                    if not tracker.total and length.isdigit():
                        tracker.total = int(length)
                    with open(part, "wb") as fh:
                        buffer = bytearray()
                        async for block in response.aiter_bytes():
                            buffer += block
                            if len(buffer) >= _WRITE_BLOCK:
                                await asyncio.to_thread(fh.write, bytes(buffer))
                                written += len(buffer)
                                tracker.add(len(buffer))
                                buffer.clear()
                        if buffer:
                            await asyncio.to_thread(fh.write, bytes(buffer))
                            written += len(buffer)
                            tracker.add(len(buffer))
                    if length.isdigit() and written != int(length):
                        raise _ShortRead
                return
            except (httpx.TransportError, _ShortRead):
                tracker.add(-written)
                if last:
                    raise TransferError("download failed after retries") from None
                await asyncio.sleep(self.retry.delay(attempt))

    async def _download_ranged(
        self,
        url: str,
        part: Path,
        journal: Journal,
        probe: _Probe,
        resume: bool,
        tracker: ProgressTracker,
    ) -> bool:
        size = probe.size or 0
        validator = probe.validator
        header: dict[str, Any] = {
            "url": url,
            "size": size,
            "validator": validator,
            "segment_size": self.segment_size,
        }
        done: set[int] = set()
        if resume and validator:
            previous, lines = journal.read()
            if previous == header and part.is_file() and part.stat().st_size == size:
                done = {int(line) for line in lines if line.isdigit()}
        if not done:
            journal.start(header)
            with open(part, "wb") as fh:
                fh.truncate(size)

        step = self.segment_size
        segments = [
            (i, start, min(start + step, size) - 1) for i, start in enumerate(range(0, size, step))
        ]
        tracker.add(sum(end - start + 1 for i, start, end in segments if i in done))
        pending = deque(seg for seg in segments if seg[0] not in done)

        async def worker() -> None:
            with open(part, "r+b") as fh:
                while pending:
                    index, start, end = pending.popleft()
                    await self._fetch_segment(url, fh, start, end, size, validator, tracker)
                    journal.append(str(index))

        tasks = [asyncio.create_task(worker()) for _ in range(min(self.concurrency, len(pending)))]
        try:
            await asyncio.gather(*tasks)
        except RemoteChangedError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            journal.remove()
            raise
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            journal.close()
        return bool(done)

    async def _fetch_segment(
        self,
        url: str,
        fh: IO[bytes],
        start: int,
        end: int,
        total: int,
        validator: str | None,
        tracker: ProgressTracker,
    ) -> None:
        headers = {**self._headers, "Range": f"bytes={start}-{end}"}
        if validator:
            headers["If-Range"] = validator
        for attempt in range(self.retry.attempts):
            last = attempt == self.retry.attempts - 1
            written = 0
            try:
                async with self._client.stream("GET", url, headers=headers) as response:
                    status = response.status_code
                    if status == 200:
                        raise RemoteChangedError("the remote file changed during the download")
                    if status != 206:
                        await response.aread()
                        if status in self.retry.retry_statuses and not last:
                            await asyncio.sleep(
                                self.retry.delay(attempt, response.headers.get("retry-after"))
                            )
                            continue
                        raise response_error(response)
                    got = _parse_content_range(response.headers.get("content-range"))
                    if got[2] is not None and got[2] != total:
                        raise RemoteChangedError("the remote file size changed")
                    if got[:2] != (start, end):
                        raise TransferError(
                            f"server sent range {got[0]}-{got[1]}, expected {start}-{end}"
                        )
                    position = start
                    buffer = bytearray()
                    async for block in response.aiter_bytes():
                        buffer += block
                        if len(buffer) >= _WRITE_BLOCK:
                            if position + len(buffer) > end + 1:
                                raise TransferError("server sent more data than requested")
                            await asyncio.to_thread(_write_at, fh, position, bytes(buffer))
                            position += len(buffer)
                            written += len(buffer)
                            tracker.add(len(buffer))
                            buffer.clear()
                    if buffer:
                        if position + len(buffer) > end + 1:
                            raise TransferError("server sent more data than requested")
                        await asyncio.to_thread(_write_at, fh, position, bytes(buffer))
                        position += len(buffer)
                        written += len(buffer)
                        tracker.add(len(buffer))
                    if position != end + 1:
                        raise _ShortRead
                return
            except (httpx.TransportError, _ShortRead):
                tracker.add(-written)
                if last:
                    raise TransferError(f"segment {start}-{end} failed after retries") from None
                await asyncio.sleep(self.retry.delay(attempt))
            except BaseException:
                tracker.add(-written)
                raise


class Downloader:
    """Synchronous facade over :class:`AsyncDownloader` (same arguments)."""

    def __init__(self, **options: Any) -> None:
        self.options = options

    def download(self, url: str, dest: str | Path, **kwargs: Any) -> DownloadResult:
        async def run() -> DownloadResult:
            async with AsyncDownloader(**self.options) as downloader:
                return await downloader.download(url, dest, **kwargs)

        return run_sync(run)


def download_file(
    url: str,
    dest: str | Path,
    *,
    overwrite: bool = False,
    resume: bool = True,
    checksum: str | None = None,
    progress: ProgressCallback | None = None,
    **options: Any,
) -> DownloadResult:
    """Download one file synchronously. ``options`` go to :class:`AsyncDownloader`."""
    return Downloader(**options).download(
        url, dest, overwrite=overwrite, resume=resume, checksum=checksum, progress=progress
    )
