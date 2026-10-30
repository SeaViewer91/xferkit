"""Parallel, resumable uploader for the xferkit protocol."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import secrets
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

import httpx

from ..errors import (
    IntegrityError,
    InvalidRequest,
    UploadConflict,
    UploadIncomplete,
    UploadNotFound,
)
from ..protocol import CHECKSUM_HEADER, MiB, composite_checksum, format_checksum, total_parts
from ._common import (
    Journal,
    ProgressCallback,
    ProgressTracker,
    RetryPolicy,
    make_client,
    raise_for_error,
    run_sync,
    send_with_retry,
)

__all__ = ["AsyncUploader", "UploadResult", "Uploader", "upload_file"]

_PART_RETRY_STATUSES = frozenset({409, 422})  # part still locked / corrupted in transit


@dataclass(frozen=True)
class UploadResult:
    upload_id: str
    filename: str
    size: int
    composite_checksum: str
    checksum: str | None
    resumed: bool
    metadata: dict[str, str] = field(default_factory=dict)
    response: dict[str, Any] = field(default_factory=dict)


def _read_part(fh: IO[bytes], offset: int, length: int) -> tuple[bytes, bytes]:
    fh.seek(offset)
    data = fh.read(length)
    if len(data) != length:
        raise OSError("file changed while uploading (short read)")
    return data, hashlib.sha256(data).digest()


def _hash_parts(path: Path, size: int, chunk_size: int, indices: list[int]) -> dict[int, bytes]:
    digests = {}
    with open(path, "rb") as fh:
        for index in indices:
            offset = index * chunk_size
            digests[index] = _read_part(fh, offset, min(chunk_size, size - offset))[1]
    return digests


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(4 * MiB):
            digest.update(block)
    return digest.hexdigest()


def _parse_digest_lines(lines: list[str]) -> dict[int, bytes]:
    digests: dict[int, bytes] = {}
    for line in lines:
        index, _, hexdigest = line.partition(" ")
        if index.isdigit() and len(hexdigest) == 64:
            try:
                digests[int(index)] = bytes.fromhex(hexdigest)
            except ValueError:
                continue
    return digests


def _token_for_key(key: str) -> str:
    """Map an application-level idempotency key to a protocol client token."""
    digest = hashlib.sha256(b"xferkit-idempotency\0" + key.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def _journal_upload_id(lines: list[str]) -> str | None:
    upload_id = None
    for line in lines:
        key, _, value = line.partition(" ")
        if key == "upload_id" and value:
            upload_id = value  # the last one wins
    return upload_id


def _check_size(status: dict[str, Any], size: int) -> None:
    if int(status["size"]) != size:
        raise InvalidRequest(
            f"upload {status.get('upload_id')} expects {status['size']} bytes, file has {size}"
        )


class AsyncUploader:
    """Uploads files to an xferkit server in parallel parts.

    Args:
        base_url: URL of the upload router, e.g. ``https://host/uploads``.
        client: An existing :class:`httpx.AsyncClient` (not closed by us).
        headers: Extra headers for every request (e.g. ``Authorization``).
        chunk_size: Requested part size; ``None`` lets the server choose.
        concurrency: Parts in flight at once. Memory use is about
            ``concurrency * chunk_size``.
        retry: Retry policy for network errors and 5xx/429 responses.
        timeout: httpx timeout (default 60 s, 15 s connect).
        state_dir: Directory for resume journals. When set, re-running an
            interrupted upload of the same file continues where it stopped.
        send_checksum: Hash the whole file first and let the server verify it
            after assembly. Every part is always verified, and the composite
            checksum is always compared, so this is an extra safety net that
            costs one more full read of the file.
    """

    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient | None = None,
        headers: dict[str, str] | None = None,
        chunk_size: int | None = None,
        concurrency: int = 4,
        retry: RetryPolicy | None = None,
        timeout: httpx.Timeout | float | None = None,
        state_dir: str | Path | None = None,
        send_checksum: bool = False,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.base_url = base_url.rstrip("/")
        self.chunk_size = chunk_size
        self.concurrency = concurrency
        self.retry = retry or RetryPolicy()
        self.state_dir = Path(state_dir).expanduser() if state_dir else None
        self.send_checksum = send_checksum
        self._client, self._owns_client = make_client(
            client, headers=headers, timeout=timeout, concurrency=concurrency
        )
        self._headers = {} if self._owns_client else dict(headers or {})

    async def __aenter__(self) -> AsyncUploader:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    # ------------------------------------------------------------------ requests

    def _url(self, *segments: str) -> str:
        return "/".join([self.base_url, *segments])

    async def _request(
        self,
        method: str,
        url: str,
        *,
        extra_retry: frozenset[int] = frozenset(),
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        merged = {**self._headers, **(headers or {})}
        response = await send_with_retry(
            self.retry,
            lambda: self._client.request(method, url, headers=merged, **kwargs),
            extra_retry_statuses=extra_retry,
        )
        raise_for_error(response)
        return response

    async def get_status(self, upload_id: str) -> dict[str, Any]:
        """Server-side status of an upload session."""
        return (await self._request("GET", self._url(upload_id))).json()

    async def abort(self, upload_id: str) -> None:
        """Abort an upload session and discard its data on the server."""
        await self._request("DELETE", self._url(upload_id))

    # ------------------------------------------------------------------ upload

    async def upload(
        self,
        path: str | Path,
        *,
        filename: str | None = None,
        metadata: dict[str, str] | None = None,
        upload_id: str | None = None,
        idempotency_key: str | None = None,
        progress: ProgressCallback | None = None,
    ) -> UploadResult:
        """Upload ``path`` and return once the server has assembled it.

        Args:
            filename: Name to store on the server (default: the file's name).
            metadata: String key/value pairs kept with the upload.
            upload_id: Resume this specific session (e.g. one created in
                another process). Without it, a journal in ``state_dir`` is
                used to resume automatically.
            idempotency_key: Any string that identifies this upload in *your*
                application (e.g. ``"survey-42/DJI_0001.JPG"``). Uploading again
                with the same key returns the existing upload, pending or
                completed, instead of creating a second copy, even if your
                process crashed after this call returned but before you
                recorded the result.
            progress: Called with :class:`TransferProgress` after every part.
        """
        path = Path(path).expanduser()
        stat = path.stat()
        if not path.is_file():
            raise IsADirectoryError(str(path))
        size = stat.st_size
        filename = filename or path.name
        journal = self._journal(path)
        journal_header = {
            "base_url": self.base_url,
            "path": str(path.resolve()),
            "size": size,
            "mtime_ns": stat.st_mtime_ns,
        }
        digests: dict[int, bytes] = {}
        explicit = upload_id is not None
        client_token = _token_for_key(idempotency_key) if idempotency_key is not None else None

        if upload_id is None and journal is not None:
            header, lines = journal.read()
            if (
                header
                and all(header.get(k) == v for k, v in journal_header.items())
                and (client_token is None or header.get("client_token") == client_token)
            ):
                upload_id = header.get("upload_id") or _journal_upload_id(lines)
                client_token = header.get("client_token")
                digests = _parse_digest_lines(lines)

        status: dict[str, Any] | None = None
        if upload_id is not None:
            try:
                status = await self.get_status(upload_id)
            except UploadNotFound:  # also UploadExpired
                if explicit:
                    raise
                digests = {}
            else:
                _check_size(status, size)
                if explicit and journal is not None:
                    journal.start({**journal_header, "upload_id": upload_id})
        resumed = status is not None

        if status is None:
            # The token is recorded *before* the request, so a crash at any point
            # (even before the response arrives) can find this session again: the
            # server answers a repeated token with the existing upload.
            client_token = client_token or secrets.token_urlsafe(18)
            digests = {}
            if journal is not None:
                journal.start({**journal_header, "client_token": client_token})
            status, created = await self._create(path, filename, size, metadata, client_token)
            _check_size(status, size)
            upload_id = str(status["upload_id"])
            resumed = not created
            if journal is not None:
                journal.append(f"upload_id {upload_id}")
        assert upload_id is not None

        chunk_size = int(status.get("chunk_size") or 0)
        count = total_parts(size, chunk_size) if chunk_size else 0
        # The server may already have completed this upload, e.g. when an earlier
        # run crashed before it saw the completion response. Verify it instead of
        # uploading the file a second time.
        already_completed = status.get("state") == "completed"
        if already_completed:
            received = set(range(count))
        else:
            received = {int(i) for i in status.get("received_parts", [])}
        tracker = ProgressTracker(size, progress)
        tracker.add(sum(min(chunk_size, size - i * chunk_size) for i in received))

        try:
            if already_completed:
                response = status
            else:
                missing = [i for i in range(count) if i not in received]
                await self._send_parts(
                    path, upload_id, size, chunk_size, missing, digests, journal, tracker
                )
                response = await self._complete(
                    path, upload_id, size, chunk_size, digests, journal, tracker
                )

            unknown = [i for i in range(count) if i not in digests]
            if unknown:
                digests.update(
                    await asyncio.to_thread(_hash_parts, path, size, chunk_size, unknown)
                )
            local = composite_checksum(digests[i] for i in range(count))
            if response.get("composite_checksum") != local:
                raise IntegrityError(
                    "server-side composite checksum does not match the local file",
                    details={"local": local, "remote": response.get("composite_checksum")},
                )
        finally:
            if journal is not None:
                journal.close()
        if journal is not None:
            journal.remove()
        return UploadResult(
            upload_id=upload_id,
            filename=str(response.get("filename", filename)),
            size=size,
            composite_checksum=local,
            checksum=response.get("checksum"),
            resumed=resumed,
            metadata=dict(response.get("metadata") or {}),
            response=response,
        )

    def _journal(self, path: Path) -> Journal | None:
        if self.state_dir is None:
            return None
        key = hashlib.sha256(f"{self.base_url}\n{path.resolve()}".encode()).hexdigest()[:32]
        return Journal(self.state_dir / f"upload-{key}.journal")

    async def _create(
        self,
        path: Path,
        filename: str,
        size: int,
        metadata: dict[str, str] | None,
        client_token: str,
    ) -> tuple[dict[str, Any], bool]:
        """Create the session; ``created`` is false when the server returned the
        upload already bound to ``client_token``."""
        body: dict[str, Any] = {
            "filename": filename,
            "size": size,
            "metadata": metadata or {},
            "client_token": client_token,
        }
        if self.chunk_size is not None:
            body["chunk_size"] = self.chunk_size
        if self.send_checksum:
            body["checksum"] = format_checksum(await asyncio.to_thread(_sha256_file, path))
        # Retrying this POST is safe: the token makes it idempotent.
        response = await self._request("POST", self.base_url, json=body)
        status = response.json()
        status.setdefault("received_parts", [])
        return status, response.status_code == 201

    async def _send_parts(
        self,
        path: Path,
        upload_id: str,
        size: int,
        chunk_size: int,
        indices: list[int],
        digests: dict[int, bytes],
        journal: Journal | None,
        tracker: ProgressTracker,
    ) -> None:
        if not indices:
            return
        pending = deque(indices)

        async def worker() -> None:
            with open(path, "rb") as fh:
                while pending:
                    index = pending.popleft()
                    offset = index * chunk_size
                    length = min(chunk_size, size - offset)
                    data, digest = await asyncio.to_thread(_read_part, fh, offset, length)
                    await self._request(
                        "PUT",
                        self._url(upload_id, "parts", str(index)),
                        content=data,
                        headers={
                            CHECKSUM_HEADER: format_checksum(digest.hex()),
                            "Content-Type": "application/octet-stream",
                        },
                        extra_retry=_PART_RETRY_STATUSES,
                    )
                    digests[index] = digest
                    if journal is not None:
                        journal.append(f"{index} {digest.hex()}")
                    tracker.add(length)

        tasks = [asyncio.create_task(worker()) for _ in range(min(self.concurrency, len(indices)))]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _complete(
        self,
        path: Path,
        upload_id: str,
        size: int,
        chunk_size: int,
        digests: dict[int, bytes],
        journal: Journal | None,
        tracker: ProgressTracker,
    ) -> dict[str, Any]:
        url = self._url(upload_id, "complete")
        for attempt in range(self.retry.attempts):
            try:
                return (await self._request("POST", url)).json()
            except UploadIncomplete as exc:
                # The server lost or rejected some parts: send them again.
                missing = [int(i) for i in exc.details.get("missing_parts", [])]
                if not missing or attempt == self.retry.attempts - 1:
                    raise
                tracker.add(-sum(min(chunk_size, size - i * chunk_size) for i in missing))
                await self._send_parts(
                    path, upload_id, size, chunk_size, missing, digests, journal, tracker
                )
            except UploadConflict:
                # Parts still being written server-side (e.g. a timed-out request
                # that is still running there), or a concurrent completion.
                if attempt == self.retry.attempts - 1:
                    raise
                await asyncio.sleep(self.retry.delay(attempt))
        raise AssertionError("unreachable")  # pragma: no cover


class Uploader:
    """Synchronous facade over :class:`AsyncUploader` (same arguments)."""

    def __init__(self, base_url: str, **options: Any) -> None:
        self.base_url = base_url
        self.options = options

    def upload(self, path: str | Path, **kwargs: Any) -> UploadResult:
        async def run() -> UploadResult:
            async with AsyncUploader(self.base_url, **self.options) as uploader:
                return await uploader.upload(path, **kwargs)

        return run_sync(run)

    def get_status(self, upload_id: str) -> dict[str, Any]:
        async def run() -> dict[str, Any]:
            async with AsyncUploader(self.base_url, **self.options) as uploader:
                return await uploader.get_status(upload_id)

        return run_sync(run)

    def abort(self, upload_id: str) -> None:
        async def run() -> None:
            async with AsyncUploader(self.base_url, **self.options) as uploader:
                await uploader.abort(upload_id)

        run_sync(run)


def upload_file(
    base_url: str,
    path: str | Path,
    *,
    filename: str | None = None,
    metadata: dict[str, str] | None = None,
    upload_id: str | None = None,
    idempotency_key: str | None = None,
    progress: ProgressCallback | None = None,
    **options: Any,
) -> UploadResult:
    """Upload one file synchronously. ``options`` go to :class:`AsyncUploader`."""
    return Uploader(base_url, **options).upload(
        path,
        filename=filename,
        metadata=metadata,
        upload_id=upload_id,
        idempotency_key=idempotency_key,
        progress=progress,
    )
