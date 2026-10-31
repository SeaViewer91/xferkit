"""Parallel, resumable uploader for the xferkit protocol."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import fnmatch
import hashlib
import os
import secrets
from collections import deque
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

import httpx

from ..errors import (
    IntegrityError,
    InvalidRequest,
    TooManyUploads,
    UploadConflict,
    UploadIncomplete,
    UploadNotFound,
    error_from_response,
)
from ..grants import GRANT_HEADER
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

__all__ = [
    "AsyncUploader",
    "BatchProgress",
    "BatchResult",
    "BatchUploadError",
    "FailedUpload",
    "UploadItem",
    "UploadResult",
    "Uploader",
    "upload_file",
    "upload_folder",
]

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
    path: Path | None = None


@dataclass(frozen=True)
class UploadItem:
    """One file for :meth:`AsyncUploader.upload_many`.

    ``filename`` defaults to the file's name. Without an ``idempotency_key`` the
    uploader derives one from the path, name, size and modification time when
    ``dedupe`` is on.
    """

    path: str | Path
    filename: str | None = None
    metadata: dict[str, str] | None = None
    idempotency_key: str | None = None


@dataclass(frozen=True)
class FailedUpload:
    """A file :meth:`AsyncUploader.upload_many` could not upload."""

    path: Path
    error: BaseException


class BatchUploadError(Exception):
    """Raised by :meth:`BatchResult.raise_for_failures`."""

    def __init__(self, failed: list[FailedUpload]) -> None:
        first = failed[0]
        super().__init__(f"{len(failed)} file(s) failed, first: {first.path}: {first.error}")
        self.failed = failed


@dataclass(frozen=True)
class BatchResult:
    """Outcome of a batch upload; files fail independently."""

    uploaded: list[UploadResult]
    failed: list[FailedUpload]

    @property
    def ok(self) -> bool:
        return not self.failed

    def raise_for_failures(self) -> BatchResult:
        if self.failed:
            raise BatchUploadError(self.failed)
        return self


@dataclass(frozen=True)
class BatchProgress:
    """Progress of a batch upload (also usable as a :class:`TransferProgress`)."""

    bytes_done: int
    bytes_total: int
    files_done: int
    files_failed: int
    files_total: int

    @property
    def fraction(self) -> float:
        return 1.0 if self.bytes_total == 0 else self.bytes_done / self.bytes_total


BatchProgressCallback = Callable[[BatchProgress], None]


class _BatchTracker(ProgressTracker):
    def __init__(self, total: int, files: int, callback: BatchProgressCallback | None) -> None:
        super().__init__(total, None)
        self.files_total = files
        self.files_done = 0
        self.files_failed = 0
        self._batch_callback = callback

    def add(self, nbytes: int) -> None:
        if nbytes:
            self.done += nbytes
            self._emit()

    def file_finished(self, ok: bool, unsent: int = 0) -> None:
        if ok:
            self.files_done += 1
        else:
            self.files_failed += 1
            self.done += unsent  # count the failed file as dealt with
        self._emit()

    def _emit(self) -> None:
        if self._batch_callback is not None:
            self._batch_callback(
                BatchProgress(
                    self.done, self.total, self.files_done, self.files_failed, self.files_total
                )
            )


@dataclass
class _Planned:
    index: int
    path: Path
    filename: str
    size: int
    metadata: dict[str, str]
    client_token: str
    sent: int = 0  # bytes counted in the progress so far


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


def _completion_in(response: httpx.Response) -> dict[str, Any] | None:
    """The completion record an ``auto_complete`` part response may carry."""
    try:
        body = response.json()
    except ValueError:
        return None
    record = body.get("completed") if isinstance(body, dict) else None
    return record if isinstance(record, dict) else None


def _check_size(status: dict[str, Any], size: int) -> None:
    if int(status["size"]) != size:
        raise InvalidRequest(
            f"upload {status.get('upload_id')} expects {status['size']} bytes, file has {size}"
        )


class _FileTracker(ProgressTracker):
    """Per-file view onto a batch's progress."""

    def __init__(self, batch: ProgressTracker) -> None:
        super().__init__(0, None)
        self._batch = batch

    def add(self, nbytes: int) -> None:
        self.done += nbytes
        self._batch.add(nbytes)


def _dedupe_key(base: str, filename: str, size: int, mtime_ns: int) -> str:
    """Idempotency key of an unchanged file (same in the browser client)."""
    return f"xferkit-file\n{base}\n{filename}\n{size}\n{mtime_ns // 1_000_000}"


def _plan_item(
    index: int, item: UploadItem, base: str | None, extra: dict[str, str] | None
) -> _Planned:
    path = Path(item.path).expanduser()
    stat = path.stat()
    if not path.is_file():
        raise IsADirectoryError(str(path))
    filename = item.filename or path.name
    if item.idempotency_key is not None:
        token = _token_for_key(item.idempotency_key)
    elif base is not None:
        token = _token_for_key(_dedupe_key(base, filename, stat.st_size, stat.st_mtime_ns))
    else:
        token = secrets.token_urlsafe(18)
    metadata = {**(extra or {}), **(item.metadata or {})}
    return _Planned(index, path, filename, stat.st_size, metadata, token)


def _batch_outcome(entry: dict[str, Any]) -> tuple[dict[str, Any], bool] | BaseException:
    if "error" in entry:
        return error_from_response(int(entry.get("status") or 400), {"error": entry["error"]})
    created = bool(entry.pop("created", False))
    entry.setdefault("received_parts", [])
    return entry, created


def _patterns(value: str | Sequence[str] | None) -> list[str]:
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)


def _walk_folder(
    root: Path,
    include: str | Sequence[str] | None,
    exclude: str | Sequence[str] | None,
    include_hidden: bool,
    follow_symlinks: bool,
) -> list[tuple[Path, str]]:
    """Files below ``root`` as ``(path, relative posix path)``, sorted."""
    includes, excludes = _patterns(include), _patterns(exclude)

    def matches(rel: str, name: str, patterns: list[str]) -> bool:
        return any(fnmatch.fnmatchcase(rel, p) or fnmatch.fnmatchcase(name, p) for p in patterns)

    found: list[tuple[Path, str]] = []
    for directory, dirnames, filenames in os.walk(root, followlinks=follow_symlinks):
        if not include_hidden:
            dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        dirnames.sort()
        for name in filenames:
            if not include_hidden and name.startswith("."):
                continue
            path = Path(directory, name)
            if not follow_symlinks and path.is_symlink():
                continue
            if not path.is_file():
                continue
            rel = path.relative_to(root).as_posix()
            if includes and not matches(rel, name, includes):
                continue
            if excludes and matches(rel, name, excludes):
                continue
            found.append((path, rel))
    found.sort(key=lambda entry: entry[1])
    return found


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
        grant: A signed grant (see :mod:`xferkit.grants`) sent with every
            request, for servers that require one.
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
        grant: str | None = None,
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
        if grant is not None:
            headers = {**(headers or {}), GRANT_HEADER: grant}
        self._client, self._owns_client = make_client(
            client, headers=headers, timeout=timeout, concurrency=concurrency
        )
        self._headers = {} if self._owns_client else dict(headers or {})
        self._batch_supported: bool | None = None

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

        try:
            result = await self._transfer(
                path,
                filename,
                size,
                status,
                digests,
                journal,
                ProgressTracker(size, progress),
                resumed=resumed,
            )
        finally:
            if journal is not None:
                journal.close()
        if journal is not None:
            journal.remove()
        return result

    async def _transfer(
        self,
        path: Path,
        filename: str,
        size: int,
        status: dict[str, Any],
        digests: dict[int, bytes],
        journal: Journal | None,
        tracker: ProgressTracker,
        *,
        resumed: bool,
        limit: asyncio.Semaphore | None = None,
    ) -> UploadResult:
        """Send the parts the server is missing, complete and verify.

        ``status`` is the session's server-side status (or its completion
        record). ``limit`` bounds the parts in flight across several files.
        """
        upload_id = str(status["upload_id"])
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
        tracker.add(sum(min(chunk_size, size - i * chunk_size) for i in received))

        if already_completed:
            response = status
        else:
            missing = [i for i in range(count) if i not in received]
            # An ``auto_complete`` session reports its completion record in the
            # response to its last part, which saves the ``complete`` request.
            response = await self._send_parts(
                path, upload_id, size, chunk_size, missing, digests, journal, tracker, limit
            ) or await self._complete(
                path, upload_id, size, chunk_size, digests, journal, tracker, limit
            )

        unknown = [i for i in range(count) if i not in digests]
        if unknown:
            digests.update(await asyncio.to_thread(_hash_parts, path, size, chunk_size, unknown))
        local = composite_checksum(digests[i] for i in range(count))
        if response.get("composite_checksum") != local:
            raise IntegrityError(
                "server-side composite checksum does not match the local file",
                details={"local": local, "remote": response.get("composite_checksum")},
            )
        return UploadResult(
            upload_id=upload_id,
            filename=str(response.get("filename", filename)),
            size=size,
            composite_checksum=local,
            checksum=response.get("checksum"),
            resumed=resumed,
            metadata=dict(response.get("metadata") or {}),
            response=response,
            path=path,
        )

    # ------------------------------------------------------------------ batches

    async def upload_many(
        self,
        files: Iterable[str | Path | UploadItem],
        *,
        metadata: dict[str, str] | None = None,
        dedupe: bool = True,
        key_prefix: str | None = None,
        file_concurrency: int | None = None,
        batch_size: int = 100,
        auto_complete_below: int = 64 * MiB,
        progress: BatchProgressCallback | None = None,
        on_file: Callable[[UploadResult | FailedUpload], None] | None = None,
    ) -> BatchResult:
        """Upload many files; each one fails or succeeds on its own.

        Sessions are created ``batch_size`` at a time with one request
        (``POST {base_url}/batch``; servers without it get one request per
        file), and small files (up to ``auto_complete_below``) are completed by
        the server when their last part arrives, so a small file costs about
        one request instead of three.

        Args:
            files: Paths or :class:`UploadItem` objects.
            metadata: Added to every file's metadata.
            dedupe: Derive each file's idempotency key from its absolute path
                (or ``key_prefix`` + path), name, size and modification time.
                Re-running the same batch then resumes unfinished files and
                skips (after verifying) files that are already uploaded,
                without a local journal. Off: every run uploads new copies.
            file_concurrency: Files in progress at once (default
                ``max(concurrency, 8)``). Parts in flight across all files
                stay limited to ``concurrency``.
            batch_size: Files per session-creation request.
            auto_complete_below: Largest file the server completes by itself.
            progress: Called with :class:`BatchProgress` as bytes and files
                finish.
            on_file: Called with each file's :class:`UploadResult` or
                :class:`FailedUpload` as soon as it is done.
        """
        entries = []
        for entry in files:
            item = entry if isinstance(entry, UploadItem) else UploadItem(entry)
            base = None
            if dedupe and item.idempotency_key is None:
                base = (key_prefix or "") + str(Path(item.path).expanduser().resolve())
            entries.append((item, base))
        return await self._run_batch(
            entries,
            metadata=metadata,
            file_concurrency=file_concurrency,
            batch_size=batch_size,
            auto_complete_below=auto_complete_below,
            progress=progress,
            on_file=on_file,
        )

    async def upload_folder(
        self,
        root: str | Path,
        *,
        include: str | Sequence[str] | None = None,
        exclude: str | Sequence[str] | None = None,
        include_hidden: bool = False,
        follow_symlinks: bool = False,
        metadata: dict[str, str] | None = None,
        dedupe: bool = True,
        key_prefix: str | None = None,
        file_concurrency: int | None = None,
        batch_size: int = 100,
        auto_complete_below: int = 64 * MiB,
        progress: BatchProgressCallback | None = None,
        on_file: Callable[[UploadResult | FailedUpload], None] | None = None,
    ) -> BatchResult:
        """Upload every file below ``root`` (see :meth:`upload_many`).

        Each file keeps its own name and gets its path relative to ``root``
        (``/``-separated) as ``metadata["relative_path"]``, so the server
        application can rebuild the folder structure.

        Args:
            include: Glob pattern(s) a file must match, e.g. ``"*.jpg"``.
                Patterns are matched against the relative path and the file
                name; ``*`` also matches ``/``.
            exclude: Glob pattern(s) of files to skip.
            include_hidden: Also upload files and folders starting with ``.``.
            follow_symlinks: Follow symbolic links to files and folders.
            key_prefix: With ``dedupe``, idempotency keys are built from
                ``key_prefix`` + relative path instead of the absolute path,
                so the same folder uploaded from another location or machine
                (or from the browser client with the same prefix) maps to the
                same uploads.
        """
        root = Path(root).expanduser()
        if not root.is_dir():
            raise NotADirectoryError(str(root))
        entries = []
        for path, rel in _walk_folder(root, include, exclude, include_hidden, follow_symlinks):
            item = UploadItem(path, metadata={**(metadata or {}), "relative_path": rel})
            base = None
            if dedupe:
                base = key_prefix + rel if key_prefix is not None else str(path.resolve())
            entries.append((item, base))
        return await self._run_batch(
            entries,
            metadata=None,
            file_concurrency=file_concurrency,
            batch_size=batch_size,
            auto_complete_below=auto_complete_below,
            progress=progress,
            on_file=on_file,
        )

    async def _run_batch(
        self,
        entries: list[tuple[UploadItem, str | None]],
        *,
        metadata: dict[str, str] | None,
        file_concurrency: int | None,
        batch_size: int,
        auto_complete_below: int,
        progress: BatchProgressCallback | None,
        on_file: Callable[[UploadResult | FailedUpload], None] | None,
    ) -> BatchResult:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        files_at_once = file_concurrency or max(self.concurrency, 8)
        if files_at_once < 1:
            raise ValueError("file_concurrency must be >= 1")

        planned: list[_Planned] = []
        early: list[tuple[int, FailedUpload]] = []
        for index, (item, base) in enumerate(entries):
            try:
                planned.append(_plan_item(index, item, base, metadata))
            except OSError as exc:
                early.append((index, FailedUpload(Path(item.path), exc)))

        tracker = _BatchTracker(sum(p.size for p in planned), len(entries), progress)
        uploaded: dict[int, UploadResult] = {}
        failed: dict[int, FailedUpload] = {}

        def finish(item_index: int, outcome: UploadResult | FailedUpload, unsent: int = 0) -> None:
            if isinstance(outcome, FailedUpload):
                failed[item_index] = outcome
            else:
                uploaded[item_index] = outcome
            tracker.file_finished(not isinstance(outcome, FailedUpload), unsent)
            if on_file is not None:
                on_file(outcome)

        for index, failure in early:
            finish(index, failure)

        part_limit = asyncio.Semaphore(self.concurrency)
        in_hand = max(batch_size, files_at_once)
        pending: deque[_Planned] = deque(planned)
        ready: deque[tuple[_Planned, dict[str, Any], bool]] = deque()
        running: dict[asyncio.Task[UploadResult], tuple[_Planned, _FileTracker]] = {}
        throttled = False  # the server refused new sessions; wait for one of ours
        backoff = 0

        try:
            while pending or ready or running:
                can_create = not (throttled and running)
                if pending and len(ready) < files_at_once and can_create:
                    room = in_hand - len(ready) - len(running)
                    chunk = [pending.popleft() for _ in range(min(room, len(pending), batch_size))]
                    retry_later: list[_Planned] = []
                    if chunk:
                        outcomes = await self._create_many(chunk, auto_complete_below)
                        for item, outcome in zip(chunk, outcomes, strict=True):
                            if isinstance(outcome, TooManyUploads):
                                retry_later.append(item)
                            elif isinstance(outcome, BaseException):
                                finish(item.index, FailedUpload(item.path, outcome), item.size)
                            else:
                                ready.append((item, *outcome))
                    throttled = bool(retry_later)
                    if retry_later:
                        pending.extendleft(reversed(retry_later))
                        if not running and not ready:
                            # Nothing of ours is in flight: other uploads fill the
                            # server's limit. Back off, then give up on these files.
                            if backoff >= self.retry.attempts - 1:
                                error = TooManyUploads("the server accepts no more uploads now")
                                while pending:
                                    item = pending.popleft()
                                    finish(item.index, FailedUpload(item.path, error), item.size)
                                continue
                            await asyncio.sleep(self.retry.delay(backoff))
                            backoff += 1
                            continue
                    else:
                        backoff = 0

                while ready and len(running) < files_at_once:
                    item, status, created = ready.popleft()
                    file_tracker = _FileTracker(tracker)
                    task = asyncio.create_task(
                        self._transfer_item(item, status, created, file_tracker, part_limit)
                    )
                    running[task] = (item, file_tracker)

                if not running:
                    continue
                done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    item, file_tracker = running.pop(task)
                    throttled = False
                    error = task.exception()
                    if error is None:
                        finish(item.index, task.result())
                    elif isinstance(error, Exception):
                        finish(
                            item.index,
                            FailedUpload(item.path, error),
                            item.size - file_tracker.done,
                        )
                    else:
                        raise error
        except BaseException:
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
            raise

        return BatchResult(
            uploaded=[uploaded[i] for i in sorted(uploaded)],
            failed=[failed[i] for i in sorted(failed)],
        )

    async def _transfer_item(
        self,
        item: _Planned,
        status: dict[str, Any],
        created: bool,
        tracker: ProgressTracker,
        limit: asyncio.Semaphore,
    ) -> UploadResult:
        _check_size(status, item.size)
        return await self._transfer(
            item.path,
            item.filename,
            item.size,
            status,
            {},
            None,
            tracker,
            resumed=not created,
            limit=limit,
        )

    async def _create_body(
        self, path: Path, filename: str, size: int, metadata: dict[str, str] | None, token: str
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "filename": filename,
            "size": size,
            "metadata": metadata or {},
            "client_token": token,
        }
        if self.chunk_size is not None:
            body["chunk_size"] = self.chunk_size
        if self.send_checksum:
            body["checksum"] = format_checksum(await asyncio.to_thread(_sha256_file, path))
        return body

    async def _create_many(
        self, items: list[_Planned], auto_complete_below: int
    ) -> list[tuple[dict[str, Any], bool] | BaseException]:
        """Create (or find) the sessions for ``items``; errors are per item."""
        bodies: list[dict[str, Any] | BaseException] = []
        for item in items:
            try:
                body = await self._create_body(
                    item.path, item.filename, item.size, item.metadata, item.client_token
                )
            except OSError as exc:
                bodies.append(exc)
                continue
            if item.size <= auto_complete_below:
                body["auto_complete"] = True
            bodies.append(body)
        valid = [b for b in bodies if isinstance(b, dict)]
        try:
            created = await self._post_batch(valid) if valid else []
        except Exception as exc:  # the whole request failed: every file failed
            created = [exc] * len(valid)
        results = iter(created)
        return [next(results) if isinstance(b, dict) else b for b in bodies]

    async def _post_batch(
        self, bodies: list[dict[str, Any]]
    ) -> list[tuple[dict[str, Any], bool] | BaseException]:
        if self._batch_supported is not False:
            url = self._url("batch")
            response = await send_with_retry(
                self.retry,
                lambda: self._client.post(url, json={"files": bodies}, headers=self._headers),
            )
            if response.status_code in (404, 405) and self._batch_supported is None:
                self._batch_supported = False  # an older or minimal server
            else:
                raise_for_error(response)
                self._batch_supported = True
                return [_batch_outcome(entry) for entry in response.json()["uploads"]]

        async def one(body: dict[str, Any]) -> tuple[dict[str, Any], bool] | BaseException:
            try:
                response = await self._request("POST", self.base_url, json=body)
            except Exception as exc:
                return exc
            status = response.json()
            status.setdefault("received_parts", [])
            return status, response.status_code == 201

        return list(await asyncio.gather(*(one(body) for body in bodies)))

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
        body = await self._create_body(path, filename, size, metadata, client_token)
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
        limit: asyncio.Semaphore | None = None,
    ) -> dict[str, Any] | None:
        """Send ``indices``; return the completion record if a part response
        carried one (``auto_complete`` sessions)."""
        if not indices:
            return None
        pending = deque(indices)
        completed: dict[str, Any] | None = None
        gate = limit if limit is not None else contextlib.nullcontext()

        async def worker() -> None:
            nonlocal completed
            with open(path, "rb") as fh:
                while pending:
                    index = pending.popleft()
                    offset = index * chunk_size
                    length = min(chunk_size, size - offset)
                    async with gate:
                        data, digest = await asyncio.to_thread(_read_part, fh, offset, length)
                        response = await self._request(
                            "PUT",
                            self._url(upload_id, "parts", str(index)),
                            content=data,
                            headers={
                                CHECKSUM_HEADER: format_checksum(digest.hex()),
                                "Content-Type": "application/octet-stream",
                            },
                            extra_retry=_PART_RETRY_STATUSES,
                        )
                        del data
                    digests[index] = digest
                    if journal is not None:
                        journal.append(f"{index} {digest.hex()}")
                    tracker.add(length)
                    record = _completion_in(response)
                    if record is not None:
                        completed = record

        workers = min(self.concurrency, len(indices))
        tasks = [asyncio.create_task(worker()) for _ in range(workers)]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        return completed

    async def _complete(
        self,
        path: Path,
        upload_id: str,
        size: int,
        chunk_size: int,
        digests: dict[int, bytes],
        journal: Journal | None,
        tracker: ProgressTracker,
        limit: asyncio.Semaphore | None = None,
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
                record = await self._send_parts(
                    path, upload_id, size, chunk_size, missing, digests, journal, tracker, limit
                )
                if record is not None:
                    return record
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

    def upload_many(self, files: Iterable[str | Path | UploadItem], **kwargs: Any) -> BatchResult:
        """See :meth:`AsyncUploader.upload_many`."""
        files = list(files)

        async def run() -> BatchResult:
            async with AsyncUploader(self.base_url, **self.options) as uploader:
                return await uploader.upload_many(files, **kwargs)

        return run_sync(run)

    def upload_folder(self, root: str | Path, **kwargs: Any) -> BatchResult:
        """See :meth:`AsyncUploader.upload_folder`."""

        async def run() -> BatchResult:
            async with AsyncUploader(self.base_url, **self.options) as uploader:
                return await uploader.upload_folder(root, **kwargs)

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


_FOLDER_OPTIONS = frozenset(
    {
        "include",
        "exclude",
        "include_hidden",
        "follow_symlinks",
        "metadata",
        "dedupe",
        "key_prefix",
        "file_concurrency",
        "batch_size",
        "auto_complete_below",
        "progress",
        "on_file",
    }
)


def upload_folder(base_url: str, root: str | Path, **kwargs: Any) -> BatchResult:
    """Upload a folder synchronously.

    Keyword arguments of :meth:`AsyncUploader.upload_folder` select the files;
    the rest go to :class:`AsyncUploader` (``concurrency``, ``grant``, ...).
    """
    folder = {k: v for k, v in kwargs.items() if k in _FOLDER_OPTIONS}
    options = {k: v for k, v in kwargs.items() if k not in _FOLDER_OPTIONS}
    return Uploader(base_url, **options).upload_folder(root, **folder)
