"""Framework-agnostic upload manager.

:class:`UploadManager` implements the upload protocol on top of a
:class:`~xferkit.server.backend.StorageBackend`. It knows nothing about HTTP:
the FastAPI integration (or your own Flask/Django/aiohttp/socket server) only
translates requests into these method calls.

All methods are synchronous and do blocking file I/O. In async frameworks run
them in a thread pool (the FastAPI integration does this for you).
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import secrets
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import IO, Any

from ..errors import (
    InvalidRequest,
    PayloadTooLarge,
    TooManyUploads,
    UploadConflict,
    UploadExpired,
    UploadNotFound,
)
from ..models import CompletedUpload, PartRecord, UploadSession, UploadStatus
from ..protocol import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_MAX_CHUNK_SIZE,
    DEFAULT_MAX_PARTS,
    DEFAULT_MIN_CHUNK_SIZE,
    MiB,
    format_checksum,
    parse_checksum,
    total_parts,
)
from .backend import PartSink, StorageBackend
from .local import LocalStorage, validate_upload_id

logger = logging.getLogger("xferkit")

CompleteHook = Callable[[CompletedUpload], Any]


@dataclass(frozen=True)
class CreateRequest:
    """What :attr:`UploadManager.before_create` gets to decide on."""

    owner: str | None
    filename: str
    size: int
    metadata: dict[str, str] = field(default_factory=dict)


AdmissionHook = Callable[[CreateRequest], None]
PartData = bytes | bytearray | memoryview | IO[bytes] | Iterable[bytes]

_MAX_FILENAME_CHARS = 1024
_MAX_METADATA_KEY_CHARS = 128


def _default_id() -> str:
    return secrets.token_urlsafe(18)  # 144 bits, 24 url-safe chars


class UploadManager:
    """Coordinates chunked, resumable uploads.

    Args:
        storage: A :class:`StorageBackend`, or a directory path for a
            :class:`LocalStorage`.
        max_upload_size: Largest accepted file in bytes (``None`` = unlimited).
        default_chunk_size: Chunk size used when the client does not ask for one.
        min_chunk_size / max_chunk_size: Accepted range for client-requested
            chunk sizes.
        max_parts: Upper bound on parts per upload. When the client lets the
            server choose, the chunk size grows to stay under this bound.
        ttl: Seconds a session stays valid after creation (``None`` = forever).
            Expired sessions are rejected and removed by :meth:`cleanup_expired`.
        verify_checksum: Re-hash the assembled file on completion when the
            client declared a whole-file checksum.
        max_metadata_bytes: Size limit of the JSON-encoded metadata.
        on_complete: Called once, synchronously, with the
            :class:`CompletedUpload` when an upload finishes. Typical use: move
            the file to its final place and record it in your database.
        before_create: Called with a :class:`CreateRequest` before a session
            is created; raise (e.g. :class:`~xferkit.errors.QuotaExceeded`) to
            refuse it. Use it for storage quotas, file-type policies, etc.
        max_pending_uploads: Limit on unfinished uploads on this server.
        max_pending_per_owner: Limit on unfinished uploads per owner.
        max_pending_bytes_per_owner: Limit on the total size of one owner's
            unfinished uploads.
        completed_ttl: Seconds a completion record is kept (``None`` = forever).
            Records let clients that crashed recognise finished uploads, so keep
            them at least as long as clients may retry. Removed by
            :meth:`cleanup_expired`.
        remove_files_on_expiry: Also delete the assembled file when its record
            expires. Leave off if ``on_complete`` moves files elsewhere.
        id_factory: Generates upload ids (must match ``[A-Za-z0-9_-]{8,128}``).
        clock: Time source, injectable for tests.
    """

    def __init__(
        self,
        storage: StorageBackend | str | os.PathLike[str],
        *,
        max_upload_size: int | None = None,
        default_chunk_size: int = DEFAULT_CHUNK_SIZE,
        min_chunk_size: int = DEFAULT_MIN_CHUNK_SIZE,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        max_parts: int = DEFAULT_MAX_PARTS,
        ttl: float | None = 24 * 3600,
        verify_checksum: bool = True,
        max_metadata_bytes: int = 8 * 1024,
        on_complete: CompleteHook | None = None,
        before_create: AdmissionHook | None = None,
        max_pending_uploads: int | None = None,
        max_pending_per_owner: int | None = None,
        max_pending_bytes_per_owner: int | None = None,
        completed_ttl: float | None = 7 * 24 * 3600,
        remove_files_on_expiry: bool = False,
        id_factory: Callable[[], str] = _default_id,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not 0 < min_chunk_size <= default_chunk_size <= max_chunk_size:
            raise ValueError("require 0 < min_chunk_size <= default_chunk_size <= max_chunk_size")
        if max_parts < 1:
            raise ValueError("max_parts must be >= 1")
        self.storage: StorageBackend = (
            storage if isinstance(storage, StorageBackend) else LocalStorage(storage)
        )
        self.max_upload_size = max_upload_size
        self.default_chunk_size = default_chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.max_parts = max_parts
        self.ttl = ttl
        self.verify_checksum = verify_checksum
        self.max_metadata_bytes = max_metadata_bytes
        self.on_complete = on_complete
        self.before_create = before_create
        self.max_pending_uploads = max_pending_uploads
        self.max_pending_per_owner = max_pending_per_owner
        self.max_pending_bytes_per_owner = max_pending_bytes_per_owner
        self.completed_ttl = completed_ttl
        self.remove_files_on_expiry = remove_files_on_expiry
        self.garbage_age = 3600.0  # seconds before crash leftovers are collected
        self._id_factory = id_factory
        self._clock = clock

    # ------------------------------------------------------------------ create

    def create_upload(
        self,
        filename: str,
        size: int,
        *,
        chunk_size: int | None = None,
        checksum: str | None = None,
        metadata: dict[str, str] | None = None,
        owner: str | None = None,
    ) -> UploadSession:
        """Open a new upload session (see :meth:`open_upload` for the idempotent
        variant used by the HTTP integration).

        Args:
            filename: Original file name (stored as given; sanitised only when
                used as a path).
            size: Exact file size in bytes.
            chunk_size: Requested part size. ``None`` lets the server choose.
            checksum: Optional whole-file checksum ``sha256=<hex>``, verified on
                completion when ``verify_checksum`` is enabled.
            metadata: Free-form string key/value pairs kept with the upload.
            owner: Principal the upload belongs to (``None`` = anonymous).
        """
        session = self._new_session(
            filename, size, chunk_size=chunk_size, checksum=checksum, metadata=metadata,
            owner=owner,
        )  # fmt: skip
        self.storage.create(session)
        logger.debug("created upload %s (%s, %d bytes)", session.upload_id, filename, size)
        return session

    def _new_session(
        self,
        filename: str,
        size: int,
        *,
        chunk_size: int | None = None,
        checksum: str | None = None,
        metadata: dict[str, str] | None = None,
        owner: str | None = None,
    ) -> UploadSession:
        """Validate and admit the request, and build (but not store) a session.

        Args:
            filename: Original file name (stored as given; sanitised only when
                used as a path).
            size: Exact file size in bytes.
            chunk_size: Requested part size. ``None`` lets the server choose.
            checksum: Optional whole-file checksum ``sha256=<hex>``, verified on
                completion when ``verify_checksum`` is enabled.
            metadata: Free-form string key/value pairs kept with the upload.
        """
        filename = self._validate_filename(filename)
        size = self._validate_size(size)
        chunk_size = self._choose_chunk_size(size, chunk_size)
        if checksum is not None:
            try:
                checksum = format_checksum(parse_checksum(checksum)[1])
            except ValueError as exc:
                raise InvalidRequest(str(exc)) from None
        metadata = self._validate_metadata(metadata)
        self._admit(CreateRequest(owner, filename, size, metadata))

        now = self._clock()
        return UploadSession(
            upload_id=validate_upload_id(self._id_factory()),
            filename=filename,
            size=size,
            chunk_size=chunk_size,
            created_at=now,
            expires_at=None if self.ttl is None else now + self.ttl,
            checksum=checksum,
            metadata=metadata,
            owner=owner,
        )

    def _admit(self, request: CreateRequest) -> None:
        """Apply the pending-upload limits and the ``before_create`` hook."""
        limits = (
            self.max_pending_uploads,
            self.max_pending_per_owner,
            self.max_pending_bytes_per_owner,
        )
        if any(limit is not None for limit in limits):
            now = self._clock()
            pending = [s for s in self.storage.iter_sessions() if not s.is_expired(now)]
            mine = [s for s in pending if s.owner == request.owner]
            if self.max_pending_uploads is not None and len(pending) >= self.max_pending_uploads:
                raise TooManyUploads(
                    "too many uploads in progress on this server",
                    details={"retry_after": 30},
                )
            if self.max_pending_per_owner is not None and len(mine) >= self.max_pending_per_owner:
                raise TooManyUploads(
                    f"at most {self.max_pending_per_owner} uploads may be in progress at once",
                    details={"retry_after": 30, "limit": self.max_pending_per_owner},
                )
            if self.max_pending_bytes_per_owner is not None:
                in_flight = sum(s.size for s in mine)
                if in_flight + request.size > self.max_pending_bytes_per_owner:
                    if request.size > self.max_pending_bytes_per_owner:
                        raise PayloadTooLarge(
                            "file is larger than the per-owner in-flight limit",
                            details={"limit": self.max_pending_bytes_per_owner},
                        )
                    raise TooManyUploads(
                        "too many bytes in flight; finish other uploads first",
                        details={"retry_after": 30, "limit": self.max_pending_bytes_per_owner},
                    )
        if self.before_create is not None:
            self.before_create(request)

    def open_upload(
        self,
        filename: str,
        size: int,
        *,
        chunk_size: int | None = None,
        checksum: str | None = None,
        metadata: dict[str, str] | None = None,
        client_token: str | None = None,
        owner: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Create a session, idempotently when the client sends a token.

        A client that generates ``client_token`` *before* the request and keeps
        it can always find its session again, even if it crashed or lost the
        response before learning the upload id. Re-sending the same token
        returns the existing upload (pending or already completed) instead of
        creating a second one.

        Tokens are scoped to ``owner``: two owners using the same token get two
        different uploads.

        Returns:
            ``(state, created)`` where ``state`` is :meth:`describe` of the
            upload and ``created`` tells whether a new session was made.
        """
        token: str | None = None
        if client_token is not None:
            try:
                validate_upload_id(client_token)
            except UploadNotFound:
                raise InvalidRequest("client_token must match [A-Za-z0-9_-]{8,128}") from None
            token = _scoped_token(owner, client_token)
            existing = self.storage.lookup_token(token)
            if existing is not None:
                try:
                    state = self.describe(existing)
                except UploadNotFound:  # expired or aborted: start over below
                    pass
                else:
                    self._check_same_file(state, filename, size)
                    return state, False

        session = self._new_session(
            filename, size, chunk_size=chunk_size, checksum=checksum, metadata=metadata,
            owner=owner,
        )  # fmt: skip
        if token is not None:
            # Bind the token *before* creating the session: if the server dies in
            # between, the token points at nothing and the retry simply creates
            # the session, instead of leaving an unreachable orphan behind.
            winner = self.storage.bind_token(token, session.upload_id)
            if winner != session.upload_id:
                try:  # a concurrent request with the same token was faster
                    state = self.describe(winner)
                except UploadNotFound:  # ...or its binding is stale
                    self.storage.bind_token(token, session.upload_id, replace=True)
                else:
                    self._check_same_file(state, filename, size)
                    return state, False
        self.storage.create(session)
        logger.debug("created upload %s (%s, %d bytes)", session.upload_id, filename, size)
        return self.get_status(session.upload_id).to_dict(), True

    @staticmethod
    def _check_same_file(state: dict[str, Any], filename: str, size: int) -> None:
        if state.get("size") != size or state.get("filename") != filename:
            raise UploadConflict(
                "client_token was already used for a different file",
                details={"upload_id": state.get("upload_id")},
            )

    # ------------------------------------------------------------------ query

    def get_session(self, upload_id: str) -> UploadSession:
        """Return a pending, non-expired session."""
        session = self.storage.load(upload_id)
        if session.is_expired(self._clock()):
            raise UploadExpired(f"upload {upload_id!r} has expired")
        return session

    def get_status(self, upload_id: str) -> UploadStatus:
        session = self.get_session(upload_id)
        received = tuple(self.storage.received_parts(session))
        state = "completing" if self.storage.is_completing(session) else "pending"
        return UploadStatus(session=session, received_parts=received, state=state)

    def describe(self, upload_id: str) -> dict[str, Any]:
        """Protocol view of an upload, pending or completed.

        Pending uploads return their status (``state`` = ``"pending"`` or
        ``"completing"``). Completed uploads return their completion record with
        ``state`` = ``"completed"``, so a client that lost the completion
        response (crash, timeout) can recognise the upload as finished instead
        of uploading the file a second time.
        """
        try:
            return self.get_status(upload_id).to_dict()
        except UploadExpired:
            raise
        except UploadNotFound:
            completed = self.storage.get_completed(upload_id)
            if completed is None:
                raise
            return {**completed.to_dict(), "state": "completed"}

    def get_completed(self, upload_id: str) -> CompletedUpload:
        completed = self.storage.get_completed(upload_id)
        if completed is None:
            raise UploadNotFound(f"completed upload {upload_id!r} not found")
        return completed

    def owner_of(self, upload_id: str) -> str | None:
        """Owner of a pending or completed upload (``UploadNotFound`` if neither)."""
        try:
            return self.storage.load(upload_id).owner
        except UploadNotFound:
            completed = self.storage.get_completed(upload_id)
            if completed is None:
                raise
            return completed.owner

    def ensure_access(self, upload_id: str, caller: str | None) -> None:
        """Raise ``UploadNotFound`` unless ``caller`` may use this upload.

        Uploads created without an owner are usable by anyone who knows the id.
        Owned uploads are only visible to their owner; to everyone else they do
        not exist (404, so ids cannot be probed).
        """
        owner = self.owner_of(upload_id)
        if owner is not None and owner != caller:
            raise UploadNotFound(f"upload {upload_id!r} not found")

    # ------------------------------------------------------------------ parts

    def open_part(self, upload_id: str, index: int) -> PartSink:
        """Start receiving one part; stream the body into the returned sink.

        This is the building block for servers that read request bodies
        incrementally. Use it as a context manager so an interrupted transfer
        releases the part::

            with manager.open_part(upload_id, index) as sink:
                for block in request_body:
                    sink.write(block)
                record = sink.commit(checksum_header)
        """
        session = self.get_session(upload_id)
        return self.storage.open_part(session, index)

    def put_part(
        self,
        upload_id: str,
        index: int,
        data: PartData,
        *,
        checksum: str | None = None,
    ) -> PartRecord:
        """Receive a whole part in one call (bytes, a file object, or an
        iterable of byte blocks)."""
        with self.open_part(upload_id, index) as sink:
            if isinstance(data, (bytes, bytearray, memoryview)):
                sink.write(bytes(data))
            elif hasattr(data, "read"):
                while block := data.read(MiB):  # type: ignore[union-attr]
                    sink.write(block)
            else:
                for block in data:  # type: ignore[union-attr]
                    sink.write(block)
            return sink.commit(checksum)

    # ------------------------------------------------------------------ finish

    def complete_upload(self, upload_id: str) -> tuple[CompletedUpload, bool]:
        """Assemble the upload.

        Idempotent: completing an already completed upload returns its record.

        Returns:
            ``(completed, created)`` where ``created`` is ``False`` when the
            upload had already been completed by an earlier call.
        """
        validate_upload_id(upload_id)
        existing = self.storage.get_completed(upload_id)
        if existing is not None:
            return existing, False
        try:
            session = self.get_session(upload_id)
        except UploadExpired:
            raise
        except UploadNotFound:
            # It may have been completed by a concurrent call in the meantime.
            existing = self.storage.get_completed(upload_id)
            if existing is not None:
                return existing, False
            raise
        try:
            completed = self.storage.finalize(session, verify_checksum=self.verify_checksum)
        except UploadConflict:
            existing = self.storage.get_completed(upload_id)
            if existing is not None:
                return existing, False
            raise
        logger.info("completed upload %s (%s)", upload_id, completed.filename)
        if self.on_complete is not None:
            self.on_complete(completed)
        return completed, True

    def complete(self, upload_id: str) -> CompletedUpload:
        """Like :meth:`complete_upload` but returns only the record."""
        return self.complete_upload(upload_id)[0]

    def abort(self, upload_id: str) -> None:
        """Discard a pending upload and its data."""
        session = self.storage.load(upload_id)  # 404 for unknown ids
        if self.storage.is_completing(session):
            raise UploadConflict("upload is being completed")
        self.storage.delete(upload_id)
        logger.debug("aborted upload %s", upload_id)

    def delete_completed(self, upload_id: str, *, remove_file: bool = True) -> None:
        """Forget a completed upload (and by default delete its file)."""
        self.get_completed(upload_id)
        self.storage.delete_completed(upload_id, remove_file=remove_file)

    def cleanup_expired(self, now: float | None = None) -> list[str]:
        """Delete expired pending sessions. Call it periodically.

        Returns the ids that were removed.
        """
        now = self._clock() if now is None else now
        removed = []
        for session in list(self.storage.iter_sessions()):
            if session.is_expired(now) and not self.storage.is_completing(session):
                self.storage.delete(session.upload_id)
                removed.append(session.upload_id)
        if removed:
            logger.info("removed %d expired upload(s)", len(removed))
        self.expire_completed(now)
        self.storage.prune_tokens(self._upload_exists)
        self.storage.collect_garbage(older_than=self.garbage_age)
        return removed

    def expire_completed(self, now: float | None = None) -> list[str]:
        """Forget completion records older than ``completed_ttl`` (and delete
        their files if ``remove_files_on_expiry``). Returns the ids removed."""
        if self.completed_ttl is None:
            return []
        cutoff = (self._clock() if now is None else now) - self.completed_ttl
        expired = [c.upload_id for c in self.storage.iter_completed() if c.completed_at < cutoff]
        for upload_id in expired:
            self.storage.delete_completed(upload_id, remove_file=self.remove_files_on_expiry)
        if expired:
            logger.info("forgot %d expired completion record(s)", len(expired))
        return expired

    def _upload_exists(self, upload_id: str) -> bool:
        if self.storage.get_completed(upload_id) is not None:
            return True
        try:
            self.storage.load(upload_id)
        except UploadNotFound:
            return False
        return True

    # ------------------------------------------------------------------ validation

    def _validate_filename(self, filename: object) -> str:
        if not isinstance(filename, str) or not filename.strip():
            raise InvalidRequest("filename must be a non-empty string")
        if len(filename) > _MAX_FILENAME_CHARS or "\x00" in filename:
            raise InvalidRequest("filename is too long or contains NUL")
        return filename

    def _validate_size(self, size: object) -> int:
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise InvalidRequest("size must be a non-negative integer")
        if self.max_upload_size is not None and size > self.max_upload_size:
            raise PayloadTooLarge(
                f"file is larger than the {self.max_upload_size} byte limit",
                details={"max_upload_size": self.max_upload_size},
            )
        return size

    def _choose_chunk_size(self, size: int, requested: int | None) -> int:
        if requested is None:
            chunk = self.default_chunk_size
            if total_parts(size, chunk) > self.max_parts:
                needed = -(-size // self.max_parts)
                chunk = -(-needed // MiB) * MiB  # round up to a whole MiB
            if chunk > self.max_chunk_size:
                raise PayloadTooLarge(
                    "file is too large for the configured max_parts/max_chunk_size",
                    details={"max_parts": self.max_parts, "max_chunk_size": self.max_chunk_size},
                )
            return chunk
        if not isinstance(requested, int) or isinstance(requested, bool):
            raise InvalidRequest("chunk_size must be an integer")
        if not self.min_chunk_size <= requested <= self.max_chunk_size:
            raise InvalidRequest(
                f"chunk_size must be between {self.min_chunk_size} and {self.max_chunk_size}",
                details={
                    "min_chunk_size": self.min_chunk_size,
                    "max_chunk_size": self.max_chunk_size,
                },
            )
        if total_parts(size, requested) > self.max_parts:
            raise InvalidRequest(
                f"chunk_size {requested} would need more than {self.max_parts} parts",
                details={"max_parts": self.max_parts},
            )
        return requested

    def _validate_metadata(self, metadata: object) -> dict[str, str]:
        if metadata is None:
            return {}
        if not isinstance(metadata, dict):
            raise InvalidRequest("metadata must be an object of string values")
        clean: dict[str, str] = {}
        for key, value in metadata.items():
            if not isinstance(key, str) or not key or len(key) > _MAX_METADATA_KEY_CHARS:
                raise InvalidRequest(f"invalid metadata key {key!r}")
            if not isinstance(value, str):
                raise InvalidRequest(f"metadata value for {key!r} must be a string")
            clean[key] = value
        encoded = json.dumps(clean, ensure_ascii=False).encode("utf-8")
        if len(encoded) > self.max_metadata_bytes:
            raise InvalidRequest(f"metadata exceeds {self.max_metadata_bytes} bytes")
        return clean


def _scoped_token(owner: str | None, client_token: str) -> str:
    """Storage key of a client token: tokens of different owners never collide."""
    if owner is None:
        return client_token
    digest = hashlib.sha256(f"{owner}\0{client_token}".encode()).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
