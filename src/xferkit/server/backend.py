"""Storage backend interface.

A backend owns the bytes and the persistent state of upload sessions. The
:class:`~xferkit.server.manager.UploadManager` owns the rules (limits, expiry,
validation) and talks to the backend only through this interface, so other
backends (object storage, databases for metadata, ...) can be plugged in.
"""

from __future__ import annotations

import abc
from collections.abc import Callable, Iterator
from typing import Protocol

from ..models import CompletedUpload, PartRecord, UploadSession


class PartSink(Protocol):
    """Receives the body of one part. Returned by :meth:`StorageBackend.open_part`.

    Usage::

        with backend.open_part(session, index) as sink:
            for block in body:
                sink.write(block)
            record = sink.commit(checksum)

    Leaving the ``with`` block without ``commit`` aborts the part.
    """

    @property
    def expected_size(self) -> int: ...

    @property
    def bytes_written(self) -> int: ...

    def write(self, data: bytes) -> None: ...

    def commit(self, checksum: str | None = None) -> PartRecord: ...

    def abort(self) -> None: ...

    def __enter__(self) -> PartSink: ...

    def __exit__(self, *exc_info: object) -> None: ...


class StorageBackend(abc.ABC):
    """Abstract persistence layer for upload sessions.

    Implementations must be safe to use from several threads and, ideally,
    from several processes (e.g. multiple uvicorn workers) sharing the store.
    """

    #: Smallest part (all but the last) the backend accepts; the manager never
    #: chooses or accepts a smaller ``chunk_size`` for multi-part uploads.
    min_part_size: int = 1
    #: Most parts one upload may have.
    max_parts: int | None = None

    @abc.abstractmethod
    def create(self, session: UploadSession) -> None:
        """Persist a new session and prepare space for its data."""

    @abc.abstractmethod
    def load(self, upload_id: str) -> UploadSession:
        """Return a pending session. Raises ``UploadNotFound`` if absent."""

    @abc.abstractmethod
    def open_part(self, session: UploadSession, index: int) -> PartSink:
        """Start receiving part ``index``.

        Re-uploading an already received part replaces it. Raises
        ``UploadConflict`` if the same part is being written concurrently or the
        session is being completed.
        """

    @abc.abstractmethod
    def received_parts(self, session: UploadSession) -> list[int]:
        """Sorted indices of the parts received so far (cheap)."""

    @abc.abstractmethod
    def part_records(self, session: UploadSession) -> dict[int, PartRecord]:
        """Full records (size, checksum) of the received parts."""

    @abc.abstractmethod
    def is_completing(self, session: UploadSession) -> bool:
        """Whether a completion is currently in progress."""

    @abc.abstractmethod
    def finalize(self, session: UploadSession, *, verify_checksum: bool) -> CompletedUpload:
        """Assemble the parts into the final file and persist the result.

        Raises ``UploadIncomplete`` if parts are missing, ``UploadConflict`` if
        parts are still being written or another completion is running, and
        ``ChecksumMismatch`` if ``verify_checksum`` is set and the assembled file
        does not match ``session.checksum``.
        """

    @abc.abstractmethod
    def get_completed(self, upload_id: str) -> CompletedUpload | None:
        """Return the completion record of an upload, or ``None``."""

    @abc.abstractmethod
    def delete(self, upload_id: str) -> None:
        """Discard a pending session and its data. Missing sessions are ignored."""

    @abc.abstractmethod
    def delete_completed(self, upload_id: str, *, remove_file: bool = True) -> None:
        """Forget a completion record and optionally remove the assembled file."""

    @abc.abstractmethod
    def iter_sessions(self) -> Iterator[UploadSession]:
        """Iterate over all pending sessions (used for expiry cleanup)."""

    def iter_completed(self) -> Iterator[CompletedUpload]:
        """Iterate over completion records (used for retention). Optional."""
        return iter(())

    # Idempotent creation. Optional: the defaults disable it (every create is new).

    def lookup_token(self, token: str) -> str | None:
        """Upload id bound to a client token, or ``None``."""
        return None

    def bind_token(self, token: str, upload_id: str, *, replace: bool = False) -> str:
        """Bind a client token to an upload id.

        Returns the id the token ends up bound to: ``upload_id``, or the id of
        an earlier binding when ``replace`` is false and the token was taken by
        a concurrent request.
        """
        return upload_id

    def prune_tokens(self, is_live: Callable[[str], bool]) -> int:
        """Delete token bindings whose upload no longer exists. Returns the count."""
        return 0

    def download_url(self, completed: CompletedUpload, *, expires_in: int = 600) -> str | None:
        """A URL the client can be redirected to for downloading a completed
        upload (e.g. a presigned object-storage URL), or ``None`` to have the
        server stream ``completed.path`` itself."""
        return None

    def collect_garbage(self, *, older_than: float) -> int:
        """Remove leftovers of crashed operations older than ``older_than``
        seconds (half-created sessions, temp files). Returns the count."""
        return 0
