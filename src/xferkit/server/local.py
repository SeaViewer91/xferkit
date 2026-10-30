"""Local filesystem storage backend.

Layout under ``root``::

    uploads/<upload_id>/session.json      session description (written once)
    uploads/<upload_id>/data              pre-sized file; parts are written in place
    uploads/<upload_id>/parts/<n>.json    marker of a verified part (size, sha256)
    uploads/<upload_id>/parts/<n>.lock    present while part n is being written
    uploads/<upload_id>/.completing       present while the upload is being completed
    tokens/<client_token>                 upload id created for that client token
    files/<upload_id>/<filename>          assembled file after completion
    records/<upload_id>.json              completion record

Design notes:

* Parts are written directly at their final offset in one pre-sized file, so
  completion is a rename instead of a concatenation. A 100 GB upload never
  needs 200 GB of disk, and completing it is instant.
* Received parts are tracked by marker files, not by rewriting a shared index.
  Each part only touches its own files, which makes the store safe for several
  threads *and* several worker processes without a database.
* All JSON documents are written to a temporary file and renamed into place,
  so readers never observe partial writes.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import time
import unicodedata
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from ..errors import (
    ChecksumMismatch,
    InvalidPart,
    InvalidRequest,
    PayloadTooLarge,
    UploadConflict,
    UploadIncomplete,
    UploadNotFound,
)
from ..models import CompletedUpload, PartRecord, UploadSession
from ..protocol import MiB, composite_checksum, format_checksum, parse_checksum
from .backend import StorageBackend

_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,128}$")
_UNSAFE_CHARS = re.compile(r'[\x00-\x1f\x7f<>:"/\\|?*]')
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}  # fmt: skip
_MAX_NAME_BYTES = 200
_HOSTNAME = socket.gethostname().replace(" ", "_") or "localhost"


def validate_upload_id(upload_id: object) -> str:
    """Return ``upload_id`` if it is well-formed, else raise ``UploadNotFound``.

    Ids end up in filesystem paths, so anything outside ``[A-Za-z0-9_-]`` is
    rejected (this blocks path traversal such as ``../../etc``).
    """
    if not isinstance(upload_id, str) or _ID_RE.match(upload_id) is None:
        raise UploadNotFound(f"upload {upload_id!r} not found")
    return upload_id


def safe_filename(name: str, default: str = "file") -> str:
    """Make a client-supplied filename safe to use as a single path component.

    Keeps Unicode (e.g. Korean) names, normalised to NFC so names sent from
    macOS (NFD) match names typed elsewhere.
    """
    name = unicodedata.normalize("NFC", str(name))
    name = re.split(r"[/\\]", name)[-1]
    name = _UNSAFE_CHARS.sub("_", name).strip().strip(".").strip()
    if not name:
        return default
    if name.split(".")[0].upper() in _WINDOWS_RESERVED:
        name = "_" + name
    if len(name.encode("utf-8")) > _MAX_NAME_BYTES:
        base, dot, ext = name.rpartition(".")
        ext_bytes = len(ext.encode("utf-8")) + 1
        if dot and base and ext_bytes <= 16:
            budget = _MAX_NAME_BYTES - ext_bytes
            name = base.encode("utf-8")[:budget].decode("utf-8", "ignore") + "." + ext
        else:
            name = name.encode("utf-8")[:_MAX_NAME_BYTES].decode("utf-8", "ignore")
    return name or default


def sha256_file(path: str | os.PathLike[str], block_size: int = 4 * MiB) -> str:
    """Hex SHA-256 of a file, read in blocks."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(block_size):
            digest.update(block)
    return digest.hexdigest()


def _atomic_write_json(path: Path, data: Any, *, fsync: bool = False) -> None:
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, separators=(",", ":"))
            if fsync:
                fh.flush()
                os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise


def _read_json(path: Path) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _lock_owner_is_dead(content: str) -> bool:
    """Whether a lock was left behind by a process that no longer exists.

    Locks record ``"<hostname> <pid> <timestamp>"``. The owner can only be
    checked when it ran on this host; locks from other hosts (shared network
    storage) fall back to the age-based timeout.
    """
    if os.name == "nt":  # os.kill(pid, 0) would terminate the process on Windows
        return False
    fields = content.split()
    if len(fields) < 2 or fields[0] != _HOSTNAME:
        return False
    try:
        pid = int(fields[1])
    except ValueError:
        return False
    if pid == os.getpid():
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except OSError:  # PermissionError: alive but owned by another user
        return False
    return False


def _try_lock(path: Path, stale_after: float | None) -> bool:
    """Create ``path`` exclusively.

    An existing lock is taken over when its owner process has died (e.g. a
    server worker was killed mid-upload) or when it is older than
    ``stale_after`` seconds.
    """
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        if stale_after is None:
            return False
        try:
            content = path.read_text(encoding="utf-8", errors="replace")
            age = time.time() - path.stat().st_mtime
        except FileNotFoundError:
            return _try_lock(path, None)
        if not (_lock_owner_is_dead(content) or age > stale_after):
            return False
        with contextlib.suppress(FileNotFoundError, OSError):
            # Only remove the lock we inspected, not one created in the meantime.
            if path.read_text(encoding="utf-8", errors="replace") == content:
                path.unlink()
        return _try_lock(path, None)
    with os.fdopen(fd, "w") as fh:
        fh.write(f"{_HOSTNAME} {os.getpid()} {time.time():.3f}\n")
    return True


class LocalPartWriter:
    """Writes one part directly into the session's data file."""

    def __init__(
        self,
        *,
        index: int,
        data_path: Path,
        marker_path: Path,
        lock_path: Path,
        offset: int,
        expected_size: int,
        fsync: bool,
    ) -> None:
        self.index = index
        self._marker_path = marker_path
        self._lock_path = lock_path
        self._expected = expected_size
        self._fsync = fsync
        self._hash = hashlib.sha256()
        self._written = 0
        self._done = False
        try:
            self._fh = open(data_path, "r+b")  # noqa: SIM115 - closed in commit/abort
        except FileNotFoundError:
            self._release()
            raise UploadNotFound("upload no longer accepts parts") from None
        self._fh.seek(offset)

    @property
    def expected_size(self) -> int:
        return self._expected

    @property
    def bytes_written(self) -> int:
        return self._written

    def write(self, data: bytes) -> None:
        if self._done:
            raise RuntimeError("part writer is closed")
        size = len(data)
        if not size:
            return
        if self._written + size > self._expected:
            raise PayloadTooLarge(
                f"part {self.index} is larger than the expected {self._expected} bytes",
                details={"part": self.index, "expected_size": self._expected},
            )
        self._fh.write(data)
        self._hash.update(data)
        self._written += size

    def commit(self, checksum: str | None = None) -> PartRecord:
        if self._done:
            raise RuntimeError("part writer is closed")
        try:
            if self._written != self._expected:
                raise InvalidPart(
                    f"part {self.index} is incomplete: received {self._written} "
                    f"of {self._expected} bytes",
                    details={"part": self.index, "expected_size": self._expected},
                )
            digest = self._hash.hexdigest()
            if checksum is not None:
                try:
                    _, expected_hex = parse_checksum(checksum)
                except ValueError as exc:
                    raise InvalidRequest(str(exc)) from None
                if expected_hex != digest:
                    raise ChecksumMismatch(
                        f"part {self.index} does not match its checksum",
                        details={
                            "part": self.index,
                            "expected": checksum,
                            "actual": format_checksum(digest),
                        },
                    )
            self._fh.flush()
            if self._fsync:
                os.fsync(self._fh.fileno())
            self._fh.close()
            record = PartRecord(self.index, self._written, format_checksum(digest))
            _atomic_write_json(self._marker_path, record.to_dict(), fsync=self._fsync)
        except BaseException:
            self.abort()
            raise
        self._done = True
        self._release()
        return record

    def abort(self) -> None:
        if self._done:
            return
        self._done = True
        with contextlib.suppress(Exception):
            self._fh.close()
        self._release()

    def _release(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            self._lock_path.unlink()

    def __enter__(self) -> LocalPartWriter:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.abort()


class LocalStorage(StorageBackend):
    """Stores uploads on a local (or network-mounted) filesystem.

    Args:
        root: Directory that holds all uploads. Created if missing. It must be
            on a single filesystem so the final rename is atomic.
        fsync: ``fsync`` data and markers before acknowledging a part. Slower,
            but survives power loss.
        part_lock_timeout: Seconds after which a part lock left by a crashed
            process is taken over.
        completion_lock_timeout: Same, for the completion lock.
    """

    SESSION_FILE = "session.json"
    DATA_FILE = "data"
    PARTS_DIR = "parts"
    COMPLETING_LOCK = ".completing"
    STAGING_PREFIX = ".creating-"
    TRASH_PREFIX = ".trash-"

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        fsync: bool = False,
        part_lock_timeout: float = 600.0,
        completion_lock_timeout: float = 3600.0,
    ) -> None:
        self.root = Path(root).resolve()
        self.fsync = fsync
        self.part_lock_timeout = part_lock_timeout
        self.completion_lock_timeout = completion_lock_timeout
        self._uploads = self.root / "uploads"
        self._files = self.root / "files"
        self._records = self.root / "records"
        self._tokens = self.root / "tokens"
        for directory in (self._uploads, self._files, self._records, self._tokens):
            directory.mkdir(parents=True, exist_ok=True)

    def __repr__(self) -> str:
        return f"LocalStorage({str(self.root)!r})"

    # ------------------------------------------------------------------ paths

    def _dir(self, upload_id: str) -> Path:
        return self._uploads / validate_upload_id(upload_id)

    def _record_path(self, upload_id: str) -> Path:
        return self._records / f"{validate_upload_id(upload_id)}.json"

    # ------------------------------------------------------------------ sessions

    def create(self, session: UploadSession) -> None:
        # Build the session in a staging directory and rename it into place, so a
        # crash never leaves a half-created session where clients can see it.
        directory = self._dir(session.upload_id)
        if directory.exists():
            raise UploadConflict(f"upload {session.upload_id!r} already exists")
        staging = self._uploads / f"{self.STAGING_PREFIX}{session.upload_id}"
        try:
            staging.mkdir()
            (staging / self.PARTS_DIR).mkdir()
            with open(staging / self.DATA_FILE, "wb") as fh:
                fh.truncate(session.size)  # sparse where the filesystem supports it
            _atomic_write_json(staging / self.SESSION_FILE, session.to_dict(), fsync=self.fsync)
            os.rename(staging, directory)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise

    def load(self, upload_id: str) -> UploadSession:
        path = self._dir(upload_id) / self.SESSION_FILE
        try:
            return UploadSession.from_dict(_read_json(path))
        except FileNotFoundError:
            raise UploadNotFound(f"upload {upload_id!r} not found") from None

    def iter_sessions(self) -> Iterator[UploadSession]:
        for directory in self._uploads.iterdir():
            if not directory.is_dir() or _ID_RE.match(directory.name) is None:
                continue
            try:
                yield self.load(directory.name)
            except (UploadNotFound, ValueError, KeyError):
                continue

    def delete(self, upload_id: str) -> None:
        self._discard(self._dir(upload_id))

    def _discard(self, directory: Path) -> None:
        """Remove a session directory. It is renamed out of the way first, so a
        crash mid-removal never leaves a half-deleted session under a valid id."""
        trash = self._uploads / f"{self.TRASH_PREFIX}{directory.name}-{secrets.token_hex(4)}"
        try:
            os.rename(directory, trash)
        except FileNotFoundError:
            return
        shutil.rmtree(trash, ignore_errors=True)

    # ------------------------------------------------------------------ parts

    def open_part(self, session: UploadSession, index: int) -> LocalPartWriter:
        offset = session.part_offset(index)
        size = session.part_size(index)
        directory = self._dir(session.upload_id)
        parts = directory / self.PARTS_DIR
        lock = parts / f"{index}.lock"
        try:
            locked = _try_lock(lock, self.part_lock_timeout)
        except FileNotFoundError:
            raise UploadNotFound(f"upload {session.upload_id!r} not found") from None
        if not locked:
            raise UploadConflict(f"part {index} is already being uploaded", details={"part": index})
        try:
            # Checked *after* taking the part lock; finalize() takes the completion
            # lock *before* looking for part locks, so the two can never interleave.
            if (directory / self.COMPLETING_LOCK).exists():
                raise UploadConflict("upload is being completed")
            # A re-uploaded part is "missing" until it is verified again.
            with contextlib.suppress(FileNotFoundError):
                (parts / f"{index}.json").unlink()
            return LocalPartWriter(
                index=index,
                data_path=directory / self.DATA_FILE,
                marker_path=parts / f"{index}.json",
                lock_path=lock,
                offset=offset,
                expected_size=size,
                fsync=self.fsync,
            )
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                lock.unlink()
            raise

    def received_parts(self, session: UploadSession) -> list[int]:
        parts = self._dir(session.upload_id) / self.PARTS_DIR
        try:
            names = os.listdir(parts)
        except FileNotFoundError:
            raise UploadNotFound(f"upload {session.upload_id!r} not found") from None
        total = session.total_parts
        indices = []
        for name in names:
            stem, dot, ext = name.partition(".")
            if dot and ext == "json" and stem.isdigit() and int(stem) < total:
                indices.append(int(stem))
        indices.sort()
        return indices

    def part_records(self, session: UploadSession) -> dict[int, PartRecord]:
        parts = self._dir(session.upload_id) / self.PARTS_DIR
        records: dict[int, PartRecord] = {}
        for index in self.received_parts(session):
            try:
                records[index] = PartRecord.from_dict(_read_json(parts / f"{index}.json"))
            except FileNotFoundError:  # replaced concurrently
                continue
        return records

    # ------------------------------------------------------------------ completion

    def is_completing(self, session: UploadSession) -> bool:
        return (self._dir(session.upload_id) / self.COMPLETING_LOCK).exists()

    def finalize(self, session: UploadSession, *, verify_checksum: bool) -> CompletedUpload:
        directory = self._dir(session.upload_id)
        if not directory.is_dir():
            raise UploadNotFound(f"upload {session.upload_id!r} not found")
        lock = directory / self.COMPLETING_LOCK
        if not _try_lock(lock, self.completion_lock_timeout):
            raise UploadConflict("upload is already being completed")
        try:
            completed = self._finalize_locked(session, directory, verify_checksum)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                lock.unlink()
            raise
        self._discard(directory)
        return completed

    def _finalize_locked(
        self, session: UploadSession, directory: Path, verify_checksum: bool
    ) -> CompletedUpload:
        parts_dir = directory / self.PARTS_DIR
        writing = sorted(int(p.stem) for p in parts_dir.glob("*.lock") if p.stem.isdigit())
        if writing:
            raise UploadConflict("parts are still being uploaded", details={"parts": writing})

        records = self.part_records(session)
        total = session.total_parts
        for index, record in list(records.items()):
            if record.size != session.part_size(index):  # should never happen
                del records[index]
        missing = [i for i in range(total) if i not in records]
        if missing:
            raise UploadIncomplete(
                f"{len(missing)} of {total} parts are missing",
                details={"missing_parts": missing},
            )

        data = directory / self.DATA_FILE
        target_dir = self._files / session.upload_id
        target = target_dir / safe_filename(session.filename)
        verified: str | None = None
        if data.exists():
            if data.stat().st_size != session.size:
                raise UploadConflict("stored data does not have the declared size")
            if session.checksum and verify_checksum:
                _, expected_hex = parse_checksum(session.checksum)
                actual_hex = sha256_file(data)
                if actual_hex != expected_hex:
                    raise ChecksumMismatch(
                        "assembled file does not match the declared checksum",
                        details={
                            "expected": session.checksum,
                            "actual": format_checksum(actual_hex),
                        },
                    )
                verified = session.checksum
            if self.fsync:
                with open(data, "rb") as fh:
                    os.fsync(fh.fileno())
            target_dir.mkdir(parents=True, exist_ok=True)
            os.replace(data, target)
        elif not target.exists():
            # Neither the data nor the moved file exists: the store was tampered with.
            raise UploadNotFound(f"data of upload {session.upload_id!r} is missing")

        digests = (bytes.fromhex(parse_checksum(records[i].checksum)[1]) for i in range(total))
        completed = CompletedUpload(
            upload_id=session.upload_id,
            filename=session.filename,
            size=session.size,
            path=target,
            composite_checksum=composite_checksum(digests),
            checksum=verified,
            created_at=session.created_at,
            completed_at=time.time(),
            metadata=dict(session.metadata),
            chunk_size=session.chunk_size,
        )
        record = completed.to_dict()
        record["path"] = target.relative_to(self.root).as_posix()
        _atomic_write_json(self._record_path(session.upload_id), record, fsync=self.fsync)
        return completed

    def get_completed(self, upload_id: str) -> CompletedUpload | None:
        try:
            path = self._record_path(upload_id)
        except UploadNotFound:
            return None
        try:
            data = _read_json(path)
        except FileNotFoundError:
            return None
        return CompletedUpload.from_dict(data, path=self.root / data["path"])

    def delete_completed(self, upload_id: str, *, remove_file: bool = True) -> None:
        completed = self.get_completed(upload_id)
        with contextlib.suppress(FileNotFoundError):
            self._record_path(upload_id).unlink()
        if remove_file and completed is not None:
            shutil.rmtree(self._files / upload_id, ignore_errors=True)

    # ------------------------------------------------------------------ client tokens

    def lookup_token(self, token: str) -> str | None:
        try:
            value = (self._tokens / validate_upload_id(token)).read_text(encoding="utf-8").strip()
        except (FileNotFoundError, UploadNotFound):
            return None
        return value if _ID_RE.match(value) else None

    def bind_token(self, token: str, upload_id: str, *, replace: bool = False) -> str:
        path = self._tokens / validate_upload_id(token)
        tmp = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
        tmp.write_text(validate_upload_id(upload_id), encoding="utf-8")
        try:
            if replace:
                os.replace(tmp, path)
                return upload_id
            try:
                os.link(tmp, path)  # atomic, fails if the token is already bound
            except FileExistsError:
                return self.lookup_token(token) or upload_id
            except OSError:  # filesystem without hard links (FAT, some SMB mounts)
                try:
                    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                except FileExistsError:
                    return self.lookup_token(token) or upload_id
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(upload_id)
            return upload_id
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()

    def prune_tokens(self, is_live: Callable[[str], bool]) -> int:
        removed = 0
        for path in self._tokens.iterdir():
            if path.name.startswith("."):
                continue
            upload_id = self.lookup_token(path.name)
            if upload_id is None or not is_live(upload_id):
                with contextlib.suppress(FileNotFoundError):
                    path.unlink()
                removed += 1
        return removed

    # ------------------------------------------------------------------ garbage

    def collect_garbage(self, *, older_than: float) -> int:
        """Remove crash leftovers: half-removed (``.trash-*``) and half-created
        (``.creating-*``) session directories, session directories without
        ``session.json`` and orphaned ``*.tmp`` files."""
        cutoff = time.time() - older_than
        removed = 0

        def old(path: Path) -> bool:
            try:
                return path.stat().st_mtime < cutoff
            except FileNotFoundError:
                return False

        for path in self._uploads.iterdir():
            if path.name.startswith(self.TRASH_PREFIX):  # never in use once renamed
                shutil.rmtree(path, ignore_errors=True)
                removed += 1
                continue
            half_created = path.name.startswith(self.STAGING_PREFIX) or (
                path.is_dir()
                and _ID_RE.match(path.name) is not None
                and not (path / self.SESSION_FILE).exists()
            )
            if half_created and old(path):
                shutil.rmtree(path, ignore_errors=True)
                removed += 1
        for directory in (self._records, self._tokens):
            for path in directory.glob(".*.tmp"):
                if old(path):
                    with contextlib.suppress(FileNotFoundError):
                        path.unlink()
                    removed += 1
        return removed
