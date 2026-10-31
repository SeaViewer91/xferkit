"""S3-compatible object storage backend (AWS S3, MinIO, Ceph RGW, R2, ...).

Requires ``pip install "xferkit[s3]"`` (boto3).

Every xferkit upload is one S3 multipart upload: part ``n`` is uploaded as S3
part ``n + 1`` and completion is ``CompleteMultipartUpload``, so the file is
assembled inside the object store without passing through the server again.
The protocol, the clients and all guarantees stay the same.

Bucket layout (``prefix`` defaults to ``xferkit/``)::

    <prefix>sessions/<id>.json          session (written once)
    <prefix>parts/<id>/<nnnnn>-<sha256> empty marker: part n verified
    <prefix>locks/<id>                  completion in progress
    <prefix>records/<id>.json           completion record
    <prefix>tokens/<token>              upload id created for a client token
    <prefix>files/<id>/<filename>       the assembled object (``object_key=``)

Like :class:`~xferkit.server.LocalStorage`, all state lives in the store, so
any number of server processes and machines can share one bucket. Exclusive
creation (token bindings, the completion lock) uses conditional writes
(``If-None-Match: *``), which AWS S3, MinIO and most S3-compatible stores
support; ``conditional_writes=False`` falls back to check-then-write.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import re
import tempfile
import time
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
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
from .download import content_disposition
from .local import safe_filename, validate_upload_id

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError as exc:  # pragma: no cover
    raise ImportError('S3Storage requires boto3: pip install "xferkit[s3]"') from exc

__all__ = ["S3Storage"]

_MARKER = re.compile(r"^(\d{5})-([0-9a-f]{64})$")
_NOT_FOUND = frozenset({"NoSuchKey", "404", "NotFound"})


def _code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _status(exc: ClientError) -> int:
    return int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") or 0)


def _default_object_key(prefix: str) -> Callable[[UploadSession], str]:
    def object_key(session: UploadSession) -> str:
        return f"{prefix}files/{session.upload_id}/{safe_filename(session.filename)}"

    return object_key


class S3Storage(StorageBackend):
    """Keeps uploads in an S3-compatible bucket.

    Args:
        bucket: Bucket name (must exist).
        client: A boto3 S3 client. Without one, ``boto3.client("s3",
            **client_options)`` is created, e.g. ``endpoint_url=`` for MinIO.
        prefix: Key prefix for everything xferkit stores.
        object_key: Maps a session to the key of its final object (default
            ``<prefix>files/<upload_id>/<filename>``). Use it to write straight
            to where your application expects the files.
        spool_dir: Where parts are buffered (default: the system temp dir).
            Each part is received completely and verified before it is sent to
            S3, so a corrupted or interrupted part never reaches the bucket.
        spool_memory: Parts up to this size are buffered in memory.
        send_checksums: Send each part's SHA-256 (``ChecksumSHA256``) so S3
            verifies it again on arrival. Turn off for stores that reject it.
        conditional_writes: Use ``If-None-Match: *`` for exclusive creation.
        completion_lock_timeout: Seconds after which a completion lock left by
            a crashed server is taken over.
        download_expiry: Lifetime of presigned download URLs in seconds.
        client_options: Passed to ``boto3.client`` when ``client`` is omitted.
    """

    min_part_size = 5 * MiB  # S3: every part but the last
    max_parts = 10_000

    def __init__(
        self,
        bucket: str,
        *,
        client: Any = None,
        prefix: str = "xferkit/",
        object_key: Callable[[UploadSession], str] | None = None,
        spool_dir: str | Path | None = None,
        spool_memory: int = 8 * MiB,
        send_checksums: bool = True,
        conditional_writes: bool = True,
        completion_lock_timeout: float = 600.0,
        download_expiry: int = 600,
        **client_options: Any,
    ) -> None:
        self.bucket = bucket
        self.client = client if client is not None else boto3.client("s3", **client_options)
        self.prefix = prefix
        self.object_key = object_key or _default_object_key(prefix)
        self.spool_dir = str(spool_dir) if spool_dir is not None else None
        self.spool_memory = spool_memory
        self.send_checksums = send_checksums
        self.conditional_writes = conditional_writes
        self.completion_lock_timeout = completion_lock_timeout
        self.download_expiry = download_expiry

    # ------------------------------------------------------------------ keys

    def _session_key(self, upload_id: str) -> str:
        return f"{self.prefix}sessions/{validate_upload_id(upload_id)}.json"

    def _parts_prefix(self, upload_id: str) -> str:
        return f"{self.prefix}parts/{upload_id}/"

    def _lock_key(self, upload_id: str) -> str:
        return f"{self.prefix}locks/{upload_id}"

    def _record_key(self, upload_id: str) -> str:
        return f"{self.prefix}records/{validate_upload_id(upload_id)}.json"

    def _token_key(self, token: str) -> str:
        return f"{self.prefix}tokens/{token}"

    # ------------------------------------------------------------------ S3 helpers

    def _get_json(self, key: str) -> dict[str, Any] | None:
        try:
            body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except ClientError as exc:
            if _code(exc) in _NOT_FOUND:
                return None
            raise
        return json.loads(body)

    def _put_json(self, key: str, data: dict[str, Any]) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

    def _put_if_absent(self, key: str, body: bytes) -> bool:
        """Create ``key`` unless it exists. Returns whether we created it."""
        if self.conditional_writes:
            try:
                self.client.put_object(Bucket=self.bucket, Key=key, Body=body, IfNoneMatch="*")
                return True
            except ClientError as exc:
                if _code(exc) in ("PreconditionFailed", "ConditionalRequestConflict"):
                    return False
                if _status(exc) != 501 and _code(exc) != "NotImplemented":
                    raise
                self.conditional_writes = False  # store without conditional writes
        if self._exists(key):
            return False
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body)
        return True

    def _head(self, key: str) -> dict[str, Any] | None:
        try:
            return self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if _code(exc) in _NOT_FOUND:
                return None
            raise

    def _exists(self, key: str) -> bool:
        return self._head(key) is not None

    def _list(self, prefix: str) -> Iterator[dict[str, Any]]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            yield from page.get("Contents", ())

    def _delete_keys(self, keys: list[str]) -> None:
        for start in range(0, len(keys), 1000):
            chunk = keys[start : start + 1000]
            if chunk:
                self.client.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
                )

    def _raw(self, upload_id: str) -> dict[str, Any]:
        data = self._get_json(self._session_key(upload_id))
        if data is None:
            raise UploadNotFound(f"upload {upload_id!r} not found")
        return data

    def _markers(self, upload_id: str) -> dict[int, str]:
        """Verified parts: index -> sha256 hex (from the marker key names)."""
        prefix = self._parts_prefix(upload_id)
        found: dict[int, str] = {}
        for item in self._list(prefix):
            match = _MARKER.match(item["Key"][len(prefix) :])
            if match:
                found[int(match.group(1))] = match.group(2)
        return found

    # ------------------------------------------------------------------ sessions

    def create(self, session: UploadSession) -> None:
        if session.total_parts > self.max_parts:
            raise PayloadTooLarge(f"S3 allows at most {self.max_parts} parts per upload")
        if session.total_parts > 1 and session.chunk_size < self.min_part_size:
            raise InvalidRequest(
                f"S3 needs parts of at least {self.min_part_size} bytes",
                details={"min_chunk_size": self.min_part_size},
            )
        key = self.object_key(session)
        multipart = None
        if session.size > 0:  # an empty object is written with one PutObject at the end
            multipart = self.client.create_multipart_upload(
                Bucket=self.bucket, Key=key, **self._object_options(session)
            )["UploadId"]
        data = session.to_dict()
        data["s3"] = {"key": key, "multipart": multipart}
        self._put_json(self._session_key(session.upload_id), data)

    @staticmethod
    def _object_options(session: UploadSession) -> dict[str, Any]:
        options: dict[str, Any] = {"ContentDisposition": content_disposition(session.filename)}
        return options

    def load(self, upload_id: str) -> UploadSession:
        return UploadSession.from_dict(self._raw(upload_id))

    def iter_sessions(self) -> Iterator[UploadSession]:
        prefix = f"{self.prefix}sessions/"
        for item in self._list(prefix):
            name = item["Key"][len(prefix) :]
            if name.endswith(".json"):
                with contextlib.suppress(UploadNotFound):
                    yield self.load(name[: -len(".json")])

    def delete(self, upload_id: str) -> None:
        try:
            raw = self._raw(upload_id)
        except UploadNotFound:
            return
        self._abort_multipart(raw)
        keys = [item["Key"] for item in self._list(self._parts_prefix(upload_id))]
        self._delete_keys([*keys, self._lock_key(upload_id), self._session_key(upload_id)])

    def _abort_multipart(self, raw: dict[str, Any]) -> None:
        multipart = raw["s3"].get("multipart")
        if multipart:
            try:
                self.client.abort_multipart_upload(
                    Bucket=self.bucket, Key=raw["s3"]["key"], UploadId=multipart
                )
            except ClientError as exc:
                if _code(exc) not in ("NoSuchUpload", *_NOT_FOUND):
                    raise

    # ------------------------------------------------------------------ parts

    def open_part(self, session: UploadSession, index: int) -> S3PartWriter:
        raw = self._raw(session.upload_id)
        if self._exists(self._lock_key(session.upload_id)):
            raise UploadConflict("upload is being completed")
        # A re-uploaded part is "missing" until it is verified again.
        stale = [
            item["Key"]
            for item in self._list(f"{self._parts_prefix(session.upload_id)}{index:05d}-")
        ]
        self._delete_keys(stale)
        return S3PartWriter(self, raw, index, session.part_size(index))

    def received_parts(self, session: UploadSession) -> list[int]:
        return sorted(self._markers(session.upload_id))

    def part_records(self, session: UploadSession) -> dict[int, PartRecord]:
        return {
            index: PartRecord(index, session.part_size(index), format_checksum(digest))
            for index, digest in self._markers(session.upload_id).items()
            if index < session.total_parts
        }

    # ------------------------------------------------------------------ completion

    def is_completing(self, session: UploadSession) -> bool:
        return self._exists(self._lock_key(session.upload_id))

    def _take_completion_lock(self, upload_id: str) -> None:
        key = self._lock_key(upload_id)
        body = f"{time.time():.3f}".encode()
        if self._put_if_absent(key, body):
            return
        head = self._head(key)
        if head is None:  # released in the meantime
            if self._put_if_absent(key, body):
                return
        else:
            age = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds()
            if age > self.completion_lock_timeout:  # left behind by a crashed server
                self.client.delete_object(Bucket=self.bucket, Key=key)
                if self._put_if_absent(key, body):
                    return
        raise UploadConflict("upload is already being completed")

    def finalize(self, session: UploadSession, *, verify_checksum: bool) -> CompletedUpload:
        upload_id = session.upload_id
        raw = self._raw(upload_id)
        self._take_completion_lock(upload_id)
        try:
            completed = self._finalize_locked(session, raw, verify_checksum)
        except BaseException:
            with contextlib.suppress(Exception):
                self.client.delete_object(Bucket=self.bucket, Key=self._lock_key(upload_id))
            raise
        # The record exists, so the session can go (a crash here leaves a session
        # whose completion simply returns the record).
        keys = [item["Key"] for item in self._list(self._parts_prefix(upload_id))]
        with contextlib.suppress(Exception):
            self._delete_keys([*keys, self._session_key(upload_id), self._lock_key(upload_id)])
        return completed

    def _finalize_locked(
        self, session: UploadSession, raw: dict[str, Any], verify_checksum: bool
    ) -> CompletedUpload:
        upload_id = session.upload_id
        key = raw["s3"]["key"]
        multipart = raw["s3"].get("multipart")
        total = session.total_parts
        digests = self._markers(upload_id)
        missing = [i for i in range(total) if i not in digests]
        if missing:
            raise UploadIncomplete(
                f"{len(missing)} of {total} parts are missing",
                details={"missing_parts": missing},
            )

        if multipart is None:  # empty file
            self.client.put_object(
                Bucket=self.bucket, Key=key, Body=b"", **self._object_options(session)
            )
        else:
            etags = self._uploaded_parts(key, multipart, session.size)
            if etags is not None:  # None: assembled by an attempt that crashed afterwards
                bad = [
                    i for i in range(total) if etags.get(i + 1, ("", -1))[1] != session.part_size(i)
                ]
                if bad:
                    raise UploadIncomplete(
                        f"{len(bad)} of {total} parts are missing in the object store",
                        details={"missing_parts": bad},
                    )
                self._complete_multipart(session, key, multipart, etags)

        verified = None
        if verify_checksum and session.checksum is not None:
            _, expected_hex = parse_checksum(session.checksum)
            actual_hex = self._sha256_object(key)
            if actual_hex != expected_hex:
                # The multipart upload no longer exists, so the session cannot be
                # repaired: drop the object and let the client start over.
                self.client.delete_object(Bucket=self.bucket, Key=key)
                self.delete(upload_id)
                raise ChecksumMismatch(
                    "assembled file does not match the declared checksum",
                    details={"expected": session.checksum, "actual": format_checksum(actual_hex)},
                )
            verified = session.checksum

        completed = CompletedUpload(
            upload_id=upload_id,
            filename=session.filename,
            size=session.size,
            path=Path(key),
            composite_checksum=composite_checksum(bytes.fromhex(digests[i]) for i in range(total)),
            checksum=verified,
            created_at=session.created_at,
            completed_at=time.time(),
            metadata=dict(session.metadata),
            chunk_size=session.chunk_size,
            owner=session.owner,
            location=f"s3://{self.bucket}/{key}",
        )
        record = completed.to_dict()
        record.update(key=key, location=completed.location)
        self._put_json(self._record_key(upload_id), record)
        return completed

    def _uploaded_parts(
        self, key: str, multipart: str, size: int
    ) -> dict[int, tuple[str, int]] | None:
        """S3 part number -> (ETag, size), or ``None`` if the multipart upload
        is gone because an earlier completion already assembled the object."""
        parts: dict[int, tuple[str, int]] = {}
        paginator = self.client.get_paginator("list_parts")
        try:
            for page in paginator.paginate(Bucket=self.bucket, Key=key, UploadId=multipart):
                for part in page.get("Parts", ()):
                    parts[int(part["PartNumber"])] = (part["ETag"], int(part["Size"]))
        except ClientError as exc:
            if _code(exc) != "NoSuchUpload":
                raise
            if self._object_has_size(key, size):
                return None
            raise UploadNotFound("the multipart upload no longer exists") from None
        return parts

    def _complete_multipart(
        self,
        session: UploadSession,
        key: str,
        multipart: str,
        etags: dict[int, tuple[str, int]],
    ) -> None:
        parts = [{"PartNumber": i + 1, "ETag": etags[i + 1][0]} for i in range(session.total_parts)]
        try:
            self.client.complete_multipart_upload(
                Bucket=self.bucket, Key=key, UploadId=multipart, MultipartUpload={"Parts": parts}
            )
        except ClientError as exc:
            code = _code(exc)
            if code == "NoSuchUpload" and self._object_has_size(key, session.size):
                return
            if code in ("InvalidPart", "InvalidPartOrder"):
                raise UploadConflict("parts changed while completing; try again") from None
            if code == "EntityTooSmall":
                raise InvalidRequest(
                    f"S3 needs parts of at least {self.min_part_size} bytes"
                ) from None
            raise

    def _object_has_size(self, key: str, size: int) -> bool:
        head = self._head(key)
        return head is not None and int(head["ContentLength"]) == size

    def _sha256_object(self, key: str) -> str:
        digest = hashlib.sha256()
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"]
        for block in iter(lambda: body.read(4 * MiB), b""):
            digest.update(block)
        return digest.hexdigest()

    # ------------------------------------------------------------------ records

    def get_completed(self, upload_id: str) -> CompletedUpload | None:
        try:
            data = self._get_json(self._record_key(upload_id))
        except UploadNotFound:  # invalid id
            return None
        if data is None:
            return None
        return CompletedUpload.from_dict(data, path=Path(data["key"]))

    def iter_completed(self) -> Iterator[CompletedUpload]:
        prefix = f"{self.prefix}records/"
        for item in self._list(prefix):
            name = item["Key"][len(prefix) :]
            if name.endswith(".json"):
                completed = self.get_completed(name[: -len(".json")])
                if completed is not None:
                    yield completed

    def delete_completed(self, upload_id: str, *, remove_file: bool = True) -> None:
        completed = self.get_completed(upload_id)
        keys = [self._record_key(upload_id)]
        if completed is not None and remove_file:
            keys.append(completed.path.as_posix())
        self._delete_keys(keys)

    def download_url(self, completed: CompletedUpload, *, expires_in: int | None = None) -> str:
        return self.client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.bucket,
                "Key": completed.path.as_posix(),
                "ResponseContentDisposition": content_disposition(completed.filename),
            },
            ExpiresIn=expires_in or self.download_expiry,
        )

    # ------------------------------------------------------------------ tokens

    def lookup_token(self, token: str) -> str | None:
        try:
            body = self.client.get_object(Bucket=self.bucket, Key=self._token_key(token))["Body"]
        except ClientError as exc:
            if _code(exc) in _NOT_FOUND:
                return None
            raise
        return body.read().decode("ascii").strip() or None

    def bind_token(self, token: str, upload_id: str, *, replace: bool = False) -> str:
        key = self._token_key(token)
        if replace:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=upload_id.encode("ascii"))
            return upload_id
        if self._put_if_absent(key, upload_id.encode("ascii")):
            return upload_id
        return self.lookup_token(token) or upload_id

    def prune_tokens(self, is_live: Callable[[str], bool]) -> int:
        dead = []
        for item in self._list(f"{self.prefix}tokens/"):
            token = item["Key"].rsplit("/", 1)[-1]
            upload_id = self.lookup_token(token)
            if upload_id is None or not is_live(upload_id):
                dead.append(item["Key"])
        self._delete_keys(dead)
        return len(dead)

    # ------------------------------------------------------------------ garbage

    def collect_garbage(self, *, older_than: float) -> int:
        """Abort multipart uploads and remove part markers that belong to no
        session (a server crashed while creating or removing one).

        Only multipart uploads below ``prefix`` are considered; with a custom
        ``object_key`` outside it, add the usual bucket lifecycle rule
        (``AbortIncompleteMultipartUpload``) instead."""
        cutoff = time.time() - older_than
        live = {s.upload_id for s in self.iter_sessions()}
        live_multiparts = set()
        for upload_id in live:
            with contextlib.suppress(UploadNotFound):
                live_multiparts.add(self._raw(upload_id)["s3"].get("multipart"))
        removed = 0
        paginator = self.client.get_paginator("list_multipart_uploads")
        for page in paginator.paginate(Bucket=self.bucket):
            for upload in page.get("Uploads", ()):
                if upload["UploadId"] in live_multiparts:
                    continue
                if upload["Initiated"].timestamp() > cutoff:
                    continue  # may be a session being created right now
                if not upload["Key"].startswith(self.prefix):
                    continue  # custom object_key layouts: use a bucket lifecycle rule
                with contextlib.suppress(ClientError):
                    self.client.abort_multipart_upload(
                        Bucket=self.bucket, Key=upload["Key"], UploadId=upload["UploadId"]
                    )
                    removed += 1
        stray = []
        prefix = f"{self.prefix}parts/"
        for item in self._list(prefix):
            upload_id = item["Key"][len(prefix) :].split("/", 1)[0]
            if upload_id not in live and item["LastModified"].timestamp() < cutoff:
                stray.append(item["Key"])
        locks = f"{self.prefix}locks/"
        for item in self._list(locks):
            if item["Key"][len(locks) :] not in live and item["LastModified"].timestamp() < cutoff:
                stray.append(item["Key"])
        self._delete_keys(stray)
        return removed + len(stray)


class S3PartWriter:
    """Buffers one part, verifies it, then uploads it as an S3 part."""

    def __init__(
        self, storage: S3Storage, raw: dict[str, Any], index: int, expected_size: int
    ) -> None:
        self.index = index
        self._storage = storage
        self._raw = raw
        self._expected = expected_size
        self._hash = hashlib.sha256()
        self._written = 0
        self._done = False
        self._buffer = tempfile.SpooledTemporaryFile(  # noqa: SIM115 - closed in commit/abort
            max_size=storage.spool_memory, dir=storage.spool_dir
        )

    @property
    def expected_size(self) -> int:
        return self._expected

    @property
    def bytes_written(self) -> int:
        return self._written

    def __enter__(self) -> S3PartWriter:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.abort()

    def write(self, data: bytes) -> None:
        if self._done:
            raise RuntimeError("part writer is closed")
        if not data:
            return
        if self._written + len(data) > self._expected:
            raise PayloadTooLarge(
                f"part {self.index} is larger than the expected {self._expected} bytes",
                details={"part": self.index, "expected_size": self._expected},
            )
        self._buffer.write(data)
        self._hash.update(data)
        self._written += len(data)

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
            digest = self._hash.digest()
            if checksum is not None:
                try:
                    _, expected_hex = parse_checksum(checksum)
                except ValueError as exc:
                    raise InvalidRequest(str(exc)) from None
                if expected_hex != digest.hex():
                    raise ChecksumMismatch(
                        f"part {self.index} does not match its checksum",
                        details={
                            "part": self.index,
                            "expected": checksum,
                            "actual": format_checksum(digest.hex()),
                        },
                    )
            self._upload(digest)
        finally:
            self.abort()
        return PartRecord(self.index, self._written, format_checksum(digest.hex()))

    def _upload(self, digest: bytes) -> None:
        storage, s3 = self._storage, self._raw["s3"]
        self._buffer.seek(0)
        options: dict[str, Any] = {}
        if storage.send_checksums:
            options["ChecksumSHA256"] = base64.b64encode(digest).decode("ascii")
        try:
            storage.client.upload_part(
                Bucket=storage.bucket,
                Key=s3["key"],
                UploadId=s3["multipart"],
                PartNumber=self.index + 1,
                Body=self._buffer,
                ContentLength=self._expected,
                **options,
            )
        except ClientError as exc:
            if _code(exc) in ("NoSuchUpload", *_NOT_FOUND):
                raise UploadNotFound("upload no longer accepts parts") from None
            if _code(exc) in ("BadDigest", "InvalidDigest"):
                raise ChecksumMismatch(
                    f"part {self.index} was corrupted on its way to S3"
                ) from None
            raise
        marker = f"{storage._parts_prefix(self._raw['upload_id'])}{self.index:05d}-{digest.hex()}"
        storage.client.put_object(Bucket=storage.bucket, Key=marker, Body=b"")

    def abort(self) -> None:
        if self._done:
            return
        self._done = True
        with contextlib.suppress(Exception):
            self._buffer.close()
