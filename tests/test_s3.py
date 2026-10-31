"""S3Storage against a local S3 implementation (moto server, real HTTP)."""

import hashlib
import itertools
import os

import httpx
import pytest
from fastapi import FastAPI

from conftest import LiveServer
from xferkit import (
    ChecksumMismatch,
    InvalidRequest,
    UploadConflict,
    UploadIncomplete,
    UploadManager,
    UploadNotFound,
)
from xferkit.client import AsyncDownloader, AsyncUploader, RetryPolicy
from xferkit.integrations.fastapi import create_upload_router
from xferkit.protocol import KiB, MiB, format_checksum

boto3 = pytest.importorskip("boto3")
moto_server = pytest.importorskip("moto.server")

from xferkit.server.s3 import S3Storage  # noqa: E402

FAST_RETRY = RetryPolicy(attempts=4, backoff=0.01, max_backoff=0.05)
_buckets = itertools.count()


@pytest.fixture(scope="module")
def s3_endpoint():
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def s3(s3_endpoint):
    client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    bucket = f"xferkit-test-{next(_buckets)}"
    client.create_bucket(Bucket=bucket)
    return client, bucket


@pytest.fixture
def storage(s3):
    client, bucket = s3
    return S3Storage(bucket, client=client)


def object_bytes(storage, completed):
    key = completed.path.as_posix()
    return storage.client.get_object(Bucket=storage.bucket, Key=key)["Body"].read()


def sha(data):
    return format_checksum(hashlib.sha256(data).hexdigest())


def test_chunk_size_follows_s3_limits(storage):
    manager = UploadManager(storage)
    assert manager.min_chunk_size == 5 * MiB and manager.max_parts == 10_000
    with pytest.raises(InvalidRequest):
        manager.create_upload("a", 20 * MiB, chunk_size=1 * MiB)
    # A file that fits into one part may use any chunk size.
    session = manager.create_upload("small", 10, chunk_size=64 * KiB)
    assert session.total_parts == 1
    with pytest.raises(ValueError):
        UploadManager(storage, default_chunk_size=1 * MiB, min_chunk_size=1 * MiB)


def test_upload_assembles_object_in_bucket(storage):
    manager = UploadManager(storage)
    data = os.urandom(11 * MiB)
    session = manager.create_upload(
        "드론 영상.mp4", len(data), chunk_size=5 * MiB, checksum=sha(data), metadata={"k": "v"}
    )
    assert session.total_parts == 3
    for index in (2, 0):  # any order
        part = data[index * 5 * MiB : (index + 1) * 5 * MiB]
        manager.put_part(session.upload_id, index, part, checksum=sha(part))
    assert manager.get_status(session.upload_id).received_parts == (0, 2)
    with pytest.raises(UploadIncomplete) as info:
        manager.complete(session.upload_id)
    assert info.value.details["missing_parts"] == [1]

    with pytest.raises(ChecksumMismatch):
        manager.put_part(session.upload_id, 1, data[5 * MiB : 10 * MiB], checksum=sha(b"x"))
    manager.put_part(session.upload_id, 1, data[5 * MiB : 10 * MiB])

    completed = manager.complete(session.upload_id)
    assert completed.checksum == sha(data)
    assert completed.location == f"s3://{storage.bucket}/{completed.path.as_posix()}"
    assert object_bytes(storage, completed) == data
    assert manager.complete(session.upload_id) == completed  # idempotent
    assert manager.describe(session.upload_id)["state"] == "completed"
    # Session state is gone, the record stays.
    keys = [o["Key"] for o in storage._list("xferkit/")]
    assert not [k for k in keys if "/sessions/" in k or "/parts/" in k or "/locks/" in k]
    assert storage.client.list_multipart_uploads(Bucket=storage.bucket).get("Uploads") is None


def test_empty_file_and_abort(storage):
    manager = UploadManager(storage)
    empty = manager.create_upload("empty.txt", 0)
    assert object_bytes(storage, manager.complete(empty.upload_id)) == b""

    session = manager.create_upload("gone.bin", 6 * MiB, chunk_size=5 * MiB)
    manager.put_part(session.upload_id, 0, os.urandom(5 * MiB))
    manager.abort(session.upload_id)
    with pytest.raises(UploadNotFound):
        manager.get_status(session.upload_id)
    assert storage.client.list_multipart_uploads(Bucket=storage.bucket).get("Uploads") is None


def test_wrong_whole_file_checksum_discards_upload(storage):
    manager = UploadManager(storage)
    data = os.urandom(100)
    session = manager.create_upload("x.bin", len(data), checksum=sha(b"other"))
    manager.put_part(session.upload_id, 0, data)
    with pytest.raises(ChecksumMismatch):
        manager.complete(session.upload_id)
    with pytest.raises(UploadNotFound):
        manager.get_status(session.upload_id)


def test_completion_lock_and_crash_recovery(storage, monkeypatch):
    manager = UploadManager(storage)
    data = os.urandom(6 * MiB)
    session = manager.create_upload("c.bin", len(data), chunk_size=5 * MiB)
    manager.put_part(session.upload_id, 0, data[: 5 * MiB])
    manager.put_part(session.upload_id, 1, data[5 * MiB :])

    # Someone else is completing it: parts and a second completion are refused.
    storage._take_completion_lock(session.upload_id)
    assert manager.get_status(session.upload_id).state == "completing"
    with pytest.raises(UploadConflict):
        manager.complete(session.upload_id)
    with pytest.raises(UploadConflict):
        manager.put_part(session.upload_id, 0, data[: 5 * MiB])
    storage.client.delete_object(Bucket=storage.bucket, Key=storage._lock_key(session.upload_id))

    # The server crashes after S3 assembled the object, before the record is written.
    real_put_json = storage._put_json

    def crash_on_record(key, value):
        if "/records/" in key:
            raise RuntimeError("crash")
        real_put_json(key, value)

    monkeypatch.setattr(storage, "_put_json", crash_on_record)
    with pytest.raises(RuntimeError):
        manager.complete(session.upload_id)
    monkeypatch.setattr(storage, "_put_json", real_put_json)
    # Completing again finds the assembled object instead of failing.
    completed, created = manager.complete_upload(session.upload_id)
    assert created and object_bytes(storage, completed) == data


def test_tokens_records_and_garbage(storage):
    manager = UploadManager(storage, completed_ttl=0)
    state, created = manager.open_upload("t.bin", 5, client_token="token-123456")
    again, created_again = manager.open_upload("t.bin", 5, client_token="token-123456")
    assert created and not created_again and again["upload_id"] == state["upload_id"]
    manager.put_part(state["upload_id"], 0, b"hello")
    manager.complete(state["upload_id"])
    assert [c.upload_id for c in storage.iter_completed()] == [state["upload_id"]]

    # Crash leftovers: a multipart upload and markers without a session.
    storage.client.create_multipart_upload(Bucket=storage.bucket, Key="xferkit/files/orphan/x")
    storage.client.put_object(Bucket=storage.bucket, Key=f"xferkit/parts/orphan/00000-{'0' * 64}")
    manager.garbage_age = 0
    manager.cleanup_expired()
    manager.expire_completed()
    keys = [o["Key"] for o in storage._list("xferkit/")]
    assert not [k for k in keys if "orphan" in k or "/tokens/" in k or "/records/" in k]
    assert storage.client.list_multipart_uploads(Bucket=storage.bucket).get("Uploads") is None


# ------------------------------------------------------------------ over HTTP


@pytest.fixture
def s3_server(storage):
    completed = []
    manager = UploadManager(storage)
    app = FastAPI()
    app.include_router(
        create_upload_router(
            manager, expose_downloads=True, on_complete=lambda r, request: completed.append(r)
        )
    )
    server = LiveServer(app)
    server.start()
    yield server, manager, completed
    server.stop()


async def test_client_upload_resume_and_download(s3_server, storage, tmp_path):
    server, _, completed = s3_server
    source = tmp_path / "scene.tif"
    data = os.urandom(16 * MiB + 123)
    source.write_bytes(data)

    class Stop(Exception):
        pass

    def stop_early(progress):
        if progress.bytes_done >= 5 * MiB:
            raise Stop

    url = f"{server.url}/uploads"
    async with AsyncUploader(
        url, chunk_size=5 * MiB, concurrency=2, state_dir=tmp_path / "st", retry=FAST_RETRY
    ) as up:
        with pytest.raises(Stop):
            await up.upload(source, progress=stop_early)
        result = await up.upload(source)
    assert result.resumed
    assert object_bytes(storage, completed[0]) == data

    # Downloads are redirected to a presigned URL and still use ranges.
    dest = tmp_path / "copy.tif"
    async with AsyncDownloader(segment_size=4 * MiB, concurrency=3, retry=FAST_RETRY) as down:
        downloaded = await down.download(
            f"{url}/{result.upload_id}/content", dest, checksum=sha(data)
        )
    assert downloaded.path.read_bytes() == data
    async with httpx.AsyncClient() as http:
        r = await http.get(f"{url}/{result.upload_id}/content")
    assert r.status_code == 307
    assert r.headers["location"].startswith(storage.client.meta.endpoint_url)


async def test_folder_batch_on_s3(s3_server, storage, tmp_path):
    server, _, completed = s3_server
    root = tmp_path / "photos"
    (root / "sub").mkdir(parents=True)
    files = {"a.jpg": os.urandom(30 * KiB), "sub/b.jpg": os.urandom(7 * MiB), "empty": b""}
    for rel, content in files.items():
        (root / rel).write_bytes(content)
    async with AsyncUploader(f"{server.url}/uploads", retry=FAST_RETRY) as up:
        result = await up.upload_folder(root)
        again = await up.upload_folder(root)
    assert result.ok and again.ok and all(r.resumed for r in again.uploaded)
    assert len(completed) == 3
    for record in completed:
        assert object_bytes(storage, record) == files[record.metadata["relative_path"]]
